"""core_v4 Cordis -> OpenAire project match diagnosis (read-only, run on prod).

Why did the match `cordis.project.id_original = openaire.project.grantId` fall from 68.6% (core_v3,
READ_TRANSFORMATION.md: 96,056 of 139,925) to 61.2% (Phase 1: 87,439 of 142,773), and how much does a
DOI join (`cordis.project.doi = openaire.project.doi`, lower-cased) recover?

Every rule below is a join between a Cordis project and an OpenAire project; a Cordis project counts as
matched when any rule of the set links it. Rules, in the order they are tried:

  1 exact       id_original = grantId (the core_v3 / Phase 1 rule)
  2 lower       lower(trim()) of both
  3 alnum       + only [a-z0-9] kept (whitespace, hyphens, slashes, dots dropped)
  4 zeros       + leading zeros dropped
  5 prefix      + prefixes like GA / H2020 / FP7 / grantagreement dropped
  6 doi         lower-cased DOI equal (a leading https://doi.org/ or doi: is stripped)
  7 doi_suffix  the Cordis H2020 DOI `10.3030/<n>` suffix = OpenAire grantId
Normalised keys shorter than 4 characters are ignored (leading-zero stripping must not create collisions).

Outputs (markdown to stdout, JSON next to this file): matched vs unmatched by start-year bucket, by
funding programme (j_project_fundingprogramme -> fundingprogramme.framework_programme) and by
id_original shape; for the unmatched: DOI present / DOI found in OpenAire / recoverable with a
normalised grantId, each yield reported separately; Cordis projects matching more than one OpenAire
project split by cause (the OpenAire `openaireId` prefix names the source datasource: `corda*` is EC
CORDA, everything else another funder); a final cumulative "recommended match rule" table.

Reads only, both databases are attached READ_ONLY. Light (3.9M OpenAire projects, 143k Cordis
projects) but run it on SLURM, not on the login node:

  sbatch --partition=fat --cpus-per-task=8 --mem=32G --time=01:00:00 \\
    --wrap 'ENV=prod python src/pipelines/core_v4/investigation/cordis_project_match.py'

Defaults: staging = dumps.yaml openaire_dump.path_duck_staging_v4 (the one with project.doi), Cordis =
the full_projects_no_pdfs query db. Override with --staging-db / --cordis-db. Writes
cordis_project_match.json next to this file (override with --out).
"""

import argparse
import json
from datetime import date
from pathlib import Path

import duckdb

MIN_KEY_LEN = 4
PREFIXES = "grantagreementno|grantagreement|grantno|agreement|grant|ga|h2020|fp7|fp6|fp5|erc|ec"
# the recommendation heuristic: a rule is recommended when it adds projects and at most this share of the
# newly linked pairs points to a non-EC OpenAire record (a grantId collision with another funder)
MAX_NON_EC_SHARE = 0.10

# (rank, name, description); the rank is what `rules` and every cumulative table refer to
RULES = [
    (1, "exact", "id_original = grantId"),
    (2, "lower", "lower(trim())"),
    (3, "alnum", "+ only a-z0-9 kept"),
    (4, "zeros", "+ leading zeros dropped"),
    (5, "prefix", "+ GA/H2020/FP7/... prefix dropped"),
    (6, "doi", "doi = doi (lower-cased)"),
    (7, "doi_suffix", "Cordis 10.3030/<n> suffix = grantId"),
]
NORM_RULES = (2, 3, 4, 5)
DOI_RULES = (6, 7)

OUT = {}


def md_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        lines.append("| " + " | ".join(fmt(c) for c in r) + " |")
    return "\n".join(lines)


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return str(v)


def pct(a, b):
    return round(100.0 * a / b, 2) if b else None


def section(title):
    print(f"\n## {title}\n", flush=True)


def register_macros(con):
    """Key normalisations, one macro per rule (rank 2..5) and the DOI cleaner."""
    con.execute("CREATE OR REPLACE MACRO k_lower(x) AS nullif(lower(trim(x)), '')")
    con.execute(
        f"CREATE OR REPLACE MACRO k_alnum(x) AS CASE WHEN length(regexp_replace(lower(x), '[^a-z0-9]+', '', 'g')) >= {MIN_KEY_LEN} "
        "THEN regexp_replace(lower(x), '[^a-z0-9]+', '', 'g') END"
    )
    con.execute(
        f"CREATE OR REPLACE MACRO k_zeros(x) AS CASE WHEN length(regexp_replace(k_alnum(x), '^0+', '')) >= {MIN_KEY_LEN} "
        "THEN regexp_replace(k_alnum(x), '^0+', '') END"
    )
    stripped = f"regexp_replace(regexp_replace(k_alnum(x), '^({PREFIXES})+', ''), '^0+', '')"
    con.execute(f"CREATE OR REPLACE MACRO k_prefix(x) AS CASE WHEN length({stripped}) >= {MIN_KEY_LEN} THEN {stripped} END")
    con.execute(
        "CREATE OR REPLACE MACRO doi_norm(x) AS nullif(regexp_replace(lower(trim(x)), '^(https?://(dx\\.)?doi\\.org/|doi:\\s*)', ''), '')"
    )


def _columns(con, alias, table):
    return {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_catalog = ? AND table_name = ?", [alias, table]
        ).fetchall()
    }


def build_tables(con):
    """cp_t / op_t (narrow, keyed) and `pairs`: every (cordis project, openaire project, rule) link."""
    oa_cols = _columns(con, "oa", "project")
    missing = {"id", "grantId"} - oa_cols
    if missing:
        raise SystemExit(f"oa.project lacks {sorted(missing)}: is --staging-db an OpenAire staging database?")
    has_doi = "doi" in oa_cols
    if not has_doi:
        print("**Warning:** oa.project has no `doi` column: the DOI rules match nothing (use the v4 staging).\n")
    has_oid = "openaireId" in oa_cols
    OUT["meta"]["openaire_project_has_doi"] = has_doi
    OUT["meta"]["openaire_project_has_openaireId"] = has_oid

    con.execute(
        """CREATE TEMP TABLE fp AS
           SELECT j.project_id AS id, coalesce(min(f.framework_programme), 'none') AS prog, count(DISTINCT f.framework_programme) AS n_prog
           FROM cordis.j_project_fundingprogramme j LEFT JOIN cordis.fundingprogramme f ON f.id = j.fundingprogramme_id
           GROUP BY 1"""
    )
    con.execute(
        """CREATE TEMP TABLE cp_t AS
           SELECT p.id, p.id_original, doi_norm(p.doi) AS doi,
                  regexp_extract(doi_norm(p.doi), '^10\\.3030/(.+)$', 1) AS doi_sfx,
                  CASE WHEN p.start_date IS NULL THEN 'no date' WHEN year(p.start_date) < 1990 THEN '<1990'
                       ELSE (year(p.start_date) // 5 * 5)::VARCHAR || '-' || (year(p.start_date) // 5 * 5 + 4)::VARCHAR END AS bucket,
                  coalesce(fp.prog, 'none') AS prog, coalesce(fp.n_prog, 0) AS n_prog,
                  k_lower(p.id_original) AS k2, k_alnum(p.id_original) AS k3, k_zeros(p.id_original) AS k4, k_prefix(p.id_original) AS k5,
                  CASE WHEN p.id_original ~ '^[0-9]+$' THEN CASE WHEN length(p.id_original) IN (6, 9) THEN 'digits only, ' || length(p.id_original) || ' digits' ELSE 'digits only, other length' END
                       WHEN p.id_original ~ '\\s' THEN 'contains whitespace'
                       WHEN p.id_original LIKE '%/%' THEN 'contains /'
                       WHEN p.id_original LIKE '%-%' THEN 'letters/digits with -'
                       ELSE 'other' END AS shape
           FROM cordis.project p LEFT JOIN fp ON fp.id = p.id"""
    )
    con.execute("UPDATE cp_t SET doi_sfx = NULL WHERE doi_sfx = ''")
    doi = "doi_norm(doi)" if has_doi else "NULL::VARCHAR"
    oid = "split_part(openaireId, '::', 1)" if has_oid else "NULL::VARCHAR"
    con.execute(
        f"""CREATE TEMP TABLE op_t AS
            SELECT id, grantId, k_lower(grantId) AS k2, k_alnum(grantId) AS k3, k_zeros(grantId) AS k4, k_prefix(grantId) AS k5,
                   {doi} AS doi, {oid} AS src, coalesce({oid} LIKE 'corda%', false) AS ec
            FROM oa.project"""
    )
    keys = {2: "k2", 3: "k3", 4: "k4", 5: "k5"}
    parts = ["SELECT 1 AS rule, cp.id AS cp, op.id AS op FROM cp_t cp JOIN op_t op ON cp.id_original = op.grantId"]
    parts += [f"SELECT {r}, cp.id, op.id FROM cp_t cp JOIN op_t op ON cp.{k} = op.{k}" for r, k in keys.items()]
    parts.append("SELECT 6, cp.id, op.id FROM cp_t cp JOIN op_t op ON cp.doi = op.doi")
    parts.append("SELECT 7, cp.id, op.id FROM cp_t cp JOIN op_t op ON cp.doi_sfx = op.grantId")
    con.execute("CREATE TEMP TABLE pairs AS " + " UNION ALL ".join(parts))
    # one row per (cordis, openaire) pair with the first rule that finds it; one row per cordis project with its first rule
    con.execute("CREATE TEMP TABLE pairs_min AS SELECT cp, op, min(rule) AS rule FROM pairs GROUP BY cp, op")
    con.execute("CREATE TEMP TABLE cp_first AS SELECT cp, min(rule) AS first_rule FROM pairs GROUP BY cp")
    flags = ", ".join(f"coalesce(bool_or(rule = {r}), false) AS r{r}" for r, *_ in RULES)
    con.execute(
        f"""CREATE TEMP TABLE cp_flag AS
            SELECT c.id, c.bucket, c.prog, c.n_prog, c.shape, c.id_original, c.doi, c.doi_sfx, c.doi IS NOT NULL AS has_doi,
                   {", ".join(f"coalesce(f.r{r}, false) AS r{r}" for r, *_ in RULES)}
            FROM cp_t c LEFT JOIN (SELECT cp, {flags} FROM pairs GROUP BY cp) f ON f.cp = c.id"""
    )


def coverage_by(q, col, order_sql, label):
    """matched / unmatched and what each recovery route adds, grouped by `col` of cp_flag."""
    norm = " OR ".join(f"r{r}" for r in NORM_RULES)
    doi_any = " OR ".join(f"r{r}" for r in DOI_RULES)
    rows = q(
        f"""SELECT {col}, count(*) AS n, count(*) FILTER (WHERE r1) AS exact,
                   count(*) FILTER (WHERE NOT r1) AS unmatched,
                   count(*) FILTER (WHERE NOT r1 AND has_doi) AS un_doi,
                   count(*) FILTER (WHERE NOT r1 AND ({doi_any})) AS un_doi_in_oa,
                   count(*) FILTER (WHERE NOT r1 AND ({norm})) AS un_norm,
                   count(*) FILTER (WHERE NOT r1 AND ({norm} OR {doi_any})) AS un_any
            FROM cp_flag GROUP BY 1 ORDER BY {order_sql}"""
    )
    table = [
        [k, n, ex, pct(ex, n), un, ud, di, nm, an, pct(ex + an, n)]
        for k, n, ex, un, ud, di, nm, an in rows
    ]
    total = [sum(r[i] for r in rows) for i in range(1, 8)]
    n, ex, un, ud, di, nm, an = total
    table.append(["**all**", n, ex, pct(ex, n), un, ud, di, nm, an, pct(ex + an, n)])
    print(
        md_table(
            [label, "Cordis projects", "matched (exact)", "%", "unmatched", "unmatched with DOI", "DOI found in OpenAire",
             "recoverable: normalised grantId", "recoverable: any route", "% matched after all"],
            table,
        )
    )
    return [dict(zip(["key", "projects", "exact", "pct_exact", "unmatched", "unmatched_with_doi", "doi_in_openaire",
                      "recoverable_normalised_grantId", "recoverable_any", "pct_after_all"], r)) for r in table]


def analyse(con):
    q = lambda sql, params=None: con.execute(sql, params or []).fetchall()
    q1 = lambda sql, params=None: con.execute(sql, params or []).fetchone()

    n_cp = q1("SELECT count(*) FROM cp_t")[0]
    n_op = q1("SELECT count(*) FROM op_t")[0]
    n_doi = q1("SELECT count(*) FROM cp_t WHERE doi IS NOT NULL")[0]
    n_op_doi = q1("SELECT count(*) FROM op_t WHERE doi IS NOT NULL")[0]
    n_op_ec = q1("SELECT count(*) FROM op_t WHERE ec")[0]

    section("Baseline")
    exact_cp, exact_pairs = q1("SELECT count(DISTINCT cp), count(*) FROM pairs_min WHERE rule = 1")
    exact_op, exact_op_ec = q1("SELECT count(DISTINCT op), count(DISTINCT op) FILTER (WHERE ec) FROM pairs_min JOIN op_t ON op = op_t.id WHERE rule = 1")
    print(
        md_table(
            ["metric", "value", "%"],
            [
                ["Cordis projects", n_cp, 100.0],
                ["Cordis projects with a DOI", n_doi, pct(n_doi, n_cp)],
                ["Cordis projects matched (exact id_original = grantId)", exact_cp, pct(exact_cp, n_cp)],
                ["core_v3 reference (READ_TRANSFORMATION.md)", "96,056 of 139,925", 68.6],
                ["Phase 1 reference", "87,439 of 142,773", 61.24],
                ["OpenAire projects", n_op, 100.0],
                ["OpenAire projects with a DOI", n_op_doi, pct(n_op_doi, n_op)],
                ["OpenAire EC records (openaireId `corda*`)", n_op_ec, pct(n_op_ec, n_op)],
                ["OpenAire projects matched exactly", exact_op, pct(exact_op, n_op)],
                ["... of which EC records", exact_op_ec, pct(exact_op_ec, n_op_ec)],
            ],
        )
    )
    OUT["baseline"] = {
        "cordis_projects": n_cp, "cordis_with_doi": n_doi, "cordis_matched_exact": exact_cp, "match_pairs_exact": exact_pairs,
        "openaire_projects": n_op, "openaire_with_doi": n_op_doi, "openaire_ec_records": n_op_ec,
        "openaire_matched_exact": exact_op, "openaire_ec_matched_exact": exact_op_ec,
        "reference_core_v3": {"matched": 96056, "of": 139925}, "reference_phase1": {"matched": 87439, "of": 142773},
    }

    section("Matched vs unmatched by Cordis start year (5-year buckets)")
    print("`recoverable` columns only count Cordis projects that did NOT match exactly; the routes overlap, `any route` is their union.\n")
    OUT["by_start_year"] = coverage_by(q, "bucket", "(bucket = 'no date'), (bucket <> '<1990'), bucket", "start year")

    section("Matched vs unmatched by funding programme (framework_programme)")
    multi = q1("SELECT count(*) FROM cp_t WHERE n_prog > 1")[0]
    print(
        f"A project with several framework programmes is listed under the alphabetically first one ({multi:,} such projects); "
        "`none` = no j_project_fundingprogramme row.\n"
    )
    OUT["by_programme"] = coverage_by(q, "prog", "2 DESC", "framework programme")
    OUT["projects_with_several_framework_programmes"] = multi

    section("Matched vs unmatched by id_original shape")
    OUT["by_id_shape"] = coverage_by(q, "shape", "2 DESC", "id_original shape")

    section("Sample of unmatched Cordis projects (20, deterministic)")
    sample = q("SELECT id_original, doi, bucket, prog FROM cp_flag WHERE NOT r1 ORDER BY hash(id) LIMIT 20")
    print(md_table(["id_original", "doi", "start year", "programme"], sample))
    OUT["unmatched_sample"] = [dict(zip(["id_original", "doi", "bucket", "prog"], r)) for r in sample]

    section("Recovery routes for the unmatched, each on its own")
    un = q1("SELECT count(*) FROM cp_flag WHERE NOT r1")[0]
    un_doi = q1("SELECT count(*) FROM cp_flag WHERE NOT r1 AND has_doi")[0]
    rows = [["unmatched Cordis projects (exact)", un, 100.0], ["... with a DOI", un_doi, pct(un_doi, un)]]
    routes = {}
    for rank, name, desc in RULES[1:]:
        n_all = q1(f"SELECT count(*) FROM cp_flag WHERE r{rank}")[0]
        n_un = q1(f"SELECT count(*) FROM cp_flag WHERE NOT r1 AND r{rank}")[0]
        routes[name] = {"description": desc, "all_cordis": n_all, "recovered_unmatched": n_un}
        rows.append([f"recovered by `{name}` ({desc})", n_un, pct(n_un, un)])
    print(md_table(["route", "unmatched projects", "% of unmatched"], rows))
    print(
        "\nEach route is applied alone to the unmatched. The normalisations are nested (`alnum` finds everything `lower` finds, `zeros` "
        "everything `alnum` finds, `prefix` everything `zeros` finds), so `prefix` is the yield of all grantId normalisations together and the "
        "differences between the rows are what each step adds. `doi` = the DOIs of unmatched projects that exist in OpenAire; `doi_suffix` "
        "reads the H2020 DOI `10.3030/<n>` as a grant id."
    )
    OUT["unmatched"] = {"unmatched": un, "with_doi": un_doi, "routes": routes}

    section("Cordis projects matching more than one OpenAire project (exact rule)")
    multi_rows = q(
        """WITH m AS (
             SELECT cp, count(*) AS n, count(*) FILTER (WHERE ec) AS n_ec, count(*) FILTER (WHERE NOT ec) AS n_non_ec,
                    count(DISTINCT src) FILTER (WHERE ec) AS n_ec_src
             FROM pairs_min JOIN op_t ON op = op_t.id WHERE rule = 1 GROUP BY cp)
           SELECT CASE WHEN n_non_ec = 0 AND n_ec_src = 1 THEN 'EC duplicates (same EC datasource)'
                       WHEN n_non_ec = 0 THEN 'EC records from different EC datasources'
                       WHEN n_ec > 0 THEN 'EC + non-EC (grantId collision with another funder)'
                       ELSE 'non-EC only (grantId collision, no EC record)' END AS cause,
                  count(*) AS projects, sum(n)::BIGINT AS pairs,
                  list(id_original ORDER BY id_original)[1:3] AS examples
           FROM m JOIN cp_t ON cp_t.id = m.cp WHERE n > 1 GROUP BY 1 ORDER BY 2 DESC"""
    )
    total_multi = sum(r[1] for r in multi_rows)
    print(md_table(["cause", "Cordis projects", "match pairs", "examples (id_original)"], [[c, n, p, ", ".join(e)] for c, n, p, e in multi_rows]))
    print(f"\nTotal: **{total_multi:,}** Cordis projects match more than one OpenAire project (Phase 1: 9,250).")
    if not OUT["meta"]["openaire_project_has_openaireId"]:
        print("\n_oa.project has no openaireId: every match is counted as non-EC, the split is meaningless._")
    ec_only, non_ec_only = q1(
        """SELECT count(*) FILTER (WHERE n_ec > 0), count(*) FILTER (WHERE n_ec = 0) FROM (
             SELECT cp, count(*) FILTER (WHERE ec) AS n_ec FROM pairs_min JOIN op_t ON op = op_t.id WHERE rule = 1 GROUP BY cp)"""
    )
    print(
        f"\nExact match quality: {ec_only:,} matched Cordis projects link to at least one EC record, "
        f"**{non_ec_only:,}** only to non-EC records (probably false matches: the grantId belongs to another funder)."
    )
    OUT["multiple_matches"] = {
        "total": total_multi,
        "causes": [dict(zip(["cause", "projects", "pairs", "examples"], r)) for r in multi_rows],
        "exact_matched_with_ec_record": ec_only,
        "exact_matched_only_non_ec": non_ec_only,
    }

    section("Recommended match rule (cumulative)")
    print(
        "Rules are added in order; a Cordis project counts from the first rule that links it. `non-EC share` is the share of a rule's "
        f"*new* pairs pointing at a non-EC OpenAire record; `recommend` is a heuristic (adds projects and non-EC share <= {MAX_NON_EC_SHARE:.0%}).\n"
    )
    rows, rec, prev_matched = [], [], 0
    for rank, name, desc in RULES:
        matched = q1(f"SELECT count(*) FROM cp_first WHERE first_rule <= {rank}")[0]
        pairs = q1(f"SELECT count(*) FROM pairs_min WHERE rule <= {rank}")[0]
        fan = q1(f"SELECT count(*) FROM (SELECT cp FROM pairs_min WHERE rule <= {rank} GROUP BY cp HAVING count(*) > 1)")[0]
        new_pairs, new_non_ec = q1(
            f"SELECT count(*), count(*) FILTER (WHERE NOT ec) FROM pairs_min JOIN op_t ON op = op_t.id WHERE rule = {rank}"
        )
        share = new_non_ec / new_pairs if new_pairs else 0.0
        gain = matched - prev_matched
        recommend = "yes" if rank == 1 or (gain > 0 and share <= MAX_NON_EC_SHARE) else "no"
        rows.append([f"{rank} {name}", desc, gain, matched, pct(matched, n_cp), pairs, fan, pct(new_non_ec, new_pairs) if new_pairs else None, recommend])
        rec.append({"rule": name, "description": desc, "new_projects": gain, "cumulative_projects": matched,
                    "cumulative_pct": pct(matched, n_cp), "cumulative_pairs": pairs, "cumulative_projects_with_multiple_matches": fan,
                    "new_pairs": new_pairs, "new_pairs_non_ec": new_non_ec, "recommended": recommend == "yes"})
        prev_matched = matched
    print(
        md_table(["rule", "definition", "new Cordis projects", "cumulative matched", "cumulative %", "cumulative pairs",
                  "projects with >1 match", "non-EC share of new pairs (%)", "recommend"], rows)
    )
    OUT["recommended_rule"] = rec
    OUT["recommended_rule_note"] = f"heuristic: recommended when the rule adds projects and at most {MAX_NON_EC_SHARE:.0%} of its new pairs point to non-EC records"


def main(argv=None):
    ap = argparse.ArgumentParser(description="core_v4 Cordis -> OpenAire project match diagnosis (read-only)")
    ap.add_argument("--mem-mb", type=int, default=32_000, help="SLURM mem_mb; DuckDB gets this minus 4 GB headroom")
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--out", default=str(Path(__file__).with_name("cordis_project_match.json")))
    ap.add_argument("--temp-dir", default=None, help="DuckDB spill dir (default: <hpc_root>/duckdb_tmp/cordis_match)")
    ap.add_argument("--staging-db", help="OpenAire staging with project.doi (default: dumps.yaml path_duck_staging_v4)")
    ap.add_argument("--cordis-db", help="Cordis duckdb (default: the full_projects_no_pdfs query db)")
    args = ap.parse_args(argv)

    if args.staging_db and args.cordis_db:
        staging_db, cordis_db = args.staging_db, args.cordis_db
    else:
        from common.config.api_runner import get_query_settings
        from common.config.dumps import get_dumps_paths

        staging_db = args.staging_db or get_dumps_paths()["openaire_dump"]["path_duck_staging_v4"]
        cordis_db = args.cordis_db or get_query_settings()["cordis"].queries["full_projects_no_pdfs"].path_duck

    temp_dir = args.temp_dir
    if temp_dir is None:
        from common.config.settings import get_settings

        hpc_root = get_settings().hpc_root
        temp_dir = str(Path(hpc_root) / "duckdb_tmp" / "cordis_match") if hpc_root else "/tmp/cordis_match_duckdb"
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{max(args.mem_mb - 4_000, 1_000)}MB'")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET temp_directory='{temp_dir}'")
    for alias, path in [("oa", staging_db), ("cordis", cordis_db)]:
        con.execute(f"ATTACH '{path}' AS {alias} (READ_ONLY)")

    OUT.clear()
    OUT["meta"] = {"run_date": date.today().isoformat(), "staging_db": staging_db, "cordis_db": cordis_db}
    print("# core_v4 Cordis -> OpenAire project match\n")
    print(md_table(["source", "path"], [[k, v] for k, v in OUT["meta"].items() if k.endswith("_db")]))

    register_macros(con)
    build_tables(con)
    analyse(con)

    Path(args.out).write_text(json.dumps(OUT, indent=2, default=str))
    print(f"\nWrote {args.out}")
    con.close()


if __name__ == "__main__":
    main()
