"""core_v4 phase 1 measurements (read-only, kept for reproducibility).

Measures what the later core_v4 phases depend on: the 50M works cap, NLLB text
sizes, org geolocation coverage, the Cordis merge yield and country-code hygiene.

Reads only, every database is attached READ_ONLY:
  openaire_staging_2.duckdb   ids already hashed (same ids core_v3 uses)
  openaire_raw.duckdb         only for work.countries (staging_2 does not carry it)
  ror_raw.duckdb
  cordis_full_projects_no_pdfs_raw.duckdb

Heavy: scans the 218M-row work table and the 297M-row relation table. Run it on
SLURM, not on the login node:

  sbatch --partition=fat --cpus-per-task=16 --mem=200G --time=04:00:00 \\
    --wrap 'ENV=prod python src/pipelines/core_v4/investigation/phase1_measurements.py'

Writes phase1_measurements.json next to this file (override with --out) and
prints markdown tables to stdout.
"""

import argparse
import json
import time
from datetime import date
from pathlib import Path

import duckdb

from common.config.api_runner import get_query_settings
from common.config.dumps import get_dumps_paths
from common.config.settings import get_settings

# ISO 3166-1 alpha-2, 249 officially assigned codes.
ISO_ALPHA2 = set(
    """AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ
    CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR
    GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO
    JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR
    MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO
    RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV
    TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW""".split()
)
assert len(ISO_ALPHA2) == 249

# Non-ISO codes seen in the data -> ISO alpha-2. Applied by norm_cc() everywhere a
# country is compared; anything still outside ISO after this is reported as unmapped.
# EL/UK: EU institutional codes (Cordis). XK: Kosovo, user-assigned but used by the
# EU/ROR, kept as its own valid code. Retired ISO codes map to their main successor
# (YU/CS -> RS is a judgement call: both were split states; row counts are tiny).
# None = not a country (regions, unknown), drop to NULL.
COUNTRY_MAPPING = {
    "EL": "GR",
    "UK": "GB",
    "ZR": "CD",
    "YU": "RS",
    "CS": "RS",
    "AN": "CW",
    "QAT": "QA",  # alpha-3 leaked into work.countries
    "LIE": "LI",
    "ZZ": None,
    "EU": None,
    "DC": None,
    "DD": None,
    "OC": None,
    "EUROPE": None,
    "WORLD": None,
}
KEEP_NON_ISO = {"XK"}

DEFAULT_CAPS = [25_000_000, 50_000_000, 75_000_000, 100_000_000]
DEFAULT_CAP = 50_000_000  # the cap the "kept per tier" table is printed for
TIERS = ["both", "project_only", "org_only", "neither"]  # strict order of the cap

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


def _proposal(code):
    key = code.upper().strip()
    if key not in COUNTRY_MAPPING:
        return "UNMAPPED"
    return COUNTRY_MAPPING[key] or "NULL (drop)"


def section(title):
    print(f"\n## {title}\n", flush=True)


def main():
    ap = argparse.ArgumentParser(description="core_v4 phase 1 measurements (read-only)")
    ap.add_argument("--mem-mb", type=int, default=200_000, help="SLURM mem_mb; DuckDB gets this minus 20 GB headroom")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--out", default=str(Path(__file__).with_name("phase1_measurements.json")))
    ap.add_argument("--temp-dir", default=None, help="DuckDB spill dir (default: <hpc_root>/duckdb_tmp/phase1)")
    # Overrides so the script can be smoke-tested against small local copies.
    ap.add_argument("--caps", default=",".join(map(str, DEFAULT_CAPS)), help="comma-separated works caps to evaluate")
    ap.add_argument("--staging-db")
    ap.add_argument("--raw-db")
    ap.add_argument("--ror-db")
    ap.add_argument("--cordis-db")
    args = ap.parse_args()

    CAPS = [int(c) for c in args.caps.split(",")]
    default_cap = DEFAULT_CAP if DEFAULT_CAP in CAPS else CAPS[0]

    dumps = get_dumps_paths()
    staging_db = args.staging_db or dumps["openaire_dump"]["path_duck_staging_2"]
    raw_db = args.raw_db or dumps["openaire_dump"]["path_duck"]
    ror_db = args.ror_db or dumps["ror_dump"]["path_duck"]
    cordis_db = args.cordis_db or get_query_settings()["cordis"].queries["full_projects_no_pdfs"].path_duck

    settings = get_settings()
    temp_dir = args.temp_dir or (
        str(Path(settings.hpc_root) / "duckdb_tmp" / "phase1") if settings.hpc_root else "/tmp/phase1_duckdb"
    )
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"SET memory_limit='{max(args.mem_mb - 20_000, 2_000)}MB'")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET temp_directory='{temp_dir}'")
    for alias, path in [("oa", staging_db), ("raw", raw_db), ("ror", ror_db), ("cordis", cordis_db)]:
        con.execute(f"ATTACH '{path}' AS {alias} (READ_ONLY)")

    OUT["meta"] = {
        "run_date": date.today().isoformat(),
        "staging_db": staging_db,
        "raw_db": raw_db,
        "ror_db": ror_db,
        "cordis_db": cordis_db,
    }
    print("# core_v4 phase 1 measurements\n")
    print(md_table(["source", "path"], [[k, v] for k, v in OUT["meta"].items() if k.endswith("_db")]))

    def q(sql, params=None):
        return con.execute(sql, params or []).fetchall()

    def q1(sql, params=None):
        return con.execute(sql, params or []).fetchone()

    def timed(label):
        class T:
            def __enter__(self):
                self.t = time.time()

            def __exit__(self, *a):
                print(f"\n_({label}: {time.time() - self.t:.0f}s)_", flush=True)

        return T()

    # country normalisation shared by every comparison below
    con.execute(
        "CREATE MACRO norm_cc(c) AS CASE upper(trim(c)) "
        + " ".join(f"WHEN '{k}' THEN {'NULL' if v is None else repr(v)}" for k, v in COUNTRY_MAPPING.items())
        + " ELSE NULLIF(upper(trim(c)), '') END"
    )

    # ------------------------------------------------------------------
    # Relations overview (also verifies the relType / entity type names used below)
    # ------------------------------------------------------------------
    section("Relation types in openaire_staging_2")
    with timed("relation overview"):
        rows = q(
            "SELECT sourceType, relType.name, targetType, count(*) FROM oa.relation GROUP BY ALL ORDER BY 4 DESC"
        )
    print(md_table(["sourceType", "relType.name", "targetType", "rows"], rows))
    OUT["relations"] = [dict(zip(["sourceType", "relType", "targetType", "rows"], r)) for r in rows]

    # ------------------------------------------------------------------
    # WORKS: one narrow temp table, everything below aggregates it
    # ------------------------------------------------------------------
    section("Works: link groups")
    with timed("build wk"):
        con.execute(
            """CREATE TEMP TABLE w_proj AS
               SELECT DISTINCT target AS id FROM oa.relation
               WHERE relType.name = 'produces' AND sourceType = 'project' AND targetType = 'product'"""
        )
        con.execute(
            """CREATE TEMP TABLE w_org AS
               SELECT DISTINCT source AS id FROM oa.relation
               WHERE relType.name = 'hasAuthorInstitution' AND sourceType = 'product' AND targetType = 'organization'"""
        )
        con.execute(
            """CREATE TEMP TABLE wk AS
               SELECT w.id,
                      w.publicationDate                              AS d,
                      year(w.publicationDate)                        AS y,
                      w.title IS NOT NULL                            AS has_title,
                      COALESCE(len(w.descriptions) > 0, false)       AS has_desc,
                      w.publicationDate IS NOT NULL                  AS has_date,
                      w.language.code                                AS lang,
                      length(w.title)                                AS title_len,
                      length(w.descriptions[1])                      AS desc_len,
                      p.id IS NOT NULL                               AS has_proj,
                      o.id IS NOT NULL                               AS has_org
               FROM oa.work w
               LEFT JOIN w_proj p ON p.id = w.id
               LEFT JOIN w_org  o ON o.id = w.id"""
        )
        con.execute(
            """ALTER TABLE wk ADD COLUMN tier VARCHAR;
               UPDATE wk SET tier = CASE WHEN has_proj AND has_org THEN 'both'
                                         WHEN has_proj THEN 'project_only'
                                         WHEN has_org THEN 'org_only'
                                         ELSE 'neither' END"""
        )
    total_works = q1("SELECT count(*) FROM wk")[0]
    n_w_proj_rel, n_w_org_rel = q1("SELECT (SELECT count(*) FROM w_proj), (SELECT count(*) FROM w_org)")
    if not n_w_proj_rel or not n_w_org_rel:
        raise SystemExit(
            "w_proj/w_org is empty: the relType.name / sourceType / targetType filters do not match "
            "openaire_staging_2 -- compare with the relation overview table above."
        )
    print(f"Total works: **{total_works:,}**  ")
    print(
        f"Distinct works referenced by a project_produces relation: {n_w_proj_rel:,}; "
        f"by a product_hasAuthorInstitution relation: {n_w_org_rel:,} "
        "(the difference to the counts below is relation targets that are not in `work`)."
    )

    grp = {
        r[0]: r
        for r in q(
            """SELECT tier, count(*), count(*) FILTER (WHERE has_title), count(*) FILTER (WHERE has_desc),
                      count(*) FILTER (WHERE has_date)
               FROM wk GROUP BY tier"""
        )
    }
    rows, works_out = [], {}
    for t in TIERS:
        _, n, ti, de, da = grp.get(t, (t, 0, 0, 0, 0))
        rows.append([t, n, pct(n, total_works), ti, pct(ti, n), de, pct(de, n), da, pct(da, n)])
        works_out[t] = {"n": n, "title": ti, "description": de, "valid_date": da}
    print(
        "\n" + md_table(
            ["group", "works", "% of all", "title", "%", "descriptions[1]", "%", "valid date", "%"], rows
        )
    )
    n_proj = works_out["both"]["n"] + works_out["project_only"]["n"]
    n_org = works_out["both"]["n"] + works_out["org_only"]["n"]
    print(
        f"\nLinked to a project (any): **{n_proj:,}** ({pct(n_proj, total_works)}%); "
        f"linked to an org (any): **{n_org:,}** ({pct(n_org, total_works)}%)."
    )
    OUT["works"] = {
        "total": total_works,
        "groups": works_out,
        "linked_project_any": n_proj,
        "linked_org_any": n_org,
        "produces_targets_distinct": n_w_proj_rel,
        "author_institution_sources_distinct": n_w_org_rel,
    }

    section("Works: publication year, 5-year buckets (rows = bucket, cells = works)")
    bucket = (
        "CASE WHEN y IS NULL THEN 'no date' WHEN y < 1900 THEN '<1900' "
        "ELSE (y // 5 * 5)::VARCHAR || '-' || (y // 5 * 5 + 4)::VARCHAR END"
    )
    hist = q(f"SELECT {bucket} AS b, tier, count(*) FROM wk GROUP BY ALL")
    buckets = sorted({b for b, _, _ in hist}, key=lambda b: (b == "no date", b != "<1900", b))
    cell = {(b, t): n for b, t, n in hist}
    print(md_table(["bucket", *TIERS, "all"], [[b, *[cell.get((b, t), 0) for t in TIERS], sum(cell.get((b, t), 0) for t in TIERS)] for b in buckets]))
    OUT["works"]["year_histogram_5y"] = {b: {t: cell.get((b, t), 0) for t in TIERS} for b in buckets}
    this_year = date.today().year
    future = q1(f"SELECT count(*) FROM wk WHERE y > {this_year}")[0]
    print(f"\nWorks dated after {this_year}: {future:,}")
    OUT["works"]["future_dated"] = future

    section("Works: where the cap falls (strict order: project link, then org link, then newest date)")
    print(
        "Order: `both` > `project_only` > `org_only` > `neither`; inside the tier that straddles the cap, "
        "newest `publicationDate` first, works without a date last. Ties on the cutoff date are split arbitrarily.\n"
    )
    with timed("cap histogram"):
        dh = q(
            """SELECT tier, d, has_title, has_desc, count(*) FROM wk GROUP BY ALL
               ORDER BY d DESC NULLS LAST"""
        )
    cap_out = {}
    for cap in CAPS:
        kept = {"n": 0, "title": 0, "description": 0, "valid_date": 0}
        before, cut = 0, None
        per_tier = {}
        for t in TIERS:
            tier_rows = [r for r in dh if r[0] == t]  # already newest-first
            tier_n = sum(r[4] for r in tier_rows)
            if before + tier_n <= cap:
                for r in tier_rows:
                    kept["n"] += r[4]
                    kept["title"] += r[4] if r[2] else 0
                    kept["description"] += r[4] if r[3] else 0
                    kept["valid_date"] += r[4] if r[1] is not None else 0
                per_tier[t] = {"kept": tier_n, "of": tier_n}
                before += tier_n
                continue
            # this tier straddles the cap: newest dates whole, the cutoff date pro rata
            # across its (has_title, has_desc) groups so the split is deterministic.
            remaining = cap - before
            taken = 0
            by_date = {}
            for r in tier_rows:
                by_date.setdefault(r[1], []).append(r)
            dates = sorted((d for d in by_date if d is not None), reverse=True)
            if None in by_date:
                dates.append(None)
            for d in dates:
                rows_d = by_date[d]
                n_d = sum(r[4] for r in rows_d)
                need = min(n_d, remaining - taken)
                if need <= 0:
                    break
                shares = [need * r[4] // n_d for r in rows_d]
                shares[0] += need - sum(shares)  # rounding remainder
                for r, take in zip(rows_d, shares):
                    kept["n"] += take
                    kept["title"] += take if r[2] else 0
                    kept["description"] += take if r[3] else 0
                    kept["valid_date"] += take if r[1] is not None else 0
                taken += need
                if taken >= remaining:
                    cut = {"tier": t, "cutoff_date": str(d) if d is not None else None}
                    break
            per_tier[t] = {"kept": taken, "of": tier_n}
            before += taken
            break
        if before < cap and cut is None:
            cut = {"tier": None, "note": "cap larger than all works"}
        # exact cutoff-date tie handling
        if cut and cut.get("cutoff_date"):
            t = cut["tier"]
            newer = sum(r[4] for r in dh if r[0] == t and r[1] is not None and str(r[1]) > cut["cutoff_date"])
            at = sum(r[4] for r in dh if r[0] == t and r[1] is not None and str(r[1]) == cut["cutoff_date"])
            cut["tier_rows_newer_than_cutoff"] = newer
            cut["tier_rows_on_cutoff_date"] = at
        cap_out[str(cap)] = {"kept": kept, "per_tier": per_tier, "cutoff": cut}
    rows = []
    for cap in CAPS:
        c = cap_out[str(cap)]
        k = c["kept"]
        cut = c["cutoff"] or {}
        rows.append(
            [
                f"{cap / 1_000_000:g}M",
                cut.get("tier"),
                cut.get("cutoff_date"),
                cut.get("tier_rows_on_cutoff_date"),
                k["n"],
                pct(k["title"], k["n"]),
                pct(k["description"], k["n"]),
                pct(k["valid_date"], k["n"]),
            ]
        )
    print(md_table(["cap", "cutoff falls in tier", "cutoff date", "rows on that date", "kept", "% title", "% descr", "% date"], rows))
    print(f"\nKept per tier at cap {default_cap:,}:\n")
    pt = cap_out[str(default_cap)]["per_tier"]
    print(md_table(["tier", "kept", "of"], [[t, pt.get(t, {}).get("kept", 0), pt.get(t, {}).get("of", works_out[t]["n"])] for t in TIERS]))
    OUT["works"]["cap"] = cap_out

    # ------------------------------------------------------------------
    # WORKS for NLLB
    # ------------------------------------------------------------------
    section("Works for NLLB: top 20 language.code")
    rows = q("SELECT lang, count(*) n FROM wk GROUP BY lang ORDER BY n DESC LIMIT 20")
    print(md_table(["language.code", "works", "%"], [[r[0], r[1], pct(r[1], total_works)] for r in rows]))
    OUT["nllb"] = {"top_languages": [{"code": r[0], "works": r[1]} for r in rows]}

    section("Works for NLLB: text length in characters")
    rows = []
    OUT["nllb"]["length_chars"] = {}
    for label, col in [("title", "title_len"), ("descriptions[1]", "desc_len")]:
        n, avg, p95, mx = q1(
            f"SELECT count({col}), avg({col}), quantile_cont({col}, 0.95), max({col}) FROM wk"
        )
        rows.append([label, n, avg, p95, mx])
        OUT["nllb"]["length_chars"][label] = {"n": n, "avg": avg, "p95": p95, "max": mx}
    print(md_table(["text", "non-null", "avg", "p95", "max"], rows))

    # ------------------------------------------------------------------
    # ORGS + geolocation
    # ------------------------------------------------------------------
    section("Organizations and geolocation")
    con.execute(
        """CREATE TEMP TABLE ror_geo AS
           SELECT id AS rorId, locations[1].geonames_details.lat AS lat, locations[1].geonames_details.lng AS lng
           FROM ror.organizations"""
    )
    con.execute(
        """CREATE TEMP TABLE org AS
           SELECT o.id, o.openaireId, o.legalName, o.countryCode, o.rorId,
                  (SELECT list(p.value) FROM (SELECT unnest(o.pids) p) WHERE p.scheme = 'PIC') AS pics,
                  g.rorId IS NOT NULL                  AS ror_found,
                  g.lat IS NOT NULL AND g.lng IS NOT NULL AS ror_coords
           FROM oa.organization o LEFT JOIN ror_geo g ON g.rorId = o.rorId"""
    )
    n_org, n_ror, n_found, n_coords, n_cc_null, n_pic = q1(
        """SELECT count(*), count(rorId), count(*) FILTER (WHERE ror_found), count(*) FILTER (WHERE ror_coords),
                  count(*) FILTER (WHERE countryCode IS NULL), count(*) FILTER (WHERE pics IS NOT NULL)
           FROM org"""
    )
    print(
        md_table(
            ["metric", "orgs", "% of orgs"],
            [
                ["organizations", n_org, 100.0],
                ["with rorId", n_ror, pct(n_ror, n_org)],
                ["rorId found in ror_raw", n_found, pct(n_found, n_org)],
                ["with ROR coordinates", n_coords, pct(n_coords, n_org)],
                ["without any coordinates (no ROR coords)", n_org - n_coords, pct(n_org - n_coords, n_org)],
                ["with a PIC", n_pic, pct(n_pic, n_org)],
                ["countryCode null", n_cc_null, pct(n_cc_null, n_org)],
            ],
        )
    )
    rows = q("SELECT countryCode, count(*) n FROM org GROUP BY 1 ORDER BY n DESC LIMIT 20")
    print("\nTop 20 countryCode:\n")
    print(md_table(["countryCode", "orgs", "%"], [[r[0] if r[0] is not None else "(null)", r[1], pct(r[1], n_org)] for r in rows]))
    OUT["orgs"] = {
        "total": n_org,
        "with_rorId": n_ror,
        "rorId_found_in_ror_raw": n_found,
        "with_ror_coordinates": n_coords,
        "without_coordinates": n_org - n_coords,
        "with_pic": n_pic,
        "countryCode_null": n_cc_null,
        "top_countryCode": [{"code": r[0], "orgs": r[1]} for r in rows],
    }

    # ------------------------------------------------------------------
    # CORDIS
    # ------------------------------------------------------------------
    section("Cordis institutions")
    n_inst, n_geo, n_sc = q1(
        """SELECT count(*),
                  count(*) FILTER (WHERE geolocation IS NOT NULL AND geolocation::VARCHAR <> 'null'),
                  count(*) FILTER (WHERE street IS NOT NULL AND trim(street) <> '' AND city IS NOT NULL AND trim(city) <> '')
           FROM cordis.institution"""
    )
    print(
        md_table(
            ["metric", "institutions", "%"],
            [
                ["institutions", n_inst, 100.0],
                ["real coordinates (geolocation::varchar <> 'null')", n_geo, pct(n_geo, n_inst)],
                ["street and city", n_sc, pct(n_sc, n_inst)],
            ],
        )
    )
    OUT["cordis"] = {"institutions": n_inst, "real_coordinates": n_geo, "street_and_city": n_sc}

    con.execute(
        """CREATE TEMP TABLE inst AS
           SELECT id, legal_name, lower(trim(legal_name)) AS name_key, norm_cc(country) AS cc, street, city,
                  (street IS NOT NULL AND trim(street) <> '' AND city IS NOT NULL AND trim(city) <> '') AS has_addr,
                  (geolocation IS NOT NULL AND geolocation::VARCHAR <> 'null') AS has_geo
           FROM cordis.institution"""
    )
    con.execute("CREATE TEMP TABLE oorg AS SELECT id, lower(trim(legalName)) AS name_key, norm_cc(countryCode) AS cc, ror_coords, pics FROM org")

    section("Cordis project match on grantId")
    n_cp = q1("SELECT count(*) FROM cordis.project")[0]
    con.execute(
        """CREATE TEMP TABLE pm AS
           SELECT cp.id AS cproj, op.id AS oproj
           FROM cordis.project cp JOIN oa.project op ON cp.id_original = op.grantId"""
    )
    m_c, m_pairs, m_o = q1("SELECT count(DISTINCT cproj), count(*), count(DISTINCT oproj) FROM pm")
    n_op = q1("SELECT count(*) FROM oa.project")[0]
    fan = q1("SELECT count(*) FROM (SELECT cproj FROM pm GROUP BY 1 HAVING count(*) > 1)")[0]
    print(
        md_table(
            ["metric", "value", "%"],
            [
                ["Cordis projects", n_cp, 100.0],
                ["Cordis projects matched (id_original = grantId)", m_c, pct(m_c, n_cp)],
                ["OpenAire projects matched", m_o, pct(m_o, n_op)],
                ["match pairs", m_pairs, None],
                ["Cordis projects matching >1 OpenAire project", fan, None],
            ],
        )
    )
    OUT["cordis"]["project_match"] = {
        "cordis_projects": n_cp,
        "matched": m_c,
        "openaire_projects_matched": m_o,
        "pairs": m_pairs,
        "cordis_projects_with_multiple_openaire_matches": fan,
    }

    section("Cordis org match yield (triplets = Cordis project x institution inside matched projects)")
    con.execute(
        """CREATE TEMP TABLE trip AS
           SELECT DISTINCT pm.cproj, pm.oproj, jpi.institution_id AS inst_id, jpi.organization_id AS pic
           FROM pm JOIN cordis.j_project_institution jpi ON jpi.project_id = pm.cproj"""
    )
    con.execute(
        """CREATE TEMP TABLE rel_part AS
           SELECT DISTINCT source AS oproj, target AS org_id FROM oa.relation
           WHERE relType.name = 'hasParticipant' AND sourceType = 'project'"""
    )
    n_trip = q1("SELECT count(DISTINCT (cproj, inst_id)) FROM trip")[0]
    variants = {
        "name only": "i.name_key = o.name_key",
        "name + country": "i.name_key = o.name_key AND i.cc IS NOT NULL AND i.cc = o.cc",
        "name + country (null on either side allowed)": "i.name_key = o.name_key AND (i.cc IS NULL OR o.cc IS NULL OR i.cc = o.cc)",
        "PIC": "o.pics IS NOT NULL AND t.pic IS NOT NULL AND list_contains(o.pics, t.pic)",
    }
    rows, yields = [], {}
    for name, cond in variants.items():
        # PIC needs the triplet's pic; the name variants ignore it.
        con.execute(f"DROP TABLE IF EXISTS tm")
        con.execute(
            f"""CREATE TEMP TABLE tm AS
                SELECT DISTINCT t.cproj, t.oproj, t.inst_id, o.id AS org_id
                FROM trip t JOIN inst i ON i.id = t.inst_id JOIN oorg o ON {cond}"""
        )
        d_trip, pairs, d_inst, d_org = q1(
            "SELECT count(DISTINCT (cproj, inst_id)), count(*), count(DISTINCT inst_id), count(DISTINCT org_id) FROM tm"
        )
        confirmed = q1(
            """SELECT count(DISTINCT (tm.cproj, tm.inst_id)), count(*) FROM tm
               JOIN rel_part r ON r.oproj = tm.oproj AND r.org_id = tm.org_id"""
        )
        rows.append([name, d_trip, pct(d_trip, n_trip), pairs, round(pairs / d_trip, 2) if d_trip else None, d_inst, d_org, confirmed[0], pct(confirmed[0], n_trip)])
        yields[name] = {
            "distinct_triplets": d_trip,
            "pairs": pairs,
            "cordis_institutions": d_inst,
            "openaire_orgs": d_org,
            "triplets_confirmed_by_hasParticipant": confirmed[0],
            "pairs_confirmed_by_hasParticipant": confirmed[1],
        }
        con.execute(f"CREATE TEMP TABLE tm_{len(yields)} AS SELECT * FROM tm")
    print(f"Triplets in matched projects: **{n_trip:,}**\n")
    print(
        md_table(
            ["variant", "triplets matched", "% of triplets", "pairs (fan-out)", "pairs / triplet", "Cordis institutions", "OpenAire orgs", "triplets also in OA hasParticipant", "% of triplets"],
            rows,
        )
    )
    OUT["cordis"]["org_match_yield"] = {"triplets_in_matched_projects": n_trip, "variants": yields}

    # PIC vs name agreement on the triplet-org level
    tm_name_country, tm_pic = "tm_2", "tm_4"
    agree = q1(
        f"""SELECT count(*), count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM {tm_name_country} n WHERE n.cproj = p.cproj AND n.inst_id = p.inst_id AND n.org_id = p.org_id))
            FROM {tm_pic} p"""
    )
    pic_only, pic_new_vs_name = q1(
        f"""SELECT
              count(DISTINCT (p.cproj, p.inst_id)) FILTER (WHERE NOT EXISTS (
                  SELECT 1 FROM {tm_name_country} n WHERE n.cproj = p.cproj AND n.inst_id = p.inst_id)),
              count(DISTINCT (p.cproj, p.inst_id)) FILTER (WHERE NOT EXISTS (
                  SELECT 1 FROM tm_1 n WHERE n.cproj = p.cproj AND n.inst_id = p.inst_id))
            FROM {tm_pic} p"""
    )
    print(
        f"\nPIC-matched pairs that name+country also finds (same OpenAire org): {agree[1]:,} / {agree[0]:,} ({pct(agree[1], agree[0])}%). "
        f"Triplets found by PIC but not by name+country: {pic_only:,}; not by name only: {pic_new_vs_name:,}."
    )
    OUT["cordis"]["pic_vs_name"] = {
        "pic_pairs": agree[0],
        "pic_pairs_also_name_country": agree[1],
        "triplets_pic_not_name_country": pic_only,
        "triplets_pic_not_name_only": pic_new_vs_name,
    }

    section("Cordis PIC overlap")
    c_pics, o_pics, both_pics, c_null, c_rows = q1(
        """WITH cp AS (SELECT DISTINCT organization_id AS pic FROM cordis.j_project_institution WHERE organization_id IS NOT NULL),
                op AS (SELECT DISTINCT unnest(pics) AS pic FROM oorg WHERE pics IS NOT NULL)
           SELECT (SELECT count(*) FROM cp), (SELECT count(*) FROM op),
                  (SELECT count(*) FROM cp JOIN op USING (pic)),
                  (SELECT count(*) FROM cordis.j_project_institution WHERE organization_id IS NULL),
                  (SELECT count(*) FROM cordis.j_project_institution)"""
    )
    print(
        md_table(
            ["metric", "value", "%"],
            [
                ["j_project_institution rows", c_rows, 100.0],
                ["rows with organization_id (PIC)", c_rows - c_null, pct(c_rows - c_null, c_rows)],
                ["distinct Cordis PICs", c_pics, None],
                ["distinct OpenAire org PICs", o_pics, None],
                ["PICs in both", both_pics, pct(both_pics, c_pics)],
            ],
        )
    )
    OUT["cordis"]["pic_overlap"] = {
        "jpi_rows": c_rows,
        "jpi_rows_with_pic": c_rows - c_null,
        "cordis_distinct_pics": c_pics,
        "openaire_distinct_pics": o_pics,
        "in_both": both_pics,
    }

    section("Mapbox candidates: matched orgs with a Cordis address but no ROR coordinates")
    print(
        "Matching is org level (not restricted to matched projects). `Cordis coords` = at least one matched "
        "institution already has real coordinates, so Mapbox is not needed for it.\n"
    )
    mb_variants = {
        "name only": "i.name_key = o.name_key",
        "name + country": "i.name_key = o.name_key AND i.cc IS NOT NULL AND i.cc = o.cc",
        "PIC": (
            "o.pics IS NOT NULL AND list_contains(o.pics, jp.pic)"
        ),
    }
    rows, mb_out = [], {}
    for name, cond in mb_variants.items():
        if name == "PIC":
            src = (
                "oorg o JOIN (SELECT DISTINCT organization_id AS pic, institution_id FROM cordis.j_project_institution "
                "WHERE organization_id IS NOT NULL) jp ON list_contains(o.pics, jp.pic) JOIN inst i ON i.id = jp.institution_id"
            )
        else:
            src = f"oorg o JOIN inst i ON {cond}"
        r = q1(
            f"""WITH m AS (
                   SELECT o.id, o.ror_coords, bool_or(i.has_addr) AS addr, bool_or(i.has_geo) AS geo
                   FROM {src} GROUP BY o.id, o.ror_coords)
                SELECT count(*), count(*) FILTER (WHERE NOT ror_coords),
                       count(*) FILTER (WHERE NOT ror_coords AND addr),
                       count(*) FILTER (WHERE NOT ror_coords AND addr AND geo),
                       count(*) FILTER (WHERE NOT ror_coords AND addr AND NOT geo),
                       count(*) FILTER (WHERE NOT ror_coords AND geo)
                FROM m"""
        )
        rows.append([name, *r])
        mb_out[name] = dict(zip(["matched_orgs", "no_ror_coords", "mapbox_candidates", "candidates_with_cordis_coords", "candidates_needing_mapbox", "no_ror_coords_but_cordis_coords"], r))
    print(md_table(["match", "matched orgs", "no ROR coords", "**Mapbox candidates** (+ address)", "of which Cordis coords", "of which need Mapbox", "no ROR coords, any Cordis coords"], rows))
    OUT["cordis"]["mapbox_candidates"] = mb_out

    section("Duplicate legal names with different addresses")
    r = q1(
        """WITH g AS (
              SELECT name_key, count(*) n,
                     count(DISTINCT concat_ws('|', lower(trim(street)), lower(trim(city)), cc)) AS addr,
                     count(DISTINCT concat_ws('|', lower(trim(city)), cc)) AS city_cc
              FROM inst GROUP BY name_key)
           SELECT count(*), count(*) FILTER (WHERE n > 1), sum(n) FILTER (WHERE n > 1),
                  count(*) FILTER (WHERE addr > 1), sum(n) FILTER (WHERE addr > 1),
                  count(*) FILTER (WHERE city_cc > 1), sum(n) FILTER (WHERE city_cc > 1)
           FROM g"""
    )
    print(
        md_table(
            ["metric", "names", "institution rows"],
            [
                ["distinct lower(trim(legal_name))", r[0], n_inst],
                ["names on >1 institution", r[1], r[2]],
                ["... with >1 distinct (street, city, country)", r[3], r[4]],
                ["... with >1 distinct (city, country)", r[5], r[6]],
            ],
        )
    )
    dup_oa = q1(
        """SELECT count(*) FILTER (WHERE n > 1), sum(n) FILTER (WHERE n > 1), count(*) FILTER (WHERE cc > 1)
           FROM (SELECT name_key, count(*) n, count(DISTINCT cc) cc FROM oorg GROUP BY name_key)"""
    )
    print(
        f"\nOpenAire orgs, same lower(trim(legalName)): {dup_oa[0]:,} names on >1 org ({dup_oa[1]:,} orgs), "
        f"{dup_oa[2]:,} of them with >1 distinct countryCode."
    )
    OUT["cordis"]["duplicate_names"] = {
        "distinct_names": r[0],
        "names_multi_institution": r[1],
        "institution_rows_multi": r[2],
        "names_multi_street_city_country": r[3],
        "institution_rows_multi_street_city_country": r[4],
        "names_multi_city_country": r[5],
        "institution_rows_multi_city_country": r[6],
        "openaire_names_multi_org": dup_oa[0],
        "openaire_orgs_in_multi_name": dup_oa[1],
        "openaire_names_multi_country": dup_oa[2],
    }

    # ------------------------------------------------------------------
    # COUNTRY CODES
    # ------------------------------------------------------------------
    section("Country codes")
    with timed("country codes"):
        sources = {
            "openaire org.countryCode": "SELECT countryCode AS c FROM org",
            "cordis institution.country": "SELECT country AS c FROM cordis.institution",
            "ror location country_code": "SELECT unnest(locations).geonames_details.country_code AS c FROM ror.organizations",
            "openaire work.countries": "SELECT unnest(list_distinct(list_transform(countries, x -> x.code))) AS c FROM raw.work",
        }
        cc_out = {}
        rows = []
        all_values = {}
        for name, sql in sources.items():
            counts = q(f"SELECT c, count(*) FROM ({sql}) GROUP BY c")
            vals = {}
            for c, n in counts:
                vals[c] = vals.get(c, 0) + n
            all_values[name] = vals
            nonnull = {c: n for c, n in vals.items() if c is not None}
            non_iso = {c: n for c, n in nonnull.items() if c not in ISO_ALPHA2 and c not in KEEP_NON_ISO}
            unmapped = {
                c: n
                for c, n in non_iso.items()
                if c.upper().strip() not in COUNTRY_MAPPING and c.upper().strip() not in ISO_ALPHA2
            }
            rows.append([name, len(nonnull), vals.get(None, 0), len(non_iso), sum(non_iso.values()), len(unmapped)])
            cc_out[name] = {
                "distinct": len(nonnull),
                "null": vals.get(None, 0),
                "non_iso": dict(sorted(non_iso.items(), key=lambda kv: -kv[1])),
                "still_unmapped": dict(sorted(unmapped.items(), key=lambda kv: -kv[1])),
            }
    print(md_table(["source", "distinct values", "null rows", "non-ISO distinct", "non-ISO rows", "unmapped after mapping"], rows))
    union = set().union(*[{c for c in v if c is not None} for v in all_values.values()])
    print(f"\nDistinct values across all four sources: **{len(union)}** (ISO alpha-2 has 249).")
    for name, o in cc_out.items():
        if o["non_iso"]:
            print(f"\nNon-ISO in **{name}**:\n")
            print(md_table(["value", "rows", "proposed"], [[c, n, _proposal(c)] for c, n in list(o["non_iso"].items())[:40]]))
    print(f"\nProposed mapping (applied by `norm_cc` above): `{COUNTRY_MAPPING}`; kept as-is: `{sorted(KEEP_NON_ISO)}`.")
    OUT["country_codes"] = {
        "sources": cc_out,
        "distinct_union": len(union),
        "proposed_mapping": COUNTRY_MAPPING,
        "kept_non_iso": sorted(KEEP_NON_ISO),
    }

    Path(args.out).write_text(json.dumps(OUT, indent=2, default=str))
    print(f"\nWrote {args.out}")


if __name__ == "__main__":
    main()
