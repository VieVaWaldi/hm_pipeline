"""
Exports a small, fully connected test copy of core_v4_noworkenrichment.duckdb (fits on a laptop):

    core_v4_noworkenrichment.duckdb  ->  core_v4_noworkenrichment-min.duckdb

N projects, N organizations and N works (default 1000 each), ALL topics and ALL minorities, and only the relations
whose two ends are both in the selection. Same DDL as the source, so types and constraints are identical.

The selection is graph-driven and deterministic (rank by connectivity, ties broken by hash(id, seed), never random()),
because independent samples of 1000 would share almost no relations:
  1. organizations: a quota of hubs of minority projects, the rest hubs of all projects (>= 85% with a geolocation)
  2. projects (all linked to the chosen organizations and to at least one work): many minority projects, plus cultural
     heritage / translated / theme / pillar projects, the rest by most links to the chosen organizations
  3. works: 70% project-linked (each chosen project gets its best-fitting work first) and 30% organization-only
     (link_tier 1, no project), preferring moderate numbers of chosen organizations per work
  4. repair: organizations without any relation to the chosen projects/works are swapped for ones that have some
Result: every project and every work has at least one organization, every organization has at least one relation.

The source is opened read-only. The result is written to `<out>.tmp`, verified there and renamed at the end.

Usage (sbatch, it scans the 153M-row relation table a few times):
    ENV=prod uv run python -m pipelines.core_v4.export_min [--n 1000] [--seed 42]
"""

import argparse
import logging
import os
import shutil
import time
from pathlib import Path

import duckdb

from common.log.logger import setup_logging

CORE_DIR = "/work/lu72hip/data/duckdb/core"
DEFAULT_SRC = f"{CORE_DIR}/core_v4_noworkenrichment.duckdb"
DEFAULT_OUT = f"{CORE_DIR}/core_v4_noworkenrichment-min.duckdb"
TABLES = ["work", "project", "organization", "topic", "minority", "relation", "relation_topic"]

GEO_SHARE = 0.85  # share of the chosen organizations that must have a geolocation
MINORITY_ORG_SHARE = 0.35  # share of organizations picked as hubs of minority projects
MINORITY_PROJECT_SHARE = 0.40
TIER0_WORK_SHARE = 0.70  # rest is link_tier 1 (linked to organizations only)
WORK_ORGS_TARGET = 20  # preferred number of chosen organizations per work


def _q(con, sql: str, params=None):
    return con.execute(sql, params or []).fetchall()


def _count(con, sql: str) -> int:
    return con.execute(sql).fetchone()[0]


def _h(seed: int, col: str = "id") -> str:
    return f"hash(CAST({col} AS VARCHAR) || '-{seed}')"


def take(con, target: str, candidates: str, n: int, tag: str) -> int:
    """Adds up to n ids from `candidates` (columns id, score, h) that are not in `target` yet, best score first."""
    before = _count(con, f"SELECT count(*) FROM {target}")
    con.execute(
        f"INSERT INTO {target} SELECT id, '{tag}' FROM ({candidates}) c "
        f"WHERE id NOT IN (SELECT id FROM {target}) ORDER BY score DESC, h LIMIT {n}"
    )
    got = _count(con, f"SELECT count(*) FROM {target}") - before
    logging.info(f"  {target} +{got:,} ({tag}, wanted {n:,})")
    return got


def take_orgs(con, candidates: str, n: int, tag: str) -> None:
    """Like take() for organizations, but GEO_SHARE of the quota comes from organizations with a geolocation."""
    goal = _count(con, "SELECT count(*) FROM sel_o") + n
    n_geo = round(n * GEO_SHARE)
    take(con, "sel_o", f"SELECT * FROM ({candidates}) WHERE geo", n_geo, tag + "_geo")
    take(con, "sel_o", f"SELECT * FROM ({candidates}) WHERE NOT geo", n - n_geo, tag + "_nogeo")
    short = goal - _count(con, "SELECT count(*) FROM sel_o")
    if short > 0:  # not enough of one kind: fill from anything
        take(con, "sel_o", candidates, short, tag + "_fill")


def select_ids(con, n: int, seed: int) -> None:
    n_min_o = round(n * MINORITY_ORG_SHARE)
    n_t0 = round(n * TIER0_WORK_SHARE)

    logging.info("edge tables")
    con.execute(
        "CREATE TEMP TABLE po AS SELECT DISTINCT source AS p, target AS o FROM src.relation "
        "WHERE sourceType = 'project' AND targetType = 'organization'"
    )
    con.execute(
        "CREATE TEMP TABLE pw AS SELECT DISTINCT source AS p, target AS w FROM src.relation "
        "WHERE sourceType = 'project' AND targetType = 'product'"
    )
    con.execute(
        "CREATE TEMP TABLE pinfo AS SELECT p.id, coalesce(len(p.minority_qid), 0) > 0 AS minority, coalesce(p.is_ch, false) AS is_ch, "
        "coalesce(p.is_translated, false) AS is_translated, p.theme IS NOT NULL AS has_theme, coalesce(p.pillars, 0) > 0 AS has_pillar "
        "FROM src.project p"
    )
    con.execute("CREATE TEMP TABLE po_deg AS SELECT p, count(*) AS n_o FROM po GROUP BY p")
    con.execute("CREATE TEMP TABLE pw_deg AS SELECT p, count(*) AS n_w FROM pw GROUP BY p")
    # projects that can be connected at all: at least one organization and one work
    con.execute("CREATE TEMP TABLE pcand AS SELECT i.* FROM pinfo i JOIN po_deg d ON d.p = i.id JOIN pw_deg w ON w.p = i.id")
    logging.info(
        "project candidates (>=1 org and >=1 work): %s",
        _q(con, "SELECT count(*), count(*) FILTER (minority), count(*) FILTER (is_ch), count(*) FILTER (is_translated), "
                "count(*) FILTER (has_theme), count(*) FILTER (has_pillar) FROM pcand")[0],
    )
    con.execute(
        "CREATE TEMP TABLE oinfo AS SELECT o.id, o.geolocation IS NOT NULL AND len(o.geolocation) = 2 AS geo, "
        "coalesce(d.n, 0) AS pdeg, coalesce(m.n, 0) AS mdeg FROM src.organization o "
        "LEFT JOIN (SELECT o AS id, count(DISTINCT p) AS n FROM po GROUP BY o) d USING (id) "
        "LEFT JOIN (SELECT o AS id, count(DISTINCT p) AS n FROM po JOIN pcand c ON c.id = po.p WHERE c.minority GROUP BY o) m USING (id)"
    )

    # 1) organizations
    logging.info("select organizations")
    con.execute("CREATE TEMP TABLE sel_o (id UBIGINT, why VARCHAR)")
    take_orgs(con, f"SELECT id, geo, mdeg AS score, {_h(seed)} AS h FROM oinfo WHERE mdeg > 0", n_min_o, "org_minority_hub")
    take_orgs(con, f"SELECT id, geo, pdeg AS score, {_h(seed)} AS h FROM oinfo WHERE pdeg > 0",
              n - _count(con, "SELECT count(*) FROM sel_o"), "org_project_hub")

    # 2) work->organization edges of the chosen organizations; only works that reach one can be chosen
    logging.info("work edges")
    con.execute(
        "CREATE TEMP TABLE wo_o AS SELECT source AS w, target AS o FROM src.relation "
        "WHERE sourceType = 'product' AND targetType = 'organization' AND target IN (SELECT id FROM sel_o)"
    )
    logging.info(f"  work->organization edges into the chosen organizations: {_count(con, 'SELECT count(*) FROM wo_o'):,}")
    # fit = how close a work is to WORK_ORGS_TARGET chosen organizations (moderate sizes instead of mega-collaborations)
    con.execute(
        f"CREATE TEMP TABLE w_org_deg AS SELECT w, count(DISTINCT o) AS n_o, -abs(count(DISTINCT o) - {WORK_ORGS_TARGET}) AS fit "
        "FROM wo_o GROUP BY w"
    )
    con.execute("CREATE TEMP TABLE pw_ok AS SELECT pw.p, pw.w, d.fit FROM pw JOIN w_org_deg d ON d.w = pw.w")

    # 3) projects: every one has >= 1 chosen organization and >= 1 work that reaches a chosen organization
    logging.info("select projects")
    con.execute(
        "CREATE TEMP TABLE pscore AS SELECT c.*, count(DISTINCT po.o) + least(max(k.n_w), 5) AS score FROM pcand c "
        "JOIN po ON po.p = c.id JOIN sel_o ON sel_o.id = po.o "
        "JOIN (SELECT p, count(*) AS n_w FROM pw_ok GROUP BY p) k ON k.p = c.id GROUP BY ALL"
    )
    con.execute("CREATE TEMP TABLE sel_p (id UBIGINT, why VARCHAR)")
    for flag, share, tag in [("minority", MINORITY_PROJECT_SHARE, "minority"), ("is_ch", 0.05, "cultural_heritage"),
                             ("is_translated", 0.05, "translated"), ("has_theme", 0.05, "theme"), ("has_pillar", 0.05, "pillar")]:
        take(con, "sel_p", f"SELECT id, score, {_h(seed)} AS h FROM pscore WHERE {flag}", round(n * share), tag)
    take(con, "sel_p", f"SELECT id, score, {_h(seed)} AS h FROM pscore", n - _count(con, "SELECT count(*) FROM sel_p"), "most_linked")

    # 4) works: tier 0 first (each chosen project gets its best-fitting work before any project gets a second one),
    #    then tier 1 (link_tier 1 = organizations only, no project relation)
    logging.info("select works")
    con.execute("CREATE TEMP TABLE sel_w (id UBIGINT, why VARCHAR)")
    t0 = (
        "SELECT w AS id, max(1000 - 100 * rk + fit) AS score, " + _h(seed, "w") + " AS h FROM ("
        f"SELECT pw_ok.p, pw_ok.w, pw_ok.fit, row_number() OVER (PARTITION BY pw_ok.p ORDER BY pw_ok.fit DESC, {_h(seed, 'pw_ok.w')}) AS rk "
        "FROM pw_ok JOIN sel_p ON sel_p.id = pw_ok.p) GROUP BY w"
    )
    take(con, "sel_w", t0, n_t0, "tier0_project_linked")
    t1 = (
        f"SELECT d.w AS id, d.fit AS score, {_h(seed, 'd.w')} AS h FROM w_org_deg d "
        "JOIN (SELECT id FROM src.work WHERE link_tier = 1) t ON t.id = d.w "
        "WHERE NOT EXISTS (SELECT 1 FROM pw WHERE pw.w = d.w)"
    )
    take(con, "sel_w", t1, n - _count(con, "SELECT count(*) FROM sel_w"), "tier1_org_only")

    # 4) repair: swap organizations without any relation to the chosen projects/works for ones that have some
    logging.info("repair organizations")
    con.execute(
        "CREATE TEMP TABLE w_edges AS SELECT source AS w, target AS o FROM src.relation "
        "WHERE sourceType = 'product' AND targetType = 'organization' AND source IN (SELECT id FROM sel_w)"
    )
    con.execute(
        "CREATE TEMP TABLE o_links AS "
        "SELECT o, count(*) AS n FROM (SELECT o FROM po JOIN sel_p ON sel_p.id = po.p UNION ALL SELECT o FROM w_edges) GROUP BY o"
    )
    con.execute("DELETE FROM sel_o WHERE id NOT IN (SELECT o FROM o_links)")
    missing = n - _count(con, "SELECT count(*) FROM sel_o")
    logging.info(f"  organizations without a relation, swapped out: {missing}")
    if missing:
        take_orgs(con, f"SELECT i.id, i.geo, l.n AS score, {_h(seed, 'i.id')} AS h FROM oinfo i JOIN o_links l ON l.o = i.id", missing, "org_repair")


def export(src: str, out: str, n: int, seed: int, mem_mb: int, threads: int) -> None:
    out_path, tmp = Path(out), Path(out + ".tmp")
    if out_path.exists():
        raise SystemExit(f"{out} exists already: refusing to overwrite it (move it away first)")
    if not Path(src).is_file():
        raise SystemExit(f"missing source {src}")
    if Path(src + ".wal").exists():
        raise SystemExit(f"{src}.wal exists: a writer is or was active on the source, not exporting")
    tmp.unlink(missing_ok=True)

    t0 = time.perf_counter()
    con = duckdb.connect(str(tmp))
    con.execute(f"SET memory_limit = '{mem_mb}MB'")
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET temp_directory = '{tmp}.spill'")
    con.execute(f"ATTACH '{src}' AS src (READ_ONLY)")

    select_ids(con, n, seed)
    logging.info(f"selection done ({time.perf_counter() - t0:.0f}s), writing tables")

    for table in TABLES:
        ddl = con.execute("SELECT sql FROM duckdb_tables() WHERE database_name = 'src' AND table_name = ?", [table]).fetchone()[0]
        con.execute(ddl)
    con.execute("INSERT INTO project SELECT * FROM src.project WHERE id IN (SELECT id FROM sel_p)")
    con.execute("INSERT INTO organization SELECT * FROM src.organization WHERE id IN (SELECT id FROM sel_o)")
    con.execute("INSERT INTO work SELECT * FROM src.work WHERE id IN (SELECT id FROM sel_w)")
    con.execute("INSERT INTO topic SELECT * FROM src.topic")
    con.execute("INSERT INTO minority SELECT * FROM src.minority")
    for st, tt, s, t in [("project", "organization", "sel_p", "sel_o"), ("project", "product", "sel_p", "sel_w"),
                         ("product", "organization", "sel_w", "sel_o")]:
        con.execute(
            f"INSERT INTO relation SELECT * FROM src.relation WHERE sourceType = '{st}' AND targetType = '{tt}' "
            f"AND source IN (SELECT id FROM {s}) AND target IN (SELECT id FROM {t})"
        )
    con.execute("INSERT INTO relation_topic SELECT * FROM src.relation_topic WHERE source_id IN (SELECT id FROM sel_p)")

    verify(con, n)
    report(con)
    con.execute("DETACH src")
    con.execute("CHECKPOINT")
    con.close()
    shutil.rmtree(f"{tmp}.spill", ignore_errors=True)
    os.replace(tmp, out_path)
    logging.info(f"wrote {out} ({out_path.stat().st_size / 1e6:.1f} MB, {time.perf_counter() - t0:.0f}s)")


def verify(con: duckdb.DuckDBPyConnection, n: int) -> None:
    """Counts, referential and connectivity checks; raises on the first inconsistency."""
    problems = []

    def must_be_zero(name: str, sql: str) -> None:
        v = _count(con, sql)
        logging.info(f"check {name}: {v:,} (must be 0)")
        if v:
            problems.append(f"{name}: {v}")

    for table in ("project", "organization", "work"):
        got = _count(con, f"SELECT count(*) FROM {table}")
        logging.info(f"{table}: {got:,} rows")
        if got != n:
            problems.append(f"{table} has {got} rows, wanted {n}")
    for table in ("topic", "minority"):
        if _count(con, f"SELECT count(*) FROM {table}") != _count(con, f"SELECT count(*) FROM src.{table}"):
            problems.append(f"{table} is not complete")
    must_be_zero(
        "relation -> missing project/organization/work",
        "SELECT count(*) FROM relation r WHERE "
        "(r.sourceType = 'project' AND r.source NOT IN (SELECT id FROM project)) OR "
        "(r.targetType = 'organization' AND r.target NOT IN (SELECT id FROM organization)) OR "
        "(r.sourceType = 'product' AND r.source NOT IN (SELECT id FROM work)) OR "
        "(r.targetType = 'product' AND r.target NOT IN (SELECT id FROM work))",
    )
    must_be_zero("relation_topic -> missing project/topic",
                 "SELECT count(*) FROM relation_topic rt WHERE rt.source_id NOT IN (SELECT id FROM project) OR rt.topic_id NOT IN (SELECT id FROM topic)")
    must_be_zero("relation_topic rows for works", "SELECT count(*) FROM relation_topic WHERE type <> 'project'")
    must_be_zero("project.minority_qid not in minority table",
                 "SELECT count(*) FROM (SELECT DISTINCT unnest(minority_qid) AS q FROM project) p "
                 "WHERE p.q NOT IN (SELECT qid FROM minority) AND p.q NOT IN (SELECT unnest(merged_qids) FROM minority WHERE merged_qids IS NOT NULL)")
    must_be_zero("project without organization",
                 "SELECT count(*) FROM project WHERE id NOT IN (SELECT source FROM relation WHERE sourceType = 'project' AND targetType = 'organization')")
    must_be_zero("work without organization",
                 "SELECT count(*) FROM work WHERE id NOT IN (SELECT source FROM relation WHERE sourceType = 'product' AND targetType = 'organization')")
    must_be_zero("organization without any relation",
                 "SELECT count(*) FROM organization WHERE id NOT IN (SELECT target FROM relation WHERE targetType = 'organization')")
    must_be_zero("work with NULL link_tier", "SELECT count(*) FROM work WHERE link_tier IS NULL")
    if problems:
        raise RuntimeError("export failed verification: " + "; ".join(problems))


def report(con: duckdb.DuckDBPyConnection) -> None:
    for table in TABLES:
        logging.info(f"{table}: {_count(con, f'SELECT count(*) FROM {table}'):,} rows")
    logging.info("relation by type: %s", _q(con, "SELECT sourceType, targetType, count(*) FROM relation GROUP BY ALL ORDER BY ALL"))
    logging.info("work link_tier: %s", _q(con, "SELECT link_tier, count(*) FROM work GROUP BY ALL ORDER BY ALL"))
    logging.info(
        "projects: with a work %s, minority %s, is_ch %s, translated %s, theme %s, pillars %s",
        *[_count(con, s) for s in (
            "SELECT count(DISTINCT source) FROM relation WHERE sourceType = 'project' AND targetType = 'product'",
            "SELECT count(*) FROM project WHERE len(minority_qid) > 0", "SELECT count(*) FROM project WHERE is_ch",
            "SELECT count(*) FROM project WHERE is_translated", "SELECT count(*) FROM project WHERE theme IS NOT NULL",
            "SELECT count(*) FROM project WHERE pillars > 0")],
    )
    logging.info("distinct minorities used: %s", _count(con, "SELECT count(DISTINCT q) FROM (SELECT unnest(minority_qid) AS q FROM project)"))
    logging.info("works with a project: %s", _count(con, "SELECT count(DISTINCT target) FROM relation WHERE targetType = 'product'"))
    logging.info("organizations: with geolocation %s, linked to a project %s, linked to a work %s",
                 _count(con, "SELECT count(*) FROM organization WHERE geolocation IS NOT NULL"),
                 _count(con, "SELECT count(DISTINCT target) FROM relation WHERE sourceType = 'project' AND targetType = 'organization'"),
                 _count(con, "SELECT count(DISTINCT target) FROM relation WHERE sourceType = 'product' AND targetType = 'organization'"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--src", default=DEFAULT_SRC)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--n", type=int, default=1000, help="rows per project / organization / work")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--mem-mb", type=int, default=120_000)
    parser.add_argument("--threads", type=int, default=16)
    args = parser.parse_args()
    setup_logging("core_v4", "export_min")
    export(args.src, args.out, args.n, args.seed, args.mem_mb, args.threads)


if __name__ == "__main__":
    main()
