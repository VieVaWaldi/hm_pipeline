"""REAL-DATA SMOKE TEST ONLY (laptop): turn the cluster dry-run Parquet (agent_job/out/) into the final export shape.

The dry-run files were made BEFORE decisions D13-D33 (no funder/programme, currency rules, name_key, coordinator_ids, minority_qids on works ...),
so the raw tables (project, organization, relation, relation_topic, topic, minority) are rebuilt from them into a scratch DuckDB and the REAL
final export SQL (sql/*.sql, via export.py) is run on that. Only works is derived by a separate SQL here, because the dry-run works file has no raw
`instances`/`pids` any more (pdf_url/landing_url/doi already extracted on the cluster with the same macros).

Deviations vs a true cluster export (so numbers are not misread):
  * organisations.work_count / works of an org count TIER-0 works only (only tier-0 works are in the dry-run file)
  * no tier-1 works, no publishers.json
    .venv-serving/bin/python real_build.py --dry-run agent_job/out --mini <mini.duckdb> --out data/serving_real
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path

import duckdb

HERE = Path(__file__).parent


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", default=str(HERE.parent / "agent_job" / "out"))
    ap.add_argument("--mini", default="data/duckdb/core/core_v4_noworkenrichment-min.duckdb", help="source of the topic + minority tables")
    ap.add_argument("--out", default="data/serving_real")
    ap.add_argument("--works-files", type=int, default=2)
    args = ap.parse_args()
    d, out = Path(args.dry_run), Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    dbp = out / "real_core.duckdb"
    if dbp.exists():
        dbp.unlink()
    con = duckdb.connect(str(dbp))
    t = time.time()
    P, W, O = f"'{d / 'projects_full.parquet'}'", f"'{d / 'works_tier0.parquet'}'", f"'{d / 'organizations.parquet'}'"
    con.execute(f"ATTACH '{args.mini}' AS mini (READ_ONLY)")
    con.execute("CREATE TABLE topic AS SELECT * FROM mini.topic")
    con.execute("CREATE TABLE minority AS SELECT * FROM mini.minority")
    con.execute(f"CREATE TABLE project AS SELECT * REPLACE (id::UBIGINT AS id) FROM {P}")
    con.execute(f"CREATE TABLE organization AS SELECT * REPLACE (id::UBIGINT AS id) FROM {O}")
    con.execute(f"""CREATE TABLE relation AS
        SELECT id::UBIGINT AS source, 'project' AS sourceType, oid::UBIGINT AS target, 'organization' AS targetType,
               CASE WHEN oid = coordinator_id THEN 'coordinator' END AS cordis_type
        FROM (SELECT id, unnest(org_ids) AS oid, coordinator_id FROM {P})
        UNION ALL
        SELECT pid::UBIGINT, 'project', wid::UBIGINT, 'product', NULL FROM (SELECT id AS wid, unnest(project_ids) AS pid FROM {W})
        UNION ALL
        SELECT wid::UBIGINT, 'product', oid::UBIGINT, 'organization', NULL FROM (SELECT id AS wid, unnest(organisation_ids) AS oid FROM {W})""")
    con.execute(f"""CREATE TABLE relation_topic AS
        SELECT 'project' AS type, id::UBIGINT AS source_id, topic_id, 1.0::FLOAT AS score FROM {P} WHERE topic_id IS NOT NULL""")
    print(f"scratch db built in {time.time() - t:.0f}s: {dbp} ({dbp.stat().st_size / 1e9:.2f} GB)", flush=True)
    con.close()

    # 1) real final export SQL on the rebuilt raw tables (everything except works/publishers)
    subprocess.run([sys.executable, str(HERE / "export.py"), "--db", str(dbp), "--out", str(out), "--only", "organisations,projects,minorities,grants,topics"], check=True)

    # 2) works: separate derive (see docstring)
    con = duckdb.connect()
    con.execute((HERE / "sql" / "00_macros.sql").read_text())
    (out / "works").mkdir(exist_ok=True)
    n = args.works_files
    for k in range(n):
        target = out / "works" / f"works_{k:02d}.parquet"
        t = time.time()
        con.execute(f"""COPY (
          WITH w AS (SELECT * FROM {W} WHERE hash(id) % {n} = {k}),
               mq AS (SELECT x.wid, list_sort(list_distinct(flatten(list(coalesce(p.minority_qid, []))))) AS minority_qids
                      FROM (SELECT id AS wid, unnest(project_ids) AS pid FROM w) x JOIN {P} p ON p.id = x.pid GROUP BY x.wid)
          SELECT w.id, replace(w.title, '&amp;', '&') AS title, w.authors, w.author_count::INTEGER AS author_count, w.publication_date,
                 hm_year(w.publication_date) AS year, w.publisher, w.container_name, w.open_access_color, w.best_access_right,
                 hm_lang(w.language) AS language, w.citation_count::INTEGER AS citation_count, w.doi, w.pdf_url, w.landing_url,
                 w.project_ids, w.organisation_ids[1:100] AS organisation_ids, len(w.organisation_ids)::INTEGER AS org_count,
                 coalesce(w.is_ch_via_project, false) AS is_ch_via_project, coalesce(mq.minority_qids, []) AS minority_qids, w.link_tier
          FROM w LEFT JOIN mq ON mq.wid = w.id ORDER BY w.id
        ) TO '{target}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 100000)""")
        print(f"works/{target.name} {target.stat().st_size / 1e6:.0f} MB {time.time() - t:.0f}s", flush=True)


if __name__ == "__main__":
    main()
