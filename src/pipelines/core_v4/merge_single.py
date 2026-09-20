"""
Merges the two finished core_v4 gold files into ONE duckdb (works without work enrichment):

    core_v4_works_base.duckdb   work, relation, relation_topic (empty)
  + core_v4_projects.duckdb     project, organization, relation (project -> organization), topic, relation_topic
  + minorities_raw.duckdb       minorities_raw (278 groups)                              -> table `minority`
  = core_v4_noworkenrichment.duckdb   work, project, organization, topic, minority, relation, relation_topic

The works file is copied as a file (fast, keeps its 50M-row table as is), the project side is appended with the
DDL of the source tables (types and constraints stay identical). The inputs are never modified. The result is
written to `<out>.tmp`, verified there, and renamed at the end, so a failure never leaves a half-built file.

Usage (sbatch, it copies ~130 GB):
    uv run python -m pipelines.core_v4.merge_single [--variant limit]
"""

import argparse
import logging
import os
import shutil
import time
from pathlib import Path

import duckdb

from common.config.pipelines import get_pipeline_paths
from common.log.logger import setup_logging

PROJECT_TABLES = ["project", "organization", "topic"]
DEFAULT_MINORITIES_DB = "/work/lu72hip/data/duckdb/sources/minorities_raw.duckdb"


def _ddl(con: duckdb.DuckDBPyConnection, catalog: str, table: str) -> str:
    return con.execute("SELECT sql FROM duckdb_tables() WHERE database_name = ? AND table_name = ?", [catalog, table]).fetchone()[0]


def _count(con, sql: str) -> int:
    return con.execute(sql).fetchone()[0]


def merge(works_base: str, projects: str, minorities: str, out: str, mem_mb: int, threads: int) -> None:
    out_path, tmp = Path(out), Path(out + ".tmp")
    if out_path.exists():
        raise SystemExit(f"{out} exists already: refusing to overwrite it (move it away first)")
    for p in (works_base, projects, minorities):
        if not Path(p).is_file():
            raise SystemExit(f"missing input {p}")
        if Path(p + ".wal").exists():
            raise SystemExit(f"{p}.wal exists: a writer is or was active on the input, not merging")
    tmp.unlink(missing_ok=True)

    t0 = time.perf_counter()
    logging.info(f"copying {works_base} -> {tmp}")
    shutil.copyfile(works_base, tmp)
    logging.info(f"copy done ({time.perf_counter() - t0:.0f}s, {tmp.stat().st_size / 1e9:.1f} GB)")

    con = duckdb.connect(str(tmp))
    con.execute(f"SET memory_limit = '{mem_mb}MB'")
    con.execute(f"SET threads = {threads}")
    con.execute(f"SET temp_directory = '{tmp}.spill'")
    con.execute(f"ATTACH '{projects}' AS proj (READ_ONLY)")
    con.execute(f"ATTACH '{minorities}' AS mino (READ_ONLY)")

    for table in PROJECT_TABLES:
        con.execute(_ddl(con, "proj", table))
        con.execute(f"INSERT INTO {table} SELECT * FROM proj.{table}")
        logging.info(f"{table}: {_count(con, f'SELECT count(*) FROM {table}'):,} rows")
    con.execute("INSERT INTO relation SELECT * FROM proj.relation")
    con.execute("INSERT INTO relation_topic SELECT * FROM proj.relation_topic")
    con.execute("CREATE TABLE minority AS SELECT * FROM mino.minorities_raw")
    logging.info(f"minority: {_count(con, 'SELECT count(*) FROM minority'):,} rows")

    verify(con)
    con.execute("DETACH proj")
    con.execute("DETACH mino")
    con.execute("CHECKPOINT")
    con.close()
    shutil.rmtree(f"{tmp}.spill", ignore_errors=True)
    os.replace(tmp, out_path)
    logging.info(f"wrote {out} ({out_path.stat().st_size / 1e9:.1f} GB, {time.perf_counter() - t0:.0f}s)")


def verify(con: duckdb.DuckDBPyConnection) -> None:
    """Counts and referential checks on the merged tables; raises on the first inconsistency."""
    problems = []

    def must_be_zero(name: str, sql: str) -> None:
        n = _count(con, sql)
        logging.info(f"check {name}: {n:,} (must be 0)")
        if n:
            problems.append(f"{name}: {n}")

    proj_rel = _count(con, "SELECT count(*) FROM proj.relation")
    works_rel = _count(con, "SELECT count(*) FROM relation") - proj_rel
    logging.info(f"relation: {proj_rel + works_rel:,} rows ({works_rel:,} from the works file, {proj_rel:,} project->organization)")
    must_be_zero("work with NULL link_tier", "SELECT count(*) FROM work WHERE link_tier IS NULL")
    must_be_zero("relation_topic rows for works", "SELECT count(*) FROM relation_topic WHERE type <> 'project'")
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
    must_be_zero("project.minority_qid not in minority table",
                 "SELECT count(*) FROM (SELECT DISTINCT unnest(minority_qid) AS q FROM project) p "
                 "WHERE p.q NOT IN (SELECT qid FROM minority) AND p.q NOT IN (SELECT unnest(merged_qids) FROM minority WHERE merged_qids IS NOT NULL)")
    for table in ("work", "project", "organization", "topic", "minority", "relation", "relation_topic"):
        logging.info(f"{table}: {_count(con, f'SELECT count(*) FROM {table}'):,} rows")
    if problems:
        raise RuntimeError("merged file failed verification: " + "; ".join(problems))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--variant", choices=["full", "limit"], default="full")
    parser.add_argument("--works-base", default=None)
    parser.add_argument("--projects", default=None)
    parser.add_argument("--minorities", default=DEFAULT_MINORITIES_DB)
    parser.add_argument("--out", default=None)
    parser.add_argument("--mem-mb", type=int, default=100_000)
    parser.add_argument("--threads", type=int, default=16)
    args = parser.parse_args()
    setup_logging("core_v4", "merge_single" + ("_limit" if args.variant == "limit" else ""))
    limit = args.variant == "limit"
    cfg = get_pipeline_paths()["core_v4_limit" if limit else "core_v4"]
    suffix = "_limit" if limit else ""  # the limit config block suffixes its keys
    core_dir = Path(cfg[f"path_duck_projects{suffix}"]).parent
    merge(
        works_base=args.works_base or cfg[f"path_duck_works_base{suffix}"],
        projects=args.projects or cfg[f"path_duck_projects{suffix}"],
        minorities=args.minorities,
        out=args.out or str(core_dir / ("core_v4_limit_noworkenrichment.duckdb" if limit else "core_v4_noworkenrichment.duckdb")),
        mem_mb=args.mem_mb,
        threads=args.threads,
    )


if __name__ == "__main__":
    main()
