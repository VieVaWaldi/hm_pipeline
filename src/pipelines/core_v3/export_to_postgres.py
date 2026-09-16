"""
Export core_v3_final.duckdb → PostgreSQL database 'core_v3'.

Tables:
  work           – 10 000 000 most recent rows (publicationDate DESC NULLS LAST)
  relation       – all rows where neither source nor target is an excluded work
  organization   – full
  project        – full
  topic          – full
  relation_topic – full

STRUCT columns are cast to JSON before insert for postgres compatibility.

Usage:
    cd /home/lu72hip/DIGICHer/dh_pipeline
    python -m src.elt.core_v3.export_to_postgres [--pg-conn "dbname=core_v3"]

Prerequisites:
  - PostgreSQL running and 'core_v3' database already created
    (run_export_to_postgres.sh handles both)
  - DuckDB postgres extension available

Note on relation filtering:
  In OpenAire, works carry sourceType/targetType = 'product'.
  We keep relation rows where:
    (sourceType != 'product'  OR  source IN included_works)
    AND
    (targetType != 'product'  OR  target IN included_works)
  OR conditions here are in a SELECT, not an UPDATE, so DuckDB uses
  hash semi-joins and the plan is efficient.
"""

import argparse
import logging
import sys
sys.path.insert(0, '/home/lu72hip/DIGICHer/dh_pipeline/src')

from datetime import datetime
from pathlib import Path

import duckdb

from utils.config.config_loader import get_query_config
from utils.logger.logger import setup_logging
from utils.logger.timer import log_run_time

setup_logging("export_to_postgres", "core_v3")

WORK_LIMIT   = 10_000_000
WORK_TYPE    = "product"          # OpenAire sourceType/targetType value for works
TABLES_FULL  = ["organization", "project", "topic", "relation_topic"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def needs_json_cast(col_type: str) -> bool:
    """True for DuckDB types with no native postgres equivalent (STRUCT, MAP)."""
    upper = col_type.upper()
    return "STRUCT" in upper or upper.startswith("MAP(")


def build_select(con: duckdb.DuckDBPyConnection, table: str, suffix: str = "") -> str:
    """
    Return 'SELECT ... FROM <table> <suffix>' with STRUCT columns cast to JSON.
    suffix may be a WHERE clause, JOIN clause, etc.
    """
    desc = con.execute(f'DESCRIBE "{table}"').fetchall()
    parts = []
    for col_name, col_type, *_ in desc:
        if needs_json_cast(col_type):
            parts.append(f'"{col_name}"::JSON AS "{col_name}"')
        else:
            parts.append(f'"{col_name}"')
    cols = ", ".join(parts)
    q = f'SELECT {cols} FROM "{table}"'
    if suffix:
        q += f" {suffix}"
    return q


def copy_table(con: duckdb.DuckDBPyConnection, table: str, select_sql: str) -> None:
    t = datetime.now()
    logging.info(f"  DROP TABLE IF EXISTS pg.\"{table}\"")
    con.execute(f'DROP TABLE IF EXISTS pg."{table}"')
    logging.info(f"  CREATE TABLE pg.\"{table}\" AS SELECT ...")
    con.execute(f'CREATE TABLE pg."{table}" AS {select_sql}')
    n = con.execute(f'SELECT COUNT(*) FROM pg."{table}"').fetchone()[0]
    log_run_time(t)
    logging.info(f"  {table}: {n:,} rows copied")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Export core_v3_final.duckdb to PostgreSQL")
    parser.add_argument(
        "--pg-conn", default="dbname=core_v3 port=5433 host=localhost",
        help="DuckDB postgres ATTACH connection string",
    )
    args = parser.parse_args()

    config    = get_query_config()
    duck_path = Path(config["core_v3"]["path_final_duck"])

    logging.info("=== CORE V3 → POSTGRES EXPORT ===")
    logging.info(f"DuckDB  : {duck_path}")
    logging.info(f"Postgres: {args.pg_conn}")
    logging.info(f"Work limit: {WORK_LIMIT:,}")

    total_start = datetime.now()

    con = duckdb.connect(str(duck_path), read_only=False)
    con.execute("SET memory_limit='160GB'")
    con.execute("SET threads=32")

    # ── Postgres extension ────────────────────────────────────────────────────
    # DuckDB 1.2.2 uses the extension name 'postgres_scanner'.
    # The .duckdb_extension file must be pre-installed (no network needed at runtime):
    #   mkdir -p ~/.duckdb/extensions/v1.2.2/linux_amd64_gcc4
    #   curl -o ~/.duckdb/extensions/v1.2.2/linux_amd64_gcc4/postgres_scanner.duckdb_extension.gz \
    #        http://extensions.duckdb.org/v1.2.2/linux_amd64_gcc4/postgres_scanner.duckdb_extension.gz
    #   gunzip ~/.duckdb/extensions/v1.2.2/linux_amd64_gcc4/postgres_scanner.duckdb_extension.gz
    logging.info("Loading postgres_scanner extension...")
    con.execute("LOAD postgres_scanner")
    con.execute(f"ATTACH '{args.pg_conn}' AS pg (TYPE postgres_scanner)")

    # ── 1. Build included-work set (10M most recent) ──────────────────────────
    logging.info("--- Building included-work set ---")
    t = datetime.now()
    con.execute(f"""
        CREATE TEMP TABLE _included_works AS
        SELECT id
        FROM work
        ORDER BY publicationDate DESC NULLS LAST
        LIMIT {WORK_LIMIT}
    """)
    n_inc = con.execute("SELECT COUNT(*) FROM _included_works").fetchone()[0]
    log_run_time(t)
    logging.info(f"Included works: {n_inc:,}")

    # Verify sourceType vocabulary assumption
    work_types = con.execute(
        "SELECT DISTINCT sourceType FROM relation WHERE sourceType IS NOT NULL LIMIT 20"
    ).fetchall()
    logging.info(f"Distinct sourceType values (sample): {[r[0] for r in work_types]}")

    # ── 2. work — 10M most recent ─────────────────────────────────────────────
    logging.info("--- Copying work ---")
    copy_table(
        con, "work",
        build_select(con, "work", "WHERE id IN (SELECT id FROM _included_works)"),
    )

    # ── 3. relation — exclude rows mentioning excluded works ──────────────────
    logging.info("--- Copying relation ---")
    rel_where = (
        f"WHERE (sourceType != '{WORK_TYPE}' OR source IN (SELECT id FROM _included_works))"
        f"  AND (targetType != '{WORK_TYPE}' OR target IN (SELECT id FROM _included_works))"
    )
    copy_table(
        con, "relation",
        build_select(con, "relation", rel_where),
    )

    # ── 4. Full tables ────────────────────────────────────────────────────────
    for table in TABLES_FULL:
        logging.info(f"--- Copying {table} ---")
        copy_table(con, table, build_select(con, table))

    # ── Done ──────────────────────────────────────────────────────────────────
    log_run_time(total_start)
    logging.info("=== Export complete ===")
    con.close()


if __name__ == "__main__":
    main()
