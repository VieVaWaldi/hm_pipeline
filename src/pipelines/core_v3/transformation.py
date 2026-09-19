"""
Core v3 Transformation — seeds core_v3's staging duckdb from OpenAire staging,
then merges in ROR + Cordis columns.

core_v3 targets running end-to-end through this merge stage on real data — it does
NOT include serving (the old export-to-postgres step is gone; core_v4 gets a real
duckdb -> OpenSearch serve stage). Not wired into Snakemake orchestration — run
directly via `python -m`.

Seed (fresh copy, no new columns):
  - organization, project, work, relation <- OpenAire staging (see
    src/sources/dumps/openaire/staging.py)

Merge (adds columns to the seeded tables, no new tables created):
  - organization: rorStatus, rorEstablished, rorTypes, rorLocations, geolocation, rorRelationships
  - relation:     cordis_ec_contribution, cordis_type

See READ_TRANSFORMATION.md for full design rationale and EDA results.

Usage:
    uv run python -m pipelines.core_v3.transformation                # full run
    uv run python -m pipelines.core_v3.transformation --limit 500    # smoke test:
        500 rows per OpenAire entity, ROR/Cordis still joined in full (per the
        "Limit Runs" convention in the root README) — relations end up sparse
        since sampling doesn't respect foreign keys, but every table gets
        touched so you can verify the full run shape quickly.

Prerequisites:
  - OpenAire staging must already exist (src/sources/dumps/openaire/staging.py) —
    its organization/project/work/relation tables are the seed for CORE_DB.
  - Close all notebooks/kernels that have the duckdb files open (DuckDB is single-writer).
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import duckdb

from common.config.api_runner import get_query_settings
from common.config.dumps import get_dumps_paths
from common.config.pipelines import get_pipeline_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Core v3 transformation: seed from OpenAire staging, then merge in ROR + Cordis."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Smoke-test mode: only copy N rows per OpenAire entity "
        "(organization/project/work/relation) into core_v3. ROR and Cordis are "
        "always joined in full — they're attached read-only, never copied.",
    )
    args = parser.parse_args()

    setup_logging("transformation", "core_v3")

    core_db = Path(get_pipeline_paths()["core_v3"]["path_staging_duck"])
    ror_db = Path(get_dumps_paths()["ror_dump"]["path_duck"])
    cordis_db = Path(get_query_settings()["cordis"].queries["full_projects_no_pdfs"].path_duck)
    openaire_staging_db = Path(get_dumps_paths()["openaire_dump"]["path_duck_staging_2"])

    logging.info("CORE V3 TRANSFORMATION")
    logging.info(f"Target:           {core_db}")
    logging.info(f"OpenAire staging: {openaire_staging_db}")
    logging.info(f"ROR:              {ror_db}")
    logging.info(f"Cordis:           {cordis_db}")
    if args.limit:
        logging.info(f"LIMIT mode: {args.limit} rows per OpenAire entity")

    core_db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(core_db))
    con.execute("SET memory_limit='160GB'")
    con.execute("SET threads=32")

    try:
        con.execute(f"ATTACH '{openaire_staging_db}' AS openaire (READ_ONLY)")
        con.execute(f"ATTACH '{ror_db}'    AS ror    (READ_ONLY)")
        con.execute(f"ATTACH '{cordis_db}' AS cordis (READ_ONLY)")
    except Exception as e:
        logging.error(f"Failed to attach source database: {e}")
        logging.error("Close all notebooks/kernels that have these files open and retry.")
        sys.exit(1)

    total_start = datetime.now()

    # ---------------------------------------------------------------------------
    # 0. Seed organization/project/work/relation from OpenAire staging
    # ---------------------------------------------------------------------------
    logging.info("--- Seeding core tables from OpenAire staging ---")
    t = datetime.now()

    limit_clause = f" LIMIT {args.limit}" if args.limit else ""
    for table in ("organization", "project", "work", "relation"):
        con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM openaire.{table}{limit_clause}")
        count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logging.info(f"  {table}: {count:,} rows")

    log_run_time(t)

    # ---------------------------------------------------------------------------
    # 1. ROR → organization
    # ---------------------------------------------------------------------------
    logging.info("--- Merging ROR into organization ---")
    t = datetime.now()

    con.execute("ALTER TABLE organization ADD COLUMN IF NOT EXISTS rorStatus        VARCHAR")
    con.execute("ALTER TABLE organization ADD COLUMN IF NOT EXISTS rorEstablished   INTEGER")
    con.execute("ALTER TABLE organization ADD COLUMN IF NOT EXISTS rorTypes         VARCHAR[]")
    con.execute("ALTER TABLE organization ADD COLUMN IF NOT EXISTS rorLocations     JSON")
    con.execute("ALTER TABLE organization ADD COLUMN IF NOT EXISTS geolocation      DOUBLE[]")
    con.execute("ALTER TABLE organization ADD COLUMN IF NOT EXISTS rorRelationships JSON")

    con.execute("""
        UPDATE organization o
        SET
            rorStatus        = r.status,
            rorEstablished   = r.established,
            rorTypes         = r.types,
            rorLocations     = r.locations::JSON,
            geolocation      = [r.locations[1].geonames_details.lat,
                                r.locations[1].geonames_details.lng],
            rorRelationships = r.relationships::JSON
        FROM ror.organizations r
        WHERE o.rorId = r.id
    """)

    enriched = con.execute("SELECT COUNT(*) FROM organization WHERE rorStatus IS NOT NULL").fetchone()[0]
    log_run_time(t)
    logging.info(f"organization rows enriched with ROR: {enriched:,}")

    # ---------------------------------------------------------------------------
    # 2. Cordis → relation
    # ---------------------------------------------------------------------------
    logging.info("--- Merging Cordis into relation ---")
    t = datetime.now()

    con.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS cordis_ec_contribution DOUBLE")
    con.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS cordis_type            VARCHAR")

    logging.info("Building cordis triplet lookup table...")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE _cordis_triplets AS
        SELECT
            op.id                  AS core_project_id,
            co.id                  AS core_org_id,
            jpi.ec_contribution    AS cordis_ec_contribution,
            jpi.type               AS cordis_type
        FROM cordis.project               cp
        JOIN project                      op  ON cp.id_original = op.grantId
        JOIN cordis.j_project_institution jpi ON jpi.project_id = cp.id
        JOIN cordis.institution           ci  ON ci.id = jpi.institution_id
        JOIN organization                 co  ON LOWER(TRIM(co.legalName)) = LOWER(TRIM(ci.legal_name))
    """)

    triplet_count = con.execute("SELECT COUNT(*) FROM _cordis_triplets").fetchone()[0]
    logging.info(f"Triplets matched: {triplet_count:,}")

    logging.info("Updating relation rows (project → organization direction only)...")
    con.execute("""
        UPDATE relation r
        SET
            cordis_ec_contribution = ct.cordis_ec_contribution,
            cordis_type            = ct.cordis_type
        FROM _cordis_triplets ct
        WHERE r.source = ct.core_project_id
          AND r.target = ct.core_org_id
    """)

    enriched_rel = con.execute(
        "SELECT COUNT(*) FROM relation WHERE cordis_type IS NOT NULL"
    ).fetchone()[0]
    log_run_time(t)
    logging.info(f"relation rows enriched with Cordis: {enriched_rel:,}")

    # ---------------------------------------------------------------------------
    # Verify
    # ---------------------------------------------------------------------------
    logging.info("--- Final verification ---")

    org_stats = con.execute("""
        SELECT
            COUNT(*)                    AS total_orgs,
            COUNT(rorStatus)            AS nn_ror_status,
            COUNT(geolocation)          AS nn_geolocation,
            COUNT(rorRelationships)     AS nn_relationships
        FROM organization
    """).fetchone()
    logging.info(
        f"organization — total: {org_stats[0]:,} | rorStatus: {org_stats[1]:,} "
        f"| geolocation: {org_stats[2]:,} | rorRelationships: {org_stats[3]:,}"
    )

    rel_stats = con.execute("""
        SELECT
            COUNT(*)                        AS total_relations,
            COUNT(cordis_ec_contribution)   AS nn_ec_contribution,
            COUNT(cordis_type)              AS nn_type
        FROM relation
    """).fetchone()
    logging.info(
        f"relation — total: {rel_stats[0]:,} | cordis_ec_contribution: {rel_stats[1]:,} "
        f"| cordis_type: {rel_stats[2]:,}"
    )

    log_run_time(total_start)
    logging.info(f"Database: {core_db}")

    con.close()


if __name__ == "__main__":
    main()
