"""
Core v3 Transformation — merges ROR and Cordis into core_v3_topics.duckdb

Adds columns to existing tables (no new tables created):
  - organization: rorStatus, rorEstablished, rorTypes, rorLocations, geolocation, rorRelationships
  - relation:     cordis_ec_contribution, cordis_type

See TRANSFORMATION.md for full design rationale and EDA results.

Usage:
    cd /home/lu72hip/DIGICHer/dh_pipeline
    python -m src.elt.core_v3.transformation

Prerequisites:
  - Close all notebooks/kernels that have the duckdb files open (DuckDB is single-writer).
"""

import logging
import sys
sys.path.insert(0, '/home/lu72hip/DIGICHer/dh_pipeline/src')

from datetime import datetime
from pathlib import Path

import duckdb

from utils.config.config_loader import get_query_config
from utils.logger.logger import setup_logging
from utils.logger.timer import log_run_time

setup_logging("transformation", "core_v3")

config    = get_query_config()
CORE_DB   = Path(config["core_v3"]["path_topics_duck"])
ROR_DB    = Path(config["ror_dump"]["path_duck"])
CORDIS_DB = Path(config["cordis"]["queries"][1]["path_duck"])

logging.info("CORE V3 TRANSFORMATION")
logging.info(f"Target: {CORE_DB}")
logging.info(f"ROR:    {ROR_DB}")
logging.info(f"Cordis: {CORDIS_DB}")

con = duckdb.connect(str(CORE_DB))
con.execute("SET memory_limit='160GB'")
con.execute("SET threads=32")

try:
    con.execute(f"ATTACH '{ROR_DB}'    AS ror    (READ_ONLY)")
    con.execute(f"ATTACH '{CORDIS_DB}' AS cordis (READ_ONLY)")
except Exception as e:
    logging.error(f"Failed to attach source database: {e}")
    logging.error("Close all notebooks/kernels that have these files open and retry.")
    sys.exit(1)

total_start = datetime.now()


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
    CREATE TEMP TABLE _cordis_triplets AS
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
logging.info(f"Database: {CORE_DB}")

con.close()
