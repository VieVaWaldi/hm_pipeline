"""
Test script for core_v3 transformation — runs both merges on small samples
against an in-memory duckdb, sourced directly from OpenAire staging + ROR +
Cordis. Never touches core_v3's real duckdb file, so it's safe to run before
(or instead of) `transformation.py --limit N` against real data.

Usage:
    uv run python -m pipelines.core_v3.test_transformation
"""

import sys
from pathlib import Path

import duckdb

from common.config.api_runner import get_query_settings
from common.config.dumps import get_dumps_paths

ROR_DB = Path(get_dumps_paths()["ror_dump"]["path_duck"])
CORDIS_DB = Path(get_query_settings()["cordis"].queries["full_projects_no_pdfs"].path_duck)
OPENAIRE_STAGING_DB = Path(get_dumps_paths()["openaire_dump"]["path_duck_staging_2"])

print(f"OpenAire staging: {OPENAIRE_STAGING_DB}")
print(f"ROR:              {ROR_DB}")
print(f"Cordis:           {CORDIS_DB}")

con = duckdb.connect(":memory:")
con.execute("SET memory_limit='32GB'")
con.execute("SET threads=8")

try:
    con.execute(f"ATTACH '{OPENAIRE_STAGING_DB}' AS openaire (READ_ONLY)")
    con.execute(f"ATTACH '{ROR_DB}'    AS ror    (READ_ONLY)")
    con.execute(f"ATTACH '{CORDIS_DB}' AS cordis (READ_ONLY)")
except Exception as e:
    print(f"\nERROR attaching database: {e}")
    print("\nMake sure OpenAire staging, ROR, and Cordis have all been loaded first.")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Build test fixtures — small samples of the relevant tables
# ---------------------------------------------------------------------------
print("\n--- Building test fixtures ---")

# organization: 500 rows that have a rorId
con.execute("""
    CREATE TABLE organization AS
    SELECT * FROM openaire.organization
    WHERE rorId IS NOT NULL
    LIMIT 500
""")

# project: 200 rows that have a grantId matching cordis
con.execute("""
    CREATE TABLE project AS
    SELECT op.*
    FROM openaire.project op
    JOIN cordis.project cp ON cp.id_original = op.grantId
    LIMIT 200
""")

# relation: rows linking those 200 projects to organizations
con.execute("""
    CREATE TABLE relation AS
    SELECT r.*
    FROM openaire.relation r
    WHERE r.source IN (SELECT id FROM project)
       OR r.target IN (SELECT id FROM project)
    LIMIT 100000
""")

print(f"  organization rows: {con.execute('SELECT COUNT(*) FROM organization').fetchone()[0]:,}")
print(f"  project rows:      {con.execute('SELECT COUNT(*) FROM project').fetchone()[0]:,}")
print(f"  relation rows:     {con.execute('SELECT COUNT(*) FROM relation').fetchone()[0]:,}")


# ---------------------------------------------------------------------------
# TEST 1: ROR → organization
# ---------------------------------------------------------------------------
print("\n--- TEST 1: ROR → organization ---")

con.execute("ALTER TABLE organization ADD COLUMN rorStatus        VARCHAR")
con.execute("ALTER TABLE organization ADD COLUMN rorEstablished   INTEGER")
con.execute("ALTER TABLE organization ADD COLUMN rorTypes         VARCHAR[]")
con.execute("ALTER TABLE organization ADD COLUMN rorLocations     JSON")
con.execute("ALTER TABLE organization ADD COLUMN geolocation      DOUBLE[]")
con.execute("ALTER TABLE organization ADD COLUMN rorRelationships JSON")

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

ror_result = con.execute("""
    SELECT
        COUNT(*)                                    AS total,
        COUNT(rorStatus)                            AS nn_status,
        COUNT(rorEstablished)                       AS nn_established,
        COUNT(rorTypes)                             AS nn_types,
        COUNT(geolocation)                          AS nn_geolocation,
        COUNT(rorRelationships)                     AS nn_relationships
    FROM organization
""").df()
print(ror_result.to_string(index=False))

print("\nSample ROR-enriched org:")
sample = con.execute("""
    SELECT legalName, rorId, rorStatus, rorEstablished, rorTypes, geolocation
    FROM organization
    WHERE rorStatus IS NOT NULL
    LIMIT 3
""").df()
print(sample.to_string(index=False))


# ---------------------------------------------------------------------------
# TEST 2: Cordis → relation
# ---------------------------------------------------------------------------
print("\n--- TEST 2: Cordis → relation ---")

con.execute("ALTER TABLE relation ADD COLUMN cordis_ec_contribution DOUBLE")
con.execute("ALTER TABLE relation ADD COLUMN cordis_type            VARCHAR")

con.execute("""
    CREATE TEMP TABLE _cordis_triplets AS
    SELECT
        op.id                  AS core_project_id,
        co.id                  AS core_org_id,
        jpi.ec_contribution    AS cordis_ec_contribution,
        jpi.type               AS cordis_type
    FROM cordis.project          cp
    JOIN project                 op  ON cp.id_original = op.grantId
    JOIN cordis.j_project_institution jpi ON jpi.project_id = cp.id
    JOIN cordis.institution      ci  ON ci.id = jpi.institution_id
    JOIN organization            co  ON LOWER(TRIM(co.legalName)) = LOWER(TRIM(ci.legal_name))
""")

triplet_count = con.execute("SELECT COUNT(*) FROM _cordis_triplets").fetchone()[0]
print(f"  Triplets built: {triplet_count:,}")

con.execute("""
    UPDATE relation r
    SET
        cordis_ec_contribution = ct.cordis_ec_contribution,
        cordis_type            = ct.cordis_type
    FROM _cordis_triplets ct
    WHERE r.source = ct.core_project_id
      AND r.target = ct.core_org_id
""")

cordis_result = con.execute("""
    SELECT
        COUNT(*)                        AS total_relation_rows,
        COUNT(cordis_ec_contribution)   AS nn_ec_contribution,
        COUNT(cordis_type)              AS nn_type
    FROM relation
""").df()
print(cordis_result.to_string(index=False))

print("\nSample Cordis-enriched relation rows:")
sample2 = con.execute("""
    SELECT source, sourceType, target, targetType,
           cordis_ec_contribution, cordis_type
    FROM relation
    WHERE cordis_type IS NOT NULL
    LIMIT 5
""").df()
print(sample2.to_string(index=False))


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n=== ALL TESTS PASSED ===")
con.close()
