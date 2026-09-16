"""
OpenAIRE Dump Ingestion - Load into DuckDB

Loads 4 entities in order:
  1. organization  (~448K rows)
  2. project       (~3.7M rows)
  3. work          (~165M rows, from publication/ directory only; loaded as 'work')
  4. relation      (~2B raw -> filtered by relType + work ID semi-join)

Target: single openaire.duckdb file.
"""

import logging
from datetime import datetime
from pathlib import Path

from common.database.duck.create_connection import create_duck_connection
from common.file_handling.file_utils import ensure_path_exists
from common.config.paths import get_source_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time

setup_logging("loader", "openaire_dump")

config = get_source_paths()["openaire_dump"]
SOURCE = Path(config["path_raw"])
DB = Path(config["path_duck"])

ensure_path_exists(DB)

logging.info("OPENAIRE DUMP INGESTION")
logging.info(f"Source: {SOURCE}")
logging.info(f"Target: {DB}")

con = create_duck_connection(str(DB))

# Override defaults from create_duck_connection — this job needs more
con.execute("SET memory_limit='160GB'")
con.execute("SET threads=32")

total_start = datetime.now()


# -------------------------------------------------------------------------
# 1. ORGANIZATION
# -------------------------------------------------------------------------
logging.info("--- Loading organization ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE organization AS
    SELECT * FROM read_json('{SOURCE}/organization/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM organization").fetchone()[0]
logging.info(f"organization rows: {count:,}")


# -------------------------------------------------------------------------
# 2. PROJECT
# -------------------------------------------------------------------------
logging.info("--- Loading project ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE project AS
    SELECT * FROM read_json('{SOURCE}/project/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM project").fetchone()[0]
logging.info(f"project rows: {count:,}")


# -------------------------------------------------------------------------
# 3. WORK  (loaded from publication/ directory only)
# -------------------------------------------------------------------------
logging.info("--- Loading work ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE work AS
    SELECT * FROM read_json('{SOURCE}/publication/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true)
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM work").fetchone()[0]
logging.info(f"work rows: {count:,}")

# publicationDate has bad values (e.g. 0001-12-30) — loaded as-is, cleaned in sanitization step.


# -------------------------------------------------------------------------
# 4. RELATION  (filtered — must run after work is loaded)
# -------------------------------------------------------------------------
# Filter logic:
#   a) relType.type must be one of: affiliation, outcome, participation
#      (excludes citation/product↔product, provision, and similarity)
#   b) Where either side is a 'product', that product must exist in work.
#      The work table is loaded from publication/ only, so the semi-join
#      implicitly restricts products to publications.
#   c) participation (organization <-> project) has no product side — passes through.
#
logging.info("--- Loading relation (filtered) ---")
t = datetime.now()

con.execute(f"""
    CREATE TABLE relation AS
    SELECT r.*
    FROM read_json('{SOURCE}/relation/*.json.gz',
        format='newline_delimited', compression='gzip', union_by_name=true) r
    WHERE r.relType.type IN ('affiliation', 'outcome', 'participation')
      AND (
          (r.sourceType = 'product' AND r.source IN (SELECT id FROM work))
          OR (r.targetType = 'product' AND r.target IN (SELECT id FROM work))
          OR (r.sourceType != 'product' AND r.targetType != 'product')
      )
""")

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM relation").fetchone()[0]
logging.info(f"relation rows (filtered): {count:,}")

type_dist = con.execute("""
    SELECT relType.type, COUNT(*) as cnt
    FROM relation
    GROUP BY relType.type ORDER BY cnt DESC
""").fetchall()
logging.info("relation type distribution:")
for row in type_dist:
    logging.info(f"  {row[0]}: {row[1]:,}")


# -------------------------------------------------------------------------
# VERIFY
# -------------------------------------------------------------------------
logging.info("--- Final verification ---")

for table in ["organization", "project", "work", "relation"]:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logging.info(f"  {table}: {count:,} rows")

schema_work = con.execute("DESCRIBE work").df()
logging.info(f"work schema:\n{schema_work.to_string(index=False)}")

sample_work = con.execute("""
    SELECT id, mainTitle, publicationDate, publisher, language.code
    FROM work LIMIT 3
""").df()
logging.info(f"work sample:\n{sample_work.to_string(index=False)}")

sample_rel = con.execute("""
    SELECT source, sourceType, target, targetType, relType.type, relType.name
    FROM relation LIMIT 5
""").df()
logging.info(f"relation sample:\n{sample_rel.to_string(index=False)}")

db_size_mb = DB.stat().st_size / (1024 ** 2)
logging.info(f"DuckDB file size: {db_size_mb:,.1f} MB")

log_run_time(total_start)
logging.info(f"Database location: {DB}")

con.close()
