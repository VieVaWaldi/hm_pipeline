"""
Minorities Staging 2 — Transform minorities_staging.duckdb -> minorities_staging_2.duckdb

Two independent passes over the staged table:

1. Titular-majority filter -- drops groups that are the majority/titular
   population of at least one of their own listed countries (Austrians ->
   Austria, Poles -> Poland, French -> France, ...). These read as "minorities"
   only because they also show up as a smaller community in some *other*
   listed country, but as a standalone filter option they're too broad to be
   useful for keyword search (searching "Poles" or "Germans" against project
   text matches almost everything). The classification lives in
   titular_majority_overrides.csv, keyed by qid, and was worked out by hand
   against the actual staged data rather than derived from a Wikidata
   property -- a handful of genuinely ambiguous cases (Bosniaks, Flemish
   people, Turkish people, Russians, Khalkhas, South Slavs) are recorded
   there too, with a note on why they were kept in despite looking similar.

2. has_parts rollup -- for a row whose `part_of` matches another surviving
   row's group_name_en (e.g. "Northern Sámi people" -> "Sámi people"), the
   child is removed as a separate top-level entry and folded into a new
   `known_subgroups` column on the parent -- a LIST(STRUCT(name, qid)), not a
   joined string -- the same way this project already applies "reduce more
   data" everywhere else. Run *after* the majority filter so that dropping a
   majority-population
   parent (e.g. "Poles") correctly un-orphans its real-minority children
   (Kashubians, Silesians) as their own top-level rows, rather than nesting
   them under a row we just removed. A child matching more than one surviving
   parent (e.g. "Mizrahi Jews" -> "Jewish people" and "Sephardi Jews") is
   deterministically assigned to the lexicographically smallest parent qid.
"""

import logging
from datetime import datetime
from pathlib import Path

from common.database.duck.create_connection import create_duck_connection
from common.file_handling.file_utils import ensure_path_exists
from common.config.dumps import get_dumps_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time

setup_logging("staging_2", "minorities")

config = get_dumps_paths()["minorities"]
STAGING_DB = Path(config["path_duck_staging"])
STAGING_2_DB = Path(config["path_duck_staging_2"])
OVERRIDES_CSV = Path(__file__).resolve().parent / "titular_majority_overrides.csv"

# Group names that coincidentally contain a European country's name but are
# NOT a reference to that country -- found while evaluating (and rejecting)
# a "drop if name contains a country" heuristic in favor of the titular-
# majority classification above. Not wired into any filter here; kept purely
# as a documented gotcha in case a name-based country check is ever added.
COINCIDENTAL_COUNTRY_NAME_MATCHES = {
    "Aromanians": "contains 'Romania' but is a distinct Vlach ethnonym, not a reference to modern Romania",
}

ensure_path_exists(STAGING_2_DB)

logging.info("MINORITIES STAGING 2")
logging.info(f"Source: {STAGING_DB}")
logging.info(f"Overrides: {OVERRIDES_CSV}")
logging.info(f"Target: {STAGING_2_DB}")

start_time = datetime.now()

con = create_duck_connection(str(STAGING_2_DB))
con.execute(f"ATTACH '{STAGING_DB}' AS staging (READ_ONLY)")
con.execute(
    f"""
    CREATE TEMP TABLE overrides AS
    SELECT * FROM read_csv('{OVERRIDES_CSV}', header=true)
    """
)

con.execute(
    """
    CREATE OR REPLACE TABLE minorities_staging_2 AS
    WITH after_majority AS (
        SELECT s.*
        FROM staging.minorities_staging s
        LEFT JOIN overrides o ON s.qid = o.qid AND o.is_titular_majority
        WHERE o.qid IS NULL
    ),
    -- One row per (child, candidate parent name) from part_of (already a
    -- LIST(VARCHAR) coming out of staging.py's merge_multi).
    candidate_children AS (
        SELECT
            r.qid AS child_qid,
            r.group_name_en AS child_name,
            p AS parent_name
        FROM after_majority r, UNNEST(r.part_of) AS t(p)
    ),
    -- Keep only candidates whose parent name matches another surviving row.
    matched_children AS (
        SELECT c.child_qid, c.child_name, a.qid AS parent_qid
        FROM candidate_children c
        JOIN after_majority a ON c.parent_name = a.group_name_en AND a.qid <> c.child_qid
    ),
    -- A child can match more than one surviving parent; pick deterministically.
    resolved_children AS (
        SELECT child_qid, child_name, parent_qid
        FROM (
            SELECT *, row_number() OVER (PARTITION BY child_qid ORDER BY parent_qid) AS rn
            FROM matched_children
        )
        WHERE rn = 1
    ),
    parent_subgroups AS (
        SELECT
            parent_qid,
            list(struct_pack(name := child_name, qid := child_qid) ORDER BY child_name) AS known_subgroups
        FROM resolved_children
        GROUP BY parent_qid
    )
    SELECT
        a.*,
        COALESCE(ps.known_subgroups, []::STRUCT(name VARCHAR, qid VARCHAR)[]) AS known_subgroups
    FROM after_majority a
    LEFT JOIN parent_subgroups ps ON a.qid = ps.parent_qid
    LEFT JOIN resolved_children rc ON a.qid = rc.child_qid
    WHERE rc.child_qid IS NULL
    ORDER BY a.group_name_en
    """
)

log_run_time(start_time)

""" VERIFY """
count = con.execute("SELECT COUNT(*) FROM minorities_staging_2").fetchone()[0]
logging.info(f"Total staging_2 groups: {count:,}")

rolled_up = con.execute(
    "SELECT COUNT(*) FROM minorities_staging_2 WHERE len(known_subgroups) > 0"
).fetchone()[0]
logging.info(f"Groups with rolled-up subgroups: {rolled_up:,}")

schema = con.execute("DESCRIBE minorities_staging_2").df()
logging.info(f"Table schema:\n{schema.to_string(index=False)}")

logging.info(f"Database location: {STAGING_2_DB}")

con.close()
