"""
Minorities Staging — Transform minorities_raw.duckdb -> minorities_staging.duckdb

Replaces the former scripts/reduce_candidates.py (pandas) with the same logic
in SQL. Applies three documented, reproducible filters (see D4.3 section 6.1
for rationale) and merges rows that Wikidata models as two separate QIDs for
the same group (e.g. a merged/deprecated item alongside the canonical one).

See staging_2.py for the second pass: dropping groups that are the titular/
majority population of one of their own listed countries, and rolling up
known subgroups (e.g. Sámi subgroups) into their parent row.

Filters applied (each removes rows that cannot function as a minority filter
option at all -- none of them are a judgement about minority status):

  A. Unresolved labels   -- group_name_en is empty or just the QID echoed
                            back (e.g. "Q10262245"): no name to display or
                            match against.
  B. Diaspora typing     -- Wikidata's own `diaspora` property (P3833) is
                            set: recent/dispersed migrant populations, not
                            the territorially rooted groups the filter
                            targets.
  C. Non-European only   -- every country in `countries` falls outside
                            Europe (artifact of the "indigenous to" anchor
                            query picking up entities via continent rather
                            than the strict country allowlist).

Merging: for duplicate group_name_en values, the primary qid is the
lexicographically smallest of the group's QIDs (a deterministic stand-in for
"first row encountered", which pandas used); merged_qids lists all of them.
Every multi-value column (originally "|"-joined in the raw CSV) is flattened,
deduplicated, re-sorted, and stored as a native LIST(VARCHAR) across the
merged rows; population takes the max of the numeric values.
"""

import logging
from datetime import datetime
from pathlib import Path

from common.database.duck.create_connection import create_duck_connection
from common.file_handling.file_utils import ensure_path_exists
from common.config.dumps import get_dumps_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time

setup_logging("staging", "minorities")

config = get_dumps_paths()["minorities"]
RAW_DB = Path(config["path_duck"])
STAGING_DB = Path(config["path_duck_staging"])

NON_EUROPEAN_COUNTRIES = "'Mongolia', 'Kazakhstan'"

ensure_path_exists(STAGING_DB)

logging.info("MINORITIES STAGING")
logging.info(f"Source: {RAW_DB}")
logging.info(f"Target: {STAGING_DB}")

start_time = datetime.now()

con = create_duck_connection(str(STAGING_DB))
con.execute(f"ATTACH '{RAW_DB}' AS raw (READ_ONLY)")

# Flattens every "|"-joined value across the rows in a GROUP BY, trims
# whitespace, drops empties, dedupes, and re-sorts into a native LIST(VARCHAR)
# -- the SQL equivalent of split_values() + join_values() from the old pandas
# script, minus the final join (downstream consumers get a real array instead
# of a "|"-joined blob).
con.execute(
    """
    CREATE OR REPLACE MACRO merge_multi(col) AS (
        list_sort(
            list_distinct(
                list_filter(
                    flatten(list(list_transform(string_split(col, '|'), x -> trim(x)))),
                    v -> v <> ''
                )
            )
        )
    )
    """
)

con.execute(
    f"""
    CREATE OR REPLACE TABLE minorities_staging AS
    WITH base AS (
        SELECT
            qid,
            COALESCE(group_name_en, '') AS group_name_en,
            COALESCE(countries, '') AS countries,
            COALESCE(source_class, '') AS source_class,
            TRY_CAST(population AS DOUBLE) AS population,
            COALESCE(religions, '') AS religions,
            COALESCE(native_languages, '') AS native_languages,
            COALESCE(part_of, '') AS part_of,
            COALESCE(subclass_of, '') AS subclass_of,
            COALESCE(diaspora, '') AS diaspora,
            COALESCE(ancestral_home, '') AS ancestral_home,
            COALESCE(admin_territory, '') AS admin_territory,
            COALESCE(has_parts, '') AS has_parts
        FROM raw.minorities_raw
    ),
    flagged AS (
        SELECT
            *,
            list_transform(string_split(countries, '|'), x -> trim(x)) AS country_list
        FROM base
    ),
    survivors AS (
        SELECT * EXCLUDE (country_list)
        FROM flagged
        WHERE NOT (
            -- Filter A: unresolved label
            (trim(group_name_en) = '' OR regexp_matches(trim(group_name_en), '^Q[0-9]+$'))
            -- Filter B: diaspora typing
            OR trim(diaspora) <> ''
            -- Filter C: non-European only
            OR (
                len(country_list) > 0
                AND len(list_filter(country_list, x -> x NOT IN ({NON_EUROPEAN_COUNTRIES}))) = 0
            )
        )
    )
    SELECT
        MIN(qid) AS qid,
        list_sort(list(DISTINCT qid)) AS merged_qids,
        group_name_en,
        merge_multi(countries) AS countries,
        merge_multi(source_class) AS source_class,
        MAX(population) AS population,
        merge_multi(religions) AS religions,
        merge_multi(native_languages) AS native_languages,
        merge_multi(part_of) AS part_of,
        merge_multi(subclass_of) AS subclass_of,
        merge_multi(diaspora) AS diaspora,
        merge_multi(ancestral_home) AS ancestral_home,
        merge_multi(admin_territory) AS admin_territory,
        merge_multi(has_parts) AS has_parts
    FROM survivors
    GROUP BY group_name_en
    ORDER BY group_name_en
    """
)

log_run_time(start_time)

""" VERIFY """
count = con.execute("SELECT COUNT(*) FROM minorities_staging").fetchone()[0]
logging.info(f"Total staged groups: {count:,}")

schema = con.execute("DESCRIBE minorities_staging").df()
logging.info(f"Table schema:\n{schema.to_string(index=False)}")

logging.info(f"Database location: {STAGING_DB}")

con.close()
