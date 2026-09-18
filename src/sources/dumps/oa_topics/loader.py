"""
OA Topics Raw Ingestion - Load into DuckDB

Loads the OpenAlex topic mapping CSV (data/pile/oa_topics/openalex_topic_mapping.csv,
downloaded manually from OpenAlex - not fetched by this pipeline) into
oa_topics_raw.duckdb. Numeric id columns are cast to integers and the
semicolon-separated `keywords` column is turned into a JSON array; every other
column is loaded as-is.
"""

import logging
from datetime import datetime
from pathlib import Path

from common.database.duck.create_connection import create_duck_connection
from common.database.duck.utils import get_size_log
from common.file_handling.file_utils import ensure_path_exists
from common.config.dumps import get_dumps_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time

setup_logging("loader", "oa_topics")

config = get_dumps_paths()["oa_topics"]
OA_TOPICS_SOURCE = Path(config["path_raw"])
OA_TOPICS_DB = Path(config["path_duck"])

if not OA_TOPICS_SOURCE.exists():
    raise FileNotFoundError(
        f"OA topics source file not found at {OA_TOPICS_SOURCE}. "
        "Download openalex_topic_mapping.csv from OpenAlex and place it at "
        "this path before running the loader."
    )

ensure_path_exists(OA_TOPICS_DB)

logging.info(f"OA TOPICS RAW INGESTION")
logging.info(f"Source: {OA_TOPICS_SOURCE}")
logging.info(f"Target: {OA_TOPICS_DB}")

start_time = datetime.now()

con = create_duck_connection(str(OA_TOPICS_DB))
con.execute(
    f"""
    CREATE OR REPLACE TABLE oa_topics_raw AS
    SELECT
        topic_id::BIGINT AS topic_id,
        topic_name,
        subfield_id::BIGINT AS subfield_id,
        subfield_name,
        field_id::BIGINT AS field_id,
        field_name,
        domain_id::BIGINT AS domain_id,
        domain_name,
        to_json(list_transform(string_split(keywords, ';'), x -> trim(x))) AS keywords,
        summary,
        wikipedia_url
    FROM read_csv('{OA_TOPICS_SOURCE}', all_varchar=true)
"""
)

log_run_time(start_time)

""" VERIFY """
count = con.execute("SELECT COUNT(*) FROM oa_topics_raw").fetchone()[0]
logging.info(f"Total topics: {count:,}")

schema = con.execute("DESCRIBE oa_topics_raw").df()
logging.info(f"Table schema:\n{schema.to_string(index=False)}")

sample = con.execute(
    "SELECT topic_id, topic_name, keywords FROM oa_topics_raw LIMIT 3"
).df()
logging.info(f"Sample record:\n{sample.to_string(index=False)}")

logging.info(get_size_log(OA_TOPICS_SOURCE, OA_TOPICS_DB))

logging.info(f"Database location: {OA_TOPICS_DB}")

con.close()
