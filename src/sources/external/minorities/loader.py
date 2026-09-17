"""
Minorities Raw Ingestion - Load into DuckDB

Loads the Wikidata harvest CSV (data/pile/minorities/minorities.csv, written by
extract.py) into minorities_raw.duckdb, unmodified. Filtering and
deduplication happen downstream in reducer.py.
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

setup_logging("loader", "minorities")

config = get_dumps_paths()["minorities"]
MINORITIES_SOURCE = Path(config["path_raw"])
MINORITIES_DB = Path(config["path_duck"])

ensure_path_exists(MINORITIES_DB)

logging.info(f"MINORITIES RAW INGESTION")
logging.info(f"Source: {MINORITIES_SOURCE}")
logging.info(f"Target: {MINORITIES_DB}")

start_time = datetime.now()

con = create_duck_connection(str(MINORITIES_DB))
con.execute(
    f"""
    CREATE OR REPLACE TABLE minorities_raw AS
    SELECT *
    FROM read_csv('{MINORITIES_SOURCE}', all_varchar=true)
"""
)

log_run_time(start_time)

""" VERIFY """
count = con.execute("SELECT COUNT(*) FROM minorities_raw").fetchone()[0]
logging.info(f"Total candidate groups: {count:,}")

schema = con.execute("DESCRIBE minorities_raw").df()
logging.info(f"Table schema:\n{schema.to_string(index=False)}")

logging.info(get_size_log(MINORITIES_SOURCE, MINORITIES_DB))

logging.info(f"Database location: {MINORITIES_DB}")

con.close()
