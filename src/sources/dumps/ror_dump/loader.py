"""
ROR Raw Ingestion - Load into DuckDB
"""

import logging
from datetime import datetime
from pathlib import Path

from common.database.duck.create_connection import create_duck_connection
from common.database.duck.utils import get_size_log
from common.file_handling.file_utils import ensure_path_exists
from common.config.paths import get_source_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time

setup_logging("loader", "ror_dump")

config = get_source_paths()["ror_dump"]
ROR_SOURCE = Path(config["path_raw"])
ROR_DB = Path(config["path_duck"])

ensure_path_exists(ROR_DB)

logging.info(f"ROR INGESTION")
logging.info(f"Source: {ROR_SOURCE}")
logging.info(f"Target: {ROR_DB}")

start_time = datetime.now()

con = create_duck_connection(str(ROR_DB))
con.execute(
    f"""
    CREATE TABLE organizations AS
    SELECT *
    FROM read_json('{ROR_SOURCE}', format='auto')
"""
)

log_run_time(start_time)

""" VERIFY """
count = con.execute("SELECT COUNT(*) FROM organizations").fetchone()[0]
logging.info(f"Total organizations: {count:,}")

schema = con.execute("DESCRIBE organizations").df()
logging.info(f"Table schema:\n{schema.to_string(index=False)}")

sample = con.execute(
    "SELECT id, names[1].value as name, status, types FROM organizations LIMIT 3"
).df()
logging.info(f"Sample record (first organization):\n{sample.to_string(index=False)}")

logging.info(get_size_log(ROR_SOURCE, ROR_DB))

logging.info(f"Database location: {ROR_DB}")

con.close()
