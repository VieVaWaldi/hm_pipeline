import duckdb
import logging

from common.errors.error_handling import log_and_exit


def create_duck_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    try:
        con = duckdb.connect(db_path)
        con.execute("SET memory_limit='16GB'")
        con.execute("SET threads=8")
        con.execute("SET enable_progress_bar=true")
        logging.info(f"Successfully connected to: DuckDB")
        return con
    except Exception as e:
        log_and_exit(f"Failed to connect to transformation: ", e)
