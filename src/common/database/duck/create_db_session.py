import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from common.errors.error_handling import log_and_exit


def create_duck_db_session(db_path: str) -> tuple:
    """Create and return a (engine, sessionmaker) tuple for a DuckDB file.

    NullPool is required — DuckDB allows only one writer per file,
    so we must not pool connections across sessions.
    implicit_returning=False is required — DuckDB misreports row counts on
    multi-row INSERT ... RETURNING, which SQLAlchemy uses by default to fetch
    Sequence-generated PKs. Disabling it forces single-row inserts instead.
    """
    database_url = f"duckdb:///{db_path}"
    try:
        engine = create_engine(database_url, poolclass=NullPool, implicit_returning=False)
        logging.info(f"Successfully connected to DuckDB: {db_path}")
        return engine, sessionmaker(bind=engine)
    except Exception as e:
        log_and_exit("Failed to connect to DuckDB: ", e)
