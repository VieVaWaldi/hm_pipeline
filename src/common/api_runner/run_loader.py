import argparse
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Type

from sqlalchemy.orm import Session

from common.api_runner.checkpoint_manager import CheckpointManager
from common.config.api_runner import get_query_settings
from common.database.duck.create_db_session import create_duck_db_session
from common.database.postgres.create_db_session import create_db_session
from common.database.shared.get_or_create import ModelCreationMonitor
from common.errors.error_handling import log_and_exit
from common.file_handling.path_utils import get_source_data_path
from common.file_handling.yield_documents import yield_all_documents
from common.log.logger import setup_logging
from common.log.timer import log_run_time
from sources.apis.arxiv.orm_model import Base as ArxivBase
from sources.apis.coreac.orm_model import Base as CoreacBase
from sources.apis.cordis.orm_model import Base as CordisBase


class ILoader(ABC):
    """
    Base Class for all Data Loaders.
    Takes a dict and transform it into respective ORM Models.
    Load models into transformation using the session.
    """

    def __init__(self, path_to_file: Path):
        self.path_to_file = path_to_file
        self.MAX_NAME_LENGTH = 500

    @abstractmethod
    def load(self, session: Session, document: Dict):
        pass


@dataclass
class LoaderConfig:
    name: str
    loader_class: Type[ILoader]
    source_path: Path
    query_id: str
    batch_size: int = 1000


# Maps source name to its SQLAlchemy Base for DuckDB table creation
DUCK_BASES = {
    "arxiv": ArxivBase,
    "cordis": CordisBase,
    "coreac": CoreacBase,
}


def run_loader(config: LoaderConfig, db: str = "duck"):
    start_time = datetime.now()
    query_config = get_query_settings()[config.name].queries[config.query_id]
    logging.info(
        f"Starting loading for {config.name}, query_id: {config.query_id}, db: {db}"
        f"\n\t- for query: {query_config.query}"
        f"\n\t- at path: {config.source_path}"
    )

    try:
        if db == "duck":
            duck_path = query_config.path_duck
            engine, Session = create_duck_db_session(duck_path)
            if config.name in DUCK_BASES:
                DUCK_BASES[config.name].metadata.create_all(engine)
        else:
            Session = create_db_session()
    except Exception as e:
        log_and_exit(f"Failed to set up {db} database session for {config.name}", e)

    cp = CheckpointManager(config.name, config.query_id)
    doc_count = 0
    skip_count = 0

    with Session() as session:
        for doc_idx, (document, path, mtime) in enumerate(
            yield_all_documents(config.source_path)
        ):
            doc_count += 1
            if cp.should_skip_or_store(mtime):
                skip_count += 1
                continue

            try:
                loader = config.loader_class(path)
                loader.load(session, document)

                if doc_idx % config.batch_size == 0:
                    session.commit()
                    session.expunge_all()

                if doc_idx % 1000 == 0:
                    logging.info(f"Processed # {doc_idx} documents")

            except Exception as e:
                session.rollback()
                logging.error(f"Error ingesting batch at document {doc_idx}:")
                logging.error(f"Error details: {str(e)}")
                logging.error(f"Document path: {path}")
                raise

        session.commit()  # Also commit leftovers
        cp.update_cp()  # Only update cp at the end, because files are not ordered

    ModelCreationMonitor.log_stats()
    validate(config.name, doc_count, skip_count)
    log_run_time(start_time)


def validate(source_name: str, doc_count: int, skip_count: int):
    logging.info(
        f"{source_name}: Files processed {doc_count} and files skipped {skip_count}, used {doc_count-skip_count} files."
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Loader Runner")
    parser.add_argument("--source", help="Select data source", required=True)
    parser.add_argument(
        "--query_id",
        help="Named query key for the source (see config/api_runner.yaml)",
        required=True,
    )
    parser.add_argument(
        "--db",
        help="Target database: duck (default) or postgres",
        default="duck",
        choices=["duck", "postgres"],
    )
    args = parser.parse_args()

    setup_logging("loader", f"{args.source}-query_id-{args.query_id}")

    # Imported here, not at module level: these subclass ILoader defined above,
    # so importing them at the top would be a circular import.
    from sources.apis.arxiv.loader import ArxivLoader
    from sources.apis.cordis.loader import CordisLoader
    from sources.apis.coreac.loader import CoreacLoader

    loader_classes = {
        "arxiv": ArxivLoader,
        "cordis": CordisLoader,
        "coreac": CoreacLoader,
    }

    loader_config = LoaderConfig(
        name=args.source,
        loader_class=loader_classes[args.source],
        source_path=get_source_data_path(args.source, query_id=args.query_id),
        query_id=args.query_id,
    )

    run_loader(loader_config, db=args.db)
