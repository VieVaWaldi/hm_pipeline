import argparse
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Type

from dotenv import load_dotenv

from common.config.queries import get_query_settings
from common.config.settings import get_settings
from common.errors.error_handling import log_and_exit
from common.file_handling.file_utils import ensure_path_exists, load_file, write_file
from common.file_handling.path_utils import get_project_root_path, get_source_data_path
from common.log.logger import setup_logging
from common.log.timer import log_run_time


@dataclass
class ExtractorConfig:
    name: str
    query: str
    query_id: str
    checkpoint_name: str
    checkpoint_start: str
    checkpoint_range: str
    download_attachments: bool


class IExtractor(ABC):
    """
    Base Extractor class that sets up paths and checkpoint.
    """

    def __init__(
        self, extractor_config: ExtractorConfig, sleep_between_extractions: int = 5
    ):
        self.extractor_name: str = (
            f"{extractor_config.name}-query_id-{extractor_config.query_id}"
        )
        self.query: str = extractor_config.query
        self.download_attachments: bool = extractor_config.download_attachments

        self.checkpoint_name: str = extractor_config.checkpoint_name
        self.checkpoint_start: str = extractor_config.checkpoint_start
        self.checkpoint_range: str = extractor_config.checkpoint_range
        self.checkpoint_path = self._get_checkpoint_path()
        ensure_path_exists(self.checkpoint_path)
        self.checkpoint = self.restore_checkpoint()

        self.data_path = (
            get_source_data_path(extractor_config.name, extractor_config.query_id)
            / f"cp_{self.checkpoint}"
        )
        ensure_path_exists(self.data_path)
        time.sleep(sleep_between_extractions)

    @abstractmethod
    def extract_until_next_checkpoint(self) -> bool:
        """Extract until the next checkpoint and return whether to continue extraction."""

    @abstractmethod
    def should_continue(self) -> bool:
        """
        Whether to continue this extraction; Should be returned by
        @extract_until_checkpoint_range
        """

    @abstractmethod
    def get_checkpoint_end(self, minus_1_day=False) -> Any:
        """
        Returns the end of this extraction by returning the max checkpoint value.
        Use minus_1_day=True to stop this extraction 1 day before the next extraction starts.
        """

    def restore_checkpoint(self) -> str:
        """
        Loads the checkpoint from the checkpoint file and returns it.
        Returns self.checkpoint_start when there is no checkpoint yet.
        """
        checkpoint = load_file(self.checkpoint_path)
        return checkpoint if checkpoint is not None else self.checkpoint_start

    def save_checkpoint(self, new_checkpoint: str):
        """
        Overwrites the checkpoint file with the latest checkpoint.
        """
        return write_file(self.checkpoint_path, new_checkpoint)

    def _get_checkpoint_path(self) -> Path:
        return Path(
            get_project_root_path()
            / get_settings().checkpoint_path
            / "extractor"
            / self.extractor_name
            / f"{self.checkpoint_name}.cp"
        )


def run_extractor(config: ExtractorConfig, extractor_class: Type[IExtractor]):
    """Main extraction runner that handles the extraction loop."""
    start_time = datetime.now()
    logging.info(
        f"Starting extraction for {config.name}, query_id: {config.query_id} \
        \n\t- with query: {config.query} \
        \n\t- with checkpoint: {config.checkpoint_name} \
        \n\t- with range: {config.checkpoint_range} \
        \n\t- with download attachment: {config.download_attachments}"
    )

    continue_extraction = True
    while continue_extraction:
        try:
            extractor = extractor_class(extractor_config)
            continue_extraction = extractor.extract_until_next_checkpoint()
        except Exception as e:
            log_and_exit(f"Error during extraction iteration", e)

    log_run_time(start_time)
    logging.info(f"Successfully completed extraction for {config.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extractor Runner")
    parser.add_argument("--source", help="Select data source", required=True)
    parser.add_argument(
        "--query_id",
        help="Named query key for the source (see config/queries.yaml)",
        required=True,
    )
    args = parser.parse_args()

    load_dotenv()
    setup_logging("extractor", f"{args.source}-query_id-{args.query_id}")

    # Imported here, not at module level: these subclass IExtractor defined above,
    # so importing them at the top would be a circular import.
    from sources.apis.arxiv.extractor import ArxivExtractor
    from sources.apis.cordis.extractor import CordisExtractor
    # from sources.apis.coreac.extractor import CoreacExtractor

    source_config = get_query_settings()[args.source]
    query_config = source_config.queries[args.query_id]

    extractor_classes = {
        "arxiv": ArxivExtractor,
        "cordis": CordisExtractor,
        # "coreac": CoreacExtractor,
    }

    extractor_config = ExtractorConfig(
        name=args.source,
        query=query_config.query,
        query_id=args.query_id,
        checkpoint_name=source_config.checkpoint,
        checkpoint_start=query_config.checkpoint_start,
        checkpoint_range=query_config.checkpoint_range,
        download_attachments=query_config.download_attachments,
    )

    run_extractor(extractor_config, extractor_classes[args.source])
