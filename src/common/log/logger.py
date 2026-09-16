import datetime
import logging

from common.file_handling.file_utils import ensure_path_exists
from common.file_handling.path_utils import get_project_root_path
from common.config.settings import get_settings

LOG_LEVEL = logging.INFO


class CustomFormatter(logging.Formatter):
    """
    Custom formatter for logger to
    - Pad all columns to a fixed width
    """

    def format(self, record):
        if record.levelname == "WARNING":
            record.levelname = "WARN"
        record.levelname = f"{record.levelname:<5}"
        record.filename = f"{record.filename:<17}"
        return super().format(record)


def setup_logging(module: str, log_name: str) -> None:
    """
    Sets up and configures the logger module.
    Call once at the beginning of each run.
    """
    settings = get_settings()
    logging_path = get_project_root_path() / settings.logging_path / module
    ensure_path_exists(logging_path)

    logger = logging.getLogger()
    if logger.hasHandlers():
        return

    time_now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_handler = logging.FileHandler(logging_path / f"{log_name}_{time_now}.log")
    console_handler = logging.StreamHandler()

    logger.setLevel(LOG_LEVEL)
    file_handler.setLevel(LOG_LEVEL)
    console_handler.setLevel(LOG_LEVEL)

    formatter = CustomFormatter(
        "[%(levelname)s] [%(asctime)s] [%(filename)-15s:%(lineno)-4d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
