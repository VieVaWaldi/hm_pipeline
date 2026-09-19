import logging

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

    Console-only: every Snakemake rule now redirects its shell command's
    stdout/stderr into its own `log:` file (a fixed, non-timestamped path --
    see orchestration/rules/*.smk), which captures this same console output.
    A file handler here would just duplicate that into a second, unfindable
    timestamped file -- the exact problem `log:` was added to fix.
    """
    logger = logging.getLogger()
    if logger.hasHandlers():
        return

    console_handler = logging.StreamHandler()
    logger.setLevel(LOG_LEVEL)
    console_handler.setLevel(LOG_LEVEL)

    formatter = CustomFormatter(
        "[%(levelname)s] [%(asctime)s] [%(filename)-15s:%(lineno)-4d] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
