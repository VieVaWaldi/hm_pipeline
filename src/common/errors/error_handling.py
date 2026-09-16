import logging
import sys


def log_and_exit(message: str, exception: Exception = None, exit_code: int = 1):
    """Logs error and exits immediately - no recovery attempted."""
    if exception:
        logging.exception(f"FATAL: {message}: {str(exception)}")
    else:
        logging.error(f"FATAL: {message}")
    sys.exit(exit_code)
