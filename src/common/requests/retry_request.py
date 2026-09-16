import logging
import time

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from common.errors.error_handling import log_and_exit


def retry_on_failure(
    max_retries=3, initial_delay=10, power_base=4, disable_retry=False
):
    """Decorator that retries the same request with delayed backoff."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            call_disable_retry = kwargs.pop("disable_retry", disable_retry)
            if call_disable_retry:
                return func(*args, **kwargs)

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        log_and_exit(str(e))
                    delay = initial_delay * (power_base**attempt)
                    logging.info(
                        f"Request failed (attempt {attempt+1}/{max_retries}). Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)

        return wrapper

    return decorator


def get_connection_retry_session() -> Session:
    """Retries failed connection establishment with backoff."""
    retry_strategy = Retry(
        total=3,  # Max retries total
        connect=2,  # Max retries for connection errors
        read=1,  # Max retries for read errors
        backoff_factor=0.5,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session
