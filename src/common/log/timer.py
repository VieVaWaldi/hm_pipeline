import logging
from datetime import datetime


def log_run_time(start_time: datetime):
    duration = datetime.now() - start_time
    hours = duration.total_seconds() / 3600
    minutes = (duration.total_seconds() % 3600) / 60
    logging.info(f"Total runtime: {int(hours)}h {int(minutes)}m")
