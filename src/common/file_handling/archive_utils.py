import logging
import zipfile
from pathlib import Path

from common.errors.error_handling import log_and_exit


def unpack_and_remove_zip(zip_path: Path):
    """
    Unpacks a zip file given a path and places all contents in the same directory.
    Deletes the original zip file.
    """
    try:
        save_directory = zip_path.parent
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(save_directory)
            logging.info(f"Extracted all files to: {save_directory}")

        zip_path.unlink()
        logging.info(f"Removed the zip file: {zip_path}")
    except Exception as e:
        log_and_exit(f"Error on handling the zip: {zip_path}", e)
