import shutil
from pathlib import Path

from common.errors.error_handling import log_and_exit

ENCODING = "utf-8"


def load_file(path: Path) -> str | None:
    """
    Opens file and returns stripped content.
    Returns None if file doesnt exist.
    """
    if not path.exists():
        return None

    try:
        with open(path, "r", encoding=ENCODING) as file:
            data = file.read().strip()
        return data
    except Exception as e:
        log_and_exit(f"Error loading file: {path}", e)


def write_file(path: Path, content: str) -> None:
    """Writes content to file."""
    try:
        ensure_path_exists(path)
        with open(path, "w", encoding=ENCODING) as file:
            file.write(content)
    except Exception as e:
        log_and_exit(f"Error writing file: {path}", e)


def ensure_path_exists(path: Path) -> None:
    """Create the directory if it doesn't exist. If given a file picks the parent directory."""
    if path.is_file() or path.suffix:
        directory = path.parent
    else:
        directory = path
    directory.mkdir(parents=True, exist_ok=True)


def delete_if_empty(folder_path: Path) -> None:
    """
    Delete the given folder if it's empty.
    """
    if folder_path.is_dir() and not any(folder_path.iterdir()):
        try:
            shutil.rmtree(folder_path)
        except OSError as e:
            log_and_exit(f"Error deleting folder: {folder_path} ", e)


def raise_error_if_directory_does_not_exist(directory_path: Path) -> None:
    """
    Check if a directory exists and raise an error if it doesn't.
    """
    if not directory_path.is_dir():
        log_and_exit(f"The directory does not exist: {directory_path}")
