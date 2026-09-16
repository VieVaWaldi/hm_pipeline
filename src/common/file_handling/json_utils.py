import json
from pathlib import Path
from typing import Any, Dict

from common.file_handling.file_utils import ensure_path_exists, ENCODING
from common.errors.error_handling import log_and_exit


def load_json_file(path: Path) -> Dict[str, Any] | None:
    """
    Opens json file and returns it.
    Returns None if file doesn't exist.
    """
    if not path.exists():
        return None

    try:
        with open(path, "r", encoding=ENCODING) as file:
            data = json.load(file)
        return data
    except Exception as e:
        log_and_exit(f"Error loading json: {path}", e)


def save_json_dict(dictionary: Dict[str, Any], path: Path):
    ensure_path_exists(path)

    try:
        with open(path, "w", encoding=ENCODING) as f:
            json.dump(dictionary, f, indent=4, ensure_ascii=False)
    except Exception as e:
        log_and_exit(f"Error saving json: {path}", e)
