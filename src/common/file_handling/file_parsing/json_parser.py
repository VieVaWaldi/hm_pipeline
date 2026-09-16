import json
import os
from pathlib import Path
from typing import List, Dict, Any, Callable, Iterator

from common.errors.error_handling import log_and_exit


def get_json_as_dict_recursively(file_path: Path) -> Iterator[Dict[str, Any]]:
    """
    Searches all JSON files in all subdirectories under the file_path
    and returns each JSON file as a complete dictionary.
    """
    yield from _process_json_files(file_path, extract_json_as_dict)


def get_all_keys_value_recursively(file_path: Path, key: str) -> Iterator[Any]:
    """
    Searches all JSON files in all subdirectories under the file_path for the specified key.
    Returns a list of all values for all matching keys.
    """
    yield from _process_json_files(file_path, lambda fp: extract_key_values(fp, key))


def _process_json_files(file_path: Path, extraction_func: Callable) -> Iterator[Any]:
    """
    Walks through all JSON files in the given file_path and applies the extraction_func to each.
    """
    for root, _, files in os.walk(file_path):
        for file in files:
            if file.endswith(".json"):
                full_file_path = os.path.join(root, file)
                yield extraction_func(full_file_path)


def extract_json_as_dict(file_path: Path) -> Dict[str, Any]:
    """
    Extracts the entire JSON file as a dictionary.
    """
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        log_and_exit(f"Error parsing JSON file: {file_path}")


def extract_key_values(file_path: Path, key: str) -> List[Any]:
    """
    Extracts the value of the specified key from a JSON file.
    """
    try:
        with open(file_path, "r") as f:
            data = json.load(f)
        return _find_key_values(data, key)
    except json.JSONDecodeError:
        print(f"Error parsing JSON file: {file_path}")
        return []


def _find_key_values(obj: Any, key: str) -> List[Any]:
    """
    Recursively searches for all values of the specified key in a JSON object.
    """
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key:
                results.append(v)
            results.extend(_find_key_values(v, key))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_find_key_values(item, key))
    return results


def _find_key_objects(obj: Any, key: str) -> List[Dict[str, Any]]:
    """
    Recursively searches for all objects containing the specified key in a JSON object.
    """
    results = []
    if isinstance(obj, dict):
        if key in obj:
            results.append(obj)
        for v in obj.values():
            results.extend(_find_key_objects(v, key))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_find_key_objects(item, key))
    return results
