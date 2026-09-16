import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Any

from common.file_handling.file_parsing.json_parser import (
    extract_json_as_dict,
)
from common.file_handling.file_parsing.xml_parser import (
    extract_xml_as_dict,
)


def yield_all_documents(document_path: Path, cordis_only_project_flag: bool = False):
    """
    Yields all documents as dictionaries including their path name and last modification time
    in utc given a path recursively.
    """
    yield from _process_files(document_path, cordis_only_project_flag)


def _process_files(file_path: Path, cordis_only_project_flag: bool) -> Iterator[Any]:
    """
    Walks through all files in the given file_path and yields the extracted documents
    as a dictionary including their path and last modification time in utc.
    """
    for root, _, files in os.walk(file_path):
        for file in files:
            full_file_path = Path(os.path.join(root, file))

            if skip_not_a_cordis_project(cordis_only_project_flag, full_file_path):
                continue

            mod_time = os.path.getmtime(full_file_path)
            mod_time_utc = datetime.fromtimestamp(mod_time, tz=timezone.utc)

            if file.endswith(".json"):
                yield extract_json_as_dict(full_file_path), full_file_path, mod_time_utc
            if file.endswith(".xml"):
                yield extract_xml_as_dict(full_file_path), full_file_path, mod_time_utc


def skip_not_a_cordis_project(is_flag: bool, path: Path):
    """
    Only returns Cordis documents that are of the type project.
    """
    if not is_flag:
        return False

    if "cordis" in str(path) and not "project" in str(path):
        return True
    else:
        return False


if __name__ == "__main__":
    path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/cordis_culturalORheritage"
    )
    for _, file_path, time in yield_all_documents(path):
        print(f"{file_path.name:40} {time}")
