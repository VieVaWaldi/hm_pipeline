from functools import lru_cache
from typing import Dict

from common.config.source_paths import SourcePaths, load_source_paths_file

DUMPS_FILE = "dumps.yaml"


@lru_cache
def get_dumps_paths() -> Dict[str, SourcePaths]:
    """Loads config/dumps.yaml: per-source disk locations for bulk dumps and
    their duckdb files (openaire_dump, openalex_dump, ror_dump)."""
    return load_source_paths_file(DUMPS_FILE)
