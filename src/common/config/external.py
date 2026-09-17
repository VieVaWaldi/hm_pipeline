from functools import lru_cache
from typing import Dict

from common.config.source_paths import SourcePaths, load_source_paths_file

EXTERNAL_FILE = "external.yaml"


@lru_cache
def get_external_paths() -> Dict[str, SourcePaths]:
    """Loads config/external.yaml: per-source disk locations for one-shot
    harvests against a live external endpoint or a manually-downloaded file
    (minorities, oa_topics) -- distinct from config/dumps.yaml's big
    versioned bulk dumps (openaire_dump, openalex_dump, ror_dump), even
    though both shapes look the same (path_raw + path_duck)."""
    return load_source_paths_file(EXTERNAL_FILE)
