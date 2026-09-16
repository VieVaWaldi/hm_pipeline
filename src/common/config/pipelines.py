from functools import lru_cache
from typing import Dict

from common.config.source_paths import SourcePaths, load_source_paths_file

PIPELINES_FILE = "pipelines.yaml"


@lru_cache
def get_pipeline_paths() -> Dict[str, SourcePaths]:
    """Loads config/pipelines.yaml: per-pipeline-version disk locations for
    duckdb files (core_v3, ...)."""
    return load_source_paths_file(PIPELINES_FILE)
