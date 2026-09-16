from functools import lru_cache
from typing import Dict

import yaml
from pydantic import RootModel

from common.file_handling.path_utils import get_project_root_path

PATHS_FILE = "paths.yaml"


class SourcePaths(RootModel[Dict[str, str]]):
    """Named disk paths for one source (e.g. path_raw, path_duck). Field names
    vary per source, so this validates as a plain string-keyed map rather than
    a fixed schema."""

    def __getitem__(self, key: str) -> str:
        return self.root[key]

    def get(self, key: str, default=None) -> str | None:
        return self.root.get(key, default)


@lru_cache
def get_source_paths() -> Dict[str, SourcePaths]:
    """Loads config/paths.yaml: per-source disk locations for bulk dumps and
    their duckdb files (openaire_dump, openalex_dump, ror_dump, core_v3)."""
    config_path = get_project_root_path() / "config" / PATHS_FILE
    raw = yaml.safe_load(config_path.read_text())
    return {name: SourcePaths(cfg) for name, cfg in raw.items()}
