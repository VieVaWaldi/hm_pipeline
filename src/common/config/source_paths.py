from pathlib import Path
from typing import Dict

import yaml
from pydantic import RootModel

from common.config.settings import get_settings
from common.file_handling.path_utils import get_project_root_path


class SourcePaths(RootModel[Dict[str, str]]):
    """Named disk paths for one source (e.g. path_raw, path_duck). Field names
    vary per source, so this validates as a plain string-keyed map rather than
    a fixed schema. Values are resolved to absolute paths at load time —
    see resolve_data_path()."""

    def __getitem__(self, key: str) -> str:
        return self.root[key]

    def get(self, key: str, default=None) -> str | None:
        return self.root.get(key, default)


def resolve_data_path(relative: str) -> str:
    """Resolves a repo-relative path (e.g. "data/duckdb/sources/ror_raw.duckdb")
    against the project root on dev, or under settings.hpc_root on prod — the
    same env split get_source_data_path() uses for api_runner sources. Config
    files should only ever contain the relative form; this is the one place
    that decides dev vs prod."""
    settings = get_settings()
    if settings.env == "dev":
        return str(get_project_root_path() / relative)
    return str(Path(settings.hpc_root) / relative)


def load_source_paths_file(filename: str) -> Dict[str, SourcePaths]:
    """Loads a config/*.yaml file shaped {source_name: {path_key: relative_path}}
    and resolves every path against the current environment."""
    config_path = get_project_root_path() / "config" / filename
    raw = yaml.safe_load(config_path.read_text())
    return {
        name: SourcePaths({key: resolve_data_path(value) for key, value in cfg.items()})
        for name, cfg in raw.items()
    }
