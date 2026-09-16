from functools import lru_cache
from typing import Dict, Optional

import yaml
from pydantic import BaseModel

from common.config.source_paths import resolve_data_path
from common.file_handling.path_utils import get_project_root_path

QUERIES_FILE = "api_runner.yaml"


class QueryConfig(BaseModel):
    query: Optional[str] = None
    download_attachments: bool = False
    checkpoint_start: str
    checkpoint_range: str
    path_duck: Optional[str] = None


class SourceQueryConfig(BaseModel):
    checkpoint: Optional[str] = None
    queries: Dict[str, QueryConfig] = {}


@lru_cache
def get_query_settings() -> Dict[str, SourceQueryConfig]:
    """Loads config/api_runner.yaml: extraction query definitions per source
    (arxiv, cordis, coreac, meta_heritage). For per-source disk paths
    (dumps, duckdb files), see common.config.dumps.get_dumps_paths instead."""
    config_path = get_project_root_path() / "config" / QUERIES_FILE
    raw = yaml.safe_load(config_path.read_text())
    settings = {name: SourceQueryConfig(**cfg) for name, cfg in raw.items()}
    for source in settings.values():
        for query in source.queries.values():
            if query.path_duck is not None:
                query.path_duck = resolve_data_path(query.path_duck)
    return settings
