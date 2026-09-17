import os
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel

from common.file_handling.path_utils import get_project_root_path

CONFIG_FILE = "config.yaml"


class DBSettings(BaseModel):
    url: str
    port: str
    db_name: str


class SearchSettings(BaseModel):
    host: str
    port: str


class Settings(BaseModel):
    env: str
    checkpoint_path: Path
    logging_path: Path
    data_path: Path
    orchestration_path: Path
    hpc_root: str = ""
    db: DBSettings
    search: SearchSettings
    opensearch: SearchSettings


@lru_cache
def get_settings() -> Settings:
    """Loads config/config.yaml, validated, for the environment set via ENV (default: dev)."""
    env = os.getenv("ENV", "dev")
    config_path = get_project_root_path() / "config" / CONFIG_FILE
    raw = yaml.safe_load(config_path.read_text())

    if env not in raw:
        raise ValueError(f"Unknown ENV={env!r}, expected one of {list(raw)}")

    return Settings(env=env, **raw[env])
