import os
from functools import lru_cache

import meilisearch

from common.config.settings import get_settings


@lru_cache
def get_meilisearch_client() -> meilisearch.Client:
    """Meilisearch client for the environment set via ENV (default: dev).

    Host/port come from config/config.yaml (search.host/search.port); the
    master key comes from MEILI_MASTER_KEY in .env, same as other API keys
    in this repo (never stored in yaml).
    """
    settings = get_settings().search
    url = f"{settings.host}:{settings.port}"
    return meilisearch.Client(url, os.getenv("MEILI_MASTER_KEY"))
