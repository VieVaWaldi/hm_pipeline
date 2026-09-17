import os
from functools import lru_cache

import meilisearch
from opensearchpy import OpenSearch

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


@lru_cache
def get_opensearch_client() -> OpenSearch:
    """OpenSearch client for the environment set via ENV (default: dev).

    Host/port come from config/config.yaml (opensearch.host/opensearch.port) --
    the dev container's host-published port (infra/docker-compose.yml shifts
    it to 9201, not the default 9200, to avoid clashing with heritagemonitor's
    own hm-opensearch container on a shared dev machine). Security is
    disabled on the dev container (plugins.security.disabled=true), so no
    auth is wired up here -- add OPENSEARCH_USERNAME/PASSWORD the same way
    heritagemonitor's packages/search/src/index.ts does once a prod instance
    has security enabled.
    """
    settings = get_settings().opensearch
    return OpenSearch(hosts=[f"{settings.host}:{settings.port}"])
