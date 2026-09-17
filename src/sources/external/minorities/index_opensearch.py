"""
Minorities OpenSearch index — minorities_raw.duckdb -> OpenSearch `minorities` index.

OpenSearch counterpart to index_meilisearch.py -- same source table/view, same
experimental status (no core_v4 gold schema yet, not wired into Snakemake),
same field tiers. See index_meilisearch.py's docstring for the facet-tier
rationale (primary/secondary/range/boolean/dropped); this file only documents
where the OpenSearch *mapping* differs from Meilisearch's settings:

  Meilisearch is schemaless and its settings are a flat list of attribute
  names; OpenSearch needs an explicit field type per column, declared before
  the first document lands. Any field that's both full-text searchable and
  facet/filterable (group_name_en, native_languages, countries, religions,
  subclass_of) gets a `text` + `.keyword` multi-field -- the analyzed `text`
  side for search, the exact-value `keyword` side for terms aggregations
  (facets) and, for group_name_en, sorting. Fields that are filter-only
  (source_class, admin_territory, ancestral_home) are `keyword`-only -- no
  need to pay for an analyzed side nothing searches. known_subgroups is
  `nested` so a search against known_subgroups.name matches within a single
  subgroup, not across the array. population is `integer` (range aggregation
  instead of Meilisearch's facetStats) and has_subgroups is `boolean` --
  both map straight across.

  search_keywords is a plain array-of-text field: it's searchable but never
  faceted (not in the filterable tier), so it doesn't need a keyword side.

  diaspora is dropped entirely, same reasoning as index_meilisearch.py (0/304
  non-empty upstream).
"""

import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from common.database.duck.create_connection import create_duck_connection
from common.config.external import get_external_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time
from common.search.index_duckdb_table import index_duckdb_table_opensearch

setup_logging("index_opensearch", "minorities")
load_dotenv()

config = get_external_paths()["minorities"]
MINORITIES_DB = Path(config["path_duck"])
INDEX_NAME = "minorities"
TABLE_NAME = "minorities_raw"
VIEW_NAME = "minorities_index_view"

# Reused by both text+keyword facet fields and keyword-only filter fields.
_TEXT_AND_KEYWORD = {"type": "text", "fields": {"keyword": {"type": "keyword"}}}

MAPPING = {
    "properties": {
        "qid": {"type": "keyword"},
        # Ranking (Meilisearch's searchableAttributes order -- an exact/prefix
        # hit on the group's own name outranking one from a nested subgroup,
        # alias, or language/country name) has no index-setting equivalent in
        # OpenSearch; it becomes a query-time concern (multi_match field
        # boosts) for whatever repository/service code queries this index,
        # not something set here.
        "group_name_en": _TEXT_AND_KEYWORD,
        "search_keywords": {"type": "text"},
        "known_subgroups": {
            "type": "nested",
            "properties": {"name": {"type": "text"}},
        },
        "native_languages": _TEXT_AND_KEYWORD,
        "countries": _TEXT_AND_KEYWORD,
        "religions": _TEXT_AND_KEYWORD,
        "subclass_of": _TEXT_AND_KEYWORD,
        "source_class": {"type": "keyword"},
        "admin_territory": {"type": "keyword"},
        "ancestral_home": {"type": "keyword"},
        "population": {"type": "integer"},
        "has_subgroups": {"type": "boolean"},
    }
}

logging.info("MINORITIES OPENSEARCH INDEX")
logging.info(f"Source: {MINORITIES_DB}")
logging.info(f"Index: {INDEX_NAME}")

start_time = datetime.now()

con = create_duck_connection(str(MINORITIES_DB))
# has_subgroups is a serving-layer convenience (a boolean toggle facet), not
# a fact about the data itself, so it's derived here via a view rather than
# persisted on minorities_raw -- keeps loader.py's output free of
# serving-specific fields. Same view index_meilisearch.py builds.
con.execute(f'CREATE OR REPLACE VIEW "{VIEW_NAME}" AS SELECT *, len(known_subgroups) > 0 AS has_subgroups FROM "{TABLE_NAME}"')
total = index_duckdb_table_opensearch(con, VIEW_NAME, INDEX_NAME, id_field="qid", mapping=MAPPING, replace_all=True)
con.close()

log_run_time(start_time)
logging.info(f"Indexed {total:,} documents into '{INDEX_NAME}'")
