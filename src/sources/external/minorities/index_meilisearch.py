"""
Minorities Meilisearch index — minorities_raw.duckdb -> Meilisearch `minorities` index.

Experimental serve step: there's no core_v4 gold schema yet (see
infra/meilisearch/README.md), so this indexes minorities_raw directly to
have something real to test Meilisearch's autocomplete/facet behaviour
against, ahead of the real serve pipeline. Not wired into Snakemake.

qid is already a column on minorities_raw and doubles as the primaryKey
below, so every document carries it -- the frontend can build
https://www.wikidata.org/wiki/{qid} straight from that, no extra field needed.

Facet tiers are based on actual non-empty rates and value shapes checked
directly against the 304-row table (not the sampled report):

  Primary (100%/high coverage, clean values) -- countries, source_class,
  religions, native_languages. countries is the natural "Map Minorities"
  axis; source_class is low-cardinality (238 ethnic group / 41 tribe / 12
  indigenous_to_europe / 5 ethnoreligious group + a few combos) but two of
  its values (manual_seed, indigenous_to_europe) are internal pipeline
  vocabulary -- fine to filter on, needs relabeling before being shown as a
  facet label in the UI.

  Secondary/advanced (sparser, or noisier values) -- subclass_of (36%
  coverage, but its facet distribution surfaces inconsistent Wikidata
  taxonomy noise like "inhabitant" or "French" as top buckets -- needs
  curation before it's a first-class facet), admin_territory (8%),
  ancestral_home (5%).

  Range, not a discrete facet -- population. Its facet *distribution* is
  ~77 near-unique values (useless as a checkbox list); request
  facets=["population"] for facetStats {min, max} instead, to drive a
  slider (verified: {min: 7, max: 133000000}).

  Boolean toggle, not a value-list facet -- has_subgroups (derived below
  from known_subgroups; only 13/304 true).

  Dropped entirely -- diaspora is 0/304 non-empty (loader.py's stage_filter()
  already drops every diaspora-typed row upstream), so it's excluded even as
  a filter -- an empty-everywhere facet is just dead weight in the UI.
"""

import logging
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

from common.database.duck.create_connection import create_duck_connection
from common.config.external import get_external_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time
from common.search.client import get_meilisearch_client
from common.search.index_duckdb_table import index_duckdb_table

setup_logging("index_meilisearch", "minorities")
load_dotenv()

config = get_external_paths()["minorities"]
MINORITIES_DB = Path(config["path_duck"])
INDEX_NAME = "minorities"
TABLE_NAME = "minorities_raw"
VIEW_NAME = "minorities_index_view"

logging.info("MINORITIES MEILISEARCH INDEX")
logging.info(f"Source: {MINORITIES_DB}")
logging.info(f"Index: {INDEX_NAME}")

start_time = datetime.now()

client = get_meilisearch_client()
index = client.index(INDEX_NAME)

# Settings first: Meilisearch applies these to whatever documents already
# exist plus whatever comes in after, but setting them before the first
# add_documents call means the initial batch is never served un-configured.
task = index.update_settings(
    {
        # Ranked: an exact/prefix hit on the group's own name should always
        # outrank a hit that only matched via a nested subgroup, an alias, or
        # a language/country name. search_keywords (Phase 2 self-designation
        # terms -- native label, demonym, English aliases; see
        # loader.py's enrich_terms()) sits right behind group_name_en since
        # it's the piece that makes e.g. "Lapps" or "gypsies" actually find
        # something.
        "searchableAttributes": [
            "group_name_en",
            "search_keywords",
            "known_subgroups.name",
            "native_languages",
            "countries",
            "religions",
            "subclass_of",
        ],
        # Array-valued columns facet over their individual elements for
        # free (a group with countries=["Italy","Austria"] contributes to
        # both facet buckets) -- exactly what a filter sidebar wants. Tiers
        # (primary/secondary/range/boolean) are explained in the module
        # docstring; Meilisearch itself doesn't distinguish them, that's a
        # frontend presentation choice layered on top of this flat list.
        "filterableAttributes": [
            # primary
            "countries",
            "source_class",
            "religions",
            "native_languages",
            # secondary / advanced
            "subclass_of",
            "admin_territory",
            "ancestral_home",
            # range (via facetStats, not facetDistribution)
            "population",
            # boolean toggle
            "has_subgroups",
        ],
        "sortableAttributes": [
            "population",
            "group_name_en",
        ],
    }
)
client.wait_for_task(task.task_uid)
logging.info("Index settings applied")

con = create_duck_connection(str(MINORITIES_DB))
# has_subgroups is a serving-layer convenience (a boolean toggle facet),
# not a fact about the data itself, so it's derived here via a view rather
# than persisted on minorities_raw -- keeps loader.py's output free of
# serving-specific fields.
con.execute(f'CREATE OR REPLACE VIEW "{VIEW_NAME}" AS SELECT *, len(known_subgroups) > 0 AS has_subgroups FROM "{TABLE_NAME}"')
total = index_duckdb_table(con, VIEW_NAME, INDEX_NAME, primary_key="qid", replace_all=True)
con.close()

log_run_time(start_time)
logging.info(f"Indexed {total:,} documents into '{INDEX_NAME}'")
