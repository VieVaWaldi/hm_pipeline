"""
Minorities Term Enrichment (Phase 2) — minorities_staging_2.duckdb -> minorities_terms.duckdb

Plan.md's Phase 2 named the properties for this ("Enrich each group with
terms") but it was only ever done by hand for the 3 pilot groups (Sami,
Ladin, Jewish) -- extract.py's DIMENSION_PROPS stops at Phase 1's filter
dimensions. This generalizes the self-designation category to every
surviving group by harvesting:

  - P1705 (native label) -- the group's own name for itself, in its own language
  - P1549 (demonym)      -- what members of the group are called
  - skos:altLabel (en)   -- English aliases/alternate spellings Wikidata already
                            tracks on the item (restricted server-side to
                            English -- aliases in other languages don't help
                            match English project/publication text)

Language and spatial-reference terms are skipped here: native_languages,
countries, and admin_territory are already in minorities_staging_2 from
Phase 1's DIMENSION_PROPS, nothing new to fetch. Tangible/intangible
heritage (P140/P793/P2596) is skipped per Plan.md's own note that it's too
sparse in Wikidata to be worth it -- UNESCO/ICH lists would be a better,
separate source for that category.

P1705/P1549/altLabel are literal-valued properties, unlike extract.py's
DIMENSION_PROPS (all entity-valued, resolved via wikibase:label) -- there's
no entity to label-resolve here, just a string, so this needs its own query
shape rather than reusing fetch_dimension().

Only queries the 304 QIDs already surviving in minorities_staging_2, not the
full discovery pool -- same "enrich the final list, not the raw harvest"
cost-saving extract.py already applies to its own dimensions.

A native label or alias can come back in any script (Cyrillic, Greek,
Arabic, CJK, ...). A term that isn't in Latin script can't match English
project text via to_tsvector('english', ...), so it's dropped from
search_keywords by is_latin_script() -- a documented, reproducible,
character-by-character Unicode check, not a per-group manual judgement.

Verified against Kat's original hand-curated keyword lists for the 3 pilot
groups (the OR-lists embedded in
src/pipelines/core_v2/analyses/minorities/full_text_minoritiy_search_*.sql):
the harvest recovers direct name variants fine, but structurally can't
recover two categories Kat's lists also carry -- topically-related terms
that aren't aliases of the group at all (e.g. "Hebrew"/"Yiddish"/"Judaism"
for Jewish people -- separate Wikidata entities, not names for the people),
and domain/cultural knowledge with no Wikidata property to harvest from
(e.g. "joik"/"yoik", a Sami singing style; "Gardenese"/"Badiese"/"Fascian"/
"Marebbano"/"Ampezzan", named Dolomite valleys where Ladin is spoken). Those
terms are preserved verbatim via manual_term_overrides.csv below rather than
pretending Wikidata alone can reproduce a domain expert's list.
"""

import logging
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

from common.database.duck.create_connection import create_duck_connection
from common.file_handling.file_utils import ensure_path_exists
from common.config.dumps import get_dumps_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time

setup_logging("enrich_terms", "minorities")

config = get_dumps_paths()["minorities"]
STAGING_2_DB = Path(config["path_duck_staging_2"])
TERMS_DB = Path(config["path_duck_terms"])
OVERRIDES_CSV = Path(__file__).resolve().parent / "manual_term_overrides.csv"

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "DIGICHer-MinorityDiscovery/0.2 (https://github.com/DIGICHer)"

LITERAL_PROPS = {
    "native_label": "P1705",
    "demonym": "P1549",
}

ensure_path_exists(TERMS_DB)


def run_query(sparql_query: str, description: str) -> list[dict]:
    """Execute a SPARQL query and return results as a list of dicts. Same
    shape as extract.py's run_query() -- duplicated rather than imported
    since extract.py's version is private to its own discovery flow."""
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.addCustomHttpHeader("User-Agent", USER_AGENT)
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(JSON)

    logging.info(f"Running query: {description} ...")
    try:
        results = sparql.query().convert()
    except HTTPError as exc:
        logging.error(f"  Query failed: {exc}")
        return []

    rows = []
    for binding in results["results"]["bindings"]:
        row = {var: binding.get(var, {}).get("value", "") for var in results["head"]["vars"]}
        rows.append(row)
    logging.info(f"  -> {len(rows)} results")
    return rows


def extract_qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1] if uri else ""


def is_latin_script(text: str) -> bool:
    for ch in text:
        if ch.isalpha():
            try:
                if "LATIN" not in unicodedata.name(ch):
                    return False
            except ValueError:
                return False
    return True


def fetch_literal_property(name: str, prop: str, qids: list[str]) -> pd.DataFrame:
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?group ?value WHERE {{
      VALUES ?group {{ {values} }}
      ?group wdt:{prop} ?value .
    }}
    """
    rows = run_query(query, f"literal: {name} ({prop})")
    if not rows:
        return pd.DataFrame(columns=["qid", name])
    df = pd.DataFrame(rows)
    df["qid"] = df["group"].apply(extract_qid)
    return (
        df.groupby("qid")["value"]
        .apply(lambda s: [v for v in dict.fromkeys(s) if v])
        .reset_index()
        .rename(columns={"value": name})
    )


def fetch_english_aliases(qids: list[str]) -> pd.DataFrame:
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?group ?value WHERE {{
      VALUES ?group {{ {values} }}
      ?group skos:altLabel ?value .
      FILTER(lang(?value) = "en")
    }}
    """
    rows = run_query(query, "literal: english aliases (skos:altLabel)")
    if not rows:
        return pd.DataFrame(columns=["qid", "aliases_en"])
    df = pd.DataFrame(rows)
    df["qid"] = df["group"].apply(extract_qid)
    return (
        df.groupby("qid")["value"]
        .apply(lambda s: [v for v in dict.fromkeys(s) if v])
        .reset_index()
        .rename(columns={"value": "aliases_en"})
    )


def main():
    start_time = datetime.now()
    logging.info("MINORITIES TERM ENRICHMENT (Phase 2)")
    logging.info(f"Source: {STAGING_2_DB}")
    logging.info(f"Overrides: {OVERRIDES_CSV}")
    logging.info(f"Target: {TERMS_DB}")

    con = create_duck_connection(str(TERMS_DB))
    con.execute(f"ATTACH '{STAGING_2_DB}' AS staging_2 (READ_ONLY)")
    qids = con.execute("SELECT qid FROM staging_2.minorities_staging_2").fetchdf()["qid"].tolist()
    logging.info(f"Enriching {len(qids)} groups")

    terms = pd.DataFrame({"qid": qids})
    for name, prop in LITERAL_PROPS.items():
        dim = fetch_literal_property(name, prop, qids)
        terms = terms.merge(dim, on="qid", how="left")
        time.sleep(2)
    aliases = fetch_english_aliases(qids)
    terms = terms.merge(aliases, on="qid", how="left")

    for col in ("native_label", "demonym", "aliases_en"):
        terms[col] = terms[col].apply(
            lambda v: [term for term in v if is_latin_script(term)] if isinstance(v, list) else []
        )

    con.register("terms_df", terms)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE manual_overrides AS
        SELECT * FROM read_csv('{OVERRIDES_CSV}', header=true)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE minorities_terms AS
        WITH manual AS (
            SELECT qid, list(DISTINCT term) AS manual_terms
            FROM manual_overrides
            GROUP BY qid
        )
        SELECT
            b.*,
            list_sort(
                list_distinct(
                    list_filter(
                        [b.group_name_en]
                            || COALESCE(t.native_label, [])
                            || COALESCE(t.demonym, [])
                            || COALESCE(t.aliases_en, [])
                            || COALESCE(m.manual_terms, []),
                        x -> x IS NOT NULL AND trim(x) <> ''
                    )
                )
            ) AS search_keywords
        FROM staging_2.minorities_staging_2 b
        LEFT JOIN terms_df t ON b.qid = t.qid
        LEFT JOIN manual m ON b.qid = m.qid
        """
    )

    log_run_time(start_time)

    """ VERIFY """
    count = con.execute("SELECT COUNT(*) FROM minorities_terms").fetchone()[0]
    logging.info(f"Total enriched groups: {count:,}")

    with_extra_terms = con.execute(
        "SELECT COUNT(*) FROM minorities_terms WHERE len(search_keywords) > 1"
    ).fetchone()[0]
    logging.info(f"Groups with keywords beyond group_name_en: {with_extra_terms:,} / {count:,}")

    schema = con.execute("DESCRIBE minorities_terms").df()
    logging.info(f"Table schema:\n{schema.to_string(index=False)}")

    logging.info(f"Database location: {TERMS_DB}")

    con.close()


if __name__ == "__main__":
    main()
