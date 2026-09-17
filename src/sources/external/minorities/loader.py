"""
Minorities Loader — extract.py's CSV harvest -> minorities_raw.duckdb

One consolidated ELT step (extract stays its own script — see below) that used
to be four separate duckdb files (raw / staging / staging_2 / terms). With the
pipeline stable at 304 groups, keeping four near-identical-schema files around
stopped earning its keep; the phases below are now just functions run in
sequence against one connection, with only the final result ever written to
disk. Kept as separate *functions*, not inlined into one blob, so each phase
still reads and is testable on its own -- only the on-disk artifact merged.

extract.py stays a separate script/step: it's the one genuinely expensive,
rate-limited part (a live Wikidata SPARQL discovery query), and it already
writes its own real artifact (data/pile/minorities/minorities.csv) worth
keeping inspectable on its own, same as every other `sources/dumps` source in
this repo. enrich_terms() below also makes live Wikidata calls (the alias/
demonym harvest) -- that cost doesn't go away by inlining it, it's just no
longer possible to iterate on the SQL phases without also re-paying it.

Phases:

1. load_candidates() -- loads extract.py's CSV as-is (was loader.py).

2. stage_filter() -- three documented, reproducible filters (none of them a
   judgement about minority status): unresolved labels (group_name_en empty
   or just the QID echoed back), diaspora typing (Wikidata's own P3833 is
   set -- recent/dispersed migrant populations, not the territorially rooted
   groups this filter targets), and non-European-only (every listed country
   falls outside Europe -- an artifact of the "indigenous to" discovery
   query anchoring on continent rather than the strict country allowlist).
   Also merges duplicate Wikidata entries for the same group (was staging.py).

3. apply_titular_and_subgroups() -- two more passes: drops groups that are
   the titular/majority population of one of their own listed countries
   (Austrians -> Austria, ...) per titular_majority_overrides.csv (hand-
   classified against the actual data; a few genuinely ambiguous cases like
   Bosniaks or Flemish people are recorded there too, kept in with a note on
   why); then rolls up any remaining row whose part_of matches another
   surviving row (e.g. the Sami subgroups) into a known_subgroups column on
   the parent (was staging_2.py).

4. enrich_terms() -- Phase 2 (per planning/Plan.md): harvests self-
   designation terms for every surviving group from Wikidata -- native label
   (P1705), demonym (P1549), English aliases (skos:altLabel, lang="en") --
   plus manual_term_overrides.csv, which preserves terms verbatim that no
   Wikidata property could ever produce (Kat's original hand-curated pilot
   lists carry topically-related terms like "Hebrew"/"Yiddish" for Jewish
   people, and domain/cultural knowledge like "joik"/"yoik" for Sami people
   -- see manual_term_overrides.csv). Adds search_keywords: group_name_en +
   every harvested/manual term, deduped (was enrich_terms.py).

Writes the final result to `minorities_raw` in minorities_raw.duckdb
(path_duck) -- this is the table intended for actual use.
"""

import logging
import time
import unicodedata
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError

import duckdb
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

from common.database.duck.create_connection import create_duck_connection
from common.database.duck.utils import get_size_log
from common.file_handling.file_utils import ensure_path_exists
from common.config.external import get_external_paths
from common.log.logger import setup_logging
from common.log.timer import log_run_time

setup_logging("loader", "minorities")

config = get_external_paths()["minorities"]
MINORITIES_SOURCE = Path(config["path_raw"])
MINORITIES_DB = Path(config["path_duck"])
TITULAR_OVERRIDES_CSV = Path(__file__).resolve().parent / "titular_majority_overrides.csv"
TERM_OVERRIDES_CSV = Path(__file__).resolve().parent / "manual_term_overrides.csv"

NON_EUROPEAN_COUNTRIES = "'Mongolia', 'Kazakhstan'"

WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_USER_AGENT = "DIGICHer-MinorityDiscovery/0.2 (https://github.com/DIGICHer)"
LITERAL_PROPS = {
    "native_label": "P1705",
    "demonym": "P1549",
}

ensure_path_exists(MINORITIES_DB)


# --------------------------------------------------------------------------
# Phase 1: load
# --------------------------------------------------------------------------


def load_candidates(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE raw_candidates AS
        SELECT * FROM read_csv('{MINORITIES_SOURCE}', all_varchar=true)
        """
    )


# --------------------------------------------------------------------------
# Phase 2: filter + dedup (was staging.py)
# --------------------------------------------------------------------------


def stage_filter(con: duckdb.DuckDBPyConnection) -> None:
    # Flattens every "|"-joined value across the rows in a GROUP BY, trims
    # whitespace, drops empties, dedupes, and re-sorts into a native
    # LIST(VARCHAR).
    con.execute(
        """
        CREATE OR REPLACE MACRO merge_multi(col) AS (
            list_sort(
                list_distinct(
                    list_filter(
                        flatten(list(list_transform(string_split(col, '|'), x -> trim(x)))),
                        v -> v <> ''
                    )
                )
            )
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE staged AS
        WITH base AS (
            SELECT
                qid,
                COALESCE(group_name_en, '') AS group_name_en,
                COALESCE(countries, '') AS countries,
                COALESCE(source_class, '') AS source_class,
                TRY_CAST(population AS DOUBLE) AS population,
                COALESCE(religions, '') AS religions,
                COALESCE(native_languages, '') AS native_languages,
                COALESCE(part_of, '') AS part_of,
                COALESCE(subclass_of, '') AS subclass_of,
                COALESCE(diaspora, '') AS diaspora,
                COALESCE(ancestral_home, '') AS ancestral_home,
                COALESCE(admin_territory, '') AS admin_territory,
                COALESCE(has_parts, '') AS has_parts
            FROM raw_candidates
        ),
        flagged AS (
            SELECT
                *,
                list_transform(string_split(countries, '|'), x -> trim(x)) AS country_list
            FROM base
        ),
        survivors AS (
            SELECT * EXCLUDE (country_list)
            FROM flagged
            WHERE NOT (
                -- Filter A: unresolved label
                (trim(group_name_en) = '' OR regexp_matches(trim(group_name_en), '^Q[0-9]+$'))
                -- Filter B: diaspora typing
                OR trim(diaspora) <> ''
                -- Filter C: non-European only
                OR (
                    len(country_list) > 0
                    AND len(list_filter(country_list, x -> x NOT IN ({NON_EUROPEAN_COUNTRIES}))) = 0
                )
            )
        )
        SELECT
            MIN(qid) AS qid,
            list_sort(list(DISTINCT qid)) AS merged_qids,
            group_name_en,
            merge_multi(countries) AS countries,
            merge_multi(source_class) AS source_class,
            MAX(population) AS population,
            merge_multi(religions) AS religions,
            merge_multi(native_languages) AS native_languages,
            merge_multi(part_of) AS part_of,
            merge_multi(subclass_of) AS subclass_of,
            merge_multi(diaspora) AS diaspora,
            merge_multi(ancestral_home) AS ancestral_home,
            merge_multi(admin_territory) AS admin_territory,
            merge_multi(has_parts) AS has_parts
        FROM survivors
        GROUP BY group_name_en
        ORDER BY group_name_en
        """
    )


# --------------------------------------------------------------------------
# Phase 3: titular-majority filter + subgroup rollup (was staging_2.py)
# --------------------------------------------------------------------------


def apply_titular_and_subgroups(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE titular_overrides AS
        SELECT * FROM read_csv('{TITULAR_OVERRIDES_CSV}', header=true)
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE refined AS
        WITH after_majority AS (
            SELECT s.*
            FROM staged s
            LEFT JOIN titular_overrides o ON s.qid = o.qid AND o.is_titular_majority
            WHERE o.qid IS NULL
        ),
        -- One row per (child, candidate parent name) from part_of.
        candidate_children AS (
            SELECT
                r.qid AS child_qid,
                r.group_name_en AS child_name,
                p AS parent_name
            FROM after_majority r, UNNEST(r.part_of) AS t(p)
        ),
        -- Keep only candidates whose parent name matches another surviving row.
        matched_children AS (
            SELECT c.child_qid, c.child_name, a.qid AS parent_qid
            FROM candidate_children c
            JOIN after_majority a ON c.parent_name = a.group_name_en AND a.qid <> c.child_qid
        ),
        -- A child can match more than one surviving parent; pick deterministically.
        resolved_children AS (
            SELECT child_qid, child_name, parent_qid
            FROM (
                SELECT *, row_number() OVER (PARTITION BY child_qid ORDER BY parent_qid) AS rn
                FROM matched_children
            )
            WHERE rn = 1
        ),
        parent_subgroups AS (
            SELECT
                parent_qid,
                list(struct_pack(name := child_name, qid := child_qid) ORDER BY child_name) AS known_subgroups
            FROM resolved_children
            GROUP BY parent_qid
        )
        SELECT
            a.*,
            COALESCE(ps.known_subgroups, []::STRUCT(name VARCHAR, qid VARCHAR)[]) AS known_subgroups
        FROM after_majority a
        LEFT JOIN parent_subgroups ps ON a.qid = ps.parent_qid
        LEFT JOIN resolved_children rc ON a.qid = rc.child_qid
        WHERE rc.child_qid IS NULL
        ORDER BY a.group_name_en
        """
    )


# --------------------------------------------------------------------------
# Phase 4: term enrichment (was enrich_terms.py)
# --------------------------------------------------------------------------


def _run_sparql_query(sparql_query: str, description: str) -> list[dict]:
    sparql = SPARQLWrapper(WIKIDATA_ENDPOINT)
    sparql.addCustomHttpHeader("User-Agent", WIKIDATA_USER_AGENT)
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


def _extract_qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1] if uri else ""


def _is_latin_script(text: str) -> bool:
    for ch in text:
        if ch.isalpha():
            try:
                if "LATIN" not in unicodedata.name(ch):
                    return False
            except ValueError:
                return False
    return True


def _fetch_literal_property(name: str, prop: str, qids: list[str]) -> pd.DataFrame:
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?group ?value WHERE {{
      VALUES ?group {{ {values} }}
      ?group wdt:{prop} ?value .
    }}
    """
    rows = _run_sparql_query(query, f"literal: {name} ({prop})")
    if not rows:
        return pd.DataFrame(columns=["qid", name])
    df = pd.DataFrame(rows)
    df["qid"] = df["group"].apply(_extract_qid)
    return (
        df.groupby("qid")["value"]
        .apply(lambda s: [v for v in dict.fromkeys(s) if v])
        .reset_index()
        .rename(columns={"value": name})
    )


def _fetch_english_aliases(qids: list[str]) -> pd.DataFrame:
    values = " ".join(f"wd:{q}" for q in qids)
    query = f"""
    SELECT ?group ?value WHERE {{
      VALUES ?group {{ {values} }}
      ?group skos:altLabel ?value .
      FILTER(lang(?value) = "en")
    }}
    """
    rows = _run_sparql_query(query, "literal: english aliases (skos:altLabel)")
    if not rows:
        return pd.DataFrame(columns=["qid", "aliases_en"])
    df = pd.DataFrame(rows)
    df["qid"] = df["group"].apply(_extract_qid)
    return (
        df.groupby("qid")["value"]
        .apply(lambda s: [v for v in dict.fromkeys(s) if v])
        .reset_index()
        .rename(columns={"value": "aliases_en"})
    )


def enrich_terms(con: duckdb.DuckDBPyConnection) -> None:
    qids = con.execute("SELECT qid FROM refined").fetchdf()["qid"].tolist()
    logging.info(f"Enriching {len(qids)} groups with Wikidata self-designation terms")

    terms = pd.DataFrame({"qid": qids})
    for name, prop in LITERAL_PROPS.items():
        dim = _fetch_literal_property(name, prop, qids)
        terms = terms.merge(dim, on="qid", how="left")
        time.sleep(2)
    aliases = _fetch_english_aliases(qids)
    terms = terms.merge(aliases, on="qid", how="left")

    for col in ("native_label", "demonym", "aliases_en"):
        terms[col] = terms[col].apply(
            lambda v: [term for term in v if _is_latin_script(term)] if isinstance(v, list) else []
        )

    con.register("terms_df", terms)
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE term_overrides AS
        SELECT * FROM read_csv('{TERM_OVERRIDES_CSV}', header=true)
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE minorities_raw AS
        WITH manual AS (
            SELECT qid, list(DISTINCT term) AS manual_terms
            FROM term_overrides
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
        FROM refined b
        LEFT JOIN terms_df t ON b.qid = t.qid
        LEFT JOIN manual m ON b.qid = m.qid
        """
    )


# --------------------------------------------------------------------------


def main():
    start_time = datetime.now()
    logging.info("MINORITIES LOADER")
    logging.info(f"Source: {MINORITIES_SOURCE}")
    logging.info(f"Titular overrides: {TITULAR_OVERRIDES_CSV}")
    logging.info(f"Term overrides: {TERM_OVERRIDES_CSV}")
    logging.info(f"Target: {MINORITIES_DB}")

    con = create_duck_connection(str(MINORITIES_DB))
    load_candidates(con)
    stage_filter(con)
    apply_titular_and_subgroups(con)
    enrich_terms(con)

    log_run_time(start_time)

    """ VERIFY """
    count = con.execute("SELECT COUNT(*) FROM minorities_raw").fetchone()[0]
    logging.info(f"Total groups: {count:,}")

    rolled_up = con.execute("SELECT COUNT(*) FROM minorities_raw WHERE len(known_subgroups) > 0").fetchone()[0]
    logging.info(f"Groups with rolled-up subgroups: {rolled_up:,}")

    with_extra_terms = con.execute(
        "SELECT COUNT(*) FROM minorities_raw WHERE len(search_keywords) > 1"
    ).fetchone()[0]
    logging.info(f"Groups with keywords beyond group_name_en: {with_extra_terms:,} / {count:,}")

    schema = con.execute("DESCRIBE minorities_raw").df()
    logging.info(f"Table schema:\n{schema.to_string(index=False)}")

    logging.info(get_size_log(MINORITIES_SOURCE, MINORITIES_DB))
    logging.info(f"Database location: {MINORITIES_DB}")

    con.close()


if __name__ == "__main__":
    main()
