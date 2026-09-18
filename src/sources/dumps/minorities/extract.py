"""Phase 1: Discover candidate minority groups from Wikidata.

Strategy (v2):
  1. Fetch core group list per P31 (instance of) class using an explicit European country
     allowlist (VALUES clause) instead of continent=Europe. This excludes
     Siberian/Caucasian groups that entered via Russia's P30=Europe.
  2. Also run a P2341 (indigenous to) query anchored on P30=Europe, which
     correctly catches groups like Sámi subgroups whose homeland is Sápmi.
  3. Filter "X in Y" sub-population entries (e.g. "Albanians in Norway") —
     these are diaspora instances of a parent group, not distinct minorities.
  4. Add manual seeds for pilot groups invisible to SPARQL discovery:
       Ladin (Q1799968)      — no P17/P2341 set in Wikidata
       Sámi people (Q48199) — P2341=Sápmi (region, not a state); no P17
       Jewish people (Q7325) — P31=nation/ethnoreligious; P17=Israel only
  5. Enrich all discovered + seed QIDs with filter dimensions via VALUES-based
     queries (so seeds are enriched automatically, no second pass needed).
"""

import re
import time
import logging
from pathlib import Path
from urllib.error import HTTPError

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

from common.config.dumps import get_dumps_paths

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = "DIGICHer-MinorityDiscovery/0.2 (https://github.com/DIGICHer)"

# ---------------------------------------------------------------------------
# European country allowlist
# EU27 + EEA (Norway, Iceland, Liechtenstein) + UK + Western Balkans +
# Eastern Partnership countries with EU candidate status (Ukraine, Moldova,
# Georgia). Deliberately excludes Russia to avoid Siberian/Caucasian groups.
# ---------------------------------------------------------------------------
EUROPEAN_COUNTRY_QIDS = [
    # EU27
    "Q40",  # Austria
    "Q31",  # Belgium
    "Q219",  # Bulgaria
    "Q224",  # Croatia
    "Q229",  # Cyprus
    "Q213",  # Czech Republic
    "Q35",  # Denmark
    "Q191",  # Estonia
    "Q33",  # Finland
    "Q142",  # France
    "Q183",  # Germany
    "Q41",  # Greece
    "Q28",  # Hungary
    "Q27",  # Ireland
    "Q38",  # Italy
    "Q211",  # Latvia
    "Q37",  # Lithuania
    "Q32",  # Luxembourg
    "Q233",  # Malta
    "Q55",  # Netherlands
    "Q36",  # Poland
    "Q45",  # Portugal
    "Q218",  # Romania
    "Q214",  # Slovakia
    "Q215",  # Slovenia
    "Q29",  # Spain
    "Q34",  # Sweden
    # EEA (non-EU)
    "Q20",  # Norway
    "Q189",  # Iceland
    "Q347",  # Liechtenstein
    # Other Western Europe
    "Q39",  # Switzerland
    "Q145",  # United Kingdom
    # Western Balkans
    "Q222",  # Albania
    "Q225",  # Bosnia and Herzegovina
    "Q232",  # Kosovo
    "Q221",  # North Macedonia
    "Q236",  # Montenegro
    "Q403",  # Serbia
    # Eastern Partnership / EU candidates
    "Q212",  # Ukraine
    "Q711",  # Moldova
    "Q230",  # Georgia (country)
]

COUNTRY_VALUES = " ".join(f"wd:{q}" for q in EUROPEAN_COUNTRY_QIDS)

# ---------------------------------------------------------------------------
# P31 classes to discover
# ---------------------------------------------------------------------------
CLASSES = {
    "Q41710": "ethnic group",
    "Q133311": "tribe",
    "Q1153484": "indigenous people",
    "Q11197007": "ethnoreligious group",
}

# ---------------------------------------------------------------------------
# Manual seeds: pilot groups structurally invisible to SPARQL discovery.
# These are added after core queries and receive full dimension enrichment.
# ---------------------------------------------------------------------------
MANUAL_SEEDS = {
    "Q1799968": {
        "group_name_en": "Ladins",
        "countries": "Italy",
        "source_class": "manual_seed",
    },
    "Q48199": {
        "group_name_en": "Sámi people",
        "countries": "Finland | Norway | Russia | Sweden",
        "source_class": "manual_seed",
    },
    "Q7325": {
        "group_name_en": "Jewish people",
        "countries": "(global — see subgroups in dataset)",
        "source_class": "manual_seed",
    },
}

# Sub-population pattern: "X in Y" (e.g. "Albanians in Norway")
_SUBPOP_RE = re.compile(r"\s+in\s+\S", re.IGNORECASE)

# ---------------------------------------------------------------------------
# Core queries (group + label + countries)
# ---------------------------------------------------------------------------

CORE_QUERY_TEMPLATE = """
SELECT ?group ?groupLabel
       (GROUP_CONCAT(DISTINCT ?countryLabel; SEPARATOR=" | ") AS ?countries)
WHERE {{
  ?group wdt:P31 wd:{class_qid} .
  ?group wdt:P17 ?country .
  VALUES ?country {{ {country_values} }}
  SERVICE wikibase:label {{
    bd:serviceParam wikibase:language "en" .
    ?group rdfs:label ?groupLabel .
    ?country rdfs:label ?countryLabel .
  }}
}}
GROUP BY ?group ?groupLabel
"""

# Separate anchor for indigenous groups: P2341 (indigenous to) -> P30=Europe.
# Kept with continent filter (not allowlist) because homelands like Sápmi
# are regions with P30=Europe, not sovereign states in the allowlist.
INDIGENOUS_TO_CORE_QUERY = """
SELECT ?group ?groupLabel
       (GROUP_CONCAT(DISTINCT ?regionLabel; SEPARATOR=" | ") AS ?countries)
WHERE {
  ?group wdt:P31/wdt:P279* wd:Q41710 .
  ?group wdt:P2341 ?region .
  ?region wdt:P30 wd:Q46 .
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "en" .
    ?group rdfs:label ?groupLabel .
    ?region rdfs:label ?regionLabel .
  }
}
GROUP BY ?group ?groupLabel
"""

# ---------------------------------------------------------------------------
# Dimension properties — fetched for all QIDs after discovery
# ---------------------------------------------------------------------------
DIMENSION_PROPS = {
    "population": ("P1082", "numeric"),
    "religions": ("P140", "label"),
    "native_languages": ("P103", "label"),
    "part_of": ("P361", "label"),
    "subclass_of": ("P279", "label"),
    "diaspora": ("P3833", "label"),
    "ancestral_home": ("P66", "label"),
    "admin_territory": ("P131", "label"),
    "has_parts": ("P527", "label"),
}


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def run_query(sparql_query: str, description: str) -> list[dict]:
    """Execute a SPARQL query and return results as a list of dicts."""
    sparql = SPARQLWrapper(ENDPOINT)
    sparql.addCustomHttpHeader("User-Agent", USER_AGENT)
    sparql.setQuery(sparql_query)
    sparql.setReturnFormat(JSON)

    log.info("Running query: %s ...", description)
    try:
        results = sparql.query().convert()
    except HTTPError as exc:
        log.error("  Query failed: %s", exc)
        return []

    rows = []
    for binding in results["results"]["bindings"]:
        row = {}
        for var in results["head"]["vars"]:
            cell = binding.get(var, {})
            row[var] = cell.get("value", "")
        rows.append(row)

    log.info("  -> %d results", len(rows))
    return rows


def extract_qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1] if uri else ""


# ---------------------------------------------------------------------------
# Discovery steps
# ---------------------------------------------------------------------------


def fetch_core_groups() -> pd.DataFrame:
    """Fetch core group list (QID + label + countries) from all class types."""
    all_rows: list[dict] = []

    for qid, label in CLASSES.items():
        query = CORE_QUERY_TEMPLATE.format(class_qid=qid, country_values=COUNTRY_VALUES)
        rows = run_query(query, f"core: instance of {label} ({qid})")
        for r in rows:
            r["source_class"] = label
        all_rows.extend(rows)
        time.sleep(2)

    rows = run_query(
        INDIGENOUS_TO_CORE_QUERY, "core: indigenous to European region (P2341)"
    )
    for r in rows:
        r["source_class"] = "indigenous_to_europe"
    all_rows.extend(rows)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["qid"] = df["group"].apply(extract_qid)
    df = df.rename(columns={"groupLabel": "group_name_en"})

    agg = (
        df.groupby("qid")
        .agg(
            group_name_en=("group_name_en", "first"),
            countries=(
                "countries",
                lambda s: " | ".join(sorted(set(" | ".join(s).split(" | ")))),
            ),
            source_class=("source_class", lambda s: " | ".join(sorted(set(s)))),
        )
        .reset_index()
    )
    log.info("Core groups after dedup: %d", len(agg))
    return agg


def filter_subpopulations(df: pd.DataFrame) -> pd.DataFrame:
    """Remove 'X in Y' sub-population entries (e.g. 'Albanians in Norway').

    These are Wikidata instances representing a specific ethnic group's
    presence in a particular country, not distinct minority groups.
    """
    mask = df["group_name_en"].apply(lambda n: bool(_SUBPOP_RE.search(str(n))))
    n_dropped = mask.sum()
    if n_dropped:
        sample = df.loc[mask, "group_name_en"].iloc[0]
        log.info("Dropping %d sub-population entries (e.g. '%s')", n_dropped, sample)
    return df[~mask].reset_index(drop=True)


def add_manual_seeds(df: pd.DataFrame) -> pd.DataFrame:
    """Inject pilot group seeds not reachable via SPARQL discovery."""
    rows = []
    for qid, meta in MANUAL_SEEDS.items():
        if qid in df["qid"].values:
            idx = df.index[df["qid"] == qid][0]
            existing = df.at[idx, "source_class"]
            if "manual_seed" not in existing:
                df.at[idx, "source_class"] = existing + " | manual_seed"
            log.info("Seed already discovered: %s (%s)", meta["group_name_en"], qid)
        else:
            rows.append({"qid": qid, **meta})
            log.info("Adding manual seed: %s (%s)", meta["group_name_en"], qid)
    if rows:
        df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    return df


def fetch_dimension(name: str, prop: str, kind: str, qids: list[str]) -> pd.DataFrame:
    """Fetch one filter dimension for a given set of QIDs."""
    values = " ".join(f"wd:{q}" for q in qids)

    if kind == "numeric":
        query = f"""
        SELECT ?group ?value WHERE {{
          VALUES ?group {{ {values} }}
          ?group wdt:{prop} ?value .
        }}
        """
        rows = run_query(query, f"dimension: {name} ({prop})")
        if not rows:
            return pd.DataFrame(columns=["qid", name])
        df = pd.DataFrame(rows)
        df["qid"] = df["group"].apply(extract_qid)
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return (
            df.groupby("qid")["value"]
            .max()
            .reset_index()
            .rename(columns={"value": name})
        )
    else:
        query = f"""
        SELECT ?group ?valueLabel WHERE {{
          VALUES ?group {{ {values} }}
          ?group wdt:{prop} ?value .
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
        }}
        """
        rows = run_query(query, f"dimension: {name} ({prop})")
        if not rows:
            return pd.DataFrame(columns=["qid", name])
        df = pd.DataFrame(rows)
        df["qid"] = df["group"].apply(extract_qid)
        return (
            df.groupby("qid")["valueLabel"]
            .apply(lambda s: " | ".join(sorted(set(s))))
            .reset_index()
            .rename(columns={"valueLabel": name})
        )


def discover() -> pd.DataFrame:
    """Run full discovery pipeline and return enriched DataFrame."""
    # Step 1: core groups via SPARQL
    core = fetch_core_groups()
    if core.empty:
        log.warning("No core groups found.")
        return core

    # Step 2: drop sub-population noise
    before = len(core)
    core = filter_subpopulations(core)
    log.info(
        "Sub-population filter: %d -> %d (dropped %d)",
        before,
        len(core),
        before - len(core),
    )

    # Step 3: inject manual seeds
    core = add_manual_seeds(core)
    log.info("Total after seeds: %d groups", len(core))

    # Step 4: enrich all QIDs with filter dimensions
    qids = core["qid"].tolist()
    for name, (prop, kind) in DIMENSION_PROPS.items():
        dim_df = fetch_dimension(name, prop, kind, qids)
        if not dim_df.empty:
            core = core.merge(dim_df, on="qid", how="left")
        else:
            core[name] = ""
        time.sleep(2)

    str_cols = core.select_dtypes(include="object").columns
    core[str_cols] = core[str_cols].fillna("")

    core = core.sort_values("group_name_en", na_position="last").reset_index(drop=True)
    return core


def main():
    out_path = Path(get_dumps_paths()["minorities"]["path_raw"])
    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = discover()
    if df.empty:
        log.error("No candidate groups found. Check queries and endpoint.")
        return

    df.to_csv(out_path, index=False)
    log.info("Wrote %d candidate groups to %s", len(df), out_path)

    log.info("--- Summary ---")
    log.info("Total unique groups: %d", len(df))

    log.info("Source class breakdown:")
    for cls, count in df["source_class"].value_counts().items():
        log.info("  %s: %d", cls, count)

    log.info("Dimension fill rates:")
    for col in DIMENSION_PROPS:
        if col in df.columns:
            n = (df[col].notna() & (df[col] != "")).sum()
            pct = 100 * n / len(df) if len(df) else 0
            log.info("  %-20s %d / %d (%.0f%%)", col, n, len(df), pct)

    log.info("Pilot group check:")
    for qid, meta in MANUAL_SEEDS.items():
        row = df[df["qid"] == qid]
        if row.empty:
            log.warning("  MISSING: %s (%s)", meta["group_name_en"], qid)
        else:
            r = row.iloc[0]
            log.info(
                "  FOUND: %s (%s) | countries=%s | lang=%s | religion=%s",
                r["group_name_en"],
                qid,
                r.get("countries", ""),
                r.get("native_languages", ""),
                r.get("religions", ""),
            )


if __name__ == "__main__":
    main()
