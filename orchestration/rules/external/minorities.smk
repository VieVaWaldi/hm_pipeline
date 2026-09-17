"""minorities — Wikidata minority-group discovery.

Structurally like dumps.smk, not meta_heritage.smk: each rule writes a real
file (pile CSV or duckdb), not a sentinel. Two steps: extract (SPARQL harvest
against Wikidata -> data/pile/minorities/minorities.csv) -> load (filter,
dedup, titular-majority drop, subgroup rollup, and Phase 2 term enrichment,
all in one script -> minorities_raw.duckdb, see
src/sources/external/minorities/loader.py).

extract is its own rule because it's the one genuinely expensive, rate-
limited step (a live Wikidata SPARQL discovery query) and already writes its
own real artifact worth keeping inspectable on its own. load used to be four
separate rules/duckdb files (raw / staging / staging_2 / terms) -- with the
pipeline stable at 304 groups, that granularity stopped earning its keep, so
those phases are now just functions run in sequence inside loader.py against
one connection, with only the final result written to disk.

Unlike meta_heritage, this *is* wired into `rule all` / sources_local, since
it's a one-shot harvest against a live external endpoint no different in kind
from an api_runner extract step, not an unattended-scale concern like the
openaire dump.
"""

MINORITIES_PATHS = get_external_paths()["minorities"]
MINORITIES_DIR = "src/sources/external/minorities"


rule discover_minorities_candidates:
    output:
        MINORITIES_PATHS["path_raw"],
    shell:
        f"python {MINORITIES_DIR}/extract.py"


rule load_minorities:
    input:
        rules.discover_minorities_candidates.output,
    output:
        MINORITIES_PATHS["path_duck"],
    shell:
        f"python {MINORITIES_DIR}/loader.py"
