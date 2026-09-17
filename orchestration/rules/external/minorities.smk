"""minorities — Wikidata minority-group discovery.

Structurally like dumps.smk, not meta_heritage.smk: each rule writes a real
file (pile CSV or duckdb), not a sentinel. Five sequential phases: extract
(SPARQL harvest against Wikidata -> data/pile/minorities/minorities.csv) ->
load (CSV -> minorities_raw.duckdb, unmodified) -> stage (filter + dedup in
SQL -> minorities_staging.duckdb, see src/sources/external/minorities/staging.py)
-> stage 2 (drop titular-majority groups + roll up subgroups -> minorities_staging_2.duckdb,
see src/sources/external/minorities/staging_2.py) -> terms (Phase 2 self-
designation harvest -> minorities_terms.duckdb, see
src/sources/external/minorities/enrich_terms.py).

Unlike meta_heritage, this *is* wired into `rule all` / sources_local, since
it's a one-shot harvest against a live external endpoint no different in kind
from an api_runner extract step, not an unattended-scale concern like the
openaire dump.
"""

MINORITIES_PATHS = get_dumps_paths()["minorities"]
MINORITIES_DIR = "src/sources/external/minorities"


rule discover_minorities_candidates:
    output:
        MINORITIES_PATHS["path_raw"],
    shell:
        f"python {MINORITIES_DIR}/extract.py"


rule load_minorities_raw:
    input:
        rules.discover_minorities_candidates.output,
    output:
        MINORITIES_PATHS["path_duck"],
    shell:
        f"python {MINORITIES_DIR}/loader.py"


rule stage_minorities_candidates:
    input:
        rules.load_minorities_raw.output,
    output:
        MINORITIES_PATHS["path_duck_staging"],
    shell:
        f"python {MINORITIES_DIR}/staging.py"


rule stage_minorities_candidates_2:
    input:
        rules.stage_minorities_candidates.output,
    output:
        MINORITIES_PATHS["path_duck_staging_2"],
    shell:
        f"python {MINORITIES_DIR}/staging_2.py"


rule enrich_minorities_terms:
    input:
        rules.stage_minorities_candidates_2.output,
    output:
        MINORITIES_PATHS["path_duck_terms"],
    shell:
        f"python {MINORITIES_DIR}/enrich_terms.py"
