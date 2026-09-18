"""Bulk dump sources: download once, then load into duckdb.

Unlike the incremental sources (rules/api_runner/), these are periodic full
snapshots — see src/sources/dumps/*/documentation/ for how each dump is versioned.
Re-run download rules manually when a new dump version is published rather
than on every snakemake run (a stale local copy doesn't look stale to
Snakemake — it only checks whether the file exists, not whether a newer
dump has been published upstream).

Every rule below declares its *real* artifact as output — the downloaded raw
path or the duckdb file itself, both from config/dumps.yaml — rather than a
touch() sentinel. That's possible here (unlike meta_heritage.smk) because each
dump has exactly one rule writing to exactly one dedicated file: no source
shares a file across multiple rule invocations the way api_runner sources
used to before they got split into one duckdb per query.

download_*.sh scripts take their target path/dir as $1 (the rule's `output:`)
and contain no SLURM directives or hardcoded paths — resourcing for the HPC
comes from each rule's `resources:` below (read by the slurm executor via
orchestration/profiles/slurm), not from #SBATCH headers in the script.
"""

DUMP_PATHS = get_dumps_paths()


rule download_ror_dump:
    output:
        DUMP_PATHS["ror_dump"]["path_raw"],
    resources:
        mem_mb=4000,
        runtime=30,
        cpus_per_task=2,
    shell:
        "bash src/sources/dumps/ror/download.sh {output}"


rule load_ror_dump:
    input:
        rules.download_ror_dump.output,
    output:
        DUMP_PATHS["ror_dump"]["path_duck"],
    shell:
        "python -m sources.dumps.ror.loader"


MINORITIES_DIR = "src/sources/dumps/minorities"


rule discover_minorities_candidates:
    # The one genuinely expensive, rate-limited step: a live Wikidata SPARQL
    # discovery query. Its own rule since it writes its own real, inspectable
    # artifact — see src/sources/dumps/minorities/README.md.
    output:
        DUMP_PATHS["minorities"]["path_raw"],
    shell:
        f"python {MINORITIES_DIR}/extract.py"


rule load_minorities:
    input:
        rules.discover_minorities_candidates.output,
    output:
        DUMP_PATHS["minorities"]["path_duck"],
    shell:
        f"python {MINORITIES_DIR}/loader.py"


rule load_oa_topics:
    # No download step: openalex_topic_mapping.csv is placed manually, not
    # fetched by this pipeline — see src/sources/dumps/oa_topics/loader.py.
    input:
        DUMP_PATHS["oa_topics"]["path_raw"],
    output:
        DUMP_PATHS["oa_topics"]["path_duck"],
    shell:
        "python -m sources.dumps.oa_topics.loader"


rule download_openaire_dump:
    # Output is a marker file inside path_raw, not path_raw itself. Without
    # it Snakemake's directory() deletes all data we already downloaded.
    output:
        DUMP_PATHS["openaire_dump"]["path_raw_marker"],
    resources:
        mem_mb=64000,
        runtime=4320,
        cpus_per_task=32,
    shell:
        "bash src/sources/dumps/openaire/download.sh {output}"


rule load_openaire_dump:
    input:
        rules.download_openaire_dump.output,
    output:
        DUMP_PATHS["openaire_dump"]["path_duck"],
    resources:
        mem_mb=200000,
        runtime=4320,
        cpus_per_task=32,
    shell:
        # Full load — no --limit. For a fast local test load, run
        # sources.dumps.openaire.loader --limit N directly (see
        # orchestration/README.md "Running Individually").
        "python -m sources.dumps.openaire.loader --mem-mb {resources.mem_mb} --threads {resources.cpus_per_task}"


rule stage_openaire_dump:
    input:
        rules.load_openaire_dump.output,
    output:
        DUMP_PATHS["openaire_dump"]["path_duck_staging_2"],
    resources:
        mem_mb=200000,
        runtime=4320,
        cpus_per_task=32,
    shell:
        "python -m sources.dumps.openaire.staging --mem-mb {resources.mem_mb} --threads {resources.cpus_per_task}"


# openalex_dump is corev5 scope — download rule kept for parity, not wired into `rule all`.
rule download_openalex_dump:
    output:
        directory(DUMP_PATHS["openalex_dump"]["path_raw"]),
    shell:
        "bash src/sources/dumps/openalex/download.sh"


# Report names, {name} in report_dump below -- also referenced from the
# Snakefile's rule all / sources_local / core_v3_sources so the exact
# reports/sources/dumps/<name>.md path only ever gets built in one place.
REPORT_NAME_ROR_DUMP = f"ror_dump_{DUMP_PATHS['ror_dump']['version']}"
REPORT_NAME_OPENAIRE_DUMP = f"openaire_dump_{DUMP_PATHS['openaire_dump']['version']}"
REPORT_NAME_OPENAIRE_DUMP_STAGING = f"openaire_dump_staging_2_{DUMP_PATHS['openaire_dump']['version']}"
REPORT_NAME_MINORITIES = "minorities"
REPORT_NAME_OA_TOPICS = "oa_topics"

DUMP_REPORT_INPUTS = {
    REPORT_NAME_ROR_DUMP: rules.load_ror_dump.output,
    REPORT_NAME_OPENAIRE_DUMP: rules.load_openaire_dump.output,
    REPORT_NAME_OPENAIRE_DUMP_STAGING: rules.stage_openaire_dump.output,
    REPORT_NAME_MINORITIES: rules.load_minorities.output,
    REPORT_NAME_OA_TOPICS: rules.load_oa_topics.output,
}


# One rule, one job per distinct {name} requested -- not one job that builds
# every report at once (same as load_source's per-source/query_id jobs).
rule report_dump:
    input:
        lambda wc: DUMP_REPORT_INPUTS[wc.name],
    output:
        "reports/sources/dumps/{name}.md",
    shell:
        "python -m common.report.generate_reports --only {output}"


rule ingest_dumps:
    input:
        f"reports/sources/dumps/{REPORT_NAME_ROR_DUMP}.md",
        f"reports/sources/dumps/{REPORT_NAME_OPENAIRE_DUMP_STAGING}.md",
