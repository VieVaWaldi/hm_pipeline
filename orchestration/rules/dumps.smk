"""Bulk dump sources: download once, then load into duckdb.

Unlike the incremental sources (rules/api_runner/), these are periodic full
snapshots — see src/sources/dumps/*/documentation/ for how each dump is versioned.
Re-run download rules manually when a new dump version is published rather
than on every snakemake run (a stale local copy doesn't look stale to
Snakemake — it only checks whether the file exists, not whether a newer
dump has been published upstream).

Every rule below declares its *real* artifact as output — the downloaded raw
path or the duckdb file itself, both from config/paths.yaml — rather than a
touch() sentinel. That's possible here (unlike meta_heritage.smk) because each
dump has exactly one rule writing to exactly one dedicated file: no source
shares a file across multiple rule invocations the way api_runner sources
used to before they got split into one duckdb per query.
"""

DUMP_PATHS = get_source_paths()


rule download_ror_dump:
    output:
        DUMP_PATHS["ror_dump"]["path_raw"],
    shell:
        "bash src/sources/dumps/ror_dump/download.sh"


rule load_ror_dump:
    input:
        rules.download_ror_dump.output,
    output:
        DUMP_PATHS["ror_dump"]["path_duck"],
    shell:
        "python -m sources.dumps.ror_dump.loader"


rule download_openaire_dump:
    output:
        directory(DUMP_PATHS["openaire_dump"]["path_raw"]),
    shell:
        "bash src/sources/dumps/openaire_dump/download.sh"


rule load_openaire_dump:
    input:
        rules.download_openaire_dump.output,
    output:
        DUMP_PATHS["openaire_dump"]["path_duck"],
    shell:
        "python -m sources.dumps.openaire_dump.loader"


rule stage_openaire_dump:
    input:
        rules.load_openaire_dump.output,
    output:
        DUMP_PATHS["openaire_dump"]["path_duck_staging_2"],
    shell:
        "python -m sources.dumps.openaire_dump.staging"


# openalex_dump is corev5 scope — download rule kept for parity, not wired into `rule all`.
rule download_openalex_dump:
    output:
        directory(DUMP_PATHS["openalex_dump"]["path_raw"]),
    shell:
        "bash src/sources/dumps/openalex_dump/download.sh"


rule ingest_dumps:
    input:
        rules.load_ror_dump.output,
        rules.stage_openaire_dump.output,
