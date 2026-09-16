"""Incremental loading — one rule invocation per (source, query_id), duckdb by default.

Wraps common/api_runner/run_loader.py. Depends on the matching extract checkpoint so
`snakemake` won't load a source before it's been extracted at least once.

Only for sources/apis/ — see extract.smk.
"""


def _load_targets():
    settings = get_query_settings()
    targets = []
    for source in QUERY_SOURCES:
        for query_id in settings[source].queries:
            targets.append(f"data/checkpoints/loading/{source}_{query_id}/mtime.cp")
    return targets


rule load_incremental_sources:
    input:
        _load_targets(),


rule load_source:
    input:
        lambda wc: (
            f"data/checkpoints/extractor/{wc.source}-query_id-{wc.query_id}/"
            f"{get_query_settings()[wc.source].checkpoint}.cp"
        ),
    output:
        # {source} is constrained to exclude "_" — query names contain underscores
        # (e.g. heritage_digital_humanities), so without this the wildcard split is ambiguous.
        "data/checkpoints/loading/{source,[^_]+}_{query_id}/mtime.cp",
    shell:
        "python -m common.api_runner.run_loader --source {wildcards.source} --query_id {wildcards.query_id} --db duck"
