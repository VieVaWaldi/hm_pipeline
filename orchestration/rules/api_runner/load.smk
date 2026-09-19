"""Incremental loading — one rule invocation per (source, query_id), duckdb by default.

Wraps common/api_runner/run_loader.py. Depends on the matching extract checkpoint so
`snakemake` won't load a source before it's been extracted at least once.

Only for sources/apis/ — see extract.smk.
"""


def _load_targets(exclude=(), only_queries=None):
    """only_queries: optional {source: [query_id, ...]} to restrict a source to
    a subset of its configured queries instead of all of them."""
    settings = get_query_settings()
    only_queries = only_queries or {}
    targets = []
    for source in QUERY_SOURCES:
        if source in exclude:
            continue
        for query_id in only_queries.get(source, settings[source].queries):
            targets.append(f"data/checkpoints/loading/{source}_{query_id}/mtime.cp")
    return targets


def _report_targets(exclude=(), only_queries=None):
    """Same selection as _load_targets, but yields each (source, query_id)'s
    report path instead of its load checkpoint -- requesting the report
    transitively requires the load first (see report_source)."""
    settings = get_query_settings()
    only_queries = only_queries or {}
    targets = []
    for source in QUERY_SOURCES:
        if source in exclude:
            continue
        for query_id in only_queries.get(source, settings[source].queries):
            targets.append(f"reports/sources/apis/{source}/{query_id}.md")
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
    log:
        str(LOGGING_PATH / "load_source_{source}_{query_id}.log"),
    shell:
        "python -m common.api_runner.run_loader --source {wildcards.source} --query_id {wildcards.query_id} --db duck &> {log}"


rule report_source:
    # Mirrors dumps.smk's report_dump: one job per distinct (source, query_id)
    # requested, not one job that builds every api report at once.
    input:
        "data/checkpoints/loading/{source}_{query_id}/mtime.cp",
    output:
        "reports/sources/apis/{source}/{query_id}.md",
    log:
        str(LOGGING_PATH / "report_source_{source}_{query_id}.log"),
    resources:
        # api duckdbs are small (~GB); the caps are so DuckDB respects this allocation
        # rather than the node's RAM, and runtime avoids the 48 min profile default.
        mem_mb=32000,
        cpus_per_task=8,
        runtime=240,
    shell:
        "python -m common.report.generate_reports --only {output} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"
