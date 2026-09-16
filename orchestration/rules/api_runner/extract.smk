"""Incremental extraction — one rule invocation per (source, query_id).

Wraps common/api_runner/run_extractor.py, which owns its own checkpointing
(see common/api_runner/checkpoint_manager.py) and loops until it catches up to
the present. The checkpoint file is used as the rule's output so re-running
`snakemake` only re-extracts sources whose checkpoint is stale or missing.

Only for sources/apis/ — the sources that use the incremental checkpointed
runner. Dumps and meta_heritage don't checkpoint like this, see dumps.smk and
meta_heritage.smk.
"""


def _extract_targets():
    settings = get_query_settings()
    targets = []
    for source in QUERY_SOURCES:
        checkpoint_name = settings[source].checkpoint
        for query_id in settings[source].queries:
            targets.append(
                f"data/checkpoints/extractor/{source}-query_id-{query_id}/{checkpoint_name}.cp"
            )
    return targets


rule extract_all_sources:
    input:
        _extract_targets(),


rule extract_source:
    output:
        "data/checkpoints/extractor/{source}-query_id-{query_id}/{checkpoint_name}.cp",
    shell:
        "python -m common.api_runner.run_extractor --source {wildcards.source} --query_id {wildcards.query_id}"
