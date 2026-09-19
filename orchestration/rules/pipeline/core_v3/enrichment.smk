"""core_v3 enrichment rules.

core_v3 is frozen as a data model (see src/pipelines/core_v3/README.md) but
its merge output is still real, real-schema data — useful as the current
proving ground for enrichment before core_v4 exists. Each script here reads
its db path from config/pipelines.yaml's core_v3 entry directly (see
src/pipelines/core_v3/enrichment/), not via a --db-path flag, since this glue
is genuinely core_v3-specific, not generic. core_v4 gets its own
rules/pipeline/core_v4/enrichment.smk once it has real tables — this one
doesn't get repointed at core_v4 later, a new one gets added alongside it.

Each enrichment writes straight into that duckdb file (ALTER/UPDATE in place),
so there's no output file to track — a touch() sentinel under
.snakemake/sentinels/enrichment/core_v3/ stands in instead. Delete a sentinel
to force that one enrichment to rerun. Not part of `rule all` — run by name:
    uv run snakemake -s orchestration/Snakefile topic_modelling
"""


rule seed_topics:
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/seed_topics_done"),
    shell:
        "uv run python -m pipelines.core_v3.enrichment.seed_topics"


rule topic_modelling:
    input:
        rules.seed_topics.output,
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/topic_modelling_done"),
    shell:
        "uv run python -m pipelines.core_v3.enrichment.topic_modelling"


rule geolocation:
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/geolocation_done"),
    shell:
        "uv run python -m pipelines.core_v3.enrichment.geolocation"


rule dch_classification:
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/dch_classification_done"),
    shell:
        "uv run python -m pipelines.core_v3.enrichment.dch_classification"
