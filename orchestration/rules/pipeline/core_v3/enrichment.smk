"""core_v3 enrichment rules.

core_v3 is frozen as a data model (see src/pipelines/core_v3/README.md) but
its merge output is still real, real-schema data — useful as the current
proving ground for enrichment before core_v4 exists. Each script here reads
its db path from config/pipelines.yaml's core_v3 entry directly (see
src/pipelines/core_v3/enrichment/), not via a --db-path flag, since this glue
is genuinely core_v3-specific, not generic. core_v4 gets its own
rules/pipeline/core_v4/enrichment.smk once it has real tables — this one
doesn't get repointed at core_v4 later, a new one gets added alongside it.

All of them write into the same duckdb file, which is single-writer — run in
parallel (Snakemake would, with --cores > 1) they die on the file lock. So they
form one chain: seed_topics -> topic_modelling -> geolocation -> dch_classification.
Only seed_topics -> topic_modelling is a real data dependency; the rest are
ordered purely to serialize writes.

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
    input:
        rules.topic_modelling.output,
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/geolocation_done"),
    shell:
        "uv run python -m pipelines.core_v3.enrichment.geolocation"


rule dch_classification:
    input:
        rules.geolocation.output,
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/dch_classification_done"),
    resources:
        slurm_partition="gpu-test",  # 12h limit, idle 80GB A100s; switch to "gpu" (runtime up to 4320) for long runs
        gres="gpu:a100:1",  # single GPU: dch_classifier only uses cuda:0
        constraint="a100_80gb",  # batch 2048 x 512 tokens is sized for 80GB VRAM
        cpus_per_task=16,
        mem_mb=128000,  # all_text_rows() holds every row in RAM; ample for entity=project
        runtime=720,  # gpu-test max is 12h; resumable, ~25 min expected for ~4M rows. --entity work needs partition gpu + more
    shell:
        "uv run python -m pipelines.core_v3.enrichment.dch_classification"
