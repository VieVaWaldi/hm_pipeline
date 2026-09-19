"""core_v3 enrichment rules.

core_v3 is frozen as a data model (see src/pipelines/core_v3/README.md) but
its merge output is still real, real-schema data — useful as the current
proving ground for enrichment before core_v4 exists. Each script here reads
its db paths from config/pipelines.yaml's core_v3 entry directly (see
src/pipelines/core_v3/enrichment/), not via a --db-path flag, since this glue
is genuinely core_v3-specific, not generic. core_v4 gets its own
rules/pipeline/core_v4/enrichment.smk once it has real tables — this one
doesn't get repointed at core_v4 later, a new one gets added alongside it.

Three duckdb files, each a snapshot of one stage (paths: config/pipelines.yaml):

    core_v3_transformation  ->  core_v3_staging.duckdb    (merge.smk; never enriched)
    seed_topics             ->  core_v3_staging_2.duckdb  (fresh copy of staging + topic taxonomy)
    topic_modelling         ->  (writes into staging_2)
    dch_classification      ->  core_v3.duckdb            (fresh copy of staging_2 + is_ch/pred; final)

seed_topics and dch_classification copy the previous stage's file themselves
(src/pipelines/core_v3/db_copy.py) and declare the result as their real output.
topic_modelling writes into staging_2 in place (ALTER/UPDATE), so it has no
output file of its own — a touch() sentinel under .snakemake/sentinels/
enrichment/core_v3/ stands in. Delete a sentinel to force that enrichment to
rerun.

The duckdb files are single-writer, so the enrichments are one chain. If a job
fails, Snakemake deletes its declared output: seed_topics/dch lose their
half-copied duckdb (cheap to redo), topic_modelling loses nothing and resumes
from relation_topic. DCH's expensive GPU progress lives in resume files next
to the final duckdb that Snakemake doesn't know about, so it survives too
(src/pipelines/core_v3/enrichment/dch_results.py).

`geolocation` is not part of the chain — nothing in core_v3 gives it input (see
the script's docstring) — but stays available by name.

Run the whole thing with the `core_v3` target (Snakefile), or one enrichment by name:
    uv run snakemake -s orchestration/Snakefile topic_modelling
"""


rule seed_topics:
    # Just a file copy + a few hundred inserts — no need for the CORE_V3_* allocation.
    input:
        rules.core_v3_transformation.output,
    output:
        CORE_V3_PATHS["path_duck_staging_2"],
    resources:
        mem_mb=16000,
        runtime=CORE_V3_RUNTIME,  # dominated by the copy of the whole duckdb file
        cpus_per_task=2,
    shell:
        "uv run python -m pipelines.core_v3.enrichment.seed_topics"


rule topic_modelling:
    input:
        rules.seed_topics.output,
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/topic_modelling_done"),
    resources:
        mem_mb=CORE_V3_MEM_MB,
        runtime=CORE_V3_RUNTIME,
        cpus_per_task=CORE_V3_CPUS,
    shell:
        "uv run python -m pipelines.core_v3.enrichment.topic_modelling "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task}"


rule geolocation:
    # Not in the core_v3 chain (nothing feeds it city + country here). Writes into staging_2.
    input:
        rules.topic_modelling.output,
    output:
        touch(".snakemake/sentinels/enrichment/core_v3/geolocation_done"),
    shell:
        "uv run python -m pipelines.core_v3.enrichment.geolocation"


rule dch_classification:
    input:
        rules.seed_topics.output,  # the file that gets copied
        rules.topic_modelling.output,  # ordering only: copy staging_2 once topics are done
    output:
        CORE_V3_PATHS["path_duck"],
    resources:
        slurm_partition="gpu-test",  # 12h limit, idle 80GB A100s; switch to "gpu" (runtime up to 4320) for long runs
        gres="gpu:a100:1",  # single GPU: dch_classifier only uses cuda:0
        constraint="a100_80gb",  # batch 2048 x 512 tokens is sized for 80GB VRAM
        cpus_per_task=16,
        mem_mb=128000,  # all_text_rows() holds every row in RAM; ample for entity=project
        runtime=720,  # gpu-test max is 12h; resumable, ~25 min expected for ~4M rows. --entity work needs partition gpu + more
    shell:
        "uv run python -m pipelines.core_v3.enrichment.dch_classification"


CORE_V3_REPORT_STAGES = ["staging", "staging_2", "main"]  # generate_reports' labels for the three duckdbs


def _core_v3_report_input(wildcards):
    return {
        "staging": rules.core_v3_transformation.output,
        # staging_2 is only "done" once topic_modelling (which writes into it) has run.
        "staging_2": [*rules.seed_topics.output, *rules.topic_modelling.output],
        "main": rules.dch_classification.output,
    }[wildcards.stage]


rule report_core_v3:
    # One job per stage, same pattern as dumps.smk's report_dump.
    wildcard_constraints:
        stage="|".join(CORE_V3_REPORT_STAGES),
    input:
        _core_v3_report_input,
    output:
        "reports/pipelines/core_v3/{stage}.md",
    log:
        str(LOGGING_PATH / "report_core_v3_{stage}.log"),
    resources:
        # ~350 GB duckdbs: DuckDB ignores the cgroup, so the script caps itself to these
        # (default 64 GB / 6 CPUs OOM-killed it).
        mem_mb=128000,
        cpus_per_task=16,
        runtime=CORE_V3_RUNTIME,  # profile default lands as a 48 min SLURM limit; a full scan of 350 GB won't fit
    shell:
        "uv run python -m common.report.generate_reports --only {output} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"
