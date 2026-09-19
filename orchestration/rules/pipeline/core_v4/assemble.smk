"""core_v4 assemble + reports: the last stage of each chain.

    core_v4_assemble_projects       staging + every enabled projects-chain _SUCCESS       -> core_v4_projects.duckdb
    core_v4_assemble_works_linked   staging + every tier-0 works _SUCCESS.tier0           -> core_v4_works_linked.duckdb
                                    (assemble --tier 0: the project-linked works only)
    core_v4_assemble_works          staging + every works _SUCCESS (both tiers complete)  -> core_v4_works.duckdb

Separate duckdbs so results exist in stages: the tier-0 works enrichments carry an ordering-only (ancient) input on the
projects duckdb, the tier-1 ones on the tier-0 markers, see enrichment.smk's `_core_v4_gate`. The linked file is a
subset of the full works file (same columns, rows of tier 1 absent); serve it until the full one exists.

`skip` (--config skip=..., plus whatever cannot run without it, and geolocation while it has no
budget) is passed as --skip; it is a rule param, so changing it reruns assemble.

Reports: reports/pipelines/core_v4/{staging,projects,works_linked,works}.md, one job per report like core_v3's
report_core_v3. In a limit run the pipeline is core_v4_limit and the stages carry the config key
suffix: reports/pipelines/core_v4_limit/{staging,projects,works_linked,works}_limit.md. Their names come from
common.report.generate_reports (_iter_pipelines: one report per path_duck* key of the
pipeline, label = key minus "path_duck"), so config/pipelines.yaml must carry the
path_duck_projects / path_duck_works_linked / path_duck_works keys (and their *_limit twins) for the reports to be generated.
"""

CORE_V4_REPORT_STAGES = [f"{stage}{CORE_V4_SUFFIX}" for stage in ("staging", "projects", "works_linked", "works")]


def _core_v4_report(stage):
    return f"reports/pipelines/{CORE_V4_PIPELINE}/{stage}{CORE_V4_SUFFIX}.md"


CORE_V4_PROJECTS_TARGETS = [_core_v4_report("staging"), _core_v4_report("projects")]
CORE_V4_WORKS_LINKED_TARGETS = [_core_v4_report("staging"), _core_v4_report("works_linked")]
# everything: the linked file too, so it is there first even when only core_v4_works is asked for
CORE_V4_WORKS_TARGETS = [*CORE_V4_WORKS_LINKED_TARGETS, _core_v4_report("works")]

_CORE_V4_ASSEMBLE_RESOURCES = dict(
    slurm_partition=CORE_V4_PARTITION, mem_mb=CORE_V4_MEM_MB, runtime=CORE_V4_RUNTIME, cpus_per_task=CORE_V4_CPUS
)
_CORE_V4_ASSEMBLE_WORKS_RESOURCES = {**_CORE_V4_ASSEMBLE_RESOURCES, "runtime": CORE_V4_WORKS_ASSEMBLE_RUNTIME}  # the full 50M works


rule core_v4_assemble_projects:
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_chain_success("projects"),
    output:
        CORE_V4_PATHS["projects"],
    params:
        variant=CORE_V4_VARIANT,
        skip=CORE_V4_ASSEMBLE_SKIP,
        skip_flag=f"--skip {CORE_V4_ASSEMBLE_SKIP}" if CORE_V4_ASSEMBLE_SKIP else "",
    resources:
        **_CORE_V4_ASSEMBLE_RESOURCES,
    log:
        str(LOGGING_PATH / f"core_v4_assemble_projects{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run python -m pipelines.core_v4.assemble --entity project --variant {params.variant} {params.skip_flag} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"


rule core_v4_assemble_works_linked:
    # Tier 0 only: needs just the tier-0 markers, so it can run long before the org-only works are enriched.
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_chain_success("works_linked"),
    output:
        CORE_V4_PATHS["works_linked"],
    params:
        variant=CORE_V4_VARIANT,
        skip=CORE_V4_ASSEMBLE_SKIP,
        skip_flag=f"--skip {CORE_V4_ASSEMBLE_SKIP}" if CORE_V4_ASSEMBLE_SKIP else "",
    resources:
        **_CORE_V4_ASSEMBLE_RESOURCES,
    log:
        str(LOGGING_PATH / f"core_v4_assemble_works_linked{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run python -m pipelines.core_v4.assemble --entity work --tier 0 --variant {params.variant} {params.skip_flag} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"


rule core_v4_assemble_works:
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_chain_success("works"),
    output:
        CORE_V4_PATHS["works"],
    params:
        variant=CORE_V4_VARIANT,
        skip=CORE_V4_ASSEMBLE_SKIP,
        skip_flag=f"--skip {CORE_V4_ASSEMBLE_SKIP}" if CORE_V4_ASSEMBLE_SKIP else "",
    resources:
        **_CORE_V4_ASSEMBLE_WORKS_RESOURCES,
    log:
        str(LOGGING_PATH / f"core_v4_assemble_works{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run python -m pipelines.core_v4.assemble --entity work --variant {params.variant} {params.skip_flag} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"


def _core_v4_report_input(wildcards):
    stage = wildcards.stage.removesuffix(CORE_V4_SUFFIX)
    return {
        "staging": rules.core_v4_transformation.output,
        "projects": rules.core_v4_assemble_projects.output,
        "works_linked": rules.core_v4_assemble_works_linked.output,
        "works": rules.core_v4_assemble_works.output,
    }[stage]


rule report_core_v4:
    # One job per stage, same pattern as report_core_v3.
    wildcard_constraints:
        stage="|".join(CORE_V4_REPORT_STAGES),
    input:
        _core_v4_report_input,
    output:
        f"reports/pipelines/{CORE_V4_PIPELINE}/{{stage}}.md",
    log:
        str(LOGGING_PATH / f"report_core_v4_{{stage}}.log"),
    resources:
        # DuckDB ignores the cgroup, so the script caps itself to these (see report_core_v3).
        slurm_partition=CORE_V4_PARTITION,
        mem_mb=128000,
        cpus_per_task=16,
        runtime=CORE_V4_REPORT_RUNTIME,
    shell:
        "uv run python -m common.report.generate_reports --only {output} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"
