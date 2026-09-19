"""core_v3 merge: OpenAire staging + ROR + Cordis -> core_v3_staging.duckdb.

First stage of the `core_v3` target (see Snakefile); enrichment.smk chains on
from this rule's output. The script rebuilds its duckdb from scratch on every
run (see src/pipelines/core_v3/transformation.py).

Resources are shared: enrichment.smk imports the CORE_V3_* constants below for
its CPU-bound rules rather than restating them. Same size as the openaire
staging/load rules (dumps.smk) — core_v3 is the same order of magnitude of data.

Limit run (smoke test on a sample, per the root README's "Limit Runs"):
    uv run snakemake -s orchestration/Snakefile core_v3 --config limit=500
`limit` only goes to the merge; enrichment works on whatever rows it produced.
It's a rule param, so switching between a limit run and a full run reruns the
merge and everything downstream automatically. Both write the same duckdb
paths — a limit run overwrites a full run's files.
"""

from common.config.pipelines import get_pipeline_paths

CORE_V3_PATHS = get_pipeline_paths()["core_v3"]

CORE_V3_MEM_MB = 200000
CORE_V3_CPUS = 32
CORE_V3_RUNTIME = 4320

CORE_V3_LIMIT = int(config["limit"]) if config.get("limit") else None
CORE_V3_CORDIS_QUERY_ID = "full_projects_no_pdfs"  # same query transformation.py joins


rule core_v3_transformation:
    input:
        rules.stage_openaire_dump.output,
        rules.load_ror_dump.output,
        f"data/checkpoints/loading/cordis_{CORE_V3_CORDIS_QUERY_ID}/mtime.cp",
    output:
        CORE_V3_PATHS["path_duck_staging"],
    params:
        limit_flag=f"--limit {CORE_V3_LIMIT}" if CORE_V3_LIMIT else "",
    resources:
        mem_mb=CORE_V3_MEM_MB,
        runtime=CORE_V3_RUNTIME,
        cpus_per_task=CORE_V3_CPUS,
    log:
        str(LOGGING_PATH / "core_v3_transformation.log"),
    shell:
        "uv run python -m pipelines.core_v3.transformation {params.limit_flag} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"
