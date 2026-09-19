"""core_v4 merge: OpenAire staging v4 + ROR + Cordis -> core_v4_staging.duckdb.

First stage of every core_v4 target (see the Snakefile: `core_v4_projects`,
`core_v4_works`, `core_v4`). enrichment.smk and assemble.smk chain on from it and
import the constants defined here.

Variants. `--config limit=N` switches the *whole* core_v4 DAG to the dev sample:
every path below comes from config/pipelines.yaml's `core_v4_limit` block
(staging, enrichment dir, final duckdbs, reports) and the transformation gets
`--variant limit --limit N`. A limit run therefore never touches a full run's
files, and both variants can sit next to each other. N is a rule param, so
changing it reruns the transformation and everything downstream.

Other config keys (all optional), documented in orchestration/README.md:
    limit=N                     dev sample variant (see above)
    work_cap=N                  passed to the transformation as --work-cap
    skip=nllb,dch               leave these enrichments out of the DAG (assemble gets --skip)
    geolocation_max_requests=N  Mapbox budget; geolocation only runs when N > 0
    geolocation_permanent=true  send permanent=true to Mapbox (paid); default is temporary
    shards=N                    override every enrichment's shard count
    shards_<name>=N             override one enrichment's shard count (e.g. shards_nllb=8)
"""

from common.config.pipelines import get_pipeline_paths
from common.config.source_paths import resolve_data_path

CORE_V4_LIMIT = int(config["limit"]) if config.get("limit") else None
CORE_V4_VARIANT = "limit" if CORE_V4_LIMIT else "full"
CORE_V4_SUFFIX = "_limit" if CORE_V4_LIMIT else ""  # config key / report stage suffix
CORE_V4_PIPELINE = f"core_v4{CORE_V4_SUFFIX}"  # config block name, reports/pipelines/<this>/, sentinel dir


def _core_v4_paths():
    cfg = get_pipeline_paths()[CORE_V4_PIPELINE]
    sfx = CORE_V4_SUFFIX

    def duck(key, fallback_name):
        # path_duck_projects / path_duck_works are added to config/pipelines.yaml by the assemble
        # work; fall back to the documented file name until they are there.
        return cfg.get(key) or resolve_data_path(f"data/duckdb/core/{fallback_name}.duckdb")

    return {
        "staging": cfg[f"path_duck_staging{sfx}"],
        "projects": duck(f"path_duck_projects{sfx}", f"{CORE_V4_PIPELINE}_projects"),
        "works": duck(f"path_duck_works{sfx}", f"{CORE_V4_PIPELINE}_works"),
        "enrichment_dir": cfg[f"path_enrichment_dir{sfx}"].rstrip("/"),
    }


CORE_V4_PATHS = _core_v4_paths()

CORE_V4_MEM_MB = 200000
CORE_V4_CPUS = 32
CORE_V4_RUNTIME = 4320

CORE_V4_CORDIS_QUERY_ID = "full_projects_no_pdfs"  # same query core_v3 joins


rule core_v4_transformation:
    input:
        rules.stage_openaire_dump_v4.output,  # openaire_staging_v4 (adds work.countries)
        rules.load_ror_dump.output,
        f"data/checkpoints/loading/cordis_{CORE_V4_CORDIS_QUERY_ID}/mtime.cp",
    output:
        CORE_V4_PATHS["staging"],
    params:
        variant=CORE_V4_VARIANT,
        limit_flag=f"--limit {CORE_V4_LIMIT}" if CORE_V4_LIMIT else "",
        work_cap_flag=f"--work-cap {int(config['work_cap'])}" if config.get("work_cap") else "",
    resources:
        mem_mb=CORE_V4_MEM_MB,
        runtime=CORE_V4_RUNTIME,
        cpus_per_task=CORE_V4_CPUS,
    log:
        str(LOGGING_PATH / f"core_v4_transformation{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run python -m pipelines.core_v4.transformation --variant {params.variant} "
        "{params.limit_flag} {params.work_cap_flag} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"
