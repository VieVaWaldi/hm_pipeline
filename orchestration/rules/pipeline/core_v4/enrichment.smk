"""core_v4 enrichment rules (src/pipelines/core_v4/enrichment/, see its README).

Enrichments never touch the staging duckdb (read-only) and never each other's files. Each writes
parquet side outputs `<enrichment_dir>/<name>/<entity>/part-<shard>-<n>.parquet`, and a `_SUCCESS`
file marks the directory complete. That `_SUCCESS` is the real artifact these rules declare, so
idempotency comes from files on disk, not from Snakemake's memory of what ran.

Dependency graph (rules only depend on `_SUCCESS` files):

    staging -> nllb -> topics (needs the TF-IDF model) -> theme
                    -> minorities
                    -> pillars
                    -> dch                        (all for entity project and work)
    staging -> regions                            (entity organization)
    staging -> geolocation                        (organization; runs last, off unless budgeted)

Sharding. Every enrichment CLI takes `--shard I/N`. Per (enrichment, entity) the DAG has

    core_v4_<name>          N jobs, one per shard I; the job's output is a temp() sentinel
                            .snakemake/sentinels/enrichment/<pipeline>/<name>/<entity>/s<I>of<N>.done
    core_v4_success         one local job that writes `_SUCCESS` (through side_outputs.SideOutput,
                            not by reimplementing its format) once all N sentinels exist

Because the sentinels are temp(), they disappear as soon as `_SUCCESS` exists. To redo one enrichment
(every shard reruns and resumes from the parquet parts already on disk, then everything downstream of it),
force its `_SUCCESS` with an ABSOLUTE path:

    snakemake ... core_v4 --forcerun $PWD/<enrichment_dir>/<name>/<entity>/_SUCCESS

Merely deleting `_SUCCESS` does not work: Snakemake does not rebuild a missing intermediate file while the
files downstream of it (assembled duckdbs, reports) are up to date, so the target answers "Nothing to be done".
(Delete `_SUCCESS` AND the assembled duckdb, or use --forcerun.)

Shard counts are provisional (`_CORE_V4_DEFAULT_SHARDS`); the NLLB and DCH GPU numbers should be replaced by
the measured throughput from src/enrichment/nllb_translator/README.md. GPU shards run on `gpu-test`
(12 h limit, idle 80 GB A100s) and are resumable, so a timed-out shard is just rerun.

Config keys (`--config`): limit, skip, shards, shards_<name>, geolocation_max_requests,
geolocation_permanent -- see merge.smk and orchestration/README.md.
"""

CORE_V4_SENTINEL_DIR = f".snakemake/sentinels/enrichment/{CORE_V4_PIPELINE}"

CORE_V4_ENRICHMENTS = ["nllb", "topics", "theme", "minorities", "pillars", "dch", "regions", "geolocation"]
CORE_V4_TEXT_ENRICHMENTS = ["topics", "minorities", "pillars", "dch"]  # read text; take --allow-untranslated


def _core_v4_flag(key):
    return str(config.get(key) or "").strip().lower() in ("1", "true", "yes")


def _core_v4_skip():
    raw = config.get("skip") or []
    names = [s.strip() for s in (raw.split(",") if isinstance(raw, str) else raw) if s.strip()]
    unknown = sorted(set(names) - set(CORE_V4_ENRICHMENTS))
    if unknown:
        raise WorkflowError(f"--config skip: unknown enrichment(s) {unknown}; choose from {CORE_V4_ENRICHMENTS}")
    return set(names)


CORE_V4_GEOLOCATION_MAX_REQUESTS = int(config.get("geolocation_max_requests") or 0)
CORE_V4_GEOLOCATION_PERMANENT = _core_v4_flag("geolocation_permanent")

# What the user asked to leave out, plus what cannot run without it. Everything in here drops out of
# the DAG and is handed to assemble as --skip.
CORE_V4_OFF = _core_v4_skip()
if "topics" in CORE_V4_OFF:
    CORE_V4_OFF.add("theme")  # theme reads topics/<entity>
if CORE_V4_GEOLOCATION_MAX_REQUESTS <= 0:
    CORE_V4_OFF.add("geolocation")  # spends money: only with an explicit budget
CORE_V4_ASSEMBLE_SKIP = ",".join(sorted(CORE_V4_OFF))

# Text enrichments refuse to start before nllb/<entity>/_SUCCESS unless told otherwise.
CORE_V4_UNTRANSLATED_FLAG = "--allow-untranslated" if "nllb" in CORE_V4_OFF else ""

# Which enrichments each chain runs, by entity. The projects chain also covers the organization
# side (regions, geolocation); the works chain is works only.
CORE_V4_CHAINS = {
    "projects": {
        "project": ["nllb", "topics", "theme", "minorities", "pillars", "dch"],
        "organization": ["regions", "geolocation"],
    },
    "works": {
        "work": ["nllb", "topics", "theme", "minorities", "pillars", "dch"],
    },
}

# Shards per (enrichment, entity). GPU rules get few, CPU rules many; provisional, see the docstring.
# A limit run is tiny, so it defaults to one shard everywhere.
_CORE_V4_DEFAULT_SHARDS = {
    "nllb": {"project": 4, "work": 16},
    "topics": {"project": 8, "work": 32},
    "theme": {"project": 1, "work": 1},  # pure SQL over topics/<entity>, recomputed wholesale
    "minorities": {"project": 8, "work": 32},
    "pillars": {"project": 8, "work": 32},
    "dch": {"project": 2, "work": 8},
    "regions": {"organization": 1},
    "geolocation": {"organization": 1},  # single writer (mapbox cache): the CLI refuses --shard
}


def _core_v4_shards(name, entity):
    if name in ("theme", "regions", "geolocation"):
        return 1
    override = config.get(f"shards_{name}") or config.get("shards")
    if override:
        return int(override)
    return 1 if CORE_V4_LIMIT else _CORE_V4_DEFAULT_SHARDS[name][entity]


def _core_v4_success(name, entity):
    return f"{CORE_V4_PATHS['enrichment_dir']}/{name}/{entity}/_SUCCESS"


def _core_v4_enabled(name):
    return name not in CORE_V4_OFF


def _core_v4_chain_success(chain, exclude=()):
    """`_SUCCESS` files of every enabled (enrichment, entity) in a chain."""
    return [
        _core_v4_success(name, entity)
        for entity, names in CORE_V4_CHAINS[chain].items()
        for name in names
        if _core_v4_enabled(name) and name not in exclude
    ]


CORE_V4_TOPICS_MODEL = f"{CORE_V4_PATHS['enrichment_dir']}/topics/tfidf_model.pkl"


def _core_v4_after_nllb(wildcards):
    """Text enrichments read the translated text: wait for nllb (unless it is skipped)."""
    return [_core_v4_success("nllb", wildcards.entity)] if _core_v4_enabled("nllb") else []


def _core_v4_gate(wildcards):
    """The works chain only starts once the projects duckdb exists (results for projects first).
    ancient(): ordering only, so rebuilding the projects duckdb does not rerun the works enrichments."""
    return [ancient(CORE_V4_PATHS["projects"])] if wildcards.entity == "work" else []


def _core_v4_sentinels(name, entity):
    n = _core_v4_shards(name, entity)
    return [f"{CORE_V4_SENTINEL_DIR}/{name}/{entity}/s{i}of{n}.done" for i in range(n)]


def _core_v4_sentinel_pattern(name):
    return f"{CORE_V4_SENTINEL_DIR}/{name}/{{entity}}/s{{shard}}of{{n}}.done"


def _core_v4_log(name):
    return str(LOGGING_PATH / f"core_v4_{name}_{{entity}}_s{{shard}}of{{n}}{CORE_V4_SUFFIX}.log")


# python -m ... run with --no-sync: the shard jobs start together, and `snakemake` itself was started
# through `uv run`, so the environment is already synced (parallel syncs would race).
# The shell strings below repeat "uv run --no-sync python" per rule for readability.


rule core_v4_nllb:
    # GPU. NLLB-1.3B via CTranslate2 (float16) on a single A100 80GB; measured numbers: src/enrichment/nllb_translator/README.md.
    wildcard_constraints:
        entity="project|work",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_gate,
    output:
        temp(_core_v4_sentinel_pattern("nllb")),
    params:
        variant=CORE_V4_VARIANT,
    resources:
        slurm_partition="gpu-test",
        gres="gpu:a100:1",
        constraint="a100_80gb",
        cpus_per_task=16,
        mem_mb=64000,
        runtime=720,  # gpu-test max is 12 h; resumable (rerun picks up the rows not yet in nllb/seen)
    log:
        _core_v4_log("nllb"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.nllb_translation --variant {params.variant} "
        "--entity {wildcards.entity} --shard {wildcards.shard}/{wildcards.n} &> {log} && touch {output}"


rule core_v4_topics_model:
    # The TF-IDF model is built once (oa_topics taxonomy + a seeded sample of translated project
    # texts) so every topics shard scores with the same one. The works chain reuses it.
    input:
        CORE_V4_PATHS["staging"],
        DUMP_PATHS["oa_topics"]["path_raw"],
        [_core_v4_success("nllb", "project")] if _core_v4_enabled("nllb") else [],
    output:
        CORE_V4_TOPICS_MODEL,
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
    resources:
        mem_mb=64000,
        runtime=240,
        cpus_per_task=8,
    log:
        str(LOGGING_PATH / f"core_v4_topics_model{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.topic_modelling --variant {params.variant} "
        "--build-model {params.untranslated} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log}"


rule core_v4_topics:
    wildcard_constraints:
        entity="project|work",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        CORE_V4_TOPICS_MODEL,
        _core_v4_after_nllb,
        _core_v4_gate,
    output:
        temp(_core_v4_sentinel_pattern("topics")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
    resources:
        mem_mb=64000,
        runtime=1440,
        cpus_per_task=16,
    log:
        _core_v4_log("topics"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.topic_modelling --variant {params.variant} "
        "--entity {wildcards.entity} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log} && touch {output}"


def _core_v4_after_topics(wildcards):
    return [_core_v4_success("topics", wildcards.entity)]


rule core_v4_theme:
    # Pure SQL over topics/<entity> (best topic per row -> Economy/Tourism), recomputed wholesale: one shard.
    wildcard_constraints:
        entity="project|work",
        shard=r"\d+",
        n=r"\d+",
    input:
        _core_v4_after_topics,
        DUMP_PATHS["oa_topics"]["path_raw"],
        _core_v4_gate,
    output:
        temp(_core_v4_sentinel_pattern("theme")),
    params:
        variant=CORE_V4_VARIANT,
    resources:
        mem_mb=64000,
        runtime=480,
        cpus_per_task=4,
    log:
        _core_v4_log("theme"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.theme --variant {params.variant} "
        "--entity {wildcards.entity} --shard {wildcards.shard}/{wildcards.n} &> {log} && touch {output}"


rule core_v4_minorities:
    wildcard_constraints:
        entity="project|work",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        rules.load_minorities.output,
        _core_v4_after_nllb,
        _core_v4_gate,
    output:
        temp(_core_v4_sentinel_pattern("minorities")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
    resources:
        mem_mb=64000,
        runtime=1440,
        cpus_per_task=16,
    log:
        _core_v4_log("minorities"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.minorities --variant {params.variant} "
        "--entity {wildcards.entity} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
        "--workers {resources.cpus_per_task} &> {log} && touch {output}"


rule core_v4_pillars:
    wildcard_constraints:
        entity="project|work",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_after_nllb,
        _core_v4_gate,
    output:
        temp(_core_v4_sentinel_pattern("pillars")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
    resources:
        mem_mb=32000,
        runtime=720,
        cpus_per_task=4,
    log:
        _core_v4_log("pillars"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.pillars --variant {params.variant} "
        "--entity {wildcards.entity} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
        "&> {log} && touch {output}"


rule core_v4_dch:
    # GPU, sized like core_v3's dch_classification (single A100 80GB, batch sized for its VRAM). Resumable per chunk
    # of 40,960 rows, so gpu-test's 12 h limit is fine for works too: use more shards instead of the long partition.
    wildcard_constraints:
        entity="project|work",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_after_nllb,
        _core_v4_gate,
    output:
        temp(_core_v4_sentinel_pattern("dch")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
    resources:
        slurm_partition="gpu-test",
        gres="gpu:a100:1",
        constraint="a100_80gb",
        cpus_per_task=16,
        mem_mb=128000,
        runtime=720,
    log:
        _core_v4_log("dch"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.dch_classification --variant {params.variant} "
        "--entity {wildcards.entity} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
        "&> {log} && touch {output}"


rule core_v4_regions:
    # Organization side, staging only. The CLI has no --entity.
    wildcard_constraints:
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
    output:
        temp(f"{CORE_V4_SENTINEL_DIR}/regions/organization/s{{shard}}of{{n}}.done"),
    params:
        variant=CORE_V4_VARIANT,
    resources:
        mem_mb=16000,
        runtime=120,
        cpus_per_task=4,
    log:
        str(LOGGING_PATH / f"core_v4_regions_organization_s{{shard}}of{{n}}{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.regions --variant {params.variant} "
        "--shard {wildcards.shard}/{wildcards.n} &> {log} && touch {output}"


CORE_V4_SHARDED_NAMES = "nllb|topics|theme|minorities|pillars|dch|regions"
_CORE_V4_COMPANIONS = {"nllb": ["nllb/seen"], "minorities": ["minorities/seen"]}


def _core_v4_shard_sentinels(wildcards):
    return _core_v4_sentinels(wildcards.name, wildcards.entity)


rule core_v4_success:
    # Writes <enrichment_dir>/<name>/<entity>/_SUCCESS once every shard's sentinel exists. Uses
    # SideOutput.finish() for each shard index: it records the shard, and writes _SUCCESS when the
    # last of the N is in (the format lives in side_outputs.py). Cheap: runs on the submit host.
    localrule: True
    wildcard_constraints:
        name=CORE_V4_SHARDED_NAMES,
        entity="project|work|organization",
    input:
        _core_v4_shard_sentinels,
    output:
        f"{CORE_V4_PATHS['enrichment_dir']}/{{name}}/{{entity}}/_SUCCESS",
    run:
        from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

        n = _core_v4_shards(wildcards.name, wildcards.entity)
        # nllb and minorities also write a companion <name>/seen output (the resume key); assemble
        # checks nllb/seen's _SUCCESS too, so finish it with the main one.
        for name in [wildcards.name, *_CORE_V4_COMPANIONS.get(wildcards.name, [])]:
            first = SideOutput(CORE_V4_PATHS["enrichment_dir"], name, wildcards.entity, Shard(0, n))
            if not first.dir.is_dir():
                raise WorkflowError(f"{first.dir} does not exist: the {name} shards wrote nothing")
            leftovers = list(first.dir.glob("*.tmp"))
            if leftovers:
                raise WorkflowError(f"{first.dir} has torn tmp files {leftovers[:3]}: rerun the shard")
            for i in range(n):
                out = SideOutput(CORE_V4_PATHS["enrichment_dir"], name, wildcards.entity, Shard(i, n))
                if not any(p.name.startswith(f"part-{i:03d}-") for p in out.parts()):
                    print(f"WARNING {name}/{wildcards.entity}: shard {i}/{n} wrote no rows", file=sys.stderr)
                complete = out.finish()
            if not complete:
                raise WorkflowError(f"{name}/{wildcards.entity}: no _SUCCESS after finishing all {n} shards")


rule core_v4_geolocation:
    # Mapbox batch geocoding for organizations with an address but no coordinates. Single process (one
    # cache file, the CLI refuses --shard), and it runs LAST in the projects chain: it takes every other
    # projects-chain _SUCCESS as an ordering-only input. Only part of the DAG with
    # --config geolocation_max_requests=N (N > 0). Free tier (temporary geocoding) unless
    # --config geolocation_permanent=true.
    # If the budget runs out the CLI writes no _SUCCESS: the job then fails with a missing-output
    # error; rerun with a larger N (cached answers cost nothing).
    input:
        CORE_V4_PATHS["staging"],
        [ancient(p) for p in _core_v4_chain_success("projects", exclude=("geolocation",))],
    output:
        _core_v4_success("geolocation", "organization"),
    params:
        variant=CORE_V4_VARIANT,
        max_requests=CORE_V4_GEOLOCATION_MAX_REQUESTS,
        permanent="--permanent" if CORE_V4_GEOLOCATION_PERMANENT else "",
    resources:
        mem_mb=16000,
        runtime=720,
        cpus_per_task=2,
    log:
        str(LOGGING_PATH / f"core_v4_geolocation{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.geolocation --variant {params.variant} "
        "--max-requests {params.max_requests} {params.permanent} &> {log}"
