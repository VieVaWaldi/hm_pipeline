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

Units. The works chain is split by link tier so the project-linked works (tier 0, about 5M of the 50M) are enriched
first and can be served while the org-only ones (tier 1) are still running. A DAG unit is therefore one of

    project        entity project                       _SUCCESS
    organization   entity organization                  _SUCCESS
    work-t0        entity work, --tier 0                _SUCCESS.tier0     (usable on its own)
    work-t1        entity work, --tier 1                _SUCCESS          (written once tier 0 AND tier 1 are complete)

Tier-1 jobs of an enrichment wait for that enrichment's tier-0 `_SUCCESS.tier0` (ordering only), so tier 0 always
goes first for GPU time and everything else. Tier 0 and tier 1 write into the same side-output directory
(enrichment/side_outputs.py explains the part names and markers); shard counts differ per unit.

Sharding. Every enrichment CLI takes `--shard I/N`. Per (enrichment, unit) the DAG has

    core_v4_<name>          N jobs, one per shard I; the job's output is a temp() sentinel
                            .snakemake/sentinels/enrichment/<pipeline>/<name>/<unit>/s<I>of<N>.done
    core_v4_success[_tier0] one local job that writes `_SUCCESS` (`_SUCCESS.tier0` for work-t0) through
                            side_outputs.SideOutput, not by reimplementing its format, once all N sentinels exist.
                            The marker records the staging fingerprint (enrichment/fingerprint.py).

Because the sentinels are temp(), they disappear as soon as `_SUCCESS` exists. To redo one enrichment
(every shard reruns and resumes from the parquet parts already on disk, then everything downstream of it),
force its `_SUCCESS` with an ABSOLUTE path:

    snakemake ... core_v4 --forcerun $PWD/<enrichment_dir>/<name>/<entity>/_SUCCESS      (tier 0: _SUCCESS.tier0)

Merely deleting `_SUCCESS` does not work: Snakemake does not rebuild a missing intermediate file while the
files downstream of it (assembled duckdbs, reports) are up to date, so the target answers "Nothing to be done".
(Delete `_SUCCESS` AND the assembled duckdb, or use --forcerun.)

Shard counts (`_CORE_V4_DEFAULT_SHARDS`): the NLLB numbers are derived from the measured throughput, see the comment
above it; the others are still provisional. GPU shards run on `gpu-test` (12 h limit) and are resumable, so a
timed-out shard is just rerun.

Config keys (`--config`): limit, skip, shards, shards_<name>, shards_<name>_t0 / _t1, geolocation_max_requests,
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

# Which enrichments each chain runs, by DAG unit (see the docstring). The projects chain also covers the organization
# side (regions, geolocation). "works" lists only the tier-1 unit: its `_SUCCESS` files exist only once tier 0 is complete
# too (core_v4_success takes the tier-0 marker as input), so asking for them pulls the tier-0 chain in as well.
_CORE_V4_TEXT_CHAIN = ["nllb", "topics", "theme", "minorities", "pillars", "dch"]
CORE_V4_CHAINS = {
    "projects": {
        "project": _CORE_V4_TEXT_CHAIN,
        "organization": ["regions", "geolocation"],
    },
    "works_linked": {"work-t0": _CORE_V4_TEXT_CHAIN},
    "works": {"work-t1": _CORE_V4_TEXT_CHAIN},
}

# Shards per (enrichment, unit). A limit run is tiny, so it defaults to one shard everywhere.
#
# NLLB (measured, src/enrichment/nllb_translator/README.md): 1.3B distilled, CTranslate2 float16, beam 1, 32,768 batch
# tokens: 13.3k source tokens/s and 10.1 GB peak VRAM on an A100 (so ANY GPU with >= 16 GB will do: no 80 GB constraint);
# end to end through the glue (LID, read, write included) 421 works/s, i.e. 33-37 GPU-hours for 50M works. Per shard, with
# rows / (works per second) as the expected time and a 3x safety margin under gpu-test's 12 h limit for slower cards,
# longer texts than the org-only sample (tier 0 is dated any year and has more descriptions) and the model load:
#     projects   3.9M rows / 4 shards = 975k rows/shard; a project has a summary (about 3x a work's tokens): ~140 rows/s
#                -> ~1.9 h expected, runtime 6 h
#     works t0   5.0M / 6 shards = 830k/shard at 421/s -> ~0.55 h, runtime 4 h (small on purpose: tier 0 finishes first)
#     works t1   45M / 16 shards = 2.8M/shard at 421/s -> ~1.9 h, runtime 6 h (16 shards on the 3 gpu-test GPUs run in waves)
# Shards are resumable (rows already in nllb/seen are skipped), so a timeout only costs a rerun of that shard.
# DCH keeps its own 80 GB A100 constraint (core_v3's batch sizes).
_CORE_V4_DEFAULT_SHARDS = {
    "nllb": {"project": 4, "work-t0": 6, "work-t1": 16},
    "topics": {"project": 8, "work-t0": 8, "work-t1": 32},
    "theme": {"project": 1, "work-t0": 1, "work-t1": 1},  # pure SQL over topics/<entity>, recomputed wholesale
    "minorities": {"project": 8, "work-t0": 8, "work-t1": 32},
    "pillars": {"project": 8, "work-t0": 8, "work-t1": 32},
    "dch": {"project": 2, "work-t0": 2, "work-t1": 8},
    "regions": {"organization": 1},
    "geolocation": {"organization": 1},  # single writer (mapbox cache): the CLI refuses --shard
}
# NLLB shard runtime in minutes, see above (measured rate x 3 margin, rounded up).
_CORE_V4_NLLB_RUNTIME = {"project": 360, "work-t0": 240, "work-t1": 360}


def _core_v4_entity(unit):
    return unit.split("-")[0]


def _core_v4_tier(unit):
    return {"work-t0": 0, "work-t1": 1}.get(unit)


def _core_v4_tier_flag(unit):
    tier = _core_v4_tier(unit)
    return "" if tier is None else f"--tier {tier}"


def _core_v4_shards(name, unit):
    if name in ("theme", "regions", "geolocation"):
        return 1
    tier = _core_v4_tier(unit)
    override = (config.get(f"shards_{name}_t{tier}") if tier is not None else None) or config.get(f"shards_{name}") or config.get("shards")
    if override:
        return int(override)
    return 1 if CORE_V4_LIMIT else _CORE_V4_DEFAULT_SHARDS[name][unit]


def _core_v4_success(name, unit):
    """The `_SUCCESS` file that says <name> is complete for the unit (`_SUCCESS.tier0` for tier 0, `_SUCCESS` for
    everything else: for work-t1 it means both tiers are complete)."""
    suffix = ".tier0" if unit == "work-t0" else ""
    return f"{CORE_V4_PATHS['enrichment_dir']}/{name}/{_core_v4_entity(unit)}/_SUCCESS{suffix}"


def _core_v4_enabled(name):
    return name not in CORE_V4_OFF


def _core_v4_chain_success(chain, exclude=()):
    """`_SUCCESS` files of every enabled (enrichment, unit) in a chain."""
    return [
        _core_v4_success(name, unit)
        for unit, names in CORE_V4_CHAINS[chain].items()
        for name in names
        if _core_v4_enabled(name) and name not in exclude
    ]


CORE_V4_TOPICS_MODEL = f"{CORE_V4_PATHS['enrichment_dir']}/topics/tfidf_model.pkl"


def _core_v4_after_nllb(wildcards):
    """Text enrichments read the translated text: wait for nllb (unless it is skipped). For tier 0 that is the
    tier-0 marker, for tier 1 the full `_SUCCESS`."""
    return [_core_v4_success("nllb", wildcards.unit)] if _core_v4_enabled("nllb") else []


def _core_v4_gate(name):
    """Ordering only (ancient(): rebuilding the earlier stage does not rerun this one):
      - tier 0 of the works chain starts once the projects duckdb exists (results for projects first);
      - tier 1 of an enrichment starts once that enrichment's tier 0 is complete (tier 0 goes first)."""

    def gate(wildcards):
        if wildcards.unit == "work-t0":
            return [ancient(CORE_V4_PATHS["projects"])]
        if wildcards.unit == "work-t1":
            return [ancient(_core_v4_success(name, "work-t0"))]
        return []

    return gate


def _core_v4_sentinels(name, unit):
    n = _core_v4_shards(name, unit)
    return [f"{CORE_V4_SENTINEL_DIR}/{name}/{unit}/s{i}of{n}.done" for i in range(n)]


def _core_v4_sentinel_pattern(name):
    return f"{CORE_V4_SENTINEL_DIR}/{name}/{{unit}}/s{{shard}}of{{n}}.done"


def _core_v4_log(name):
    return str(LOGGING_PATH / f"core_v4_{name}_{{unit}}_s{{shard}}of{{n}}{CORE_V4_SUFFIX}.log")


_CORE_V4_UNIT_PARAMS = dict(
    entity=lambda w: _core_v4_entity(w.unit),
    tier_flag=lambda w: _core_v4_tier_flag(w.unit),
)


# python -m ... run with --no-sync: the shard jobs start together, and `snakemake` itself was started
# through `uv run`, so the environment is already synced (parallel syncs would race).
# The shell strings below repeat "uv run --no-sync python" per rule for readability.


rule core_v4_nllb:
    # GPU. NLLB-1.3B via CTranslate2 (float16), 10.1 GB peak VRAM: any GPU with >= 16 GB, so only the partition and
    # gres are requested (no a100_80gb constraint). Shard counts and runtimes: see _CORE_V4_DEFAULT_SHARDS.
    # The model comes from data/models/nllb through the download guard (enrichment.nllb_translator.download); compute
    # nodes have no internet, so the hub is switched off explicitly and a missing model fails at start with the
    # download command (run it on the login node once).
    wildcard_constraints:
        unit="project|work-t0|work-t1",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_gate("nllb"),
    output:
        temp(_core_v4_sentinel_pattern("nllb")),
    params:
        variant=CORE_V4_VARIANT,
        **_CORE_V4_UNIT_PARAMS,
    resources:
        slurm_partition="gpu-test",
        gres="gpu:1",
        cpus_per_task=16,
        mem_mb=64000,
        runtime=lambda wildcards: _CORE_V4_NLLB_RUNTIME[wildcards.unit],  # gpu-test max is 12 h; resumable
    log:
        _core_v4_log("nllb"),
    shell:
        "HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 "
        "uv run --no-sync python -m pipelines.core_v4.enrichment.nllb_translation --variant {params.variant} "
        "--entity {params.entity} {params.tier_flag} --shard {wildcards.shard}/{wildcards.n} &> {log} && touch {output}"


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
        slurm_partition=CORE_V4_PARTITION,
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
        unit="project|work-t0|work-t1",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        CORE_V4_TOPICS_MODEL,
        _core_v4_after_nllb,
        _core_v4_gate("topics"),
    output:
        temp(_core_v4_sentinel_pattern("topics")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
        **_CORE_V4_UNIT_PARAMS,
    resources:
        slurm_partition=CORE_V4_PARTITION,
        mem_mb=64000,
        runtime=1440,
        cpus_per_task=16,
    log:
        _core_v4_log("topics"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.topic_modelling --variant {params.variant} "
        "--entity {params.entity} {params.tier_flag} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
        "--mem-mb {resources.mem_mb} --threads {resources.cpus_per_task} &> {log} && touch {output}"


def _core_v4_after_topics(wildcards):
    return [_core_v4_success("topics", wildcards.unit)]


rule core_v4_theme:
    # Pure SQL over topics/<entity> (best topic per row -> Economy/Tourism), recomputed wholesale: one shard.
    wildcard_constraints:
        unit="project|work-t0|work-t1",
        shard=r"\d+",
        n=r"\d+",
    input:
        _core_v4_after_topics,
        DUMP_PATHS["oa_topics"]["path_raw"],
        _core_v4_gate("theme"),
    output:
        temp(_core_v4_sentinel_pattern("theme")),
    params:
        variant=CORE_V4_VARIANT,
        **_CORE_V4_UNIT_PARAMS,
    resources:
        slurm_partition=CORE_V4_PARTITION,
        mem_mb=64000,
        runtime=480,
        cpus_per_task=4,
    log:
        _core_v4_log("theme"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.theme --variant {params.variant} "
        "--entity {params.entity} {params.tier_flag} --shard {wildcards.shard}/{wildcards.n} &> {log} && touch {output}"


rule core_v4_minorities:
    wildcard_constraints:
        unit="project|work-t0|work-t1",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        rules.load_minorities.output,
        _core_v4_after_nllb,
        _core_v4_gate("minorities"),
    output:
        temp(_core_v4_sentinel_pattern("minorities")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
        **_CORE_V4_UNIT_PARAMS,
    resources:
        slurm_partition=CORE_V4_PARTITION,
        mem_mb=64000,
        runtime=1440,
        cpus_per_task=16,
    log:
        _core_v4_log("minorities"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.minorities --variant {params.variant} "
        "--entity {params.entity} {params.tier_flag} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
        "--workers {resources.cpus_per_task} &> {log} && touch {output}"


rule core_v4_pillars:
    wildcard_constraints:
        unit="project|work-t0|work-t1",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_after_nllb,
        _core_v4_gate("pillars"),
    output:
        temp(_core_v4_sentinel_pattern("pillars")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
        **_CORE_V4_UNIT_PARAMS,
    resources:
        slurm_partition=CORE_V4_PARTITION,
        mem_mb=32000,
        runtime=720,
        cpus_per_task=4,
    log:
        _core_v4_log("pillars"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.pillars --variant {params.variant} "
        "--entity {params.entity} {params.tier_flag} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
        "&> {log} && touch {output}"


rule core_v4_dch:
    # GPU, sized like core_v3's dch_classification (single A100 80GB, batch sized for its VRAM). Resumable per chunk
    # of 40,960 rows, so gpu-test's 12 h limit is fine for works too: use more shards instead of the long partition.
    wildcard_constraints:
        unit="project|work-t0|work-t1",
        shard=r"\d+",
        n=r"\d+",
    input:
        CORE_V4_PATHS["staging"],
        _core_v4_after_nllb,
        _core_v4_gate("dch"),
    output:
        temp(_core_v4_sentinel_pattern("dch")),
    params:
        variant=CORE_V4_VARIANT,
        untranslated=CORE_V4_UNTRANSLATED_FLAG,
        **_CORE_V4_UNIT_PARAMS,
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
        "--entity {params.entity} {params.tier_flag} --shard {wildcards.shard}/{wildcards.n} {params.untranslated} "
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
        slurm_partition=CORE_V4_PARTITION,
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


def _core_v4_finish(name, unit):
    """Marks every shard of (name, unit) finished through SideOutput.finish(), which writes the completion marker
    (`_SUCCESS`, or `_SUCCESS.tier0` for tier 0) once the last shard is in, with the current staging fingerprint in it.
    The format of the markers lives in side_outputs.py. Companion outputs (nllb/seen, minorities/seen) are finished with
    the main one: assemble and the text gate check nllb/seen's marker too."""
    import time

    from pipelines.core_v4.enrichment.fingerprint import staging_stamp
    from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput
    from pipelines.core_v4.enrichment.text_sources import open_staging

    # The shard sentinels live in .snakemake/ (sub-second mtimes) while the markers land on /work, whose mtimes have
    # whole-second resolution: a marker written within the same second as the last sentinel gets an older mtime and
    # Snakemake aborts with a "clock skew" WorkflowError. Waiting past the next second boundary avoids it.
    time.sleep(1.5)

    entity, tier, edir = _core_v4_entity(unit), _core_v4_tier(unit), CORE_V4_PATHS["enrichment_dir"]
    n = _core_v4_shards(name, unit)
    con = open_staging(CORE_V4_PATHS["staging"])
    try:
        stamp = staging_stamp(con, entity, tier)
    finally:
        con.close()
    for out_name in [name, *_CORE_V4_COMPANIONS.get(name, [])]:
        first = SideOutput(edir, out_name, entity, Shard(0, n), tier=tier)
        if not first.dir.is_dir():
            raise WorkflowError(f"{first.dir} does not exist: the {out_name} shards wrote nothing")
        leftovers = list(first.dir.glob("*.tmp"))
        if leftovers:
            raise WorkflowError(f"{first.dir} has torn tmp files {leftovers[:3]}: rerun the shard")
        for i in range(n):
            out = SideOutput(edir, out_name, entity, Shard(i, n), tier=tier)
            if not any(p.name.startswith(f"part-{out.tag}{i:03d}-") for p in out.parts()):
                print(f"WARNING {out_name}/{entity} ({unit}): shard {i}/{n} wrote no rows", file=sys.stderr)
            out.finish(stamp)
        if not first.is_complete(tier):
            raise WorkflowError(f"{out_name}/{entity} ({unit}): no completion marker after finishing all {n} shards")


def _core_v4_success_inputs(wildcards):
    """Sentinels of the unit; for the full `_SUCCESS` of works (tier 1) also the tier-0 marker, which finish() combines."""
    unit = "work-t1" if wildcards.entity == "work" else wildcards.entity
    inputs = _core_v4_sentinels(wildcards.name, unit)
    if unit == "work-t1":
        inputs.append(_core_v4_success(wildcards.name, "work-t0"))
    return inputs


rule core_v4_success:
    # Writes <enrichment_dir>/<name>/<entity>/_SUCCESS once every shard's sentinel exists (for works: the tier-1
    # shards, and only when the tier-0 marker is there, so `_SUCCESS` always means "everything is complete").
    # Cheap: runs on the submit host.
    localrule: True
    wildcard_constraints:
        name=CORE_V4_SHARDED_NAMES,
        entity="project|work|organization",
    input:
        _core_v4_success_inputs,
    output:
        f"{CORE_V4_PATHS['enrichment_dir']}/{{name}}/{{entity}}/_SUCCESS",
    run:
        _core_v4_finish(wildcards.name, "work-t1" if wildcards.entity == "work" else wildcards.entity)


rule core_v4_success_tier0:
    # `_SUCCESS.tier0`: the project-linked works of <name> are complete (usable before tier 1 has run).
    localrule: True
    wildcard_constraints:
        name=CORE_V4_SHARDED_NAMES,
    input:
        lambda wildcards: _core_v4_sentinels(wildcards.name, "work-t0"),
    output:
        f"{CORE_V4_PATHS['enrichment_dir']}/{{name}}/work/_SUCCESS.tier0",
    run:
        _core_v4_finish(wildcards.name, "work-t0")


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
        slurm_partition=CORE_V4_PARTITION,
        mem_mb=16000,
        runtime=720,
        cpus_per_task=2,
    log:
        str(LOGGING_PATH / f"core_v4_geolocation{CORE_V4_SUFFIX}.log"),
    shell:
        "uv run --no-sync python -m pipelines.core_v4.enrichment.geolocation --variant {params.variant} "
        "--max-requests {params.max_requests} {params.permanent} &> {log}"
