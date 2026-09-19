# Orchestration

Snakemake DAG. This directory is DAG wiring only — it never contains pipeline
logic itself, only how to invoke `src/sources/`, `src/common/`, and `src/enrichment/`
code and in what order. See the `Snakefile` docstring for what's wired up.

```
orchestration/
├── Snakefile                       # entry point, includes rules/*.smk
├── rules/
│   ├── api_runner/                 # ↔ src/sources/apis/ (arxiv, cordis, coreac)
│   │   ├── extract.smk             # incremental extraction
│   │   └── load.smk                # incremental loading into duckdb
│   ├── dumps.smk                   # ↔ src/sources/dumps/ (ror, openaire, openalex, minorities, oa_topics)
│   ├── external/                   # ↔ src/sources/external/
│   │   └── meta_heritage.smk       # postgres-backed, not core_v4 scope, own scripts
│   └── pipeline/                   # ↔ pipeline-level code, not a source at all
│       ├── core_v3/
│       │   ├── merge.smk           # transformation -> core_v3_staging.duckdb (+ shared CORE_V3_* resources)
│       │   └── enrichment.smk      # seed_topics, topic_modelling, dch_classification, reports
│       └── core_v4/
│           ├── merge.smk           # config (limit/skip/paths) + transformation -> core_v4_staging.duckdb
│           ├── enrichment.smk      # sharded side-parquet enrichments + `_SUCCESS` finalize, geolocation
│           └── assemble.smk        # assemble --entity project|work, reports
├── envs/                           # per-rule container images (empty until core_v4 rules exist)
└── profiles/slurm/                 # SLURM executor config for HPC runs
```

Rule files mirror where the code they invoke lives — see the `Snakefile` docstring.

## Running

All commands below are run from `orchestration/` (where the `Snakefile` is — Snakemake looks
for it relative to cwd; run from the repo root instead with `-s orchestration/Snakefile`).

Locally:
```
uv run snakemake --cores 4 all
```

On the HPC:
```
uv run snakemake --workflow-profile orchestration/profiles/slurm all
```

`all` covers incremental extraction/loading (each source's own report, via
`report_source`) plus the versioned bulk dumps (ror_dump, openaire_dump,
minorities, oa_topics, each via `report_dump`). It does **not** include
`openalex_dump` (corev5 scope), `rules/pipeline/core_v3/`, or
`meta_heritage.smk` (all postgres-backed, run by name only) — see each rule
file's docstring.

`sources_dev` / `extract_sources_dev` are a narrower, local-dev-safe subset
of `all`: every incremental api source except `LOCAL_EXCLUDE_SOURCES`
(currently just `coreac`), plus `ror_dump`, `minorities`, and `oa_topics` — no
`coreac`, no `openaire_dump`. `oa_topics`'s CSV has no producing rule (placed
manually), so both targets list its raw/report path directly rather than
going through a download step — make sure it's actually in
`data/pile/oa_topics/` on a fresh clone before running these. The openaire
dump is 100s of GB and HPC-only (see "Running Individually" below); coreac
just isn't part of the default local run.

```bash
uv run snakemake -s orchestration/Snakefile --cores 4 extract_sources_dev  # extract only
uv run snakemake -s orchestration/Snakefile --cores 4 sources_dev          # extract + load + reports
```

`core_v4_sources` / `extract_core_v4_sources` are a separate, HPC-only source
set used to verify loader idempotency ahead of the core_v4 build: cordis
restricted to its `full_projects_no_pdfs` query ("all cordis docs", no pdf
subset) plus `ror_dump` and `openaire_dump` — no `arxiv`, no `coreac`. Sources
only — enrichment (`rules/pipeline/core_v3/enrichment.smk`, see "Running
Individually" below) runs against core_v3's already-merged data, not these raw
sources directly. Unlike `sources_dev` this includes openaire, so run it with
`--workflow-profile orchestration/profiles/slurm`.
Both extract + load + reports are covered directly by `core_v4_sources`'s own
input list (via `report_source`/`report_dump`), so there's no separate
report-chaining rule to run afterward.

`core_v4_sources` only makes sense against the real `/work/lu72hip` data, so
run it with `ENV=prod` explicitly rather than relying on `get_settings()`'s
"dev" default:

```bash
ENV=prod uv run snakemake --workflow-profile orchestration/profiles/slurm core_v4_sources
```

To verify idempotency (force a real rerun against already-extracted/downloaded
data instead of Snakemake skipping already-up-to-date outputs):

```bash
ENV=prod uv run snakemake --workflow-profile orchestration/profiles/slurm \
    core_v4_sources --forcerun load_source load_ror_dump load_openaire_dump
```

### core_v3

`core_v3` runs the whole core_v3 chain — sources → merge → enrichment (topics, DCH on a GPU node) → one
report per duckdb — see `src/pipelines/core_v3/README.md` for the chain and resume behaviour. HPC-only
(openaire), not part of `all`:

```bash
ENV=prod uv run snakemake --workflow-profile orchestration/profiles/slurm core_v3
ENV=prod uv run snakemake --workflow-profile orchestration/profiles/slurm core_v3 --config limit=500  # sample run
```

### core_v4

`core_v4_projects`, `core_v4_works_linked`, `core_v4_works` and `core_v4` (all of them) run the core_v4 chain: staging
(transformation) -> enrichments -> assemble -> reports. None of them is part of `all` or `core_v3`. HPC-only for real runs.
The rules are in `rules/pipeline/core_v4/`; the enrichments are documented in
`src/pipelines/core_v4/enrichment/README.md`.

```
core_v4_projects:  staging -> {nllb -> topics -> theme, minorities, pillars, dch}(project), {regions, geolocation}(organization)
                   -> assemble --entity project -> reports/pipelines/core_v4/{staging,projects}.md
core_v4_works_linked: staging -> {nllb -> topics -> theme, minorities, pillars, dch}(work, --tier 0: the project-linked works, ~5M)
                   -> assemble --entity work --tier 0 -> reports/pipelines/core_v4/{staging,works_linked}.md
core_v4_works:     core_v4_works_linked, then the same for tier 1 (the org-only works, ~45M)
                   -> assemble --entity work -> reports/pipelines/core_v4/{staging,works_linked,works}.md
```

Order of importance: projects first, then the project-linked works, then the rest.
- Every tier-0 works enrichment has the projects duckdb (`path_duck_projects`) as an ordering-only input (`ancient()`),
  so `core_v4_works_linked` never starts before the projects chain has assembled and pulls it in if it has not run.
- Every tier-1 enrichment has its own tier-0 marker (`_SUCCESS.tier0`) as an ordering-only input, so tier 0 always goes
  first for GPU time and everything else. Rebuilding an earlier stage later does not rerun the later ones.
- `core_v4_works_linked` builds `path_duck_works_linked` (`core_v4_works_linked.duckdb`): the same tables and columns as the
  works file, rows of tier 1 (and their relations) absent. It is servable long before the full file exists. `core_v4_works`
  also asks for it, so the full file is a superset built later; both keep `link_tier` (0 = project-linked, 1 = org-only).
- The tier chains share the side-output directories: tier-0 enrichments write `part-t0-*` files and `_SUCCESS.tier0`,
  tier-1 ones `part-t1-*`, and `_SUCCESS` (everything complete) appears when both tiers are. Details:
  `src/pipelines/core_v4/enrichment/README.md`.

Job counts of the dry runs (`snakemake -n`, no limit, default config, geolocation off, nothing built yet; this includes the
transformation and the reports):

| target | jobs | of which |
|---|---|---|
| `core_v4_projects` | 45 | transformation 1, topics model 1, nllb 4 / topics 8 / minorities 8 / pillars 8 / dch 2 / theme 1 / regions 1 shards, success 7, assemble 1, reports 2 |
| `core_v4_works_linked` | 85 | the projects chain up to its assembled duckdb (pulled in for ordering) plus tier 0: nllb 6 / topics 8 / minorities 8 / pillars 8 / dch 2 / theme 1 shards, success_tier0 6, assemble works_linked 1, reports 2 (staging, works_linked) |
| `core_v4_works` | 214 | everything above (with the projects report) plus the tier-1 chain: nllb 16 / topics 32 / minorities 32 / pillars 32 / dch 8 / theme 1 shards, success 6, assemble works 1, report works |
| `core_v4` | 215 | works + the umbrella target |

(With `--config limit=N` every enrichment is one shard, and `skip=...` removes the enrichments: `core_v4_works_linked --config
limit=2000 skip=nllb,dch` is 34 jobs.)

```bash
# full run on prod, in order of importance (add --keep-going to let independent jobs finish when one fails):
# 1. the projects (and organizations),  2. the project-linked works (tier 0),  3. everything else (tier 1)
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_projects
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_works_linked
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_works
# (or all in one go: ... core_v4; the DAG still runs projects, tier 0, tier 1 in that order)
# Each later command only runs what is missing. Assemble the tier-0 file, then the full one, by hand if needed:
#   uv run python -m pipelines.core_v4.assemble --entity work --tier 0     # -> path_duck_works_linked
#   uv run python -m pipelines.core_v4.assemble --entity work              # -> path_duck_works

# dry run (nothing is executed; shows job counts and shard fan-out)
ENV=prod uv run snakemake -n -s orchestration/Snakefile core_v4_projects

# limit run on the dev sample: everything goes to the core_v4_limit* paths and reports/pipelines/core_v4_limit/,
# never touching a full run's files. One shard per enrichment.
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_projects --config limit=50

# local run without a GPU: skipped enrichments drop out of the DAG, assemble gets --skip, the others read the original text
uv run snakemake -s orchestration/Snakefile --cores 4 core_v4_projects --config limit=50 skip=nllb,dch
```

Config keys (`--config key=value ...`), all optional:

| key | effect |
|---|---|
| `limit=N` | switch the whole DAG to the `core_v4_limit` variant; N goes to the transformation as `--limit`. It is a rule param, so changing N reruns the transformation and everything after it |
| `work_cap=N` | transformation `--work-cap N` |
| `skip=nllb,dch` | leave these enrichments out (names: nllb, topics, theme, minorities, pillars, dch, regions, geolocation); assemble gets `--skip`; text enrichments get `--allow-untranslated` when nllb is skipped; skipping topics also skips theme |
| `geolocation_max_requests=N` | Mapbox budget; geolocation is only part of the DAG when N > 0 (default: off) |
| `geolocation_permanent=true` | send `--permanent` (billed, $5/1,000). Default is temporary geocoding (free tier) |
| `shards=N`, `shards_<name>=N` | override the shard count of every / one enrichment (theme, regions, geolocation are always 1) |
| `shards_<name>_t0=N`, `shards_<name>_t1=N` | the same for the tier-0 / tier-1 works unit only (e.g. `shards_nllb_t1=24`) |

Local end-to-end run on a synthetic sample (dev machine, no GPU, no OpenAire dump; verified in Phase 3E). The inputs the DAG
expects are the OpenAire staging v4 (`openaire_dump.path_duck_staging_v4`), `ror_raw.duckdb` and the Cordis
`full_projects_no_pdfs` loader checkpoint. Locally there is no OpenAire dump and the full Cordis db is empty, so:

```bash
# 1. a staging-v4-shaped fixture (4,000 projects, 7,500 orgs, 80,000 works, ...; git-ignored, seeded) + a truth db of what was planted
uv run python -m pipelines.core_v4.build_sample_fixture            # -> data/duckdb/sample/
# 2. make the DAG find it where the config expects staging v4 (a symlink; a real `stage_openaire_dump_v4` would replace it)
ln -s ../sample/openaire_staging_v4_sample.duckdb data/duckdb/sources/openaire_staging_v4.duckdb
# 3. read the Cordis heritage subset instead of the empty full db (dev override, env var read by the transformation)
export CORE_V4_CORDIS_DB=data/duckdb/sources/cordis_heritage_subset_with_pdfs_raw.duckdb
UV_NO_SYNC=1 uv run snakemake -s orchestration/Snakefile --cores 4 core_v4 --config limit=2000 skip=nllb,dch
```

Enabling geolocation (runs last in the projects chain, single process, spends the Mapbox budget; cached answers are free):

```bash
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm \
    core_v4_projects --config geolocation_max_requests=20000
```

If the budget runs out the CLI writes no `_SUCCESS`, the job fails with a missing-output error and assemble does not
run; rerun with a larger number. Without `geolocation_max_requests`, assemble is called with `--skip geolocation`.

Sharding and idempotency: each (enrichment, unit) is N jobs (`--shard I/N`, one `temp()` sentinel each under
`.snakemake/sentinels/enrichment/core_v4/<name>/<unit>/`, unit = project, organization, work-t0, work-t1) and one local
`core_v4_success` job that writes the real artifact, `<enrichment_dir>/<name>/<entity>/_SUCCESS` (`_SUCCESS.tier0` for
work-t0, written by `core_v4_success_tier0`), through `SideOutput`. Downstream rules depend on that file, so what is
done is decided by files on disk. The marker holds the staging fingerprint the enrichment ran against; assemble refuses
side outputs whose fingerprint is not the current staging's (a staging rebuilt after the enrichments ran), see below. To force one enrichment to rerun (it resumes from its parquet parts, and everything
downstream reruns too), `--forcerun` its `_SUCCESS` (**absolute path**; a relative path fails with `MissingRuleException`):

```bash
ENV=prod uv run snakemake -s orchestration/Snakefile --workflow-profile orchestration/profiles/slurm core_v4_projects \
    --forcerun $PWD/data/enrichment/core_v4/dch/project/_SUCCESS   # path under /work/lu72hip/... on prod, whatever config says
# tier 0 of works: .../dch/work/_SUCCESS.tier0 with core_v4_works_linked; tier 1 (and the whole): .../dch/work/_SUCCESS
```

A staging built before `link_tier` existed (Phase 3F) must be rebuilt (`--forcerun core_v4_transformation`): the tier
enrichments stop at start with "work has no link_tier column".

Stale side outputs. Side outputs are never cleared when the staging is rebuilt (a rebuild with another `limit` / `work_cap`
leaves orphan rows). Every `_SUCCESS` therefore records a fingerprint of the id set the enrichment ran against, and assemble
compares it with the current staging (per tier for `--tier 0`): on a mismatch it stops and names the side outputs. Either
rerun those enrichments, or pass `--allow-stale name1,name2` to `pipelines.core_v4.assemble` (not exposed as a Snakemake
config key on purpose). `--allow-stale nllb` keeps the expensive cached translations: rows whose id is still in staging get
them, the others stay untranslated, ids that vanished are ignored. Risk: a row whose text changed under the same id keeps
its old translation. An empty `_SUCCESS` from before the fingerprint counts as stale.

Only deleting `_SUCCESS` is not enough: Snakemake does not rebuild a missing intermediate file while the assembled
duckdbs and reports downstream of it are up to date (the target then says "Nothing to be done").

Shard counts and GPU resources are in `rules/pipeline/core_v4/enrichment.smk` (`_CORE_V4_DEFAULT_SHARDS`). NLLB is sized from
the measured numbers (`src/enrichment/nllb_translator/README.md`: 1.3B distilled, CTranslate2 float16, beam 1, 13.3k source
tokens/s, 10.1 GB VRAM, 421 works/s end to end, ~37 A100-hours for 50M works): `gpu-test`, `gres=gpu:1`, **no `a100_80gb`
constraint** (10 GB is enough; DCH keeps its own 80 GB constraint), projects 4 shards x 6 h, works tier 0 6 shards x 4 h,
works tier 1 16 shards x 6 h, each about 3x the expected time so a slower card or longer texts still fit under the 12 h limit
(a timed-out shard resumes). The model is read from `data/models/nllb` through the download guard; compute nodes have no
internet, so download it once on the login node (`uv run python -m enrichment.nllb_translator.download`) before the first
GPU job, the rule sets `HF_HUB_OFFLINE=1`. The other numbers are still provisional.

### Running Individually

**api_runner:**

```bash
# extract a single api source (arxiv, cordis), single query_id from config/api_runner.yaml
uv run python -m common.api_runner.run_extractor --source <source> --query_id <query_id>

# load that source's extracted files into duckdb, same source/query_id
uv run python -m common.api_runner.run_loader --source <source> --query_id <query_id> --db duck
```

**dumps:**

```bash
# ror: download raw dump, then load into duckdb
# (download is tracked by a .download_complete marker, like openaire. Already have the
# dump and its duckdb? Skip the download: touch -r <ror_raw.duckdb> <marker from config/dumps.yaml>,
# then once: snakemake --cleanup-metadata <absolute path to ror_raw.duckdb>)
uv run snakemake -s orchestration/Snakefile --cores 4 download_ror_dump
uv run snakemake -s orchestration/Snakefile --cores 4 load_ror_dump

# dont use --work-profile if you are already on a worker node on the HPC
# openaire: download raw dump — 100s of GB, run on the HPC, not locally
uv run snakemake --workflow-profile orchestration/profiles/slurm download_openaire_dump

# openaire: full load into duckdb (HPC)
uv run snakemake --workflow-profile orchestration/profiles/slurm load_openaire_dump

# openaire: fast local test load, limited to N files per entity (organization/project/
# publication/relation) instead of the full dump
# --limit isn't a target the DAG exposes.
uv run python -m sources.dumps.openaire.loader --limit 1000

# minorities: live Wikidata SPARQL discovery, then load into duckdb
uv run snakemake -s orchestration/Snakefile --cores 4 discover_minorities_candidates
uv run snakemake -s orchestration/Snakefile --cores 4 load_minorities

# oa_topics: no download step, its CSV is placed manually — just load
uv run snakemake -s orchestration/Snakefile --cores 4 load_oa_topics
```

**reports:** each dump's markdown data-profile report is its own DAG target
(`rules/dumps.smk`'s `report_dump`), regenerated only when its duckdb actually
changed:

```bash
uv run snakemake -s orchestration/Snakefile --cores 4 "reports/sources/dumps/ror_dump_2026_08_03.md"
```

For a full unconditional regen of every configured duckdb's report regardless
of staleness, use the underlying script directly:

```bash
uv run python -m common.report.generate_reports
```

## Sentinels

Rules with a real file artifact (duckdb file, downloaded dump) declare that file
directly as `output:`. Rules with no file artifact at all — `meta_heritage.smk`'s
scripts write straight to postgres — use a `touch()` sentinel under
`.snakemake/sentinels/` instead, purely so Snakemake has something to check
staleness against. Delete a sentinel to force that one job to rerun.

## Adding pipeline-version rules

Once a pipeline version has real stages (`pipelines/core_v4/{merge,analysis,
enrichment,model,serve}`), add `rules/pipeline/core_vN/*.smk` for that version
rather than stubbing them out in advance — the DAG should only describe what's
actually runnable. `pipelines/core_v3/`'s data model itself is frozen (its
`transformation.py` merge is `merge.smk`), and its `enrichment/` subpackage is wired via
`rules/pipeline/core_v3/enrichment.smk` — enrichment is prep for core_v4, proven against
core_v3's real data until core_v4 exists.
