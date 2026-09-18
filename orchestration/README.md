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
│   ├── dumps.smk                   # ↔ src/sources/dumps/ (ror, openaire, openalex)
│   ├── external/                   # ↔ src/sources/external/
│   │   └── meta_heritage.smk       # postgres-backed, not core_v4 scope, own scripts
│   └── pipeline/                   # ↔ pipeline-level code, not a source at all
│       └── core_v3/
│           └── enrichment.smk      # legacy enrichment scripts (postgres-backed)
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

`./run_all_sources.sh` wraps the local-dev-safe source set (see `sources_local`
below), split into an extraction step and a load step:

```bash
./orchestration/run_all_sources.sh extract           # extraction only, no load
./orchestration/run_all_sources.sh load              # extract + load (also the default)
./orchestration/run_all_sources.sh load --report     # extract + load, then per-source reports
./orchestration/run_all_sources.sh --parallel 8      # override Snakemake's --cores (default 4)
```

`--report` (only valid with `load`) (re)generates each source's markdown
data-profile report afterwards (see `src/common/report/`). It's this script's
own flag, not Snakemake's built-in `--report <file>` (which renders an HTML
run summary, a different thing). `--parallel N` is just this script's name
for Snakemake's own `--cores N` — Snakemake already runs independent jobs
(e.g. the arxiv/cordis extractions) concurrently once enough cores are
available; this flag exposes that budget instead of hardcoding it.

Equivalent bare Snakemake targets, if you don't want the wrapper:
```bash
uv run snakemake -s orchestration/Snakefile --cores 4 extract_sources_local  # extract only
uv run snakemake -s orchestration/Snakefile --cores 4 sources_local          # extract + load
```

`all` covers incremental extraction/loading plus the versioned bulk dumps
(ror_dump, openaire_dump). It does **not** include `openalex_dump` (corev5
scope), `rules/pipeline/core_v3/enrichment.smk`, or `meta_heritage.smk` (all
postgres-backed, run by name only) — see each rule file's docstring.

`sources_local` / `extract_sources_local` are a narrower, local-dev-safe
subset of `all`: every incremental api source except `LOCAL_EXCLUDE_SOURCES`
(currently just `coreac`), plus `ror_dump` — no `coreac`, no `openaire_dump`.
The openaire dump is 100s of GB and HPC-only (see "Running Individually"
below); coreac just isn't part of the default local run. Run either by name
when you actually want it.

`core_v3_sources` / `extract_core_v3_sources` are a separate, HPC-only source
set used to verify loader idempotency ahead of the core_v3 rebuild: cordis
restricted to its `full_projects_no_pdfs` query ("all cordis docs", no pdf
subset) plus `ror_dump` and `openaire_dump` — no `arxiv`, no `coreac`. Sources
only, not the core_v3 pipeline itself (see `rules/pipeline/core_v3/enrichment.smk`
for that, under "Running Individually" below). Unlike `sources_local` this
includes openaire, so run it with `--workflow-profile orchestration/profiles/slurm`,
not through `run_all_sources.sh`. `report_core_v3_sources` chains the
per-source reports on afterward, same idea as `run_all_sources.sh load
--report` but wired into the DAG itself instead of a separate command.

`./run_core_v3_sources.sh` wraps all three (extract / load / load+report,
default), and sets `ENV=prod` itself — `core_v3_sources` only makes sense
against the real `/work/lu72hip` data, so this is the one place that forces
prod rather than leaving it to `get_settings()`'s "dev" default (see the
script's own header comment for why that can't just live in the Snakefile).
It also always passes `--forcerun load_source load_ror_dump
load_openaire_dump` on the load/report steps — without it Snakemake would see
those outputs are already up to date and skip them, defeating the point of
an idempotency check:

```bash
./orchestration/run_core_v3_sources.sh                  # extract + load + report (default)
./orchestration/run_core_v3_sources.sh extract           # extraction only
./orchestration/run_core_v3_sources.sh load               # extract + load, no report
```

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
```

## Sentinels

Rules with a real file artifact (duckdb file, downloaded dump) declare that file
directly as `output:`. Rules with no file artifact at all — `meta_heritage.smk`'s
scripts write straight to postgres — use a `touch()` sentinel under
`.snakemake/sentinels/` instead, purely so Snakemake has something to check
staleness against. Delete a sentinel to force that one job to rerun.

## Adding pipeline-version rules

Once a pipeline version has real stages (`pipelines/core_v4/{merge,analysis,
enrichment,model,serve}`, or a rebuilt-runnable `pipelines/core_v3/`), add
`rules/pipeline/core_vN/*.smk` for that version rather than stubbing them out
in advance — the DAG should only describe what's actually runnable.
