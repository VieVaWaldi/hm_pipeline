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
│       └── core_v3/
│           └── enrichment.smk      # duckdb-native enrichment, runs against core_v3's real data
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
`openalex_dump` (corev5 scope), `rules/pipeline/core_v3/enrichment.smk`, or
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
`transformation.py` merge is standalone, not a rule), but its `enrichment/`
subpackage is wired via `rules/pipeline/core_v3/enrichment.smk` — enrichment
is prep for core_v4, proven against core_v3's real data until core_v4 exists.
