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

`all` covers incremental extraction/loading plus the versioned bulk dumps
(ror_dump, openaire_dump). It does **not** include `openalex_dump` (corev5
scope), `rules/pipeline/core_v3/enrichment.smk`, or `meta_heritage.smk` (all
postgres-backed, run by name only) — see each rule file's docstring.

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
