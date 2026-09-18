# Config

Four files, validated by pydantic models in `common/config/` — no more global untyped
dict getter. `ENV` (dev/prod, from `.env`) selects which block of `config.yaml` is used.

- [`config.yaml`](config.yaml) — paths, database connection and OpenSearch host/port, per
  environment (dev/prod). Loaded via `common.config.settings.get_settings()`. Also carries
  `hpc_root` (prod only, e.g. `/work/lu72hip`) — the one place dev/prod filesystem layout
  differs; see below. The OpenSearch admin password is not here — it's
  `OPENSEARCH_INITIAL_ADMIN_PASSWORD` in `.env`, same as other API keys (see
  [infra/docker-compose.yml](../infra/docker-compose.yml)).
- [`api_runner.yaml`](api_runner.yaml) — extraction query definitions for the incrementally-extracted
  sources (arxiv, cordis, coreac, meta_heritage). Loaded via
  `common.config.api_runner.get_query_settings()`. Each source contains:
  - **checkpoint:** The field in the raw data used to track extraction progress
  - **queries:** List of search queries to execute, each with:
    - **checkpoint_range:** Extraction window size
    - **checkpoint_start:** Starting point for initial extraction (if no checkpoint exists)
- [`dumps.yaml`](dumps.yaml) — per-source disk locations for bulk dumps and their duckdb
  files (openaire_dump, openalex_dump, ror_dump). Loaded via
  `common.config.dumps.get_dumps_paths()`.
- [`pipelines.yaml`](pipelines.yaml) — per-pipeline-version duckdb file locations (core_v3, ...).
  Loaded via `common.config.pipelines.get_pipeline_paths()`.

## Path resolution (dumps.yaml / pipelines.yaml)

Field names vary per source, so both are loosely-typed maps rather than a fixed schema —
but every value in them must be a **repo-relative path** (e.g. `data/duckdb/sources/ror_raw.duckdb`),
never an environment-specific absolute one. `common.config.source_paths.resolve_data_path()`
is the single place that turns that relative path into an absolute one: on dev it's joined
with the project root, on prod it's joined under `hpc_root` from `config.yaml`. This is the
same dev/prod split `common.file_handling.path_utils.get_source_data_path()` already uses for
`api_runner.yaml`'s extracted-data paths — so nothing in `dumps.yaml`/`pipelines.yaml` (or
`api_runner.yaml`'s `path_duck` entries) should ever hardcode `/work/lu72hip` directly.
