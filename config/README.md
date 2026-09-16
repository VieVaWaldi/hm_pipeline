# Config

Three files, validated by pydantic models in `common/config/` — no more global untyped
dict getter. `ENV` (dev/prod, from `.env`) selects which block of `config.yaml` is used.

- [`config.yaml`](config.yaml) — paths and database connection, per environment (dev/prod).
  Loaded via `common.config.settings.get_settings()`.
- [`queries.yaml`](queries.yaml) — extraction query definitions for the incrementally-extracted
  sources (arxiv, cordis, coreac, meta_heritage). Loaded via
  `common.config.queries.get_query_settings()`. Each source contains:
  - **checkpoint:** The field in the raw data used to track extraction progress
  - **queries:** List of search queries to execute, each with:
    - **checkpoint_range:** Extraction window size
    - **checkpoint_start:** Starting point for initial extraction (if no checkpoint exists)
- [`paths.yaml`](paths.yaml) — per-source disk locations for bulk dumps and their duckdb
  files (openaire_dump, openalex_dump, ror_dump, core_v3). Field names vary per source, so
  this is a loosely-typed map rather than a fixed schema. Loaded via
  `common.config.paths.get_source_paths()`.