# Meilisearch

Serving index for core_v4 gold data (replaces the earlier OpenSearch plan). The pipeline's
`serve` stage (once core_v4 gold exists) indexes into this; the webapp reads from it.

We run **plain Docker on dev**, **Singularity (SIF images) on the HPC** — same pattern as
every other container in this repo (see `orchestration/envs/README.md`), except Meilisearch
is a long-lived service rather than a one-shot job container, so it isn't declared as a
Snakemake rule `container:`.

## Dev

```bash
docker compose --env-file .env -f infra/docker-compose.yml up -d
```

Run from the repo root and always pass `--env-file .env` explicitly — Compose otherwise
looks for `.env` next to the compose file (`infra/.env`), not the repo-root `.env` everything
else in this repo reads from, and `MEILI_MASTER_KEY` silently comes through empty.

Data persists in the `meili_data` docker volume across restarts. Meilisearch is reachable at
`http://localhost:7700`.

## HPC (Draco)

No cluster-wide long-lived instance. Meilisearch runs **transiently, inside the same Slurm
job as the serve step**:

```bash
# once: build the .sif from the same image used in dev
apptainer pull infra/meilisearch/meilisearch.sif docker://getmeili/meilisearch:v1.53.2

# inside the serve job:
apptainer instance start \
  --bind /work/lu72hip/data/meilisearch:/meili_data \
  infra/meilisearch/meilisearch.sif meili_instance \
  meilisearch --db-path /meili_data --master-key "$MEILI_MASTER_KEY"

# wait for health check, then run the serve step's indexing against
# http://localhost:7700, then:
apptainer instance stop meili_instance
```

After indexing, export a Meilisearch dump (`POST /dumps`) from the instance before stopping
it and ship the dump file to wherever the webapp's live Meilisearch instance runs. This repo
never hosts a long-lived Meilisearch server itself — it only produces data for one, same as
the README's existing "ready to be deployed" framing for the serving index.

`meilisearch.sif` is gitignored (build artifact, pulled on demand — same treatment as any
other `.sif`).

## Loading a duckdb table into an index

`common.search.index_duckdb_table.index_duckdb_table(con, table, index_name, primary_key,
replace_all=False)` paginates a duckdb table into an index, batched, waiting for each batch's
indexing task to finish before returning — so "the call returned" means "it's searchable", not
"it's queued". `replace_all=True` clears the index first — needed for any table that's a full
point-in-time snapshot each run (every staging table in this repo, built via `CREATE OR REPLACE
TABLE`), otherwise a row that disappeared from the source stays behind forever, since
`add_documents()` only ever adds/updates.

Verified end-to-end (running Meilisearch v1.53.2) against
`data/duckdb/sources/minorities_terms.duckdb` → 304/304 rows, with full-text search,
autocomplete-style prefix matching, and facets (including a derived boolean and numeric
range/`facetStats`) all confirmed working — see
`src/sources/external/minorities/index_meilisearch.py`. Not wired into any Snakemake rule yet
— that happens once `src/pipelines/core_v4/serve/` exists and there's a real gold schema to
decide indices from.
