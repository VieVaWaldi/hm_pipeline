# core_v3

The data model itself is frozen and not served (the old export-to-postgres step
is gone — core_v4 gets a real duckdb → OpenSearch serve stage). But the merge and
`enrichment/` are real, runnable glue, wired into Snakemake as one target:

```bash
# everything: sources -> merge -> enrichment -> reports (HPC)
ENV=prod uv run snakemake --workflow-profile orchestration/profiles/slurm core_v3

# same, on a sample — overwrites core_v3's duckdbs, see "Limit runs" below
ENV=prod uv run snakemake --workflow-profile orchestration/profiles/slurm core_v3 --config limit=500
```

## The chain

Three duckdb files, each a snapshot of one stage (`config/pipelines.yaml`):

```
openaire_raw.duckdb
      │  stage_openaire_dump
      ▼
openaire_staging_2.duckdb ─┐
ror_raw.duckdb ────────────┼─► core_v3_transformation ──► core_v3_staging.duckdb      (merge output, never enriched)
cordis_..._raw.duckdb ─────┘                                     │
                                                                 ▼  copy
                                              seed_topics ──► topic_modelling ──► core_v3_staging_2.duckdb
                                                                                        │
                                                                                        ▼  copy
                                                       dch_classification (GPU) ──► core_v3.duckdb   (final)
                                                                                        │
                                              report_core_v3 (reports/pipelines/core_v3/: staging, staging_2, main)
```

1. **`core_v3_transformation`** rebuilds `core_v3_staging.duckdb` from scratch: copies
   `organization`/`project`/`work`/`relation` from OpenAire staging, merges ROR into
   `organization` and Cordis into `relation` (`READ_TRANSFORMATION.md`).
2. **`seed_topics`** copies staging → `core_v3_staging_2.duckdb` and seeds the OpenAlex topic taxonomy.
3. **`topic_modelling`** TF-IDF-classifies `project` rows into `relation_topic`. Resumable.
4. **`dch_classification`** copies staging_2 → `core_v3.duckdb`, then adds `project.is_ch` / `project.pred`
   with the BERT classifier on a GPU node.
5. **`report_core_v3`** profiles each of the three duckdbs.

`geolocation` (Mapbox only; nothing in core_v3 has the city + country it needs) is not in the chain;
run it by name if wanted.

### Resuming a failed run

Rerun the same command; Snakemake continues where the chain stopped. If a job fails, Snakemake deletes
the duckdb *that job* was producing — so a failed `dch_classification` loses its half-made
`core_v3.duckdb` — but the GPU work isn't in that file. DCH appends every chunk to
`data/duckdb/core/core_v3_dch_project_results.{ids,preds}.bin`; a rerun re-copies staging_2, trims the
`.bin` files to the last complete row (`enrichment/dch_results.py`), and classifies only what's left.
`topic_modelling` resumes from `relation_topic` in staging_2. The transformation deletes the `.bin` files,
since they belong to the rows it rebuilds. `enrichment/test_dch_resume.py` checks this without a GPU.

### Resources

`merge.smk` defines the allocation (`CORE_V3_MEM_MB`, `CORE_V3_CPUS`, `CORE_V3_RUNTIME`, same as the openaire
rules) and passes it to `transformation.py` / `topic_modelling.py` as `--mem-mb` / `--threads`.
`dch_classification` has its own GPU spec in `enrichment.smk`.

### Limit runs

`--config limit=N` limits the OpenAire tables the merge copies to N rows each (ROR and Cordis still join in
full); every later stage just processes what's there. Switching between a limit and a full run reruns the
merge and everything after it automatically. Both use the same three duckdb paths, so **a limit run
overwrites a full run's files**.

## Running a step directly

```bash
uv run python -m pipelines.core_v3.transformation --limit 500    # fast smoke test
uv run python -m pipelines.core_v3.transformation --mem-mb 64000 --threads 8
uv run python -m pipelines.core_v3.test_transformation           # in-memory sanity check
uv run python -m pipelines.core_v3.enrichment.seed_topics
uv run python -m pipelines.core_v3.enrichment.topic_modelling --mem-mb 64000 --threads 8
uv run python -m pipelines.core_v3.enrichment.test_dch_resume    # DCH crash-recovery check, no GPU
```

Run directly, each stage starts from the previous stage's file, so run them in order.

See `READ_TRANSFORMATION.md` for the merge design and `eda.ipynb` for the source EDA
this was based on. `materialized/` predates core_v4's serve stage and is docs-only,
not runnable.

## enrichment/

Duckdb-aware glue around the reusable, table-agnostic enrichment capabilities in
`src/enrichment/` (`classifier.py`, `geocoder.py`, `dch_classifier.py` — each one
implements `enrichment.interface.Enricher`, pure input-record-in/output-record-out,
no duckdb or table knowledge at all). Everything in *this* directory is the opposite:
it knows core_v3's schema, reads `project`/`work`/`organization` rows, calls the
matching `Enricher`, and writes results back.

This is enrichment's current proving ground, not core_v3's revival — the goal is
core_v4, and this package only exists because core_v4 has no tables of its own
yet. Once it does, a parallel `pipelines/core_v4/enrichment/` gets added (its own
`rules/pipeline/core_v4/enrichment.smk` too) — this one doesn't get renamed or
repointed at core_v4's tables.

`text_sources.py` holds the one shared bit of schema knowledge two enrichments
both need (turning a `project`/`work` row into classifiable text) — every
text-based enrichment imports it instead of copying the query.
