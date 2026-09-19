# core_v3

The data model itself is frozen and not served (the old export-to-postgres step
is gone — core_v4 gets a real duckdb → OpenSearch serve stage). But the merge
runs end-to-end on real data, standalone, and `enrichment/` is real, runnable
glue — wired into Snakemake, unlike the merge:

```bash
uv run python -m pipelines.core_v3.transformation                # full run
uv run python -m pipelines.core_v3.transformation --limit 500    # fast smoke test
uv run python -m pipelines.core_v3.test_transformation           # in-memory sanity check

uv run snakemake -s orchestration/Snakefile seed_topics topic_modelling geolocation dch_classification
```

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
