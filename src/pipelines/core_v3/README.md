# core_v3

Not wired into Snakemake orchestration, and not served (the old export-to-postgres
step is gone — core_v4 gets a real duckdb → OpenSearch serve stage). But the merge
itself runs end-to-end on real data, standalone:

```bash
uv run python -m pipelines.core_v3.transformation                # full run
uv run python -m pipelines.core_v3.transformation --limit 500    # fast smoke test
uv run python -m pipelines.core_v3.test_transformation           # in-memory sanity check
```

See `READ_TRANSFORMATION.md` for the merge design and `eda.ipynb` for the source EDA
this was based on. `topics/`, `orm/`, and `materialized/` predate core_v4's enrichment
refactor and are not part of this runnable path.