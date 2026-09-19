# DCH Classification — Context for GPU Node Session

## What was built

Split across two layers:
- `dch_classifier.py` (here) — pure classifier (`DchClassifier`), no duckdb/table knowledge. Loads the BERT model, tokenises + runs GPU inference. Implements `enrichment.interface.Enricher[str, float]`.
- `src/pipelines/core_v3/enrichment/dch_classification.py` — duckdb-aware glue: reads `project`/`work` rows (via `text_sources.py`), calls the classifier, writes results back, resumable. This part is pipeline-specific — core_v4 will get its own copy once it has tables.

Classifies project/work rows in core_v3's duckdb as Cultural Heritage (CH) or not, using
the fine-tuned BERT model. Adds two columns to the target table:
- `is_ch  BOOLEAN` — True if P(is DCH) >= 0.5
- `pred   FLOAT`   — P(is DCH), the raw probability (not just 0/1)

## Key paths

| Thing           | Path |
|-----------------|------|
| Classifier       | `src/enrichment/dch_classification/dch_classifier.py` |
| Runner (duckdb glue) | `src/pipelines/core_v3/enrichment/dch_classification.py` |
| BERT model      | `data/models/bert_classifier/` (safetensors + config.json) |
| Tokenizer cache | `~/.cache/huggingface/hub/models--bert-base-uncased` (HPC: under the job user's home) |
| Target DB       | core_v3's `path_staging_duck` (see `config/pipelines.yaml`) — the runner reads this from config, not a CLI flag |

## How to run

```bash
# dry-run: N rows, no DB writes, prints throughput
uv run python -m pipelines.core_v3.enrichment.dch_classification --test 5

# production: classifies all remaining project rows, resumable
uv run python -m pipelines.core_v3.enrichment.dch_classification
uv run python -m pipelines.core_v3.enrichment.dch_classification --entity work

# or via Snakemake:
uv run snakemake -s orchestration/Snakefile dch_classification
```

## Design decisions to remember

- `--test N` → N rows, no DB writes, prints throughput and extrapolated time for 4M rows
- Production → adds `is_ch`/`pred` columns, resume-safe via `COUNT(*) WHERE is_ch IS NOT NULL` + a
  binary results-file row count (see `dch_classification.py`'s `_results_row_count`) — a crash
  mid-run loses at most one chunk (`CHUNK_ROWS` = 20 × batch size) of GPU compute, not the whole job
- Text input: `CONCAT_WS(' ', title, keywords, list_aggregate(subjects, 'string_agg', ' '), summary)`
  - `subjects` is `list<varchar>` in the project table — use `list_aggregate`, not direct concat
- Prefetch queue (depth 4, inside `DchClassifier.enrich`): background thread tokenises while main
  thread runs GPU inference
- Read connection is closed before any writing happens (`run_classification` loads everything into
  memory up front via `text_sources.all_text_rows`) — avoids a DuckDB write-lock conflict
- AMP: bf16 on Ampere+, fp16 fallback; softmax cast to fp32 outside autocast for stability
- Eager mode, no `torch.compile` — batch size (2048 by default) is sized for eager mode's FFN
  intermediate memory footprint; raising `DEFAULT_BATCH_SIZE` may need `torch.compile` to stay
  cheap on VRAM, hasn't been tried
- Batch size: 2048. On 80GB A100/H100 this is safe. Can go higher if VRAM allows.

## What to check in a test run

1. Query works — no error on `list_aggregate(subjects, ...)` (subjects might be NULL or empty list)
2. Tokenizer loads from cache (no internet needed on GPU node)
3. Model loads and moves to CUDA without OOM
4. Throughput reported at end: should be 2,000–5,000 seq/s on A100 80GB → ~15-25 min for 4M rows
5. Sample predictions look sane (mix of CH and NOT CH, not all one class)

## If something breaks

- `list_aggregate` error → subjects might be a different type in the target DB; try replacing with just `summary` in the query
- Tokenizer not found → check the HF cache path on that node, or let it fetch from the internet once
- OOM → reduce `DEFAULT_BATCH_SIZE` in `dch_classifier.py` (try 256)
