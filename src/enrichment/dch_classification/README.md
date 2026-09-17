# CH Classification — Context for GPU Node Session

## What was built

`src/enrichment/dch_classification/run_ch_classification.py`

Classifies all ~4M projects in core_v3_final.duckdb as Cultural Heritage (CH) or not,
using the fine-tuned BERT model. Adds two columns to the `project` table:
- `is_ch  BOOLEAN` — True if P(CH) >= 0.5
- `pred   FLOAT`   — P(CH), the raw probability (not just 0/1)

## Key paths

| Thing           | Path |
|-----------------|------|
| Script          | `src/enrichment/dch_classification/run_ch_classification.py` |
| BERT model      | `data/models/bert_classifier/` (safetensors + config.json) |
| Tokenizer cache | `/home/lu72hip/.cache/huggingface/hub/models--bert-base-uncased` |
| Production DB   | `/work/lu72hip/data/duckdb/core/core_v3_final.duckdb` (NOT ready yet) |
| Staging DB      | `/work/lu72hip/data/duckdb/core/core_v3.duckdb` (use for tests) |
| Config          | `config/config_queries.json` → `core_v3.path_final_duck` / `path_staging_duck` |

## How to run

Activate env first:
```bash
cd /home/lu72hip/DIGICHer/dh_pipeline
source venv/bin/activate
export PYTHONPATH=/home/lu72hip/DIGICHer/dh_pipeline/src
```

**Temp test (no writes, uses staging DB, 5 batches):**
```bash
python src/enrichment/dch_classification/run_ch_classification.py --test 5
```
This reads from `path_staging_duck`, runs 5 × 2048-batch inference passes, prints seq/s
and extrapolated time for 4M rows. Writes nothing to any DB.

**Production (when final duck is ready):**
```bash
python src/enrichment/dch_classification/run_ch_classification.py
```

## Design decisions to remember

- `--test N` flag → uses `path_staging_duck`, no DB writes, N batches only
- Production → uses `path_final_duck`, adds columns, resume-safe via `COUNT(*) WHERE is_ch IS NOT NULL`
- Text input: `CONCAT_WS(' ', title, keywords, list_aggregate(subjects, 'string_agg', ' '), summary)`
  - `subjects` is `list<varchar>` in the project table — use `list_aggregate`, not direct concat
- Prefetch queue (depth 4): background thread reads DB + tokenises while main thread runs GPU
- Separate read-only DuckDB connection in producer thread to avoid write-lock conflict
- AMP: bf16 on Ampere+, fp16 fallback; softmax cast to fp32 outside autocast for stability
- `torch.compile` attempted at startup — first batch slow (~30s), subsequent batches 20-40% faster
- Batch size: 2048. On 80GB A100/H100 this is safe. Can go higher if VRAM allows.

## What to check in the test run

1. Query works — no error on `list_aggregate(subjects, ...)` (subjects might be NULL or empty list in staging)
2. Tokenizer loads from cache (no internet needed on GPU node)
3. Model loads and moves to CUDA without OOM
4. torch.compile succeeds (if it fails, it gracefully falls back — check the log)
5. Throughput reported at end: should be 2,000–5,000 seq/s on A100 80GB → ~15-25 min for 4M rows
6. Sample predictions look sane (mix of CH and NOT CH, not all one class)

## If something breaks

- `list_aggregate` error → subjects might be a different type in staging; try replacing with just `summary` in the query
- Tokenizer not found → `BertTokenizerFast.from_pretrained("/home/lu72hip/.cache/huggingface/hub/models--bert-base-uncased")`
- torch.compile fails → already handled with try/except, just continues without it
- OOM → reduce `BATCH_SIZE` at top of script (try 256)
