#!/bin/bash
# Cluster export of the 132 GB core_v4 DuckDB into OpenSearch-ready Parquet (read-only on the database). Run from /vast/lu72hip/hm_pipeline,
# never from /home:   cd /vast/lu72hip/hm_pipeline && sbatch src/pipelines/core_v4/serving/export/export.sbatch
# Same conventions as the merge job (partition list, 200 GB); the run resumes: existing files are skipped, delete a file to redo it.
#SBATCH --job-name=hm_serving_export
#SBATCH --partition=fat,standard,long
#SBATCH --cpus-per-task=16
#SBATCH --mem=150G
#SBATCH --time=06:00:00
#SBATCH --output=/vast/lu72hip/hm_pipeline/tmp_session/hm_serving_export-%j.out
set -euo pipefail
cd /vast/lu72hip/hm_pipeline

DB=/work/lu72hip/data/duckdb/core/core_v4_noworkenrichment.duckdb
OUT=/vast/lu72hip/hm_pipeline/data/serving_export
TMP=/vast/lu72hip/hm_pipeline/tmp_session/duck_tmp

# --only-group serving = duckdb only, no torch/CUDA env needed
uv run --frozen --only-group serving python src/pipelines/core_v4/serving/export/export.py \
    --db "$DB" --out "$OUT" \
    --works-chunks 10 \
    --memory-limit 120GB --threads 16 --temp-dir "$TMP"

# smoke variant first (a few minutes): add  --works-sample 1000 --out /vast/lu72hip/hm_pipeline/data/serving_export_smoke
# then: du -sh "$OUT"/*  (expected: works ~8 GB in 10 files, projects ~0.75 GB, organisations ~60 MB, minorities/grants < 1 MB, api/*.json small)
