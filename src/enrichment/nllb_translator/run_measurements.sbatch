#!/bin/bash
# NLLB README measurements on gpu-test (12 h limit, 80 GB A100). Needs the models downloaded on the login node
# (python -m enrichment.nllb_translator.download) and the sample from sample_works.py.
#   sbatch src/enrichment/nllb_translator/run_measurements.sbatch
#SBATCH --job-name=nllb_bench
#SBATCH --partition=gpu-test
#SBATCH --gres=gpu:a100:1
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --time=03:00:00
#SBATCH --output=data/logs/nllb_bench_%j.out
cd "${SLURM_SUBMIT_DIR:-.}"
ENV=dev uv run --no-sync python -m enrichment.nllb_translator.benchmark --stage lid --out data/enrichment/nllb_sample/benchmark_lid.json
ENV=dev uv run --no-sync python -m enrichment.nllb_translator.benchmark --stage speed --models 600M 1.3B --backends ctranslate2 transformers --out data/enrichment/nllb_sample/benchmark_speed.json
