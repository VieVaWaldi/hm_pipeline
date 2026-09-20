#!/usr/bin/env bash
# Rebuild everything from the mini DB against the local OpenSearch on :9201 and run the use-case tests.
# Usage (from the repo root):  bash src/pipelines/core_v4/serving/prototype/run_all.sh [path/to/core_v4.duckdb]
set -euo pipefail
cd "$(dirname "$0")/../../../../.."                        # repo root
PY=.venv-serving/bin/python
[ -x "$PY" ] || { uv venv .venv-serving --python 3.12 && uv pip install --python "$PY" duckdb pyarrow opensearch-py; }
curl -sf localhost:9201 >/dev/null || OPENSEARCH_INITIAL_ADMIN_PASSWORD='Proto-Local-9201!x' docker compose -f infra/docker-compose.yml up -d opensearch
until curl -sf localhost:9201/_cluster/health >/dev/null; do sleep 3; done
P=src/pipelines/core_v4/serving/prototype
$PY $P/export.py --db "${1:-data/duckdb/core/core_v4_noworkenrichment-min.duckdb}" --out data/serving_proto
$PY $P/load.py --parquet data/serving_proto
(cd $P && ../../../../../$PY test_usecases.py --parquet ../../../../../data/serving_proto)
