#!/usr/bin/env bash
# Mini-DB round trip on the laptop: export -> load into the local OpenSearch (:9201, index prefix hm_) -> 40+ use-case checks.
# Usage (repo root):  bash src/pipelines/core_v4/serving/export/run_all.sh [path/to/core_v4.duckdb]
set -euo pipefail
cd "$(dirname "$0")/../../../../.."                     # repo root
PY=.venv-serving/bin/python
[ -x "$PY" ] || UV_PROJECT_ENVIRONMENT=.venv-serving uv sync --frozen --only-group serving    # duckdb, pyarrow, opensearch-py, no torch
curl -sf localhost:9201 >/dev/null || OPENSEARCH_INITIAL_ADMIN_PASSWORD='Proto-Local-9201!x' docker compose -f infra/docker-compose.yml up -d opensearch
until curl -sf localhost:9201/_cluster/health >/dev/null; do sleep 3; done
E=src/pipelines/core_v4/serving/export
OUT=data/serving_final
rm -rf "$OUT"
$PY $E/export.py --db "${1:-data/duckdb/core/core_v4_noworkenrichment-min.duckdb}" --out "$OUT" --works-chunks 2
(cd $E && ../../../../../$PY load.py --parquet ../../../../../$OUT --port 9201 --prefix hm_ --shards works=1,projects=1 --recreate)
(cd $E && ../../../../../$PY test_usecases.py --parquet ../../../../../$OUT --port 9201 --prefix hm_)
