#!/usr/bin/env bash
# Mini-DB round trip on the laptop: export -> load into the local OpenSearch (:9201, index prefix hm_) -> 40+ use-case checks.
# Usage (repo root):  bash src/pipelines/core_v4/serving/export/run_all.sh [path/to/core_v4.duckdb]
#   optional: MINORITY_EXCLUDE=path/to/minority_exclusions.csv bash .../run_all.sh   (deny-list of wrong minority pairs, the cluster run uses it)
#             MINORITY_OVERRIDE=path/to/minority_override.parquet bash .../run_all.sh   (full replacement, optional; mutually exclusive)
#   neither = the stored project.minority_qid
set -euo pipefail
cd "$(dirname "$0")/../../../../.."                     # repo root
PY=.venv-serving/bin/python
[ -x "$PY" ] || UV_PROJECT_ENVIRONMENT=.venv-serving uv sync --frozen --only-group serving    # duckdb, pyarrow, opensearch-py, no torch
curl -sf localhost:9201 >/dev/null || OPENSEARCH_INITIAL_ADMIN_PASSWORD='Proto-Local-9201!x' docker compose -f infra/docker-compose.yml up -d opensearch
until curl -sf localhost:9201/_cluster/health >/dev/null; do sleep 3; done
E=src/pipelines/core_v4/serving/export
OUT=data/serving_final
rm -rf "$OUT"
$PY $E/export.py --db "${1:-data/duckdb/core/core_v4_noworkenrichment-min.duckdb}" --out "$OUT" --works-chunks 2 ${MINORITY_EXCLUDE:+--minority-exclude "$MINORITY_EXCLUDE"} ${MINORITY_OVERRIDE:+--minority-override "$MINORITY_OVERRIDE"}
(cd $E && ../../../../../$PY load.py --parquet ../../../../../$OUT --port 9201 --prefix hm_ --shards works=1,projects=1 --recreate)
(cd $E && ../../../../../$PY test_usecases.py --parquet ../../../../../$OUT --port 9201 --prefix hm_)
