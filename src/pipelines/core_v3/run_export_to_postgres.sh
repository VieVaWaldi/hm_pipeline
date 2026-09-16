#!/usr/bin/env bash
# run_export_to_postgres.sh
#
# Starts PostgreSQL, creates the 'core_v3' database if it doesn't exist,
# then exports core_v3_final.duckdb into it via export_to_postgres.py.
#
# Usage:
#   bash src/elt/core_v3/run_export_to_postgres.sh
#
# Requirements:
#   - $PGDATA set (postgres data directory)
#   - pg_ctl, psql, createdb on PATH
#   - Python venv active with duckdb + postgres extension

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
DB_NAME="core_v3"
PG_PORT="${PGPORT:-5433}"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 1. Start PostgreSQL ───────────────────────────────────────────────────────
if [ -z "${PGDATA:-}" ]; then
    echo "ERROR: \$PGDATA is not set. Export it before running this script."
    exit 1
fi

log "Checking PostgreSQL status (PGDATA=$PGDATA)..."
if pg_ctl -D "$PGDATA" status | grep -q "server is running"; then
    log "PostgreSQL already running — skipping start."
else
    log "Starting PostgreSQL..."
    pg_ctl -D "$PGDATA" start -w -t 60
fi
log "PostgreSQL is ready."

# ── 2. Create target database (idempotent) ────────────────────────────────────
log "Ensuring database '$DB_NAME' exists..."
if psql -p "$PG_PORT" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME'" | grep -q 1; then
    log "Database '$DB_NAME' already exists — skipping createdb."
else
    createdb -p "$PG_PORT" "$DB_NAME"
    log "Database '$DB_NAME' created."
fi

# ── 3. Run the Python export ──────────────────────────────────────────────────
log "Starting DuckDB → Postgres export..."
cd "$PROJECT_ROOT"
python -m src.elt.core_v3.export_to_postgres --pg-conn "dbname=$DB_NAME port=$PG_PORT host=localhost"

log "=== Export finished ==="
