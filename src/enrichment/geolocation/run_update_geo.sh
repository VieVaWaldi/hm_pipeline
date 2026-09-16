#!/bin/bash

#SBATCH --job-name=enrichment_geolocation
#SBATCH --partition=long
#SBATCH --time=240:00:00
#SBATCH --output=data/logs/enrichment-geolocation/run_%j.log

# Restart postgres — enrichment_lib's geolocation scripts still use it directly
# (see common/database/postgres/create_db_session.py).
pg_ctl -D $PGDATA stop
sleep 5
pg_ctl -D $PGDATA start

cd "$(dirname "$0")/../.." || exit  # repo root

echo "Current user: $(whoami)"
echo "Current directory: $(pwd)"
echo "Python version: $(uv run python --version)"

# TODO: this old/ script is a starting point, not verified against core_v4 data yet.
uv run python -m enrichment_lib.geolocation.old.get_geolocations_to_csv
