#!/bin/bash

#SBATCH --job-name=enrichment_topic_modelling
#SBATCH --partition=standard
#SBATCH --time=72:00:00
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --output=data/logs/enrichment-topic_modelling/run_%j.log

# Restart postgres — enrichment_lib's topic modelling scripts still use it directly
# (see common/database/postgres/create_db_session.py).
pg_ctl -D $PGDATA stop
sleep 10
pg_ctl -D $PGDATA start

cd "$(dirname "$0")/../.." || exit  # repo root

echo "Current user: $(whoami)"
echo "Current directory: $(pwd)"
echo "Python version: $(uv run python --version)"

# TODO: this job originally ran run_topic_model_open_alex_keyword.py, which no
# longer exists in the repo (lost at some point before this restructure).
# run_topic_model_tf_idf.py is the closest surviving script — confirm it's the
# right replacement before relying on this job again.
uv run python -m enrichment_lib.topic_modelling.run_topic_model_tf_idf
