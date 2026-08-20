#!/bin/bash
#SBATCH --job-name=core_v3_topic_enrich
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=72:00:00
#SBATCH --partition=fat,gpu,standard
#SBATCH --output=/work/lu72hip/logs/core_v3_topic_enrich_%j.log
#SBATCH --mail-user=walter.ehrenberger@uni-jena.de
#SBATCH --mail-type=ALL

mkdir -p /work/lu72hip/logs

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate
export PYTHONPATH=/home/lu72hip/DIGICHer/dh_pipeline/src

echo "=== Step 1: Seed topics ==="
python src/elt/core_v3/seed_topics.py

echo "=== Step 2: TF-IDF enrichment (project + work) ==="
python src/elt/core_v3/run_topic_model_tf_idf.py
