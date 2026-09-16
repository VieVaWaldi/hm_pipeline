#!/bin/bash
#SBATCH --job-name=openaire_ingest
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --mem=200G
#SBATCH --time=9:00:00
#SBATCH --partition=fat,gpu,standard
#SBATCH --output=/work/lu72hip/logs/openaire_ingest_%j.log
#SBATCH --mail-user=walter.ehrenberger@uni-jena.de
#SBATCH --mail-type=ALL

mkdir -p /work/lu72hip/logs

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate
export PYTHONPATH=/home/lu72hip/DIGICHer/dh_pipeline/src

python src/sources/openaire_dump/loader.py
