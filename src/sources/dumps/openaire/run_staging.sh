#!/bin/bash
#SBATCH --job-name=openaire_staging
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --time=12:00:00
#SBATCH --partition=fat,gpu,standard
#SBATCH --output=/work/lu72hip/logs/openaire_staging_%j.log
#SBATCH --mail-user=walter.ehrenberger@uni-jena.de
#SBATCH --mail-type=ALL

mkdir -p /work/lu72hip/logs

cd /home/lu72hip/DIGICHer/dh_pipeline || exit
source venv/bin/activate
export PYTHONPATH=/home/lu72hip/DIGICHer/dh_pipeline/src

python src/sources/dumps/openaire/staging.py
