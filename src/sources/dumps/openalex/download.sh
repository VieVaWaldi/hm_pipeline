TODO
no more sbatch in sources,
updat this at core_v5 hehe



##!/bin/bash
##SBATCH --job-name=openalex_secure_download
##SBATCH --nodes=1
##SBATCH --ntasks=1
##SBATCH --cpus-per-task=4
##SBATCH --mem=8G
##SBATCH --time=8:00:00
##SBATCH --partition=standard
##SBATCH --output=/work/lu72hip/logs/openalex_download_%j.log
##SBATCH --mail-user=walter.ehrenberger@uni-jena.de
##SBATCH --mail-type=ALL
#
## --- Path Configuration ---
#BASE_DIR="/work/lu72hip/data/pile"
#LOG_DIR="/work/lu72hip/logs"
#DOWNLOAD_DIR="${BASE_DIR}/openalex_2026_02_03_dump"
#EXTRACT_DIR="${BASE_DIR}/openalex_query_id-0"
#VENV_PATH="${BASE_DIR}/openalex_venv"
#
#mkdir -p "$LOG_DIR"
#mkdir -p "$DOWNLOAD_DIR"
#mkdir -p "$EXTRACT_DIR"
#
## --- Venv Setup ---
#if [ ! -d "$VENV_PATH" ]; then
#    python3 -m venv "$VENV_PATH"
#    source "$VENV_PATH/bin/activate"
#    pip install --upgrade pip awscli
#else
#    source "$VENV_PATH/bin/activate"
#fi
#
## Verify aws command is available
#which aws
#aws --version
#
## --- 1. Download ---
#echo "Syncing from S3 (Excluding XPAC): $(date)"
#aws s3 sync s3://openalex/data/ "$DOWNLOAD_DIR" \
#    --no-sign-request \
#    --exclude "*xpac*" \
#    --exclude "*merged_ids*"
#
## --- 2. Validation ---
#echo "Compressed file count: $(date)"
#find "$DOWNLOAD_DIR" -name "*.gz" -type f | wc -l
#
## --- 3. Parallel Extraction ---
## echo "Decompressing with 32 cores: $(date)"
## find "$DOWNLOAD_DIR" -name "*.gz" -type f -print0 | xargs -0 -P 32 -I {} bash -c '
##     FILE="{}"
##     # Get relative path from DOWNLOAD_DIR
##     REL_PATH="${FILE#'"$DOWNLOAD_DIR"'/}"
##     OUT_FILE="'"$EXTRACT_DIR"'/${REL_PATH%.gz}"
##     OUT_DIR=$(dirname "$OUT_FILE")
#
##     # Create output directory if needed
##     mkdir -p "$OUT_DIR"
#
##     # Decompress
##     gunzip -c "$FILE" > "$OUT_FILE"
#
##     echo "Extracted: ${REL_PATH%.gz}"
## '
#
## # --- 4. Final Count ---
## echo "Final extracted file count: $(date)"
## find "$EXTRACT_DIR" -type f | wc -l
## echo "Compressed size:"
## du -sh "$DOWNLOAD_DIR"
## echo "Extracted size:"
## du -sh "$EXTRACT_DIR"
#echo "Process finished: $(date)"
