#!/bin/bash
#SBATCH --job-name=ror_download
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=4G
#SBATCH --time=00:30:00
#SBATCH --partition=standard
#SBATCH --output=/work/lu72hip/logs/ror_download_%j.log
#SBATCH --mail-user=walter.ehrenberger@uni-jena.de
#SBATCH --mail-type=ALL

# --- Path Configuration ---
BASE_DIR="/work/lu72hip/data/pile"
DOWNLOAD_DIR="${BASE_DIR}/ror_2026_02_24"
LOG_DIR="/work/lu72hip/logs"

# ROR v2.3 February 2026 record
ZENODO_RECORD="18761279"
ZIP_FILE="v2.3-2026-02-24-ror-data.zip"

mkdir -p "$LOG_DIR"
mkdir -p "$DOWNLOAD_DIR"

echo "Starting ROR data download: $(date)"

# --- Download the zip file ---
cd "$DOWNLOAD_DIR"

# Direct download URL from Zenodo
DOWNLOAD_URL="https://zenodo.org/records/${ZENODO_RECORD}/files/${ZIP_FILE}"

echo "Downloading from: $DOWNLOAD_URL"
wget --no-check-certificate -O "$ZIP_FILE" "$DOWNLOAD_URL" || \
curl -L -o "$ZIP_FILE" "$DOWNLOAD_URL"

echo "Download complete. File size:"
ls -lh "$ZIP_FILE"

# --- Extract the zip ---
echo "Extracting zip archive: $(date)"
unzip -o "$ZIP_FILE" -d "$DOWNLOAD_DIR"

# --- Remove zip file ---
echo "Removing zip file: $(date)"
rm "$ZIP_FILE"

# --- Show contents ---
echo "Extracted files in ${DOWNLOAD_DIR}:"
ls -lh "$DOWNLOAD_DIR"

echo "Process finished: $(date)"
