#!/bin/bash
# Downloads the ROR data dump from Zenodo and extracts it in place.
#
# Usage: bash download.sh <target-json-path>
#   <target-json-path> is the resolved path_raw from config/dumps.yaml
#   (dev: project-relative; prod: under hpc_root) — Snakemake passes it as
#   the rule's `output:`. Bump ZENODO_RECORD/ZIP_FILE together when a new
#   ROR version is published, and update config/dumps.yaml's ror_dump.path_raw
#   to match the new file name — see documentation/ for version history.
set -euo pipefail

TARGET_FILE="$1"
DOWNLOAD_DIR="$(dirname "$TARGET_FILE")"

ZENODO_RECORD="21773148"
ZIP_FILE="v2.11-2026-08-03-ror-data.zip"

mkdir -p "$DOWNLOAD_DIR"
cd "$DOWNLOAD_DIR"

DOWNLOAD_URL="https://zenodo.org/records/${ZENODO_RECORD}/files/${ZIP_FILE}"
echo "Downloading from: $DOWNLOAD_URL"
wget --no-check-certificate -O "$ZIP_FILE" "$DOWNLOAD_URL" || \
curl -L -o "$ZIP_FILE" "$DOWNLOAD_URL"

echo "Extracting zip archive: $(date)"
unzip -o "$ZIP_FILE" -d "$DOWNLOAD_DIR"
rm "$ZIP_FILE"

echo "Extracted files in ${DOWNLOAD_DIR}:"
ls -lh "$DOWNLOAD_DIR"

if [ ! -f "$TARGET_FILE" ]; then
    echo "FATAL: expected file not found after extraction: $TARGET_FILE" >&2
    echo "Check that config/dumps.yaml's ror_dump.path_raw matches the file name inside $ZIP_FILE" >&2
    exit 1
fi

echo "Done: $TARGET_FILE"
