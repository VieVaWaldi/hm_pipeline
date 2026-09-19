#!/bin/bash
# Downloads the ROR data dump from Zenodo and extracts it in place.
#
# Usage: bash download.sh <marker-file> <target-json-path>
#   <marker-file> is the resolved path_raw_marker from config/dumps.yaml, a
#   sentinel next to the JSON — Snakemake passes it as the rule's `output:`
#   (same pattern as the openaire download). Snakemake tracks the marker
#   rather than the JSON so a rule edit or a deleted JSON never triggers a
#   re-download; to skip this download for a dump you already have, just
#   create the marker (see the note in orchestration/rules/dumps.smk).
#   <target-json-path> is path_raw (dev: project-relative; prod: under
#   hpc_root), checked for after extraction. Bump ZENODO_RECORD/ZIP_FILE
#   together when a new ROR version is published, and update config/dumps.yaml's
#   ror_dump.path_raw and path_raw_marker to match the new file/dir name —
#   see documentation/ for version history.
set -euo pipefail

MARKER="$1"
TARGET_FILE="$2"
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

touch "$MARKER"
echo "Done: $TARGET_FILE"
