#!/bin/bash
# Downloads (and extracts) the OpenAIRE dump from Zenodo.
#
# Usage: bash download.sh <target-dir>
#   <target-dir> is the resolved path_raw from config/dumps.yaml (dev:
#   project-relative; prod: under hpc_root) — Snakemake passes it as the
#   rule's `output:`. Resourcing for the HPC (mem/cpus/runtime) comes from
#   download_openaire_dump's `resources:` in orchestration/rules/dumps.smk,
#   read by the slurm executor — not from #SBATCH headers here.
set -uo pipefail

# --- Path Configuration ---
DOWNLOAD_DIR="$1"
LOG_DIR="${DOWNLOAD_DIR}/../../logs"
VENV_PATH="${DOWNLOAD_DIR}/../oa_venv"
URLS_FILE="${DOWNLOAD_DIR}/file_urls.txt"
STATUS_LOG="${LOG_DIR}/openaire_download_${SLURM_JOB_ID:-manual}_status.log"

# OpenAIRE December 2025 record
ZENODO_RECORD="20428976"
PARALLELISM=32

# mkdir -p "$LOG_DIR"
# mkdir -p "$DOWNLOAD_DIR"

# # --- Venv Setup ---
# if [ ! -d "$VENV_PATH" ]; then
#     python3 -m venv "$VENV_PATH"
#     source "$VENV_PATH/bin/activate"
#     pip install --upgrade pip requests
# else
#     source "$VENV_PATH/bin/activate"
# fi

# # --- 1. Fetch file list from Zenodo API ---
# echo "Fetching file list from Zenodo record ${ZENODO_RECORD}: $(date)"
# python3 << EOF
# import requests
# import sys

# record_id = "${ZENODO_RECORD}"
# api_url = f"https://zenodo.org/api/records/{record_id}"
# response = requests.get(api_url, timeout=60)
# response.raise_for_status()
# data = response.json()

# files = data.get('files', [])
# if not files:
#     print("ERROR: no files found in record response", file=sys.stderr)
#     sys.exit(1)

# with open("${URLS_FILE}", 'w') as f:
#     for file_info in files:
#         url = file_info['links']['self']
#         filename = file_info['key']
#         size = file_info['size']
#         f.write(f"{url}\t{filename}\t{size}\n")

# total_gb = sum(f['size'] for f in files) / (1024**3)
# print(f"Found {len(files)} files, {total_gb:.2f} GB total")
# print(f"Saved URLs to ${URLS_FILE}")
# EOF

# if [ ! -s "$URLS_FILE" ]; then
#     echo "FATAL: ${URLS_FILE} is empty, aborting" >&2
#     exit 1
# fi

# EXPECTED_COUNT=$(wc -l < "$URLS_FILE")
# echo "Expected file count: ${EXPECTED_COUNT}"

# # --- 2. Build list of files still needed (resume-safe) ---
# REMAINING_FILE="${DOWNLOAD_DIR}/file_urls_remaining.txt"
# > "$REMAINING_FILE"
# while IFS=$'\t' read -r url filename size; do
#     dest="${DOWNLOAD_DIR}/${filename}"
#     if [ -f "$dest" ] && [ "$(stat -c%s "$dest" 2>/dev/null || echo 0)" -eq "$size" ]; then
#         continue  # already complete, skip
#     fi
#     printf '%s\t%s\t%s\n' "$url" "$filename" "$size" >> "$REMAINING_FILE"
# done < "$URLS_FILE"

# REMAINING_COUNT=$(wc -l < "$REMAINING_FILE")
# echo "Files remaining to download: ${REMAINING_COUNT} / ${EXPECTED_COUNT}: $(date)"

# # --- 3. Download in parallel, resumable, retried, logged per-file ---
# if [ "$REMAINING_COUNT" -gt 0 ]; then
#     echo "Downloading with ${PARALLELISM} parallel workers: $(date)"

#     export DOWNLOAD_DIR STATUS_LOG

#     xargs -P "$PARALLELISM" -a "$REMAINING_FILE" -L 1 bash -c '
#         url="$1"
#         filename="$2"
#         expected_size="$3"
#         dest="${DOWNLOAD_DIR}/${filename}"
#         part="${dest}.part"

#         attempt=0
#         max_attempts=5
#         ok=0
#         while [ "$attempt" -lt "$max_attempts" ]; do
#             attempt=$((attempt + 1))
#             # -c resumes a partial .part file instead of restarting from zero
#             if wget -q -c -O "$part" "$url"; then
#                 actual_size=$(stat -c%s "$part" 2>/dev/null || echo 0)
#                 if [ "$actual_size" -eq "$expected_size" ]; then
#                     mv "$part" "$dest"
#                     echo "OK ${filename} attempt=${attempt}" >> "$STATUS_LOG"
#                     ok=1
#                     break
#                 else
#                     echo "SIZE_MISMATCH ${filename} attempt=${attempt} expected=${expected_size} got=${actual_size}" >> "$STATUS_LOG"
#                 fi
#             else
#                 echo "WGET_FAIL ${filename} attempt=${attempt}" >> "$STATUS_LOG"
#             fi
#             sleep $((attempt * 5))
#         done
#         if [ "$ok" -ne 1 ]; then
#             echo "FAILED ${filename} after ${max_attempts} attempts" >> "$STATUS_LOG"
#         fi
#     ' _
# else
#     echo "Nothing to download, all files already present and correctly sized."
# fi

# # --- 4. Verification ---
# echo "Verifying downloads: $(date)"
# FINAL_OK=0
# FINAL_BAD=0
# while IFS=$'\t' read -r url filename size; do
#     dest="${DOWNLOAD_DIR}/${filename}"
#     if [ -f "$dest" ] && [ "$(stat -c%s "$dest" 2>/dev/null || echo 0)" -eq "$size" ]; then
#         FINAL_OK=$((FINAL_OK + 1))
#     else
#         FINAL_BAD=$((FINAL_BAD + 1))
#         echo "MISSING_OR_BAD: $filename"
#     fi
# done < "$URLS_FILE"

# echo "Verification complete: ${FINAL_OK}/${EXPECTED_COUNT} files OK, ${FINAL_BAD} missing/bad"
# du -sh "$DOWNLOAD_DIR"
# echo "Process finished: $(date)"

# if [ "$FINAL_BAD" -gt 0 ]; then
#     echo "WARNING: incomplete download, ${FINAL_BAD} files still need attention. Re-run this script to retry only those." >&2
#     exit 1
# fi

# echo "All ${EXPECTED_COUNT} files downloaded successfully."

# --- 5. (left commented, same as your original — run once you're ready to extract) ---
echo "Extracting tar archives in place: $(date)"
find "$DOWNLOAD_DIR" -name "*.tar" -type f -print0 | xargs -0 -P 32 -I {} bash -c '
    TARFILE="{}"
    tar -xf "$TARFILE" -C "'"${DOWNLOAD_DIR}"'" && rm "$TARFILE"
    echo "Extracted and removed: $(basename "$TARFILE")"
'
find "$DOWNLOAD_DIR" -type f -name "*.gz" | wc -l
du -sh "$DOWNLOAD_DIR"