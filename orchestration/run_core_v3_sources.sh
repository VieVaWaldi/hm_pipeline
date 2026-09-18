#!/usr/bin/env bash
# run_core_v3_sources.sh
#
# Runs core_v3_sources (see Snakefile): cordis's full_projects_no_pdfs query
# + ror_dump + openaire_dump. HPC-only (openaire is 100s of GB) -- always
# submitted through the slurm workflow profile, never --cores locally. Used
# to verify loader idempotency ahead of the core_v3 rebuild.
#
# Usage:
#   ./orchestration/run_core_v3_sources.sh                  # extract + load + report (default)
#   ./orchestration/run_core_v3_sources.sh extract           # extraction only, no load/report
#   ./orchestration/run_core_v3_sources.sh load               # extract + load, no report
#
# ENV=prod: set here, not in the Snakefile. get_settings() defaults to "dev"
# (project-relative paths) when ENV is unset, which is what sources_local /
# run_all_sources.sh want for local dev. core_v3_sources only makes sense
# against the real /work/lu72hip data, so this script is the one place that
# forces prod -- baking that default into the Snakefile itself would silently
# break local dev runs of the other targets, since DUMP_PATHS etc. are
# evaluated once, globally, at Snakefile-parse time.
#
# --forcerun on load_source/load_ror_dump/load_openaire_dump: without it,
# Snakemake sees the ror/openaire duckdb outputs are already up to date
# (their raw inputs haven't changed) and skips them -- no idempotency check
# at all. This forces those three load steps to re-execute against
# already-extracted/downloaded data every time, which is the actual point of
# this script. Extraction/download stay untouched -- not force-rerun.

set -euo pipefail

STEP="report"

while [[ $# -gt 0 ]]; do
    case "$1" in
        extract|load|report)
            STEP="$1"
            shift
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

TARGET="report_core_v3_sources"
FORCERUN=(load_source load_ror_dump load_openaire_dump)
if [[ "$STEP" == "extract" ]]; then
    TARGET="extract_core_v3_sources"
    FORCERUN=()
elif [[ "$STEP" == "load" ]]; then
    TARGET="core_v3_sources"
fi

if [[ ${#FORCERUN[@]} -gt 0 ]]; then
    ENV=prod uv run snakemake -s orchestration/Snakefile \
        --workflow-profile orchestration/profiles/slurm \
        "$TARGET" --forcerun "${FORCERUN[@]}"
else
    ENV=prod uv run snakemake -s orchestration/Snakefile \
        --workflow-profile orchestration/profiles/slurm \
        "$TARGET"
fi
