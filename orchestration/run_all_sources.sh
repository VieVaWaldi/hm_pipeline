#!/usr/bin/env bash
# run_all_sources.sh
#
# Runs the local-safe source set: every incremental api source except
# LOCAL_EXCLUDE_SOURCES, plus the ror dump (see Snakefile). Deliberately
# narrower than the "all" Snakemake target: it excludes coreac and the
# openaire dump (100s of GB, HPC-only), which aren't safe/wanted for an
# unattended local run. Run either individually by name when you actually
# want them -- see orchestration/README.md "Running Individually".
#
# Usage:
#   ./orchestration/run_all_sources.sh                    # extract + load (default)
#   ./orchestration/run_all_sources.sh extract             # extraction only, no load
#   ./orchestration/run_all_sources.sh load                # extract + load, explicit
#   ./orchestration/run_all_sources.sh load --report       # extract + load, then reports
#   ./orchestration/run_all_sources.sh -p 8                # override Snakemake's --cores (default 4), -p/--parallel
#
# --report: once "load" finishes successfully, also (re)generates the
# per-source Markdown data-profile reports under reports/ (see
# src/common/report/). Only valid with "load" -- there's nothing to report on
# straight after "extract" (data isn't in duckdb yet). This flag is
# intentionally not passed through to Snakemake: Snakemake already has its own
# built-in "--report <file.html>" (a run summary), a different thing from our
# per-source data profile. This wrapper is what owns the "--report" flag we mean.
#
# --parallel N: how many jobs Snakemake may run at once (its own "--cores").
# Snakemake already parallelizes independent jobs on its own once the DAG
# allows it and enough cores are available -- this flag just exposes that
# budget instead of hardcoding it to 4.

set -euo pipefail

STEP="load"
CORES=4
REPORT=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        extract|load)
            STEP="$1"
            shift
            ;;
        --report)
            REPORT=true
            shift
            ;;
        --parallel|-p)
            CORES="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

if [[ "$REPORT" == true && "$STEP" != "load" ]]; then
    echo "--report only applies to 'load' (needs the loaded duckdb files)" >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$PROJECT_ROOT"

TARGET="sources_local"
if [[ "$STEP" == "extract" ]]; then
    TARGET="extract_sources_local"
fi

uv run snakemake -s orchestration/Snakefile --cores "$CORES" "$TARGET"

if [[ "$REPORT" == true ]]; then
    uv run python -m common.report.generate_reports
fi
