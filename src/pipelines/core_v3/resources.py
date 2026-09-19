"""
Shared --mem-mb / --threads handling for core_v3's duckdb-heavy scripts
(transformation.py, enrichment/topic_modelling.py). Same pattern as
src/sources/dumps/openaire/staging.py: the Snakemake rule passes its own
`{resources.mem_mb}` / `{resources.cpus_per_task}`, so the allocation is
declared once, in orchestration/rules/pipeline/core_v3/merge.smk, instead of
hardcoded per script.
"""

import argparse

import duckdb

# Headroom kept below the job's total mem_mb for the OS and process overhead
# outside DuckDB's own buffer manager (same value as openaire staging.py).
DUCKDB_MEM_HEADROOM_MB = 40_000

DEFAULT_MEM_MB = 200_000


def add_resource_args(parser: argparse.ArgumentParser, default_threads: int) -> None:
    parser.add_argument(
        "--mem-mb",
        type=int,
        default=DEFAULT_MEM_MB,
        help="Total memory available to this job (matches the SLURM mem_mb in "
        "orchestration/rules/pipeline/core_v3/merge.smk). DuckDB's own memory_limit "
        f"is set to this minus {DUCKDB_MEM_HEADROOM_MB} MB of headroom.",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=default_threads,
        help="CPUs available to this job (matches the SLURM cpus_per_task).",
    )


def apply_duckdb_limits(con: duckdb.DuckDBPyConnection, mem_mb: int, threads: int) -> None:
    con.execute(f"SET memory_limit='{mem_mb - DUCKDB_MEM_HEADROOM_MB}MB'")
    con.execute(f"SET threads={threads}")
