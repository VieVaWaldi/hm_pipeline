"""Run the export SQL (sql/*.sql) against a core_v4 DuckDB (read-only) and write one zstd Parquet per index.

Same SQL runs on the mini DB locally and on the 132 GB file on the cluster:
    .venv-serving/bin/python export.py --db <path.duckdb> --out <dir> [--only works,projects]
"""
import argparse
import time
from pathlib import Path

import duckdb

SQL_DIR = Path(__file__).parent / "sql"
DEFAULT_DB = "data/duckdb/core/core_v4_noworkenrichment-min.duckdb"
DEFAULT_OUT = "data/serving_proto"
INDEXES = ["projects", "organisations", "works", "minorities", "grants", "topics"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--only", default=",".join(INDEXES))
    ap.add_argument("--memory-limit", default=None, help="e.g. 64GB (cluster)")
    ap.add_argument("--threads", type=int, default=None)
    args = ap.parse_args()

    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db, read_only=True)
    if args.memory_limit:
        con.execute(f"SET memory_limit='{args.memory_limit}'")
    if args.threads:
        con.execute(f"SET threads={args.threads}")

    t0 = time.time()
    con.execute((SQL_DIR / "00_common.sql").read_text())
    print(f"common temp tables: {time.time() - t0:.1f}s")
    for name in args.only.split(","):
        t = time.time()
        con.execute((SQL_DIR / f"{name}.sql").read_text().replace("__OUT__", str(out)))
        f = out / f"{name}.parquet"
        rows = con.execute(f"SELECT count(*) FROM read_parquet('{f}')").fetchone()[0]
        print(f"{name:14s} {rows:>10,} rows  {f.stat().st_size / 1e6:8.2f} MB  {time.time() - t:6.1f}s")


if __name__ == "__main__":
    main()
