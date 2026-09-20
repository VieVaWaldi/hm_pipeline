"""Final export: core_v4 DuckDB (read-only) -> one zstd Parquet set per OpenSearch index, same SQL on the mini DB and the 132 GB file.

    python export.py --db <core_v4.duckdb> --out <dir> [--only works,projects,...] [--works-chunks 10] [--works-sample 1000]
                     [--memory-limit 100GB --threads 16 --temp-dir /vast/.../duck_tmp] [--force]

Output layout (--out):
    projects/projects.parquet  organisations/organisations.parquet  minorities/minorities.parquet  grants/grants.parquet
    works/works_00.parquet ... works_NN.parquet         (id % N split; each file is written as .tmp then renamed = resumable)
    api/topics.json  api/publishers.json                (small tables the api keeps in memory)
    export_manifest.json                                (rows, bytes, seconds per file)
A file that already exists is skipped (rerun after a crash continues); --force redoes everything.
Runs on the cluster with only `duckdb` installed (uv sync --frozen --only-group serving).
"""
import argparse
import json
import os
import time
from pathlib import Path

import duckdb

SQL_DIR = Path(__file__).parent / "sql"
DEFAULT_DB = "data/duckdb/core/core_v4_noworkenrichment-min.duckdb"
# order matters little; small ones first so a failure in works does not hide problems in the rest
INDEXES = ["organisations", "projects", "minorities", "grants", "topics", "publishers", "works"]


def render(sql: str, out: Path, k: int = 0, n: int = 1, sample: int = 0) -> str:
    sample_filter = f"AND hash(w.id) % {sample} = 0" if sample > 1 else ""
    return (sql.replace("{OUT}", str(out)).replace("{KK}", f"{k:02d}").replace("{K}", str(k)).replace("{N}", str(n))
            .replace("{SAMPLE_FILTER}", sample_filter))


def run_copy(con, sql: str, target: Path) -> tuple[int, float]:
    """Execute a COPY ... TO '<target>.tmp' statement, rename to <target>, return (bytes, seconds)."""
    t = time.time()
    tmp = Path(str(target) + ".tmp")
    tmp.parent.mkdir(parents=True, exist_ok=True)
    if tmp.exists():
        tmp.unlink()
    con.execute(sql)
    os.replace(tmp, target)
    return target.stat().st_size, time.time() - t


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--out", required=True)
    ap.add_argument("--only", default=",".join(INDEXES))
    ap.add_argument("--works-chunks", type=int, default=1, help="split works into N files by id %% N (cluster: 10)")
    ap.add_argument("--works-sample", type=int, default=0, help="smoke run: only works with hash(id) %% S = 0 (0 = all)")
    ap.add_argument("--memory-limit", default=None)
    ap.add_argument("--threads", type=int, default=None)
    ap.add_argument("--temp-dir", default=None, help="DuckDB spill directory (cluster: under /vast/lu72hip/hm_pipeline, not /home)")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.db, read_only=True)
    if args.memory_limit:
        con.execute(f"SET memory_limit='{args.memory_limit}'")
    if args.threads:
        con.execute(f"SET threads={args.threads}")
    if args.temp_dir:
        Path(args.temp_dir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{args.temp_dir}'")
    con.execute("SET preserve_insertion_order=false")

    manifest_path = out / "export_manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {"files": {}}
    manifest.update(db=str(args.db), works_chunks=args.works_chunks, works_sample=args.works_sample)

    def done(rel: str, size: int, secs: float, rows: int | None = None):
        manifest["files"][rel] = {"bytes": size, "seconds": round(secs, 1), "rows": rows}
        manifest_path.write_text(json.dumps(manifest, indent=1))
        print(f"{rel:36s} {size / 1e6:10.2f} MB {secs:8.1f}s" + (f"  {rows:>11,} rows" if rows is not None else ""), flush=True)

    t0 = time.time()
    con.execute((SQL_DIR / "00_macros.sql").read_text())
    con.execute((SQL_DIR / "01_common.sql").read_text())
    print(f"macros + common temp tables: {time.time() - t0:.1f}s", flush=True)

    for name in args.only.split(","):
        sql = (SQL_DIR / f"{name}.sql").read_text()
        if name in ("topics", "publishers"):
            target = out / "api" / f"{name}.json"
            if target.exists() and not args.force:
                print(f"skip {target.name} (exists)")
                continue
            t = time.time()
            (out / "api").mkdir(parents=True, exist_ok=True)
            con.execute(render(sql, out))
            done(f"api/{name}.json", target.stat().st_size, time.time() - t)
        elif name == "works":
            for k in range(args.works_chunks):
                target = out / "works" / f"works_{k:02d}.parquet"
                if target.exists() and not args.force:
                    print(f"skip {target.name} (exists)")
                    continue
                size, secs = run_copy(con, render(sql, out, k, args.works_chunks, args.works_sample), target)
                rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
                done(f"works/{target.name}", size, secs, rows)
        else:
            target = out / name / f"{name}.parquet"
            if target.exists() and not args.force:
                print(f"skip {target.name} (exists)")
                continue
            size, secs = run_copy(con, render(sql, out), target)
            rows = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
            done(f"{name}/{target.name}", size, secs, rows)
    print(f"export finished in {time.time() - t0:.1f}s -> {out}")


if __name__ == "__main__":
    main()
