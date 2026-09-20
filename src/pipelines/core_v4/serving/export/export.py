"""Final export: core_v4 DuckDB (read-only) -> one zstd Parquet set per OpenSearch index, same SQL on the mini DB and the 132 GB file.

    python export.py --db <core_v4.duckdb> --out <dir> [--only works,projects,...] [--works-chunks 10] [--works-sample 1000]
                     [--memory-limit 100GB --threads 16 --temp-dir /vast/.../duck_tmp] [--force]
                     [--minority-exclude minority_exclusions.csv]      # SUBTRACTIVE deny-list of (project_id, minority_qid) pairs (cluster run: yes)
                     [--minority-override minority_override.parquet]   # OPTIONAL full replacement (not the default; mutually exclusive with --minority-exclude)
                                                                       # neither flag = the stored project.minority_qid, unchanged

Output layout (--out):
    projects/projects.parquet  organisations/organisations.parquet  minorities/minorities.parquet  grants/grants.parquet
    works/works_00.parquet ... works_NN.parquet         (id % N split; each file is written as .tmp then renamed = resumable)
    api/topics.json  api/publishers.json                (small tables the api keeps in memory)
    export_manifest.json                                (rows, bytes, seconds per file)
A file that already exists is skipped (rerun after a crash continues); --force redoes everything.
--minority-exclude (decision D32: stored tags minus a small deny-list of obviously wrong pairs; see agent_job/MINORITY_EXCLUSIONS.md): CSV or Parquet whose columns
project_id, minority_qid (only these two are read; extra columns like group_name_en/rule/title are ignored) list the pairs to REMOVE. Everything else keeps its stored
tags; a project that loses all tags ends with an empty list; all 278 groups stay in the minorities index. The file must match the database: a listed project that does not
exist or a pair that is not a stored tag aborts the export. Missing file = error.
--minority-override (optional, NOT the default; see agent_job/MINORITY_OVERRIDE.md): a Parquet (project_id VARCHAR, minority_qid VARCHAR[]) written by
minority_override.py that REPLACES project.minority_qid everywhere it is used (projects docs, works.minority_qids, minorities rollups); projects that are
not in the file get no minority. Recorded in export_manifest.json; changing it between runs requires --force.
Runs on the cluster with only `duckdb` installed (uv sync --frozen --only-group serving).
"""
import argparse
import hashlib
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
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--minority-exclude", default=None, help="CSV/Parquet with project_id, minority_qid pairs to REMOVE from the stored project.minority_qid (subtractive deny-list)")
    grp.add_argument("--minority-override", default=None, help="optional corrected project->minority tags (see minority_override.py); default = stored project.minority_qid")
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
    def ident(path_arg: str | None, flag: str):
        if not path_arg:
            return None, None
        path = Path(path_arg).resolve()
        if not path.exists():
            raise SystemExit(f"{flag} {path} does not exist")
        return path, f"{path} sha256:{hashlib.sha256(path.read_bytes()).hexdigest()[:16]}"

    override, override_id = ident(args.minority_override, "--minority-override")
    exclude, exclude_id = ident(args.minority_exclude, "--minority-exclude")
    if manifest["files"] and not args.force and (manifest.get("minority_override") != override_id or manifest.get("minority_exclude") != exclude_id):
        raise SystemExit(f"{manifest_path} was written with minority_override={manifest.get('minority_override')!r} minority_exclude={manifest.get('minority_exclude')!r}, "
                         f"now {override_id!r} / {exclude_id!r}: existing files would mix both. Use a new --out directory or --force.")
    manifest.update(db=str(args.db), works_chunks=args.works_chunks, works_sample=args.works_sample, minority_override=override_id, minority_exclude=exclude_id)

    def done(rel: str, size: int, secs: float, rows: int | None = None):
        manifest["files"][rel] = {"bytes": size, "seconds": round(secs, 1), "rows": rows}
        manifest_path.write_text(json.dumps(manifest, indent=1))
        print(f"{rel:36s} {size / 1e6:10.2f} MB {secs:8.1f}s" + (f"  {rows:>11,} rows" if rows is not None else ""), flush=True)

    t0 = time.time()
    con.execute((SQL_DIR / "00_macros.sql").read_text())
    con.execute((SQL_DIR / "01_common.sql").read_text())
    # p_minority(pid, minority_qid): the ONE place projects/minorities/works read project minority tags from (non-empty lists only)
    stored_sql = "SELECT id AS pid, minority_qid FROM project WHERE minority_qid IS NOT NULL AND len(minority_qid) > 0"
    n_stored = con.execute("SELECT count(*) FROM project WHERE minority_qid IS NOT NULL AND len(minority_qid) > 0").fetchone()[0]
    if override is not None:   # full replacement: projects absent from the file have no minority
        con.execute(f"CREATE OR REPLACE TEMP TABLE p_minority AS SELECT project_id::UBIGINT AS pid, minority_qid::VARCHAR[] AS minority_qid "
                    f"FROM read_parquet('{override}') WHERE minority_qid IS NOT NULL AND len(minority_qid) > 0")
        bad = con.execute("SELECT count(*) FROM p_minority m LEFT JOIN project p ON p.id = m.pid WHERE p.id IS NULL").fetchone()[0]
        if bad:
            raise SystemExit(f"{bad} project ids in the override do not exist in the database")
    elif exclude is not None:  # subtractive: stored tags minus the listed pairs, stored order kept, projects that lose everything drop out (= empty list downstream)
        reader = (f"read_parquet('{exclude}')" if exclude.suffix == ".parquet" else f"read_csv('{exclude}', header=true, all_varchar=true, delim=',', quote='\"', escape='\"')")
        con.execute(f"CREATE OR REPLACE TEMP TABLE m_excl AS SELECT DISTINCT project_id::UBIGINT AS pid, minority_qid AS qid FROM {reader}")
        n_excl = con.execute("SELECT count(*) FROM m_excl").fetchone()[0]
        missing = con.execute("SELECT count(*) FROM m_excl e LEFT JOIN project p ON p.id = e.pid WHERE p.id IS NULL").fetchone()[0]
        not_stored = con.execute("SELECT count(*) FROM m_excl e JOIN project p ON p.id = e.pid WHERE NOT list_contains(coalesce(p.minority_qid, []), e.qid)").fetchone()[0]
        if missing or not_stored:
            raise SystemExit(f"--minority-exclude {exclude}: {missing} listed projects do not exist and {not_stored} pairs are not stored tags of their project: "
                             "the file does not belong to this database")
        con.execute("""CREATE OR REPLACE TEMP TABLE p_minority AS
                       WITH e AS (SELECT pid, list(qid) AS qs FROM m_excl GROUP BY pid)
                       SELECT p.id AS pid, list_filter(p.minority_qid, lambda q: NOT list_contains(coalesce(e.qs, []), q)) AS minority_qid
                       FROM project p LEFT JOIN e ON e.pid = p.id WHERE p.minority_qid IS NOT NULL AND len(p.minority_qid) > 0""")
        con.execute("DELETE FROM p_minority WHERE len(minority_qid) = 0")
    else:
        con.execute(f"CREATE OR REPLACE TEMP TABLE p_minority AS {stored_sql}")
    n_min = con.execute("SELECT count(*), coalesce(sum(len(minority_qid)), 0) FROM p_minority").fetchone()
    stored_groups = {r[0] for r in con.execute("SELECT DISTINCT unnest(minority_qid) FROM project WHERE minority_qid IS NOT NULL").fetchall()}
    now_groups = {r[0] for r in con.execute("SELECT DISTINCT unnest(minority_qid) FROM p_minority").fetchall()}
    manifest["minority_source"] = {"mode": "override" if override else "exclude" if exclude else "stored", "override": override_id, "exclude": exclude_id,
                                   "projects_with_minority": n_min[0], "project_group_tags": int(n_min[1]), "stored_projects_with_minority": n_stored,
                                   "excluded_pairs": n_excl if exclude is not None else 0,
                                   "groups_emptied": sorted(stored_groups - now_groups)}   # groups that end with 0 projects (they stay in the minorities index)
    src = f"EXCLUDE {exclude} ({n_excl:,} pairs)" if exclude else f"OVERRIDE {override}" if override else "project.minority_qid"
    print(f"macros + common temp tables: {time.time() - t0:.1f}s; minority tags from {src}: {n_min[0]:,} projects, {int(n_min[1]):,} tags; "
          f"{len(stored_groups - now_groups)} groups end with 0 projects (kept in the index)", flush=True)

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
