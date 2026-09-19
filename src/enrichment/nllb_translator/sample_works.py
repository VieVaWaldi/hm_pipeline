"""
Draws the measurement sample for the NLLB README: works dated 2018 or later that are linked to an
organisation but to no project (the "org_only" tier core_v4 fills its 50M cap with, see
src/pipelines/core_v4/README.md). Read-only on the openaire staging; writes one parquet.

    columns: id, publicationDate, oa_lang (OpenAire's own language.code, for comparison only),
             title, description (descriptions[1], cut at 4000 chars)

Heavy (scans the work + relation tables): run on SLURM, see run_measurements.sbatch.

    ENV=prod uv run python -m enrichment.nllb_translator.sample_works --n 200000
"""

import argparse
import logging
import time
from pathlib import Path

import duckdb

from common.config.dumps import get_dumps_paths
from common.file_handling.path_utils import get_project_root_path

DEFAULT_OUT = get_project_root_path() / "data/enrichment/nllb_sample/works_2018_org_only.parquet"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--n", type=int, default=200_000)
    parser.add_argument("--db", default=None, help="openaire staging duckdb (default: path_duck_staging_v4)")
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--memory-gb", type=int, default=48)
    parser.add_argument("--threads", type=int, default=16)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    db = args.db or get_dumps_paths()["openaire_dump"]["path_duck_staging_v4"]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db, read_only=True, config={"memory_limit": f"{args.memory_gb}GB", "threads": args.threads})
    t = time.time()
    # ids are already uniform hashes: `id % K` is a random slice. Take a generous slice of the >= 2018 works,
    # keep the org_only ones, then the first n by id.
    con.execute(
        """CREATE TEMP TABLE cand AS
           SELECT id, publicationDate, language.code AS oa_lang, title, substr(descriptions[1], 1, 4000) AS description
           FROM work
           WHERE publicationDate >= DATE '2018-01-01' AND id % 150 = 7"""
    )
    n_cand = con.execute("SELECT count(*) FROM cand").fetchone()[0]
    logging.info(f"candidates (>= 2018, 1/150 slice): {n_cand:,} [{time.time() - t:.0f}s]")
    con.execute(
        """CREATE TEMP TABLE org AS
           SELECT DISTINCT source AS id FROM relation
           WHERE relType.name = 'hasAuthorInstitution' AND sourceType = 'product' AND targetType = 'organization'
             AND source IN (SELECT id FROM cand)"""
    )
    con.execute(
        """CREATE TEMP TABLE proj AS
           SELECT DISTINCT target AS id FROM relation
           WHERE relType.name = 'produces' AND sourceType = 'project' AND targetType = 'product'
             AND target IN (SELECT id FROM cand)"""
    )
    con.execute(
        f"""COPY (SELECT c.* FROM cand c
                  WHERE c.id IN (SELECT id FROM org) AND c.id NOT IN (SELECT id FROM proj)
                  ORDER BY c.id LIMIT {int(args.n)})
            TO '{out}' (FORMAT parquet, COMPRESSION zstd)"""
    )
    n = duckdb.sql(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
    logging.info(f"wrote {n:,} org-only works dated >= 2018 to {out} [{time.time() - t:.0f}s]")
    if n < args.n:
        logging.warning(f"only {n:,} of the requested {args.n:,}: raise the slice (id % 150) if you need more")


if __name__ == "__main__":
    main()
