"""
Classifies core_v3 project/work rows as DCH (Digital Cultural Heritage) or
not. Duckdb-aware glue: reads text via text_sources.py, calls the pure
DchClassifier, writes results back. Adds two columns to the target table:
  - is_ch  BOOLEAN  : True if P(is DCH) >= THRESHOLD
  - pred   FLOAT    : P(is DCH)

Resumability: results are appended to binary files after each chunk
(CHUNK_ROWS rows — several of DchClassifier's internal GPU batches at once, so
its background-thread tokeniser/GPU pipelining still overlaps within a chunk),
not held in memory for the whole run. A crash loses at most one chunk of GPU
compute, not the whole job. The final merge into the real table happens once,
after all chunks are done. The files live in dch_results.py, which also
explains why they survive a failed run (and Snakemake deleting the duckdb).

Writes into core_v3's final duckdb (`path_duck`), which every run starts as a
fresh copy of core_v3_staging_2 — progress lives in the resume files, not in
the duckdb, so the copy is safe to redo after a crash. --test reads staging_2
directly and copies nothing.

Usage:
    uv run python -m pipelines.core_v3.enrichment.dch_classification
    uv run python -m pipelines.core_v3.enrichment.dch_classification --entity work
    uv run python -m pipelines.core_v3.enrichment.dch_classification --test 5   # dry-run, no writes
"""

import argparse
import logging

import duckdb
import pandas as pd

from common.config.pipelines import get_pipeline_paths
from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging
from enrichment.dch_classification.dch_classifier import DEFAULT_BATCH_SIZE, DchClassifier
from pipelines.core_v3.db_copy import fresh_copy
from pipelines.core_v3.enrichment.dch_results import (
    append_to_results,
    read_results,
    repair_results,
    results_base,
)
from pipelines.core_v3.enrichment.text_sources import Entity, all_text_rows

THRESHOLD = 0.5  # P(is DCH) >= threshold  →  is_ch = True
CHUNK_ROWS = 20 * DEFAULT_BATCH_SIZE


def _merge_results_to_main(db_path: str, entity: Entity, results_path: str) -> None:
    """One-shot merge: UPDATE the target table from the binary results files.
    Run once after all inference is complete — no threading at this point."""
    logging.info(f"Loading results from {results_path} ...")
    ids, probs = read_results(results_path)
    df = pd.DataFrame({"id": ids, "is_ch": (probs >= THRESHOLD), "pred": probs})

    logging.info(f"Merging {len(df):,} rows into {entity} table of {db_path} ...")
    con = duckdb.connect(db_path)
    con.execute(f"ALTER TABLE {entity} ADD COLUMN IF NOT EXISTS is_ch BOOLEAN")
    con.execute(f"ALTER TABLE {entity} ADD COLUMN IF NOT EXISTS pred  FLOAT")
    con.register("_results", df)
    con.execute(f"""
        UPDATE {entity}
        SET    is_ch = r.is_ch,
               pred  = r.pred
        FROM   _results r
        WHERE  {entity}.id = r.id
    """)
    con.unregister("_results")
    con.close()
    logging.info("Merge complete.")


def run_classification(classifier: DchClassifier, db_path: str, entity: Entity, results_path: str, offset_start: int) -> None:
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = all_text_rows(con, entity, offset_start=offset_start)
    finally:
        con.close()
    logging.info(f"Loaded {len(rows):,} {entity} rows.")

    total = 0
    for chunk_idx in range(0, len(rows), CHUNK_ROWS):
        chunk = rows[chunk_idx : chunk_idx + CHUNK_ROWS]
        ids = [row[0] for row in chunk]
        texts = [row[1] for row in chunk]

        probs = classifier.enrich(texts)
        append_to_results(results_path, ids, probs)

        total += len(chunk)
        logging.info(f"Chunk #{chunk_idx // CHUNK_ROWS:>5}  size={len(chunk):>6,}  total={total:>9,}/{len(rows):,}")


def resume_offset(db_path: str, entity: Entity, results_path: str) -> int:
    """How many rows (in id order) are already classified, whether that's recorded
    in the duckdb, the resume files, or both."""
    con = duckdb.connect(db_path, read_only=True)
    try:
        already_in_main = con.execute(f"SELECT count(*) FROM {entity} WHERE is_ch IS NOT NULL").fetchone()[0]
    except Exception:
        already_in_main = 0
    con.close()

    # A killed run can leave the two files out of step or with a torn last record.
    already_in_results = repair_results(results_path)
    # Rows in the DB were merged *from* the results files (which aren't deleted
    # after the merge), so the two overlap — not add — when both are present.
    # The DB is empty after a failed Snakemake job or a fresh copy from staging_2.
    offset = max(already_in_main, already_in_results)
    logging.info(
        f"Already classified: {already_in_main:,} in main DB, {already_in_results:,} in results file "
        f"→ resuming from offset {offset:,}"
    )
    return offset


def run_test(classifier: DchClassifier, db_path: str, entity: Entity, n_rows: int) -> None:
    """Dry-run: reads + infers up to n_rows, prints throughput, writes nothing."""
    import datetime

    logging.info(f"[TEST] Reading up to {n_rows} {entity} rows from DB (no writes).")
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = all_text_rows(con, entity, offset_start=0)[:n_rows]
    finally:
        con.close()
    texts = [row[1] for row in rows]

    t0 = datetime.datetime.now()
    probs = classifier.enrich(texts)
    elapsed = (datetime.datetime.now() - t0).total_seconds()

    n_ch = sum(1 for p in probs if p >= THRESHOLD)
    rate = len(rows) / elapsed if elapsed > 0 else 0
    logging.info(
        f"[TEST] {len(rows)} rows  CH={n_ch}  NOT_CH={len(rows) - n_ch}  "
        f"{elapsed:.1f}s  ({rate:.0f} seq/s)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify core_v3 project/work rows as DCH or not.")
    parser.add_argument("--entity", choices=["project", "work"], default="project")
    parser.add_argument("--test", type=int, default=0, metavar="N", help="Dry-run: infer N rows, no DB writes.")
    args = parser.parse_args()

    setup_logging("enrichment-dch_classification", "bert_inference")
    paths = get_pipeline_paths()["core_v3"]
    db_path = paths["path_duck_staging_2"] if args.test else paths["path_duck"]
    logging.info(f"Mode   : {'TEST (no writes)' if args.test else 'PRODUCTION'}")
    logging.info(f"DB path: {db_path}  entity: {args.entity}")

    model_path = get_project_root_path() / "data" / "models" / "bert_classifier"
    classifier = DchClassifier.load(model_path)

    if args.test:
        run_test(classifier, db_path, args.entity, n_rows=args.test)
        return

    fresh_copy(paths["path_duck_staging_2"], db_path)

    results_path = results_base(db_path, args.entity)
    logging.info(f"Results base: {results_path}")

    offset_start = resume_offset(db_path, args.entity, results_path)

    run_classification(classifier, db_path, args.entity, results_path, offset_start)

    logging.info("Inference complete. Starting merge into main DB...")
    _merge_results_to_main(db_path, args.entity, results_path)
    logging.info("DCH classification complete.")


if __name__ == "__main__":
    main()
