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
after all chunks are done.

Usage:
    uv run python -m pipelines.core_v3.enrichment.dch_classification
    uv run python -m pipelines.core_v3.enrichment.dch_classification --entity work
    uv run python -m pipelines.core_v3.enrichment.dch_classification --test 5   # dry-run, no writes
"""

import argparse
import logging
import os
from pathlib import Path
from typing import List, Tuple

import duckdb
import numpy as np
import pandas as pd

from common.config.pipelines import get_pipeline_paths
from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging
from enrichment.dch_classification.dch_classifier import DEFAULT_BATCH_SIZE, DchClassifier
from pipelines.core_v3.enrichment.text_sources import Entity, all_text_rows

THRESHOLD = 0.5  # P(is DCH) >= threshold  →  is_ch = True
CHUNK_ROWS = 20 * DEFAULT_BATCH_SIZE


# Binary results files written during inference — avoids any DuckDB/CUDA threading conflict.
# ids_path  : N × int64   (8 bytes/row)
# preds_path: N × float32 (4 bytes/row)
def _results_paths(base: str) -> Tuple[str, str]:
    return base + ".ids.bin", base + ".preds.bin"


def _results_row_count(base: str) -> int:
    ids_path, _ = _results_paths(base)
    if not os.path.exists(ids_path):
        return 0
    return os.path.getsize(ids_path) // 8


def _append_to_results(results_base: str, ids: List[int], probs: List[float]) -> None:
    ids_path, preds_path = _results_paths(results_base)
    with open(ids_path, "ab") as f:
        f.write(np.array(ids, dtype=np.int64).tobytes())
    with open(preds_path, "ab") as f:
        f.write(np.array(probs, dtype=np.float32).tobytes())


def _merge_results_to_main(db_path: str, entity: Entity, results_base: str) -> None:
    """One-shot merge: UPDATE the target table from the binary results files.
    Run once after all inference is complete — no threading at this point."""
    ids_path, preds_path = _results_paths(results_base)
    logging.info(f"Loading results from {ids_path} ...")
    ids = np.frombuffer(open(ids_path, "rb").read(), dtype=np.int64)
    probs = np.frombuffer(open(preds_path, "rb").read(), dtype=np.float32)
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
        _append_to_results(results_path, ids, probs)

        total += len(chunk)
        logging.info(f"Chunk #{chunk_idx // CHUNK_ROWS:>5}  size={len(chunk):>6,}  total={total:>9,}/{len(rows):,}")


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
    db_path = get_pipeline_paths()["core_v3"]["path_staging_duck"]
    logging.info(f"Mode   : {'TEST (no writes)' if args.test else 'PRODUCTION'}")
    logging.info(f"DB path: {db_path}  entity: {args.entity}")

    model_path = get_project_root_path() / "data" / "models" / "bert_classifier"
    classifier = DchClassifier.load(model_path)

    if args.test:
        run_test(classifier, db_path, args.entity, n_rows=args.test)
        return

    results_path = str(Path(db_path).with_suffix("")) + f"_dch_{args.entity}_results"
    logging.info(f"Results base: {results_path}")

    con = duckdb.connect(db_path, read_only=True)
    try:
        already_in_main = con.execute(f"SELECT count(*) FROM {args.entity} WHERE is_ch IS NOT NULL").fetchone()[0]
    except Exception:
        already_in_main = 0
    con.close()

    already_in_results = _results_row_count(results_path)
    offset_start = already_in_main + already_in_results
    logging.info(
        f"Already classified: {already_in_main:,} in main DB + {already_in_results:,} in results file "
        f"→ resuming from offset {offset_start:,}"
    )

    run_classification(classifier, db_path, args.entity, results_path, offset_start)

    logging.info("Inference complete. Starting merge into main DB...")
    _merge_results_to_main(db_path, args.entity, results_path)
    logging.info("DCH classification complete.")


if __name__ == "__main__":
    main()
