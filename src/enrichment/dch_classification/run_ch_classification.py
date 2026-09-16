"""
CH (Cultural Heritage) classification of all projects in core_v3_final.duckdb.

Uses the fine-tuned BERT model (models/bert_classifier/) to classify each project
as CH or not-CH. Adds two columns to the project table:
  - is_ch  BOOLEAN  : True if P(CH) >= THRESHOLD
  - pred   FLOAT    : P(CH) — probability of being a CH project

Architecture:
  - Background thread: tokenises in-memory rows into CPU tensors, feeds a queue
  - Main thread: pulls from queue, runs GPU inference (bf16 AMP, eager mode), writes results

Resume-safe: counts already-classified rows at startup and skips via OFFSET.
"""

import datetime
import logging
import threading
from queue import Queue
from typing import List, Tuple

import os

import duckdb
import numpy as np
import pandas as pd
import torch
from torch.amp import autocast
from transformers import BertForSequenceClassification, BertTokenizerFast

from common.file_handling.path_utils import get_project_root_path
from common.config.pipelines import get_pipeline_paths
from common.log.logger import setup_logging

# ── Hyperparams ────────────────────────────────────────────────────────────────
BATCH_SIZE = 2048  # sequences per GPU forward pass — reduced from 6144: eager mode (no torch.compile) needs <10GB for FFN intermediate
MAX_LENGTH = 512  # BERT max tokens
PREFETCH = 4  # tokenised batches buffered ahead of GPU
THRESHOLD = 0.5  # P(CH) >= threshold  →  is_ch = True
SKIP_BATCHES = (
    set()
)  # batch indices to skip if a specific batch causes a hang; e.g. {891, 892}

# ── SQL ────────────────────────────────────────────────────────────────────────

# Mirrors the text construction in the topic-modelling pipeline:
# CONCAT_WS silently skips NULLs, subjects is list<varchar> → list_aggregate.
_PROJECT_QUERY_ALL = """
SELECT id,
       CONCAT_WS(' ',
           title,
           keywords,
           list_aggregate(subjects, 'string_agg', ' '),
           summary
       ) AS full_text
FROM   project
WHERE  title IS NOT NULL OR summary IS NOT NULL
ORDER  BY id
OFFSET {offset}
"""

_ADD_COLUMNS = """
ALTER TABLE project ADD COLUMN IF NOT EXISTS is_ch BOOLEAN;
ALTER TABLE project ADD COLUMN IF NOT EXISTS pred  FLOAT;
"""


# Binary results files written during inference — avoids any DuckDB/CUDA threading conflict.
# ids_path  : N × int64   (8 bytes/row)
# preds_path: N × float32 (4 bytes/row)
def _results_paths(base: str):
    return base + ".ids.bin", base + ".preds.bin"


def _results_row_count(base: str) -> int:
    ids_path, _ = _results_paths(base)
    if not os.path.exists(ids_path):
        return 0
    return os.path.getsize(ids_path) // 8


# ── Data loading (background thread) ──────────────────────────────────────────


def _load_all_rows(db_path: str, offset_start: int) -> List[Tuple]:
    """
    Reads all remaining rows into memory in one shot, then closes the connection.
    Avoids holding an open read connection while the write connection is active.
    """
    logging.info(f"[LOAD] Reading all rows from offset {offset_start:,} into memory...")
    con = duckdb.connect(db_path, read_only=True)
    try:
        rows = con.execute(_PROJECT_QUERY_ALL.format(offset=offset_start)).fetchall()
    finally:
        con.close()
    logging.info(f"[LOAD] Loaded {len(rows):,} rows. DB read connection closed.")
    return rows


def _batch_iter(rows: List[Tuple]):
    """Yields BATCH_SIZE chunks from an in-memory list."""
    for i in range(0, len(rows), BATCH_SIZE):
        yield rows[i : i + BATCH_SIZE]


def _tokeniser_worker(
    rows: List[Tuple],
    tokenizer: BertTokenizerFast,
    queue: Queue,
) -> None:
    """
    Background thread: tokenises in-memory rows into CPU tensors, feeds queue.
    Always puts the sentinel (None) even on exception so the main thread unblocks.
    """
    try:
        for batch in _batch_iter(rows):
            ids = [row[0] for row in batch]
            texts = [(row[1] or "")[:50_000] for row in batch]
            encoded = tokenizer(
                texts,
                padding=True,
                truncation=True,
                max_length=MAX_LENGTH,
                return_tensors="pt",
            )
            queue.put((ids, encoded))
    finally:
        queue.put(None)


# ── Results write (main thread) ────────────────────────────────────────────────


def _append_to_results(
    results_base: str,
    ids: List[int],
    probs: np.ndarray,
) -> None:
    """Append batch predictions to binary files — no DuckDB, no GIL interaction."""
    ids_path, preds_path = _results_paths(results_base)
    with open(ids_path, "ab") as f:
        f.write(np.array(ids, dtype=np.int64).tobytes())
    with open(preds_path, "ab") as f:
        f.write(probs.astype(np.float32).tobytes())


def _merge_results_to_main(main_db_path: str, results_base: str) -> None:
    """
    One-shot merge: UPDATE main project table from the binary results files.
    Run once after all inference is complete — no threading at this point.
    """
    ids_path, preds_path = _results_paths(results_base)
    logging.info(f"Loading results from {ids_path} ...")
    ids = np.frombuffer(open(ids_path, "rb").read(), dtype=np.int64)
    probs = np.frombuffer(open(preds_path, "rb").read(), dtype=np.float32)
    df = pd.DataFrame(
        {
            "id": ids,
            "is_ch": (probs >= THRESHOLD),
            "pred": probs,
        }
    )
    logging.info(f"Merging {len(df):,} rows into {main_db_path} ...")
    con = duckdb.connect(main_db_path)
    con.execute(_ADD_COLUMNS)
    con.register("_results", df)
    con.execute("""
        UPDATE project
        SET    is_ch = r.is_ch,
               pred  = r.pred
        FROM   _results r
        WHERE  project.id = r.id
    """)
    con.unregister("_results")
    con.close()
    logging.info("Merge complete.")


# ── Inference loop (main thread) ───────────────────────────────────────────────


def run_classification(
    model: BertForSequenceClassification,
    tokenizer: BertTokenizerFast,
    db_path: str,
    results_path: str,
    offset_start: int,
) -> None:
    device = next(model.parameters()).device

    amp_dtype = (
        torch.bfloat16
        if device.type == "cuda" and torch.cuda.is_bf16_supported()
        else torch.float16
    )
    logging.info(f"AMP dtype: {amp_dtype}")

    # Load all remaining rows into memory, close read connection before writing
    rows = _load_all_rows(db_path, offset_start)

    queue = Queue(maxsize=PREFETCH)
    producer = threading.Thread(
        target=_tokeniser_worker,
        args=(rows, tokenizer, queue),
        daemon=True,
    )
    producer.start()

    total = 0
    batch_idx = 0
    t_start = datetime.datetime.now()

    try:
        while True:
            item = queue.get()
            if item is None:
                break

            ids, encoded = item

            if batch_idx in SKIP_BATCHES:
                logging.warning(f"Batch #{batch_idx:>5}  SKIPPED (in SKIP_BATCHES)")
                batch_idx += 1
                continue

            t_batch = datetime.datetime.now()

            input_ids = encoded["input_ids"].to(device, non_blocking=True)
            attention_mask = encoded["attention_mask"].to(device, non_blocking=True)

            with torch.no_grad():
                with autocast("cuda", dtype=amp_dtype):
                    logits = model(input_ids, attention_mask=attention_mask).logits
                probs_ch = torch.softmax(logits.float(), dim=1)[:, 1].cpu().numpy()
            _append_to_results(results_path, ids, probs_ch)

            total += len(ids)
            elapsed = (datetime.datetime.now() - t_batch).total_seconds()
            rate = len(ids) / elapsed if elapsed > 0 else 0
            logging.info(
                f"Batch #{batch_idx:>5}  size={len(ids):>4}  "
                f"total={total:>9,}  {rate:>6.0f} seq/s  "
                f"wall={datetime.datetime.now() - t_start}"
            )
            batch_idx += 1
    finally:
        producer.join()


# ── Test mode (no writes) ──────────────────────────────────────────────────────


def run_test(
    model: BertForSequenceClassification,
    tokenizer: BertTokenizerFast,
    db_path: str,
    n_batches: int,
) -> None:
    """Dry-run: reads + infers N batches, prints throughput, writes nothing."""
    device = next(model.parameters()).device
    amp_dtype = (
        torch.bfloat16
        if device.type == "cuda" and torch.cuda.is_bf16_supported()
        else torch.float16
    )

    logging.info(f"[TEST] Reading up to {n_batches} batches from DB (no writes).")
    test_rows = _load_all_rows(db_path, offset_start=0)
    times = []
    for i, batch in enumerate(_batch_iter(test_rows)):
        if i >= n_batches:
            break
        ids = [row[0] for row in batch]
        texts = [(row[1] or "")[:50_000] for row in batch]

        encoded = tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=MAX_LENGTH,
            return_tensors="pt",
        )
        input_ids = encoded["input_ids"].to(device, non_blocking=True)
        attention_mask = encoded["attention_mask"].to(device, non_blocking=True)

        t0 = datetime.datetime.now()
        with torch.no_grad():
            with autocast("cuda", dtype=amp_dtype):
                logits = model(input_ids, attention_mask=attention_mask).logits
            probs_ch = torch.softmax(logits.float(), dim=1)[:, 1].cpu().numpy()
        elapsed = (datetime.datetime.now() - t0).total_seconds()
        times.append(elapsed)

        preds = probs_ch >= THRESHOLD
        n_ch = int(preds.sum())
        logging.info(
            f"[TEST] Batch #{i}  size={len(ids)}  "
            f"CH={n_ch}  NOT_CH={len(ids)-n_ch}  "
            f"gpu_time={elapsed:.3f}s  ({len(ids)/elapsed:.0f} seq/s)"
        )

    if times:
        avg_rate = BATCH_SIZE / (sum(times) / len(times))
        logging.info(
            f"[TEST] Done.  avg throughput={avg_rate:.0f} seq/s  "
            f"extrapolated 4M projects ≈ {4_000_000 / avg_rate / 60:.1f} min"
        )


# ── Entry point ────────────────────────────────────────────────────────────────


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test",
        type=int,
        default=0,
        metavar="N",
        help="Dry-run: infer N batches from path_staging_duck, no DB writes.",
    )
    args = parser.parse_args()

    setup_logging("enrichment-ch_classification", "bert_inference")

    config = get_pipeline_paths()["core_v3"]
    db_path = (
        config["path_staging_duck"]
        if args.test
        else "/work/lu72hip/data/duckdb/core/core_v3_test_final.duckdb"
    )
    model_path = get_project_root_path() / "models" / "bert_classifier"

    logging.info(f"Mode      : {'TEST (no writes)' if args.test else 'PRODUCTION'}")
    logging.info(f"DB path   : {db_path}")
    logging.info(f"Model path: {model_path}")

    if not torch.cuda.is_available():
        raise RuntimeError("No CUDA GPU detected. Submit this job to a GPU node.")

    device = torch.device("cuda")
    props = torch.cuda.get_device_properties(0)
    logging.info(
        f"GPU: {props.name}  VRAM={props.total_memory / 1e9:.1f} GB  "
        f"CUDA {torch.version.cuda}"
    )

    # ── Load tokenizer + model ─────────────────────────────────────────────────
    logging.info("Loading tokenizer (bert-base-uncased)...")
    tokenizer = BertTokenizerFast.from_pretrained("bert-base-uncased")

    logging.info(f"Loading model from {model_path} ...")
    model = BertForSequenceClassification.from_pretrained(str(model_path))
    model.to(device)
    model.eval()

    # ── Test or production ─────────────────────────────────────────────────────
    if args.test:
        run_test(model, tokenizer, db_path, n_batches=args.test)
        return

    results_path = "/scratch/lu72hip/core_v3_final_ch_results"
    logging.info(f"Results base: {results_path}")

    # Resume: rows already in main DB + rows already in results binary files
    con = duckdb.connect(db_path, read_only=True)
    try:
        already_in_main = con.execute(
            "SELECT count(*) FROM project WHERE is_ch IS NOT NULL"
        ).fetchone()[0]
    except Exception:
        already_in_main = 0
    con.close()

    already_in_results = _results_row_count(results_path)

    offset_start = already_in_main + already_in_results
    logging.info(
        f"Already classified: {already_in_main:,} in main DB + "
        f"{already_in_results:,} in results file "
        f"→ resuming from offset {offset_start:,}"
    )

    logging.info(
        f"Starting inference  "
        f"batch_size={BATCH_SIZE}  max_length={MAX_LENGTH}  "
        f"threshold={THRESHOLD}  prefetch={PREFETCH}"
    )
    run_classification(model, tokenizer, db_path, results_path, offset_start)

    logging.info("Inference complete. Starting merge into main DB...")
    _merge_results_to_main(db_path, results_path)
    logging.info("CH classification complete.")


if __name__ == "__main__":
    main()
