"""
The on-disk resume files dch_classification.py writes during inference, kept
in their own module (numpy + stdlib only) so transformation.py can invalidate
them without importing torch, and so the crash-recovery logic is testable
without a GPU.

Layout: next to the final core_v3 duckdb, two append-only binary files per
entity —
    <db stem>_dch_<entity>_results.ids.bin    N x int64   (8 bytes/row)
    <db stem>_dch_<entity>_results.preds.bin  N x float32 (4 bytes/row)

Crash safety: ids are written before preds, each append is one write, so a
kill can leave (a) preds shorter than ids, or (b) a torn trailing record in
either file. `repair_results` trims both files back to the last row present
in *both*, whole — call it before trusting the row count on resume. At most
one chunk of GPU work is lost.

These files outlive the duckdb they were computed for: Snakemake deletes the
final duckdb whenever the job fails, and resume must still work. They are only
invalid when the *rows* change, which is why transformation.py (the one step
that rebuilds project rows) calls `delete_results`.
"""

import os
from pathlib import Path
from typing import List, Tuple

import numpy as np

_ID_BYTES = 8
_PRED_BYTES = 4


def results_base(db_path: str, entity: str) -> str:
    return str(Path(db_path).with_suffix("")) + f"_dch_{entity}_results"


def results_paths(base: str) -> Tuple[str, str]:
    return base + ".ids.bin", base + ".preds.bin"


def _size(path: str) -> int:
    return os.path.getsize(path) if os.path.exists(path) else 0


def repair_results(base: str) -> int:
    """Trims both files to the last complete row present in both; returns that row count."""
    ids_path, preds_path = results_paths(base)
    n_rows = min(_size(ids_path) // _ID_BYTES, _size(preds_path) // _PRED_BYTES)
    for path, row_bytes in ((ids_path, _ID_BYTES), (preds_path, _PRED_BYTES)):
        if _size(path) > n_rows * row_bytes:
            with open(path, "r+b") as f:
                f.truncate(n_rows * row_bytes)
    return n_rows


def append_to_results(base: str, ids: List[int], probs: List[float]) -> None:
    ids_path, preds_path = results_paths(base)
    with open(ids_path, "ab") as f:
        f.write(np.array(ids, dtype=np.int64).tobytes())
    with open(preds_path, "ab") as f:
        f.write(np.array(probs, dtype=np.float32).tobytes())


def read_results(base: str) -> Tuple[np.ndarray, np.ndarray]:
    ids_path, preds_path = results_paths(base)
    return np.fromfile(ids_path, dtype=np.int64), np.fromfile(preds_path, dtype=np.float32)


def delete_results(db_path: str) -> None:
    """Removes every entity's resume files for `db_path` (used when the rows
    they were computed for are rebuilt)."""
    db = Path(db_path)
    for stale in db.parent.glob(f"{db.stem}_dch_*_results.*.bin"):
        stale.unlink()
