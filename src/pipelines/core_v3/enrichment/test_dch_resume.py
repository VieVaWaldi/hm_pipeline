"""
Verifies that dch_classification survives a run dying mid-way — no GPU needed
(a fake classifier stands in for the BERT model). Temp dir only; never touches
core_v3's real duckdbs or resume files.

Simulates, in order:
  1. a classifier that raises after 3 chunks (job killed / timed out),
  2. a hard kill mid-append: ids for the next chunk written, preds not, plus a
     torn partial record — the worst state the resume files can be left in,
  3. the resumed run finishing and merging,
  4. a rerun after completion (must not double-count and skip rows),
  5. transformation's invalidation of the resume files.
Final results must equal an uninterrupted run's, row for row.

Usage:
    uv run python -m pipelines.core_v3.enrichment.test_dch_resume
"""

import tempfile
from pathlib import Path
from typing import List

import duckdb
import numpy as np

from pipelines.core_v3.enrichment import dch_classification as dch
from pipelines.core_v3.enrichment.dch_results import (
    delete_results,
    read_results,
    repair_results,
    results_base,
    results_paths,
)
from pipelines.core_v3.enrichment.dch_classification import (
    _merge_results_to_main,
    resume_offset,
    run_classification,
)

N_ROWS = 95
CHUNK = 10
ID_BASE = 2**63  # real project ids are UBIGINT hashes, most above the int64 max


def _prob(text: str) -> float:
    return (len(text) % 100) / 100  # deterministic, so a resumed run can be compared to a clean one


class _FakeClassifier:
    def __init__(self, fail_on_call: int = -1):
        self.calls = 0
        self.fail_on_call = fail_on_call

    def enrich(self, texts: List[str]) -> List[float]:
        self.calls += 1
        if self.calls == self.fail_on_call:
            raise RuntimeError("simulated job death")
        return [_prob(t) for t in texts]


def _make_db(path: Path) -> None:
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE project (id UBIGINT, title VARCHAR, acronym VARCHAR, summary VARCHAR, keywords VARCHAR, subjects VARCHAR[])")
    con.executemany(
        "INSERT INTO project VALUES (?, ?, NULL, ?, NULL, NULL)",
        [(ID_BASE + i, f"title {i}", "s" * (i * 7 % 60)) for i in range(1, N_ROWS + 1)],
    )
    con.close()


def main() -> None:
    dch.CHUNK_ROWS = CHUNK
    with tempfile.TemporaryDirectory() as tmp:
        db = str(Path(tmp) / "core_v3.duckdb")
        _make_db(db)
        base = results_base(db, "project")
        ids_path, preds_path = results_paths(base)

        # 1. dies on the 4th chunk -> 3 chunks safely on disk
        try:
            run_classification(_FakeClassifier(fail_on_call=4), db, "project", base, offset_start=0)
            raise AssertionError("expected the simulated failure")
        except RuntimeError:
            pass
        assert repair_results(base) == 3 * CHUNK, "chunks before the failure must be kept"

        # 2. hard kill mid-append: ids of chunk 4 written, preds not, plus a torn record
        with open(ids_path, "ab") as f:
            f.write(np.arange(ID_BASE + 31, ID_BASE + 41, dtype=np.uint64).tobytes())
        with open(preds_path, "ab") as f:
            f.write(b"\x00\x01\x02")
        offset = resume_offset(db, "project", base)
        assert offset == 3 * CHUNK, f"resume must fall back to the last row in BOTH files, got {offset}"

        # 3. resume to the end, merge
        run_classification(_FakeClassifier(), db, "project", base, offset_start=offset)
        _merge_results_to_main(db, "project", base)

        ids, probs = read_results(base)
        assert list(ids) == list(range(ID_BASE + 1, ID_BASE + N_ROWS + 1)), "every row exactly once, in order"
        con = duckdb.connect(db, read_only=True)
        rows = con.execute("SELECT id, pred, is_ch FROM project ORDER BY id").fetchall()
        con.close()
        assert len(rows) == N_ROWS and all(r[1] is not None for r in rows), "every row must be merged"
        for (row_id, pred, is_ch), expected_prob in zip(rows, probs):
            assert abs(pred - expected_prob) < 1e-6 and is_ch == (expected_prob >= dch.THRESHOLD)

        # 4. rerun after completion: DB and files both hold everything — overlap, not sum
        assert resume_offset(db, "project", base) == N_ROWS

        # 5. rows rebuilt (transformation) -> resume files must go
        delete_results(db)
        assert not Path(ids_path).exists() and not Path(preds_path).exists()

    print("PASSED: dch resume survives a mid-run death, a torn append, and a rerun.")


if __name__ == "__main__":
    main()
