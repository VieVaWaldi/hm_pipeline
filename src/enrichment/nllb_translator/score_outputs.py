"""
chrF of benchmark outputs (benchmark.py --dump) against a reference configuration's outputs. There are
no human references for this corpus, so the strongest configuration (1.3B, float16, beam 4) serves as
a pseudo-reference: the score says how much a cheaper configuration deviates from it, not how good
either is.

    uv run python -m enrichment.nllb_translator.score_outputs <reference.jsonl> <other.jsonl> ...
"""

import json
import sys
from pathlib import Path

from sacrebleu.metrics import CHRF


def _load(path):
    return [json.loads(line) for line in Path(path).read_text().splitlines() if line]


def main(argv) -> None:
    reference = _load(argv[0])
    refs = [r["out"] for r in reference]
    chrf = CHRF()
    for path in argv[1:]:
        rows = _load(path)
        n = min(len(rows), len(refs))
        score = chrf.corpus_score([r["out"] for r in rows[:n]], [refs[:n]])
        print(f"{Path(path).stem:24s} chrF vs reference: {score.score:5.1f}  (n={n})")


if __name__ == "__main__":
    main(sys.argv[1:])
