"""
Documents/second of the topic classification hot path, before vs after the optimisation.

NO REAL DATA is needed or used: documents are ~150-word chunks of English prose taken from the .md/.rst/.txt/.py
files in the local virtualenv (docstrings and comments included, so it is prose-like, not clean prose), and the "taxonomy" is 500 synthetic topics built from other such chunks.
Absolute numbers therefore say little about the real project/work text (longer, other vocabulary); the
ratio between the variants is the useful part. Re-run on a compute node for cluster numbers.

    uv run python -m pipelines.core_v4.enrichment.benchmark_topics --docs 2000 --workers 8

Variants:
  before      TfidfTopicClassifier.enrich(): one nlp() call and one cosine_similarity() call per text
              (core_v3's per-process cost; core_v3 additionally re-pickled the classifier per task)
  after-1     enrich_batch(): nlp.pipe + one sparse product per batch, single process
  after-N     the same in N worker processes (pool initializer loads the model once per process)
"""

import argparse
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

from enrichment.topic_modelling.classifier import TfidfTopicClassifier
from pipelines.core_v4.enrichment import topic_modelling as tm


def prose_chunks(n_chunks: int, words: int = 150):
    root = Path(sys.prefix) / "lib"
    chunks, current = [], []
    for f in sorted(root.rglob("*.md")) + sorted(root.rglob("*.rst")) + sorted(root.rglob("*.txt")) + sorted(root.rglob("*.py")):
        try:
            text = f.read_text(errors="ignore")
        except OSError:
            continue
        for w in text.split():
            current.append(w)
            if len(current) == words:
                chunks.append(" ".join(current))
                current = []
                if len(chunks) >= n_chunks:
                    return chunks
    return chunks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs", type=int, default=2000)
    parser.add_argument("--topics", type=int, default=500)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()

    chunks = prose_chunks(args.docs + args.topics * 2)
    if len(chunks) < args.docs + args.topics * 2:
        raise SystemExit(f"only {len(chunks)} prose chunks found locally")
    topic_chunks, docs = chunks[: args.topics * 2], chunks[args.topics * 2 :]
    topics = pd.DataFrame({"topic_id": range(args.topics), "keywords": [c[:200] for c in topic_chunks[: args.topics]],
                           "summary": topic_chunks[args.topics :]})
    classifier = TfidfTopicClassifier.build(topics, docs[:500])
    print(f"{len(docs)} docs x {len(docs[0].split())} words, {args.topics} topics, vocab {len(classifier._vectorizer.vocabulary_)}")

    n_before = min(len(docs), 500)  # the slow path is measured on a subset
    t = time.time(); slow = classifier.enrich(docs[:n_before]); before = n_before / (time.time() - t)
    t = time.time(); fast = classifier.enrich_batch(docs[:n_before]); after1 = n_before / (time.time() - t)
    same = [p.topic_id for p in slow] == [p.topic_id for p in fast]
    print(f"before   (enrich, 1 process):        {before:8.1f} docs/s  ({n_before} docs)")
    print(f"after-1  (enrich_batch, 1 process):  {after1:8.1f} docs/s  ({n_before} docs)  identical topics: {same}")

    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "m.pkl"
        classifier.save(path)
        score, executor = tm.make_scorer(path, args.workers)
        score(docs[: args.workers * tm.CHUNK_DOCS // 4 or 1])  # warm-up: spawn workers + load model once
        t = time.time(); score(docs); afterN = len(docs) / (time.time() - t)
        executor.shutdown()
    print(f"after-{args.workers}  (pool, warm workers):        {afterN:8.1f} docs/s  ({len(docs)} docs)")
    print(f"speed-up vs before: 1 process x{after1 / before:.1f}, {args.workers} workers x{afterN / before:.1f}")


if __name__ == "__main__":
    main()
