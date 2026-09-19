"""
TF-IDF topic classification for core_v4: best OpenAlex topic per project/work, written to
    <enrichment_dir>/topics/<entity>/part-<shard>-<n>.parquet     (id, topic_id, score)

Differences to core_v3's topic_modelling.py (same TfidfTopicClassifier):
  - reads staging read-only through text_sources (translated text, one Arrow cursor, no OFFSET)
  - the hot path is batched: worker processes load the model once (pool initializer, not re-pickled
    per batch), use nlp.pipe, and score a whole chunk with one sparse matrix product
  - writes parquet parts; resumable by anti-joining the ids already written; shardable (--shard I/N)

Rows with no usable text get no row (topic_id -1 is never written). Rows whose text shares no
vocabulary with any topic get score 0.0 (like core_v3); the theme step's --min-score drops those.

The model is built ONCE (`--build-model`, from a seeded sample of translated project text plus the
oa_topics taxonomy) and then only loaded, so parallel shards all classify with the same model:
    <enrichment_dir>/topics/tfidf_model.pkl

Usage:
    uv run python -m pipelines.core_v4.enrichment.topic_modelling --build-model
    uv run python -m pipelines.core_v4.enrichment.topic_modelling --entity project [--shard 0/4]
    uv run python -m pipelines.core_v4.enrichment.topic_modelling --test 200      # dry run, no writes
"""

import argparse
import logging
import os
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Callable, Optional, Sequence

import duckdb
import numpy as np
import pandas as pd
import psutil
import pyarrow as pa

from common.config.dumps import get_dumps_paths
from common.log.logger import setup_logging
from pipelines.core_v3.resources import add_resource_args, apply_duckdb_limits
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.fingerprint import staging_stamp
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput
from pipelines.core_v4.enrichment.text_sources import Entity, open_staging, text_batches, text_sql

# Same fields the core_v3 topic text used, per entity.
TOPIC_FIELDS = {
    "project": ["title", "summary", "acronym", "keywords", "subjects"],
    "work": ["title", "description", "subjects", "container"],
}
BATCH_ROWS = 20_000  # rows per Arrow batch = per output part
CHUNK_DOCS = 500  # docs per task handed to a worker
MODEL_SAMPLE_SIZE = 50_000
MODEL_SAMPLE_SEED = 42

_worker_classifier = None


def model_path(enrichment_dir) -> Path:
    return Path(enrichment_dir) / "topics" / "tfidf_model.pkl"


# ---- worker pool ---------------------------------------------------------------------------
def _init_worker(path: str) -> None:
    """Pool initializer: loads the model (and, via the import, spaCy) once per process."""
    global _worker_classifier
    from enrichment.topic_modelling.classifier import TfidfTopicClassifier

    _worker_classifier = TfidfTopicClassifier.load(Path(path))


def _classify_chunk(texts: Sequence[str]):
    return _worker_classifier.classify_batch(texts)


def make_scorer(path: Path, workers: int) -> tuple[Callable, Optional[ProcessPoolExecutor]]:
    """Returns (score(texts) -> (topic_ids, scores), executor or None). workers <= 1 scores in-process."""
    if workers <= 1:
        _init_worker(str(path))
        return _classify_chunk, None
    executor = ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(str(path),))

    def score(texts):
        chunks = [texts[i : i + CHUNK_DOCS] for i in range(0, len(texts), CHUNK_DOCS)]
        results = list(executor.map(_classify_chunk, chunks))
        return np.concatenate([r[0] for r in results]), np.concatenate([r[1] for r in results])

    return score, executor


# ---- model ---------------------------------------------------------------------------------
def build_model(con: duckdb.DuckDBPyConnection, enrichment_dir, allow_untranslated: bool = False, topics_csv=None,
                sample_size: int = MODEL_SAMPLE_SIZE) -> Path:
    """Builds and saves the TF-IDF model: oa_topics taxonomy + a seeded sample of translated project text
    (the sample is only used for IDF weighting)."""
    from enrichment.topic_modelling.classifier import TfidfTopicClassifier

    topics_df = pd.read_csv(topics_csv or get_dumps_paths()["oa_topics"]["path_raw"])
    sql = text_sql("project", TOPIC_FIELDS["project"], enrichment_dir=enrichment_dir, allow_untranslated=allow_untranslated)
    texts = [
        r[0]
        for r in con.execute(
            f"SELECT full_text FROM ({sql}) USING SAMPLE reservoir({int(sample_size)} ROWS) REPEATABLE ({MODEL_SAMPLE_SEED})"
        ).fetchall()
    ]
    logging.info(f"Building TF-IDF model from {len(topics_df)} topics + {len(texts):,} sampled project texts")
    classifier = TfidfTopicClassifier.build(topics_df, texts)
    path = model_path(enrichment_dir)
    tmp = path.with_name(path.name + ".tmp")
    classifier.save(tmp)
    os.replace(tmp, path)  # atomic: a parallel shard never sees a half-written model
    return path


# ---- run -----------------------------------------------------------------------------------
def run(
    con: duckdb.DuckDBPyConnection,
    entity: Entity,
    enrichment_dir,
    score: Callable,
    *,
    shard: Shard = Shard(),
    limit: Optional[int] = None,
    dry_run: bool = False,
    allow_untranslated: bool = False,
    batch_rows: int = BATCH_ROWS,
    tier: Optional[int] = None,
) -> int:
    """Classifies every not-yet-done row of `entity` (this shard's) and writes one part per batch.
    Returns the number of rows classified in this call."""
    stamp = staging_stamp(con, entity, tier)
    out = SideOutput(enrichment_dir, "topics", entity, shard=shard, tier=tier)
    if not dry_run:
        out.begin()
    done_sql = None if dry_run else out.done_ids_sql()
    total, t0 = 0, time.time()
    for batch in text_batches(
        con, entity, TOPIC_FIELDS[entity], batch_size=batch_rows, enrichment_dir=enrichment_dir,
        allow_untranslated=allow_untranslated, shard=shard, exclude_ids_sql=done_sql, limit=limit, tier=tier,
    ):
        ids = batch.column("id").to_numpy()
        texts = batch.column("full_text").to_pylist()
        topic_ids, scores = score(texts)
        keep = topic_ids != -1  # no usable text: no row
        table = pa.table({"id": pa.array(ids[keep], pa.uint64()), "topic_id": pa.array(topic_ids[keep], pa.int32()),
                          "score": pa.array(scores[keep], pa.float32())})
        if dry_run:
            logging.info(f"[TEST] {entity} sample: {table.slice(0, 5).to_pylist()}")
        else:
            out.write(table)
        total += len(ids)
        logging.info(f"[{entity}] {total:,} docs, {total / max(time.time() - t0, 1e-9):,.0f} docs/s")
    if not dry_run and limit is None:
        out.finish(stamp)  # a --limit run is partial: never mark it complete
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="TF-IDF topic classification for core_v4.")
    add_common_args(parser)
    add_resource_args(parser, default_threads=psutil.cpu_count(logical=False) or 1)
    parser.add_argument("--build-model", action="store_true", help="build and save the TF-IDF model, then exit")
    parser.add_argument("--workers", type=int, default=None, help="scoring processes (default: --threads)")
    args = parser.parse_args()
    r = resolve(args)

    setup_logging("enrichment-topic_modelling", "tfidf_v4")
    con = open_staging(r.db)
    apply_duckdb_limits(con, args.mem_mb, args.threads)
    try:
        if args.build_model:
            build_model(con, r.enrichment_dir, args.allow_untranslated)
            return
        path = model_path(r.enrichment_dir)
        if not path.exists():
            raise SystemExit(f"No TF-IDF model at {path}: run once with --build-model first (so all shards share one model).")
        score, executor = make_scorer(path, args.workers or args.threads)
        try:
            for entity in r.entities:
                logging.info(f"=== topics: {entity} (shard {r.shard}) ===")
                run(con, entity, r.enrichment_dir, score, shard=r.shard, limit=r.limit, dry_run=r.dry_run,
                    allow_untranslated=args.allow_untranslated, tier=r.tier)
        finally:
            if executor:
                executor.shutdown()
    finally:
        con.close()


if __name__ == "__main__":
    main()
