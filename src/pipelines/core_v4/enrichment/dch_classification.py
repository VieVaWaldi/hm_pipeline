"""
DCH (Digital Cultural Heritage) classification for core_v4:
    <enrichment_dir>/dch/<entity>/part-<shard>-<n>.parquet     (id, is_ch, pred)

Glue around the unchanged DchClassifier (P(is DCH) per text via BERT on a GPU). Differs from core_v3:
staging is opened read-only and streamed in chunks (no fetchall()), text is the NLLB translation where
there is one, and every chunk is one parquet part written atomically, so a crash loses at most one chunk
and resume is an anti-join on the ids already written (this replaces core_v3's binary resume files).
Shardable across GPU nodes with --shard I/N.

Usage (GPU node):
    uv run python -m pipelines.core_v4.enrichment.dch_classification --entity work --shard 0/4
    uv run python -m pipelines.core_v4.enrichment.dch_classification --test 5      # dry run, no writes
"""

import argparse
import logging
import time
from typing import Optional

import pyarrow as pa

from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.fingerprint import staging_stamp
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput
from pipelines.core_v4.enrichment.text_sources import Entity, open_staging, text_batches

THRESHOLD = 0.5  # P(is DCH) >= threshold -> is_ch
# 20 x DchClassifier's DEFAULT_BATCH_SIZE (2048), so its tokeniser/GPU pipelining overlaps within a chunk.
CHUNK_ROWS = 20 * 2048

# Same fields the core_v3 DCH text used, per entity.
DCH_FIELDS = {
    "project": ["title", "summary", "acronym", "keywords", "subjects"],
    "work": ["title", "description", "subjects", "container"],
}


def run(
    con,
    classifier,
    entity: Entity,
    enrichment_dir,
    *,
    shard: Shard = Shard(),
    limit: Optional[int] = None,
    dry_run: bool = False,
    allow_untranslated: bool = False,
    chunk_rows: int = CHUNK_ROWS,
    tier: Optional[int] = None,
) -> int:
    """Classifies every not-yet-done row of `entity` (this shard's). `classifier` is anything with
    enrich(list[str]) -> list[float]. Returns the number of rows classified in this call."""
    stamp = staging_stamp(con, entity, tier)
    out = SideOutput(enrichment_dir, "dch", entity, shard=shard, tier=tier)
    if not dry_run:
        out.begin()
    done_sql = None if dry_run else out.done_ids_sql()
    total, n_ch, t0 = 0, 0, time.time()
    for batch in text_batches(
        con, entity, DCH_FIELDS[entity], batch_size=chunk_rows, enrichment_dir=enrichment_dir,
        allow_untranslated=allow_untranslated, shard=shard, exclude_ids_sql=done_sql, limit=limit, tier=tier,
    ):
        probs = classifier.enrich(batch.column("full_text").to_pylist())
        is_ch = [p >= THRESHOLD for p in probs]
        table = pa.table({"id": batch.column("id").cast(pa.uint64()), "is_ch": pa.array(is_ch, pa.bool_()),
                          "pred": pa.array(probs, pa.float32())})
        if not dry_run:
            out.write(table)
        total += batch.num_rows
        n_ch += sum(is_ch)
        elapsed = max(time.time() - t0, 1e-9)
        logging.info(f"[{'TEST ' if dry_run else ''}{entity}] {total:,} rows  CH={n_ch:,}  ({total / elapsed:,.0f} seq/s)")
    if not dry_run and limit is None:
        out.finish(stamp)  # a --limit run is partial: never mark it complete
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify core_v4 project/work rows as DCH or not.")
    add_common_args(parser)
    parser.add_argument("--chunk-rows", type=int, default=CHUNK_ROWS)
    args = parser.parse_args()
    r = resolve(args)

    setup_logging("enrichment-dch_classification", "bert_inference_v4")
    logging.info(f"Mode: {'TEST (no writes)' if r.dry_run else 'PRODUCTION'}  db={r.db}  shard={r.shard}")

    from enrichment.dch_classification.dch_classifier import DchClassifier  # imports torch: keep out of module import

    classifier = DchClassifier.load(get_project_root_path() / "data" / "models" / "bert_classifier")
    con = open_staging(r.db)
    try:
        for entity in r.entities:
            run(con, classifier, entity, r.enrichment_dir, shard=r.shard, limit=r.limit, dry_run=r.dry_run,
                allow_untranslated=args.allow_untranslated, chunk_rows=args.chunk_rows, tier=r.tier)
    finally:
        con.close()


if __name__ == "__main__":
    main()
