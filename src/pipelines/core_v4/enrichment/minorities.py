"""
Minority tagging: which project/work rows mention one of the minority groups in minorities_raw.

Glue around enrichment.minority_matching.matcher (pure Aho-Corasick matcher): streams
(id, full_text) from the read-only staging file through text_sources (English text where NLLB
translated), matches in worker processes, and writes side parquet files:

    minorities/<entity>/part-*.parquet       id, minority_qid VARCHAR[]   (only rows with a hit)
    minorities/seen/<entity>/part-*.parquet  id                           (every processed row: resume)
    minorities/<entity>/_keyword_counts-<shard>.json                      rows hit per keyword (additive on resume)

Text fields: project title, summary, keywords, subjects (no acronym: too short to be safe);
work title, description, subjects (no container name: a journal called "Jewish Quarterly" would tag
every article in it).

Usage:
    uv run python -m pipelines.core_v4.enrichment.minorities --entity project
    uv run python -m pipelines.core_v4.enrichment.minorities --shard 1/4 --workers 32
    uv run python -m pipelines.core_v4.enrichment.minorities --test 5000          # dry run, no writes
    uv run python -m pipelines.core_v4.enrichment.minorities --limit 200000 --review 200
        # --review N writes <dir>/minorities/<entity>/review-<shard>.csv: up to N snippets for each
        # of the 20 most frequent keywords, for the manual precision check (label the `correct` column)
"""

import argparse
import csv
import json
import logging
from collections import Counter, defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from common.config.dumps import get_dumps_paths
from common.log.logger import setup_logging
from enrichment.minority_matching.matcher import (
    Group,
    MinorityMatcher,
    Rules,
    english_words,
    load_groups_from_duckdb,
    load_rules,
)
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.fingerprint import staging_stamp
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput
from pipelines.core_v4.enrichment.text_sources import open_staging, text_batches

FIELDS = {
    "project": ["title", "summary", "keywords", "subjects"],
    "work": ["title", "description", "subjects"],
}
SOURCE_DIR = Path(__file__).resolve().parents[3] / "sources" / "dumps" / "minorities"
TERM_OVERRIDES_CSV = SOURCE_DIR / "manual_term_overrides.csv"
TITULAR_OVERRIDES_CSV = SOURCE_DIR / "titular_majority_overrides.csv"

SEEN_SCHEMA = pa.schema([("id", pa.uint64())])
SNIPPET_CONTEXT = 60
TOP_KEYWORDS = 20

_matcher: Optional[MinorityMatcher] = None


def _init_worker(groups: List[Group], rules: Rules, typo: bool, known_words) -> None:
    global _matcher
    _matcher = MinorityMatcher(groups, rules, typo=typo, known_words=known_words)


def _match_batch(ids: List[int], texts: List[str], with_snippets: bool):
    """Runs in a worker. Returns ([(id, qids)], keyword row counts, snippets)."""
    hits, counts, snippets = [], Counter(), []
    for doc_id, text in zip(ids, texts):
        matches = _matcher.find(text or "")
        if not matches:
            continue
        hits.append((doc_id, sorted({q for m in matches for q in m.qids})))
        for kw in {m.keyword for m in matches}:
            counts[kw] += 1
        if with_snippets:
            from enrichment.minority_matching.matcher import normalize

            for m in matches:
                norm = normalize(text, lower=not m.strict)
                a, b = max(0, m.start - SNIPPET_CONTEXT), m.end + SNIPPET_CONTEXT
                snippets.append((m.keyword, doc_id, m.typo, m.strict, norm[a : m.start] + "[[" + norm[m.start : m.end] + "]]" + norm[m.end : b]))
    return hits, counts, snippets


def _drain(pending: dict, results: list) -> None:
    done, _ = wait(list(pending), return_when=FIRST_COMPLETED)
    for fut in done:
        results.append((pending.pop(fut), fut.result()))


def run_entity(
    con,
    entity: str,
    groups: List[Group],
    rules: Rules,
    enrichment_dir: str,
    shard: Shard,
    *,
    limit: Optional[int] = None,
    dry_run: bool = False,
    reset: bool = False,
    allow_untranslated: bool = False,
    typo: bool = True,
    workers: int = 4,
    batch_size: int = 5_000,
    review: int = 0,
    tier: Optional[int] = None,
) -> Dict[str, object]:
    stamp = staging_stamp(con, entity, tier)
    out = SideOutput(enrichment_dir, "minorities", entity, shard=shard, tier=tier)
    seen = SideOutput(enrichment_dir, "minorities/seen", entity, shard=shard, schema=SEEN_SCHEMA, tier=tier)
    if not dry_run:
        out.begin(reset=reset)
        seen.begin(reset=reset)
        counts_file = out.dir / f"_keyword_counts-{out.tag}{shard.index}.json"
        if reset:
            counts_file.unlink(missing_ok=True)

    totals = {"rows": 0, "rows_hit": 0}
    keyword_counts: Counter = Counter()
    samples: Dict[str, list] = defaultdict(list)
    results: list = []
    pending: dict = {}  # future -> ids of its batch

    def consume(ids, hits_counts_snips) -> None:
        hits, counts, snips = hits_counts_snips
        keyword_counts.update(counts)
        totals["rows_hit"] += len(hits)
        for kw, doc_id, typo_hit, strict, snippet in snips:
            if len(samples[kw]) < review:
                samples[kw].append((kw, doc_id, typo_hit, strict, snippet))
        if hits and not dry_run:
            out.write(
                pa.table(
                    {
                        "id": pa.array([h[0] for h in hits], pa.uint64()),
                        "minority_qid": pa.array([h[1] for h in hits], pa.list_(pa.string())),
                    }
                )
            )
        if not dry_run:
            # seen only after the batch's hits are on disk: a crash in between re-matches the
            # batch (duplicate hit rows, harmless: assemble takes DISTINCT) instead of losing hits
            seen.write(pa.table({"id": pa.array(ids, pa.uint64())}))

    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker, initargs=(groups, rules, typo, english_words() if typo else ())) as pool:
        batches = text_batches(
            con,
            entity,
            FIELDS[entity],
            batch_size=batch_size,
            enrichment_dir=enrichment_dir,
            allow_untranslated=allow_untranslated,
            shard=shard,
            exclude_ids_sql=None if dry_run else seen.done_ids_sql(),
            limit=limit,
            tier=tier,
        )
        for batch in batches:
            ids = batch.column("id").to_pylist()
            texts = batch.column("full_text").to_pylist()
            totals["rows"] += len(ids)
            pending[pool.submit(_match_batch, ids, texts, review > 0)] = ids
            while len(pending) >= workers * 2:
                _drain(pending, results)
                while results:
                    consume(*results.pop())
        while pending:
            _drain(pending, results)
        while results:
            consume(*results.pop())

    top = keyword_counts.most_common(TOP_KEYWORDS)
    logging.info(f"[{entity}] {totals['rows']:,} rows, {totals['rows_hit']:,} with a minority hit")
    logging.info(f"[{entity}] top keywords (rows hit): {top}")

    if not dry_run:
        if review:
            path = out.dir / f"review-{out.tag}{shard.index}.csv"
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["keyword", "id", "typo", "capitalised_only", "snippet", "correct"])
                for kw, _ in top:
                    for row in samples[kw]:
                        w.writerow([*row, ""])
            logging.info(f"[{entity}] precision sample written to {path}")
        previous = json.loads(counts_file.read_text()) if counts_file.exists() else {}
        for kw, n in keyword_counts.items():
            previous[kw] = previous.get(kw, 0) + n
        counts_file.write_text(json.dumps(dict(sorted(previous.items(), key=lambda kv: -kv[1])), ensure_ascii=False, indent=1))
        if limit is None:
            out.finish(stamp)
            seen.finish(stamp)
        else:
            logging.info(f"[{entity}] --limit given: not marking the output complete (no _SUCCESS)")
    return {**totals, "top_keywords": top}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common_args(parser)
    parser.add_argument("--minorities-db", default=None, help="minorities_raw duckdb (default: config path_duck)")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument("--no-typos", action="store_true", help="exact keywords only (no edit-distance-1 variants)")
    parser.add_argument("--reset", action="store_true", help="discard this shard's existing output first")
    parser.add_argument("--review", type=int, default=0, metavar="N", help="write N snippets per top-20 keyword")
    args = parser.parse_args()
    setup_logging("minorities", "core_v4_enrichment")

    cfg = resolve(args)
    db = args.minorities_db or get_dumps_paths()["minorities"]["path_duck"]
    groups = load_groups_from_duckdb(Path(db), TERM_OVERRIDES_CSV, TITULAR_OVERRIDES_CSV)
    rules = load_rules()
    matcher = MinorityMatcher(groups, rules, typo=not args.no_typos)
    logging.info(
        f"{len(groups)} groups, {len(matcher.keyword_qids)} keywords ({len(matcher.ambiguous)} capitalised-only, "
        f"{len(matcher.blocked)} blocked), {matcher.n_patterns:,} patterns"
    )
    con = open_staging(cfg.db)
    for entity in cfg.entities:
        run_entity(
            con,
            entity,
            groups,
            rules,
            cfg.enrichment_dir,
            cfg.shard,
            limit=cfg.limit,
            dry_run=cfg.dry_run,
            reset=args.reset,
            allow_untranslated=args.allow_untranslated,
            typo=not args.no_typos,
            workers=args.workers,
            batch_size=args.batch_size,
            review=args.review,
            tier=cfg.tier,
        )


if __name__ == "__main__":
    main()
