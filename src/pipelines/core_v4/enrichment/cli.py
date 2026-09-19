"""
Flags every core_v4 enrichment CLI shares, and the path resolution behind them.

    --db PATH             staging duckdb (read-only); default from config/pipelines.yaml
    --variant full|limit  which config block to take paths from: core_v4 or core_v4_limit (dev sample)
    --enrichment-dir DIR  side-output root; default from the variant
    --limit N             process at most N rows
    --test N              dry run on N rows: computes and logs results, writes nothing
    --shard I/N           only ids with id % N == I (parallel nodes)
    --entity              project | work | both   (default: both)
    --tier 0|1|all        (works only) 0 = project-linked works, 1 = org-only works; default all. Needs --entity work.
                          Tier 0 gets its own completion marker (_SUCCESS.tier0), so it is usable before tier 1 runs.
    --allow-untranslated  (text enrichments) use original text when NLLB is not complete
"""

import argparse
from dataclasses import dataclass
from typing import List

from common.config.pipelines import get_pipeline_paths
from pipelines.core_v4.enrichment.side_outputs import Shard, add_shard_arg


def add_common_args(parser: argparse.ArgumentParser, *, text: bool = True, entities: bool = True) -> None:
    parser.add_argument("--db", default=None, help="staging duckdb, opened read-only (default: config path_duck_staging)")
    parser.add_argument("--variant", choices=["full", "limit"], default="full", help="config block: core_v4 or core_v4_limit")
    parser.add_argument("--enrichment-dir", default=None, help="side-output root (default: config path_enrichment_dir)")
    parser.add_argument("--limit", type=int, default=None, help="process at most N rows")
    parser.add_argument("--test", type=int, default=None, metavar="N", help="dry run on N rows, no writes")
    add_shard_arg(parser)
    if entities:
        parser.add_argument("--entity", choices=["project", "work", "both"], default="both")
        parser.add_argument(
            "--tier",
            choices=["0", "1", "all"],
            default="all",
            help="works only: process only the project-linked (0) or the org-only (1) works (staging work.link_tier)",
        )
    if text:
        parser.add_argument(
            "--allow-untranslated",
            action="store_true",
            help="use the original text for fields NLLB has not (completely) translated",
        )


@dataclass
class Resolved:
    db: str
    enrichment_dir: str
    cache_dir: str
    shard: Shard
    limit: int | None  # --test N overrides --limit
    dry_run: bool
    entities: List[str]
    tier: int | None = None  # None = all works


def resolve(args: argparse.Namespace) -> Resolved:
    if args.variant == "limit":
        cfg = get_pipeline_paths()["core_v4_limit"]
        db, edir, cache = cfg["path_duck_staging_limit"], cfg["path_enrichment_dir_limit"], cfg["path_cache_dir_limit"]
    else:
        cfg = get_pipeline_paths()["core_v4"]
        db, edir, cache = cfg["path_duck_staging"], cfg["path_enrichment_dir"], cfg["path_cache_dir"]
    entity = getattr(args, "entity", "both")
    tier = None if getattr(args, "tier", "all") == "all" else int(args.tier)
    if tier is not None and entity != "work":
        raise SystemExit("--tier only applies to works: pass --entity work")
    return Resolved(
        db=args.db or db,
        enrichment_dir=args.enrichment_dir or edir,
        cache_dir=cache,
        shard=Shard.parse(args.shard),
        limit=args.test if args.test is not None else args.limit,
        dry_run=args.test is not None,
        entities=["project", "work"] if entity == "both" else [entity],
        tier=tier,
    )
