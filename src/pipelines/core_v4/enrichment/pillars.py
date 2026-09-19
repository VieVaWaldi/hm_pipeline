"""
Pillars: five thematic flags (inclusive, sustainable, resilient, innovative, global) per
project/work, as a UTINYINT bitmask (lowest bit first, side_outputs.PILLARS). Pure SQL:
`regexp_matches` with whole-word stems from pillars.yaml over the (English) text from
text_sources; Python only streams the Arrow batches and writes the sparse parquet part files.

Output: pillars/<entity>/part-*.parquet  id, pillars UTINYINT  (rows with pillars > 0 only)
        pillars/<entity>/_match_rates-<shard>.json            share of rows per pillar

Sparse and cheap, so no resume: every run recomputes its shard from scratch (existing parts of this
shard are replaced). The run flags any pillar matching more than `flag_above` (30%) of rows.

Usage:
    uv run python -m pipelines.core_v4.enrichment.pillars
    uv run python -m pipelines.core_v4.enrichment.pillars --entity work --shard 2/8
    uv run python -m pipelines.core_v4.enrichment.pillars --test 10000       # dry run: rates only
"""

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.compute as pc
import yaml

from common.log.logger import setup_logging
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.side_outputs import PILLARS, Shard, SideOutput
from pipelines.core_v4.enrichment.text_sources import open_staging, text_sql

CONFIG_PATH = Path(__file__).with_name("pillars.yaml")
FIELDS = {
    "project": ["title", "summary", "keywords", "subjects"],
    "work": ["title", "description", "subjects"],
}
_STEM = re.compile(r"^[a-z0-9]+(?: [a-z0-9]+)*\*?$")


def load_config(path: Path = CONFIG_PATH) -> Dict:
    cfg = yaml.safe_load(path.read_text())
    if list(cfg["pillars"]) != PILLARS:
        raise ValueError(f"pillars.yaml must list exactly {PILLARS} in this order (bit order), got {list(cfg['pillars'])}")
    return cfg


def stem_regex(stems: List[str]) -> str:
    """`inclusi*` -> word starting with inclusi; a stem without `*` is a whole word."""
    parts = []
    for stem in stems:
        if not _STEM.match(stem):
            raise ValueError(f"bad pillar stem {stem!r}: lower-case letters/digits, optional trailing *")
        parts.append(stem[:-1] + r"\w*" if stem.endswith("*") else stem + r"\b")
    return r"\b(?:" + "|".join(parts) + ")"


def pillars_sql(entity: str, cfg: Dict, text_query: str) -> str:
    """id + the five bit-flags + the OR-ed bitmask, over `text_query` (id, full_text)."""
    flags = []
    for bit, name in enumerate(PILLARS):
        regex = stem_regex(cfg["pillars"][name]).replace("'", "''")
        flags.append(f"CAST(CASE WHEN regexp_matches(lower(full_text), '{regex}') THEN {1 << bit} ELSE 0 END AS UTINYINT)")
    mask = " | ".join(flags)
    return f"SELECT id, CAST({mask} AS UTINYINT) AS pillars FROM ({text_query})"


def run_entity(
    con,
    entity: str,
    enrichment_dir: str,
    shard: Shard,
    *,
    cfg: Optional[Dict] = None,
    limit: Optional[int] = None,
    dry_run: bool = False,
    allow_untranslated: bool = False,
    batch_size: int = 100_000,
) -> Dict[str, object]:
    cfg = cfg or load_config()
    query = pillars_sql(
        entity,
        cfg,
        text_sql(
            entity,
            FIELDS[entity],
            enrichment_dir=enrichment_dir,
            allow_untranslated=allow_untranslated,
            shard=shard,
            limit=limit,
        ),
    )
    out = SideOutput(enrichment_dir, "pillars", entity, shard=shard)
    if not dry_run:
        out.begin(reset=True)

    total, any_pillar = 0, 0
    per_pillar = [0] * len(PILLARS)
    cursor = con.cursor()
    try:
        for batch in cursor.execute(query).to_arrow_reader(batch_size):
            table = pa.Table.from_batches([batch])
            masks = table.column("pillars")
            total += table.num_rows
            for bit in range(len(PILLARS)):
                per_pillar[bit] += pc.sum(pc.cast(pc.not_equal(pc.bit_wise_and(masks, 1 << bit), 0), pa.int64())).as_py() or 0
            hit = table.filter(pc.greater(masks, 0))
            any_pillar += hit.num_rows
            if not dry_run:
                out.write(hit)
    finally:
        cursor.close()

    rates = {name: (per_pillar[bit] / total if total else 0.0) for bit, name in enumerate(PILLARS)}
    logging.info(f"[{entity}] {total:,} rows, {any_pillar:,} with any pillar; match rate: " + ", ".join(f"{k} {v:.1%}" for k, v in rates.items()))
    flagged = [k for k, v in rates.items() if v > cfg["flag_above"]]
    for name in flagged:
        logging.warning(f"[{entity}] FLAG: pillar '{name}' matches {rates[name]:.1%} of rows (> {cfg['flag_above']:.0%}); review the stems in pillars.yaml")
    result = {"rows": total, "rows_any": any_pillar, "counts": dict(zip(PILLARS, per_pillar)), "rates": rates, "flagged": flagged}
    if not dry_run:
        (out.dir / f"_match_rates-{shard.index}.json").write_text(json.dumps(result, indent=1))
        if limit is None:
            out.finish()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_common_args(parser)
    parser.add_argument("--batch-size", type=int, default=100_000)
    args = parser.parse_args()
    setup_logging("pillars", "core_v4_enrichment")
    cfg = resolve(args)
    con = open_staging(cfg.db)
    for entity in cfg.entities:
        run_entity(
            con,
            entity,
            cfg.enrichment_dir,
            cfg.shard,
            limit=cfg.limit,
            dry_run=cfg.dry_run,
            allow_untranslated=args.allow_untranslated,
            batch_size=args.batch_size,
        )


if __name__ == "__main__":
    main()
