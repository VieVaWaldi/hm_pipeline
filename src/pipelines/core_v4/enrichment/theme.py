"""
Theme (Economy / Tourism) per project/work from its best topic; pure SQL over the topics side output
and the OpenAlex topic taxonomy. Definitions and --min-score default live in themes.yaml.
    <enrichment_dir>/theme/<entity>/part-<shard>-<n>.parquet     (id, theme)      sparse: matched rows only

Sparse and cheap, so it is recomputed wholesale on every run (no resume). Needs topics/<entity> to be
complete (for the tier, with --tier) unless --allow-incomplete. With --tier only the works of that link tier
are themed (staging is attached read-only to filter the topic rows and to record the staging fingerprint).

Usage:
    uv run python -m pipelines.core_v4.enrichment.theme [--min-score 0.15]
    uv run python -m pipelines.core_v4.enrichment.theme --sweep       # rows per theme at several thresholds
    uv run python -m pipelines.core_v4.enrichment.theme --test 1      # dry run: prints counts, no writes
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import duckdb
import yaml

from common.config.dumps import get_dumps_paths
from common.log.logger import setup_logging
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.fingerprint import staging_stamp
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

THEMES_FILE = Path(__file__).with_name("themes.yaml")
SWEEP_THRESHOLDS = [0.0, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5]


def load_themes(path: Path = THEMES_FILE) -> dict:
    return yaml.safe_load(path.read_text())


def _in_list(column: str, values) -> Optional[str]:
    return f"{column} IN ({', '.join(str(int(v)) for v in values)})" if values else None


def theme_case_sql(themes: dict, field="tx.field_id", subfield="tx.subfield_id", topic="b.topic_id") -> str:
    whens = []
    for name, rule in themes["themes"].items():
        parts = [_in_list(field, rule.get("fields")), _in_list(subfield, rule.get("subfields")), _in_list(topic, rule.get("topics"))]
        cond = " OR ".join(p for p in parts if p)
        if cond:
            whens.append(f"WHEN {cond} THEN '{name}'")
    return f"CASE {' '.join(whens)} END"


def theme_sql(topics_sql: str, taxonomy_csv: str, themes: dict, min_score: float, shard: Shard = Shard()) -> str:
    """(id, theme) for the rows whose best topic scores >= min_score and lies in a theme.
    `topics_sql` is a SELECT of (id, topic_id, score), e.g. SideOutput.read_all_sql()."""
    shard_sql = shard.sql("id")
    return f"""
        WITH tax AS (
            SELECT TRY_CAST(topic_id AS BIGINT) AS topic_id,
                   TRY_CAST(field_id AS BIGINT) AS field_id,
                   TRY_CAST(subfield_id AS BIGINT) AS subfield_id
            FROM read_csv('{taxonomy_csv}', header = true)
        ),
        best AS (
            SELECT id, topic_id, score FROM ({topics_sql})
            {"WHERE " + shard_sql if shard_sql else ""}
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY score DESC, topic_id) = 1
        )
        SELECT id, theme FROM (
            SELECT b.id, {theme_case_sql(themes)} AS theme
            FROM best b JOIN tax tx ON tx.topic_id = b.topic_id
            WHERE b.score >= {float(min_score)}
        ) WHERE theme IS NOT NULL
    """


def run(con, entity, enrichment_dir, taxonomy_csv, themes, min_score, *, shard=Shard(), dry_run=False,
        allow_incomplete=False, tier=None, staging_catalog=None) -> int:
    """`staging_catalog` = the alias staging is ATTACHed under on `con` (needed for `tier`; also gives the
    output its staging fingerprint)."""
    if tier is not None and not staging_catalog:
        raise ValueError("tier needs the staging catalog (to know which works are in the tier)")
    topics = SideOutput(enrichment_dir, "topics", entity)
    if not topics.is_complete(tier) and not allow_incomplete:
        raise SystemExit(f"topics/{entity} is not complete (no _SUCCESS in {topics.dir}); run topics first or pass --allow-incomplete")
    stamp = staging_stamp(con, entity, tier, staging_catalog) if staging_catalog else None
    topics_sql = topics.read_all_sql()
    if tier is not None:
        topics_sql = f"SELECT t.* FROM ({topics_sql}) t SEMI JOIN {staging_catalog}.work w ON w.id = t.id AND w.link_tier = {int(tier)}"
    sql = theme_sql(topics_sql, taxonomy_csv, themes, min_score, shard)
    table = con.sql(sql).to_arrow_table()
    con.register("theme_rows", table)
    counts = con.sql("SELECT theme, count(*) n FROM theme_rows GROUP BY 1 ORDER BY 1").fetchall()
    con.unregister("theme_rows")
    logging.info(f"[{entity}] min_score={min_score}: {dict(counts) or 'no rows'}")
    if dry_run:
        return table.num_rows
    out = SideOutput(enrichment_dir, "theme", entity, shard=shard, tier=tier)
    out.begin(reset=True)
    out.write(table)
    out.finish(stamp)
    return table.num_rows


def sweep(con, entity, enrichment_dir, taxonomy_csv, themes) -> None:
    topics = SideOutput(enrichment_dir, "topics", entity)
    total = con.sql(f"SELECT count(*) FROM ({topics.read_all_sql()})").fetchone()[0]
    print(f"[{entity}] {total:,} classified rows; rows per theme by --min-score:")
    for t in SWEEP_THRESHOLDS:
        rows = con.sql(f"SELECT theme, count(*) FROM ({theme_sql(topics.read_all_sql(), taxonomy_csv, themes, t)}) GROUP BY 1 ORDER BY 1").fetchall()
        print(f"  {t:>5}: " + ", ".join(f"{k}={v:,}" for k, v in rows))


def main() -> None:
    parser = argparse.ArgumentParser(description="Theme (Economy/Tourism) from the best topic, core_v4.")
    add_common_args(parser, text=False)
    parser.add_argument("--min-score", type=float, default=None, help="default: min_score in themes.yaml")
    parser.add_argument("--themes", default=str(THEMES_FILE))
    parser.add_argument("--taxonomy-csv", default=None, help="OpenAlex topic CSV (default: oa_topics path_raw)")
    parser.add_argument("--sweep", action="store_true", help="print rows per theme at several thresholds, write nothing")
    parser.add_argument("--allow-incomplete", action="store_true")
    args = parser.parse_args()
    r = resolve(args)

    setup_logging("enrichment-theme", "theme_v4")
    themes = load_themes(Path(args.themes))
    min_score = args.min_score if args.min_score is not None else themes["min_score"]
    taxonomy = args.taxonomy_csv or get_dumps_paths()["oa_topics"]["path_raw"]
    con = duckdb.connect()
    con.execute(f"ATTACH '{r.db}' AS stg (READ_ONLY)")
    for entity in r.entities:
        if args.sweep:
            sweep(con, entity, r.enrichment_dir, taxonomy, themes)
        else:
            run(con, entity, r.enrichment_dir, taxonomy, themes, min_score, shard=r.shard, dry_run=r.dry_run,
                allow_incomplete=args.allow_incomplete, tier=r.tier, staging_catalog="stg")


if __name__ == "__main__":
    main()
