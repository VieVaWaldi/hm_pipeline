"""
Phase 3B: builds the final core_v4 gold duckdb files from the trimmed staging duckdb plus the side
parquet outputs of the enrichments (see enrichment/README.md). Two files, so projects can be finished
and served first (their enrichment runs first, works are ~10x larger):

    --entity project  ->  path_duck_projects   project, organization, relation (project rows), topic, relation_topic
    --entity work     ->  path_duck_works      work, relation (rows where a work/product is source or target),
                                               relation_topic (type = 'work')

Serve ATTACHes both. Staging and the side outputs are read-only; the output is rebuilt from scratch on
every run: written to `<out>.tmp` and renamed at the end, so a failure never leaves a half-built file
(and never touches the previous good one).

What is applied (column contract and defaults: READ_ASSEMBLE.md):
  - text overwrite: project title/summary, work title and descriptions[1] = COALESCE(nllb text_en, original)
  - new columns on project/work: is_translated, is_ch, pred, minority_qid, pillars, theme
  - organization: region, and geolocation/geolocation_source filled from the geolocation side output
    only where staging geolocation IS NULL
  - topic (seeded from oa_topics_raw) and relation_topic (from the topics side output)

Every side output without a `_SUCCESS` blocks assembly unless named in --skip; a skipped output is not read at
all and its columns keep their defaults, so runs without the GPU steps produce the same schema.

Staging fingerprint: every `_SUCCESS` records which staging (id set) the enrichment ran against (enrichment/fingerprint.py).
A side output computed against a different staging (a rebuild with another --limit / --work-cap / dump) blocks assembly
with a message naming it, unless it is listed in --allow-stale. --allow-stale nllb keeps the cached translations: rows whose
id is still in staging get them, the others are simply not translated (the translation is not recomputed, so a text that
changed under the same id keeps its old translation).

Tiers: `--entity work --tier 0` builds a works file from the project-linked works only (work.link_tier = 0), from the
tier-0 side outputs (`_SUCCESS.tier0`), long before tier 1 is enriched; rows of tier 1 are absent (and so are their
relations). Without --tier the file holds every work and needs the complete (`_SUCCESS`) side outputs. `link_tier`
passes through to the gold work table like every staging column.

Memory: one CTAS/INSERT per table with LEFT JOINs to read_parquet; the work table (and its relation_topic)
is built in id-hash shards (--shards, default 16 for work) so a hash table only ever holds 1/N of the ids.
DuckDB gets a memory_limit, thread count and a spill directory; nothing large goes through Python.

Usage:
    uv run python -m pipelines.core_v4.assemble --entity project
    uv run python -m pipelines.core_v4.assemble --entity work --mem-mb 240000 --threads 16
    uv run python -m pipelines.core_v4.assemble --entity work --tier 0        # project-linked works only (path_duck_works_linked)
    uv run python -m pipelines.core_v4.assemble --entity project --variant limit --skip nllb,dch,topics
"""

import argparse
import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Union

import duckdb
import psutil

from common.config.pipelines import get_pipeline_paths
from common.log.logger import setup_logging
from enrichment.topic_modelling.schema import CREATE_TOPIC_SQL
from pipelines.core_v3.resources import DUCKDB_MEM_HEADROOM_MB, add_resource_args
from pipelines.core_v4.enrichment import fingerprint
from pipelines.core_v4.enrichment.cli import resolve
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

# Side-output names an entity file reads (organization-level ones only go into the project file).
ENTITY_ENRICHMENTS = ["nllb", "topics", "theme", "dch", "minorities", "pillars"]
ORG_ENRICHMENTS = ["geolocation", "regions"]
SKIPPABLE = ENTITY_ENRICHMENTS + ORG_ENRICHMENTS

DEFAULT_SHARDS = {"project": 1, "work": 16}

# Columns assemble adds on project/work; a same-named staging column is replaced.
NEW_COLUMNS = ["is_translated", "is_ch", "pred", "minority_qid", "pillars", "theme"]

# Original text replaced by the translation: (target column, nllb field, SQL for the final value).
_TEXT_OVERWRITE = {
    "project": [
        ("title", "title", "COALESCE(n.t_title, e.title)"),
        ("summary", "summary", "COALESCE(n.t_summary, e.summary)"),
    ],
    # descriptions: only element 1 is replaced, the rest stays untouched
    "work": [
        ("title", "title", "COALESCE(n.t_title, e.title)"),
        (
            "descriptions",
            "description",
            "CASE WHEN n.t_description IS NULL THEN e.descriptions "
            "ELSE list_concat([n.t_description], COALESCE(e.descriptions[2:], [])) END",
        ),
    ],
}

_WORK_TYPES = "('product', 'work')"


class AssembleError(RuntimeError):
    pass


# ---- side outputs --------------------------------------------------------------------------
def _needed(entity: str) -> List[tuple]:
    needed = [(n, entity) for n in ENTITY_ENRICHMENTS]
    if entity == "project":
        needed += [(n, "organization") for n in ORG_ENRICHMENTS]
    return needed


def _needed_outputs(entity: str, skip: Iterable[str]):
    """(enrichment name, side output name, the entity dir it lives in) for every side output the file reads."""
    skip = set(skip)
    for name, out_entity in _needed(entity):
        if name in skip:
            continue
        for n in [name, "nllb/seen"] if name == "nllb" else [name]:  # nllb/seen gates NLLB completeness too
            yield name, n, out_entity


def check_complete(enrichment_dir, entity: str, skip: Iterable[str] = (), tier: Optional[int] = None) -> None:
    """Raises AssembleError listing every side output this file needs that has no `_SUCCESS` (for `tier`: its tier
    marker is enough) and is not skipped."""
    missing = [
        str(o.dir)
        for _, n, out_entity in _needed_outputs(entity, skip)
        if not (o := SideOutput(enrichment_dir, n, out_entity)).is_complete(tier)
    ]
    if missing:
        raise AssembleError(
            "side outputs without _SUCCESS"
            + (f" for tier {tier}" if tier is not None else "")
            + " block assembly (finish them, or pass --skip <name> to build without them): "
            + ", ".join(missing)
        )


def check_current(
    con: duckdb.DuckDBPyConnection,
    enrichment_dir,
    entity: str,
    skip: Iterable[str] = (),
    allow_stale: Iterable[str] = (),
    tier: Optional[int] = None,
    catalog: str = "stg",
) -> List[str]:
    """Raises AssembleError for every needed side output whose `_SUCCESS` fingerprint is not the current staging's
    (a stale output, e.g. computed before a staging rebuild), except those named in `allow_stale`. Returns the
    names that were let through although stale. Call after check_complete."""
    allow_stale = set(allow_stale)
    expected: Dict[tuple, dict] = {}
    stale, allowed = [], []
    for name, n, out_entity in _needed_outputs(entity, skip):
        out = SideOutput(enrichment_dir, n, out_entity)
        done = out.completion(tier if out_entity == "work" else None)
        # a tier marker covers that tier only, `_SUCCESS` everything: compare against the matching staging scope
        key = (out_entity, done.tier if done.tier is not None else None)
        if key not in expected:
            expected[key] = fingerprint.staging_stamp(con, out_entity, key[1], catalog)
        if fingerprint.same(done.stamp, expected[key]):
            continue
        detail = f"{out.dir}: computed against {fingerprint.describe(done.stamp)}, staging has {fingerprint.describe(expected[key])}"
        if name in allow_stale:
            allowed.append(name)
            logging.warning(f"STALE side output used because of --allow-stale {name}: {detail}")
        else:
            stale.append(detail)
    if stale:
        raise AssembleError(
            "side outputs computed against a different staging (the staging was rebuilt after they ran): "
            + "; ".join(stale)
            + ". Rerun those enrichments, or pass --allow-stale <name> to use them anyway (rows are matched by id: ids that are no "
            "longer in staging are ignored, new ids get no result; for nllb that reuses the cached translations, "
            "which are wrong for a row whose text changed under the same id)."
        )
    return sorted(set(allowed))


class _Sides:
    """SQL for each side output: the real parts, or an empty relation with the schema when skipped."""

    def __init__(self, enrichment_dir, entity: str, skip: Iterable[str]):
        self.enrichment_dir, self.entity, self.skip = enrichment_dir, entity, set(skip)

    def sql(self, name: str, entity: Optional[str] = None) -> str:
        out = SideOutput(self.enrichment_dir, name, entity or self.entity)
        return out._empty_sql() if name in self.skip else out.read_all_sql()


def _sharded(sql: str, shard: Shard) -> str:
    pred = shard.sql("id")
    return f"SELECT * FROM ({sql}) WHERE {pred}" if pred else sql


# ---- project / work ------------------------------------------------------------------------
def entity_select_sql(entity: str, sides: _Sides, shard: Shard, staging_cols: Sequence[str]) -> str:
    """SELECT producing the final project/work rows of one id shard: staging columns (text overwritten in place,
    same column order), then the new enrichment columns. Side outputs are deduplicated per id (a rerun that left
    two parts must not multiply rows)."""
    overwrite = _TEXT_OVERWRITE[entity]
    aggs = ", ".join(f"max(text_en) FILTER (WHERE field = '{f}') AS t_{f}" for _, f, _ in overwrite)
    src = lambda name: _sharded(sides.sql(name), shard)
    ctes = [
        f"nllb AS (SELECT id, {aggs} FROM ({src('nllb')}) GROUP BY id)",
        f"dch AS (SELECT DISTINCT ON (id) id, is_ch, pred FROM ({src('dch')}))",
        f"mino AS (SELECT DISTINCT ON (id) id, minority_qid FROM ({src('minorities')}))",
        f"pil AS (SELECT DISTINCT ON (id) id, pillars FROM ({src('pillars')}))",
        f"th AS (SELECT DISTINCT ON (id) id, theme FROM ({src('theme')}))",
    ]
    exclude = [c for c in NEW_COLUMNS if c in staging_cols]
    star = "e.*" + (f" EXCLUDE ({', '.join(exclude)})" if exclude else "")
    star += " REPLACE (" + ", ".join(f"{expr} AS {col}" for col, _, expr in overwrite) + ")"
    where = f" WHERE {shard.sql('e.id')}" if shard.sql("e.id") else ""
    return (
        f"WITH {', '.join(ctes)} "
        f"SELECT {star}, "
        "(n.id IS NOT NULL) AS is_translated, "
        "d.is_ch::BOOLEAN AS is_ch, "
        "d.pred::FLOAT AS pred, "
        "COALESCE(m.minority_qid, []::VARCHAR[]) AS minority_qid, "
        "COALESCE(p.pillars, 0)::UTINYINT AS pillars, "
        "t.theme::VARCHAR AS theme "
        f"FROM src_{entity} e "
        "LEFT JOIN nllb n ON n.id = e.id "
        "LEFT JOIN dch d ON d.id = e.id "
        "LEFT JOIN mino m ON m.id = e.id "
        "LEFT JOIN pil p ON p.id = e.id "
        "LEFT JOIN th t ON t.id = e.id"
        f"{where}"
    )


def build_entity_table(con: duckdb.DuckDBPyConnection, entity: str, sides: _Sides, shards: int) -> None:
    staging_cols = [r[0] for r in con.execute(f"DESCRIBE stg.{entity}").fetchall()]
    for i in range(shards):
        sql = entity_select_sql(entity, sides, Shard(i, shards), staging_cols)
        t0 = time.time()
        con.execute(f"CREATE TABLE {entity} AS {sql}" if i == 0 else f"INSERT INTO {entity} {sql}")
        if shards > 1:
            logging.info(f"{entity}: shard {i + 1}/{shards} done ({time.time() - t0:,.0f}s)")


# ---- organization --------------------------------------------------------------------------
def organization_select_sql(sides: _Sides, staging_types: Dict[str, str]) -> str:
    """Staging organization + region, and geolocation/geolocation_source from the geolocation side output only
    where staging geolocation IS NULL. The staging geolocation column type is kept (DOUBLE[] = [lat, lon] or
    STRUCT(lat, lon)); if staging has none, a STRUCT(lat DOUBLE, lon DOUBLE) column is added."""
    has_geo = "geolocation" in staging_types
    geo_type = staging_types.get("geolocation", "STRUCT(lat DOUBLE, lon DOUBLE)")
    overlay = "g.id IS NOT NULL AND g.lat IS NOT NULL AND g.lon IS NOT NULL" + (" AND o.geolocation IS NULL" if has_geo else "")
    point = "[g.lat, g.lon]" if geo_type.endswith("]") else "{'lat': g.lat, 'lon': g.lon}"
    exprs = {
        "geolocation": f"CASE WHEN {overlay} THEN CAST({point} AS {geo_type}) " + ("ELSE o.geolocation END" if has_geo else "END"),
        "geolocation_source": f"CASE WHEN {overlay} THEN g.geolocation_source "
        + ("ELSE o.geolocation_source END" if "geolocation_source" in staging_types else "END"),
        "region": "COALESCE(r.region, " + ("o.region" if "region" in staging_types else "NULL") + ")::VARCHAR",
    }
    replace = [f"{e} AS {c}" for c, e in exprs.items() if c in staging_types]
    added = [f"{e} AS {c}" for c, e in exprs.items() if c not in staging_types]
    star = "o.*" + (f" REPLACE ({', '.join(replace)})" if replace else "")
    return (
        "WITH geo AS (SELECT DISTINCT ON (id) id, lat, lon, geolocation_source "
        f"FROM ({sides.sql('geolocation', 'organization')})), "
        f"reg AS (SELECT DISTINCT ON (id) id, region FROM ({sides.sql('regions', 'organization')})) "
        f"SELECT {', '.join([star] + added)} "
        "FROM stg.organization o LEFT JOIN geo g ON g.id = o.id LEFT JOIN reg r ON r.id = o.id"
    )


def build_organization(con: duckdb.DuckDBPyConnection, sides: _Sides) -> None:
    types = {r[0]: r[1] for r in con.execute("DESCRIBE stg.organization").fetchall()}
    con.execute(f"CREATE TABLE organization AS {organization_select_sql(sides, types)}")


# ---- relation ------------------------------------------------------------------------------
def define_sources(con: duckdb.DuckDBPyConnection, entity: str, tier: Optional[int]) -> None:
    """Temp view `src_<entity>`: the staging rows this file is built from (works of one link tier with --tier)."""
    where = f" WHERE link_tier = {int(tier)}" if tier is not None else ""
    con.execute(f"CREATE TEMP VIEW src_{entity} AS SELECT * FROM stg.{entity}{where}")


def build_relation(con: duckdb.DuckDBPyConnection, entity: str, tier: Optional[int] = None) -> None:
    """Straight streaming copy. A relation row belongs to the works file when a work/product is on either side
    (product hasAuthorInstitution organization, project produces product), else to the projects file.
    With a tier, only rows whose work endpoint is in the tier's works (no relation may point at an absent work)."""
    is_work = f"(COALESCE(sourceType IN {_WORK_TYPES}, false) OR COALESCE(targetType IN {_WORK_TYPES}, false))"
    where = is_work if entity == "work" else f"NOT {is_work}"
    if tier is not None:
        where += (
            f" AND ((COALESCE(sourceType IN {_WORK_TYPES}, false) AND source IN (SELECT id FROM src_work))"
            f" OR (COALESCE(targetType IN {_WORK_TYPES}, false) AND target IN (SELECT id FROM src_work)))"
        )
    con.execute(f"CREATE TABLE relation AS SELECT * FROM stg.relation WHERE {where}")


# ---- topic / relation_topic ----------------------------------------------------------------
def build_topic(con: duckdb.DuckDBPyConnection, created_at: str) -> None:
    """topic seeded from oa_topics_raw (attached as `oa`), in the shape of enrichment/topic_modelling/schema.py."""
    con.execute(CREATE_TOPIC_SQL)
    con.execute(
        "INSERT INTO topic "
        "SELECT topic_id::INTEGER, subfield_id::VARCHAR, field_id::VARCHAR, domain_id::VARCHAR, topic_name, "
        "subfield_name, field_name, domain_name, keywords::VARCHAR, summary, wikipedia_url, "
        f"TIMESTAMP '{created_at}', NULL "
        "FROM oa.oa_topics_raw QUALIFY row_number() OVER (PARTITION BY topic_id) = 1"
    )


# Same columns as schema.py's relation_topic. The (type, source_id, topic_id) primary key is left out on
# purpose: the INSERT below already yields one row per key, and an ART index over 50M work rows is what
# would blow the memory budget.
CREATE_RELATION_TOPIC_SQL = """
    CREATE TABLE relation_topic (
        type       TEXT    NOT NULL,   -- "project" or "work"
        source_id  UBIGINT NOT NULL,
        topic_id   INTEGER NOT NULL,
        score      FLOAT,
        created_at TIMESTAMP
    )
"""


def build_relation_topic(con: duckdb.DuckDBPyConnection, entity: str, sides: _Sides, shards: int, created_at: str) -> None:
    """One row per (id, topic_id) of this entity; ids that are not in staging (stale side output) are dropped."""
    con.execute(CREATE_RELATION_TOPIC_SQL)
    for i in range(shards):
        topics = _sharded(sides.sql("topics"), Shard(i, shards))
        con.execute(
            f"INSERT INTO relation_topic SELECT '{entity}', t.id, t.topic_id, t.score::FLOAT, TIMESTAMP '{created_at}' "
            f"FROM (SELECT DISTINCT ON (id, topic_id) id, topic_id, score FROM ({topics})) t "
            f"SEMI JOIN src_{entity} e ON e.id = t.id"
        )


# ---- logging / sanity ----------------------------------------------------------------------
def _pct(n: int, total: int) -> str:
    return f"{n:,} ({100 * n / total:.1f}%)" if total else f"{n:,}"


def log_entity_stats(con: duckdb.DuckDBPyConnection, entity: str) -> Dict[str, int]:
    row = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE is_translated), count(pred), count(*) FILTER (WHERE is_ch), "
        "count(*) FILTER (WHERE len(minority_qid) > 0), count(*) FILTER (WHERE pillars <> 0), count(theme) "
        f"FROM {entity}"
    ).fetchone()
    stats = dict(zip(["rows", "translated", "dch", "dch_is_ch", "minorities", "pillars", "theme"], row))
    n = stats["rows"]
    logging.info(
        f"{entity}: {n:,} rows | translated {_pct(stats['translated'], n)} | dch {_pct(stats['dch'], n)} "
        f"(is_ch {stats['dch_is_ch']:,}) | minorities {_pct(stats['minorities'], n)} | "
        f"pillars != 0 {_pct(stats['pillars'], n)} | theme {_pct(stats['theme'], n)}"
    )
    return stats


def log_organization_stats(con: duckdb.DuckDBPyConnection, sides: _Sides) -> Dict[str, int]:
    types = {r[0]: r[1] for r in con.execute("DESCRIBE stg.organization").fetchall()}
    unset = "AND o.geolocation IS NULL" if "geolocation" in types else ""
    overlaid = con.execute(
        "SELECT count(*) FROM stg.organization o JOIN "
        f"(SELECT DISTINCT id FROM ({sides.sql('geolocation', 'organization')}) WHERE lat IS NOT NULL AND lon IS NOT NULL) g "
        f"ON g.id = o.id WHERE true {unset}"
    ).fetchone()[0]
    n, with_region = con.execute("SELECT count(*), count(region) FROM organization").fetchone()
    logging.info(f"organization: {n:,} rows | region {_pct(with_region, n)} | geolocation overlay {_pct(overlaid, n)}")
    return {"rows": n, "region": with_region, "geolocation_overlay": overlaid}


# ---- run -----------------------------------------------------------------------------------
def _sql_str(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _apply_limits(con: duckdb.DuckDBPyConnection, mem_mb: int, threads: int, spill_dir: Path) -> None:
    # apply_duckdb_limits' headroom, but never below half the budget so a small dev --mem-mb still works
    limit_mb = max(mem_mb - DUCKDB_MEM_HEADROOM_MB, mem_mb // 2)
    con.execute(f"SET memory_limit='{limit_mb}MB'")
    con.execute(f"SET threads={int(threads)}")
    con.execute(f"SET temp_directory={_sql_str(spill_dir)}")
    con.execute("SET preserve_insertion_order=false")  # lower memory for the big CTAS/INSERTs; row order is meaningless here


def _discard(*paths: Path) -> None:
    for p in paths:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink(missing_ok=True)


def assemble(
    entity: str,
    staging_db: Union[str, Path],
    enrichment_dir: Union[str, Path],
    out: Union[str, Path],
    *,
    skip: Iterable[str] = (),
    mem_mb: int = 200_000,
    threads: int = 4,
    shards: Optional[int] = None,
    oa_topics_db: Optional[Union[str, Path]] = None,
    tmp_dir: Optional[Union[str, Path]] = None,
    tier: Optional[int] = None,
    allow_stale: Iterable[str] = (),
) -> Dict[str, Dict[str, int]]:
    """Builds the gold duckdb for `entity` ('project' or 'work') at `out`. Returns the logged stats.
    `tier=0` (works only) builds it from the project-linked works only; `allow_stale` names side outputs whose
    staging fingerprint may differ from the current staging (see the module docstring)."""
    if entity not in DEFAULT_SHARDS:
        raise AssembleError(f"entity must be project or work, not {entity!r}")
    if tier is not None and (entity != "work" or tier != 0):
        raise AssembleError("--tier 0 is the only tier subset assemble builds, and only for --entity work")
    skip = set(skip)
    allow_stale = set(allow_stale)
    unknown = (skip | allow_stale) - set(SKIPPABLE)
    if unknown:
        raise AssembleError(f"unknown --skip / --allow-stale name(s) {sorted(unknown)}; known: {SKIPPABLE}")
    shards = shards or DEFAULT_SHARDS[entity]
    staging_db, out = Path(staging_db), Path(out)
    if not staging_db.exists():
        raise AssembleError(f"staging duckdb not found: {staging_db}")
    check_complete(enrichment_dir, entity, skip, tier)
    if entity == "project":
        if oa_topics_db is None:
            from common.config.dumps import get_dumps_paths

            oa_topics_db = get_dumps_paths()["oa_topics"]["path_duck"]
        if not Path(oa_topics_db).exists():
            raise AssembleError(f"oa_topics_raw duckdb not found: {oa_topics_db} (--oa-topics-db)")

    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + ".tmp")
    tmp_wal = out.with_name(out.name + ".tmp.wal")
    spill = Path(tmp_dir) if tmp_dir else out.with_name(out.name + ".spill")
    _discard(tmp, tmp_wal, spill)  # leftovers of a killed run
    spill.mkdir(parents=True)

    sides = _Sides(enrichment_dir, entity, skip)
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    stats: Dict[str, Dict[str, int]] = {}
    con = None
    t0 = time.time()
    try:
        logging.info(
            f"assemble {entity}: staging {staging_db}, side outputs {enrichment_dir}, out {out} (skipping: {sorted(skip) or 'nothing'})"
        )
        con = duckdb.connect(str(tmp))
        _apply_limits(con, mem_mb, threads, spill)
        con.execute(f"ATTACH {_sql_str(staging_db)} AS stg (READ_ONLY)")
        if tier is not None and "link_tier" not in {r[0] for r in con.execute("DESCRIBE stg.work").fetchall()}:
            raise AssembleError("staging has no work.link_tier column: rebuild it with the current transformation to use --tier")
        check_current(con, enrichment_dir, entity, skip, allow_stale, tier)
        define_sources(con, entity, tier)

        if entity == "project":
            con.execute(f"ATTACH {_sql_str(oa_topics_db)} AS oa (READ_ONLY)")
            build_organization(con, sides)
            stats["organization"] = log_organization_stats(con, sides)
            org_rows = con.execute("SELECT count(*) FROM stg.organization").fetchone()[0]
            if stats["organization"]["rows"] != org_rows:
                raise AssembleError(f"organization: {stats['organization']['rows']:,} rows assembled but staging has {org_rows:,}")
        build_entity_table(con, entity, sides, shards)
        stats[entity] = log_entity_stats(con, entity)
        staging_rows = con.execute(f"SELECT count(*) FROM src_{entity}").fetchone()[0]
        if stats[entity]["rows"] != staging_rows:  # a duplicated side-output key would show up here
            raise AssembleError(f"{entity}: {stats[entity]['rows']:,} rows assembled but staging has {staging_rows:,}")

        build_relation(con, entity, tier)
        if entity == "project":
            build_topic(con, created_at)
        build_relation_topic(con, entity, sides, shards, created_at)

        tables = ["relation", "relation_topic"] + (["topic", "organization", "project"] if entity == "project" else ["work"])
        for table in tables:
            stats.setdefault(table, {})["rows"] = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            logging.info(f"{table}: {stats[table]['rows']:,} rows")
        if entity == "project":
            orphans = con.execute("SELECT count(*) FROM relation_topic WHERE topic_id NOT IN (SELECT id FROM topic)").fetchone()[0]
            if orphans:
                logging.warning(f"relation_topic: {orphans:,} rows point at a topic_id missing from topic")

        con.execute("DETACH stg")
        con.execute("CHECKPOINT")
        con.close()
        con = None
        out.with_name(out.name + ".wal").unlink(missing_ok=True)  # a stale wal must never be replayed onto the new file
        os.replace(tmp, out)
        logging.info(f"assemble {entity}: wrote {out} ({out.stat().st_size / 1e6:,.1f} MB, {time.time() - t0:,.0f}s)")
        return stats
    except BaseException:
        if con is not None:
            try:
                con.close()
            except Exception:
                pass
        _discard(tmp, tmp_wal)
        raise
    finally:
        _discard(spill)


# ---- cli -----------------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Assemble the core_v4 gold duckdb (projects or works) from staging + side outputs.")
    parser.add_argument("--entity", choices=["project", "work"], required=True)
    parser.add_argument("--variant", choices=["full", "limit"], default="full", help="config block: core_v4 or core_v4_limit")
    parser.add_argument("--staging-db", default=None, help="staging duckdb, read-only (default: config path_duck_staging)")
    parser.add_argument("--enrichment-dir", default=None, help="side-output root (default: config path_enrichment_dir)")
    parser.add_argument("--out", default=None, help="gold duckdb to write (default: config path_duck_projects / path_duck_works)")
    parser.add_argument("--tier", choices=["0", "all"], default="all", help="work only: 0 = build from the project-linked works only (needs the tier-0 side outputs)")
    parser.add_argument("--allow-stale", default="", help="comma-separated side outputs to use even though they were computed against a different staging (e.g. nllb: reuse cached translations)")
    parser.add_argument("--skip", default="", help=f"comma-separated side outputs to leave out (defaults stay): {', '.join(SKIPPABLE)}")
    parser.add_argument("--shards", type=int, default=None, help=f"id-hash shards per big table (default: {DEFAULT_SHARDS})")
    parser.add_argument("--oa-topics-db", default=None, help="oa_topics_raw duckdb (default: config oa_topics path_duck)")
    parser.add_argument("--tmp-dir", default=None, help="DuckDB spill directory (default: <out>.spill, removed at the end)")
    add_resource_args(parser, default_threads=psutil.cpu_count(logical=False) or 1)
    return parser


def resolve_paths(args: argparse.Namespace) -> tuple:
    """(staging_db, enrichment_dir, out) from the flags, falling back to the variant's config block."""
    staging_db, enrichment_dir, out = args.staging_db, args.enrichment_dir, args.out
    if not (staging_db and enrichment_dir and out):
        r = resolve(
            argparse.Namespace(
                db=args.staging_db, variant=args.variant, enrichment_dir=args.enrichment_dir, shard="0/1",
                limit=None, test=None, entity=args.entity, tier="all",
            )
        )
        staging_db, enrichment_dir = r.db, r.enrichment_dir
        if not out:
            block = "core_v4_limit" if args.variant == "limit" else "core_v4"
            key = f"path_duck_{args.entity}s" + ("_linked" if getattr(args, "tier", "all") == "0" else "") + (
                "_limit" if args.variant == "limit" else ""
            )
            out = get_pipeline_paths()[block].get(key)
            if not out:
                raise AssembleError(f"no --out given and config/pipelines.yaml {block} has no {key}")
    return staging_db, enrichment_dir, out


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    setup_logging("core_v4", "assemble")
    try:
        staging_db, enrichment_dir, out = resolve_paths(args)
        assemble(
            args.entity, staging_db, enrichment_dir, out,
            skip=[s.strip() for s in args.skip.split(",") if s.strip()],
            mem_mb=args.mem_mb, threads=args.threads, shards=args.shards,
            oa_topics_db=args.oa_topics_db, tmp_dir=args.tmp_dir,
            tier=None if args.tier == "all" else int(args.tier),
            allow_stale=[s.strip() for s in args.allow_stale.split(",") if s.strip()],
        )
    except AssembleError as e:
        raise SystemExit(f"assemble: {e}") from None


if __name__ == "__main__":
    main()
