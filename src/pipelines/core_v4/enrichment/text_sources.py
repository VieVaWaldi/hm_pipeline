"""
Shared "get text for entity rows" for every text-based core_v4 enrichment
(NLLB, topics, DCH, minorities, pillars). Replaces core_v3's text_sources.py:
no OFFSET paging, one streaming Arrow cursor, and the text is the English
translation where NLLB produced one.

    text_sql(entity, fields, with_nllb=True)   -> SQL string, columns (id, full_text)
    text_batches(con, entity, fields, ...)     -> iterator of pyarrow RecordBatches

Field vocabulary (also the `field` values in the nllb side output):
    project: title, summary, acronym, keywords, subjects
    work:    title, description (descriptions[1]), subjects, container (container.name)

NLLB gate: with_nllb=True left-joins nllb/<entity>, `COALESCE(text_en, original)` per field.
It refuses to run unless nllb/<entity> and nllb/seen/<entity> both carry a `_SUCCESS`
(a half-translated corpus silently mixes languages); pass allow_untranslated=True
(CLI: --allow-untranslated) to fall back to the original text with a warning.

Ordering is not guaranteed (no ORDER BY on 50M rows); resume with `exclude_ids_sql`
(SideOutput.done_ids_sql()) instead of offsets.
"""

import logging
from pathlib import Path
from typing import Iterator, List, Literal, Optional, Sequence, Union

import duckdb
import pyarrow as pa

from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

Entity = Literal["project", "work"]

DEFAULT_BATCH_SIZE = 10_000

# Templates over the table alias {a} (the alias is `e`; lambda params must not reuse it).
_FIELD_SQL = {
    "project": {
        "title": "{a}.title",
        "summary": "{a}.summary",
        "acronym": "{a}.acronym",
        "keywords": "{a}.keywords",
        "subjects": "list_aggregate({a}.subjects, 'string_agg', ' ')",
    },
    "work": {
        "title": "{a}.title",
        "description": "{a}.descriptions[1]",
        "subjects": "list_aggregate(list_filter(list_transform({a}.subjects, x -> x.subject.value), y -> y IS NOT NULL), 'string_agg', ' ')",
        "container": "{a}.container.name",
    },
}

# Rows without any usable text are not classified (same filters as core_v3).
_ROW_FILTER = {
    "project": "e.title IS NOT NULL OR e.summary IS NOT NULL",
    "work": "e.title IS NOT NULL OR len(e.descriptions) > 0",
}

# The fields NLLB translates by default.
NLLB_FIELDS = {"project": ["title", "summary"], "work": ["title", "description"]}


class NllbNotReadyError(RuntimeError):
    pass


def field_sql(entity: Entity, field: str, alias: str = "e") -> str:
    """SQL expression for one original text field of `entity`, on table alias `alias`."""
    try:
        template = _FIELD_SQL[entity][field]
    except KeyError:
        raise ValueError(f"unknown {entity} text field {field!r}; known: {sorted(_FIELD_SQL[entity])}") from None
    return template.format(a=alias)


def nllb_outputs(enrichment_dir: Union[str, Path], entity: Entity) -> List[SideOutput]:
    return [SideOutput(enrichment_dir, "nllb", entity), SideOutput(enrichment_dir, "nllb/seen", entity)]


def _use_nllb(enrichment_dir, entity: Entity, with_nllb: bool, allow_untranslated: bool) -> bool:
    if not with_nllb:
        return False
    missing = [o for o in nllb_outputs(enrichment_dir, entity) if not o.is_complete()]
    if not missing:
        return True
    if allow_untranslated:
        logging.warning(
            f"NLLB for {entity} is not complete ({', '.join(str(o.dir) for o in missing)} has no _SUCCESS): "
            "using the original, untranslated text (--allow-untranslated)"
        )
        return False
    raise NllbNotReadyError(
        f"NLLB translations for {entity} are not complete: no _SUCCESS in "
        f"{', '.join(str(o.dir) for o in missing)}. Run the nllb step first, "
        "or pass --allow-untranslated to use the original text."
    )


def _default_enrichment_dir() -> str:
    from common.config.pipelines import get_pipeline_paths

    return get_pipeline_paths()["core_v4"]["path_enrichment_dir"]


def text_sql(
    entity: Entity,
    fields: Sequence[str],
    with_nllb: bool = True,
    *,
    enrichment_dir: Optional[Union[str, Path]] = None,
    allow_untranslated: bool = False,
    shard: Shard = Shard(),
    exclude_ids_sql: Optional[str] = None,
    limit: Optional[int] = None,
) -> str:
    """SQL producing (id, full_text) for every row of `entity` that has text: the space-joined
    `fields`, each `COALESCE(nllb.text_en, original)` when with_nllb.

    shard              only ids with id % N = I
    exclude_ids_sql    a `SELECT id ...` of already-done ids (resume): anti-joined away
    limit              plain LIMIT (no offset, no order)
    """
    if not fields:
        raise ValueError("fields must not be empty")
    enrichment_dir = enrichment_dir if enrichment_dir is not None else _default_enrichment_dir()
    originals = {f: field_sql(entity, f) for f in fields}  # also validates the names
    translated = _use_nllb(enrichment_dir, entity, with_nllb, allow_untranslated)

    cte = ""
    join = ""
    if translated:
        nllb = SideOutput(enrichment_dir, "nllb", entity)
        aggs = ", ".join(f"max(text_en) FILTER (WHERE field = '{f}') AS t_{f}" for f in fields)
        field_list = ", ".join(f"'{f}'" for f in fields)
        cte = f"WITH nllb AS (SELECT id, {aggs} FROM ({nllb.read_all_sql()}) WHERE field IN ({field_list}) GROUP BY id) "
        join = "LEFT JOIN nllb n ON n.id = e.id"
        parts = [f"COALESCE(n.t_{f}, {originals[f]})" for f in fields]
    else:
        parts = [originals[f] for f in fields]

    where = [f"({_ROW_FILTER[entity]})"]
    if shard.sql("e.id"):
        where.append(shard.sql("e.id"))
    if exclude_ids_sql:
        where.append(f"NOT EXISTS (SELECT 1 FROM ({exclude_ids_sql}) d WHERE d.id = e.id)")

    sql = (
        f"{cte}SELECT e.id, CONCAT_WS(' ', {', '.join(parts)}) AS full_text "
        f"FROM {entity} e {join} WHERE {' AND '.join(where)}"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql


def open_staging(db_path: Optional[Union[str, Path]] = None) -> duckdb.DuckDBPyConnection:
    """Read-only connection to the core_v4 staging duckdb (enrichments never write to it)."""
    if db_path is None:
        from common.config.pipelines import get_pipeline_paths

        db_path = get_pipeline_paths()["core_v4"]["path_duck_staging"]
    return duckdb.connect(str(db_path), read_only=True)


def text_batches(
    con: duckdb.DuckDBPyConnection,
    entity: Entity,
    fields: Sequence[str],
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    with_nllb: bool = True,
    enrichment_dir: Optional[Union[str, Path]] = None,
    allow_untranslated: bool = False,
    shard: Shard = Shard(),
    exclude_ids_sql: Optional[str] = None,
    limit: Optional[int] = None,
) -> Iterator[pa.RecordBatch]:
    """Streams (id, full_text) as Arrow record batches over one cursor: no OFFSET, no fetchall().
    The connection stays usable meanwhile (a private cursor is used)."""
    sql = text_sql(
        entity,
        fields,
        with_nllb,
        enrichment_dir=enrichment_dir,
        allow_untranslated=allow_untranslated,
        shard=shard,
        exclude_ids_sql=exclude_ids_sql,
        limit=limit,
    )
    cursor = con.cursor()
    try:
        yield from cursor.execute(sql).to_arrow_reader(batch_size)
    finally:
        cursor.close()
