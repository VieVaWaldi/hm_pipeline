"""
Shared "get text for entity rows" queries against core_v3's project/work
tables (schema inherited from OpenAire staging — see
src/sources/dumps/openaire/staging.py). Every text-based enrichment (topic
modelling, dch classification, and whatever's next — NLLB, keyword matching,
per the core_v4 roadmap) imports this instead of writing its own copy of the
CONCAT_WS/list_aggregate text construction.

This is pipeline-table-aware on purpose — it's the "glue" layer, not
src/enrichment/. core_v4 gets its own version of this module once it has its
own project/work tables; don't try to make this one generic across pipeline
versions via a --db-path flag, that's what src/pipelines/core_v4/enrichment/
is for later.
"""

from typing import Generator, List, Literal, Optional, Tuple

import duckdb

Entity = Literal["project", "work"]

_PROJECT_QUERY = """
    SELECT id, CONCAT_WS(' ',
        title,
        acronym,
        summary,
        keywords,
        list_aggregate(subjects, 'string_agg', ' ')
    ) AS full_text
    FROM project
    WHERE title IS NOT NULL OR summary IS NOT NULL
    ORDER BY id
    {limit_clause}
    OFFSET {offset}
"""

_WORK_QUERY = """
    SELECT id, CONCAT_WS(' ',
        title,
        descriptions[1],
        list_aggregate(
            list_filter(list_transform(subjects, s -> s.subject.value), x -> x IS NOT NULL),
            'string_agg', ' '
        ),
        container.name
    ) AS full_text
    FROM work
    WHERE title IS NOT NULL OR len(descriptions) > 0
    ORDER BY id
    {limit_clause}
    OFFSET {offset}
"""

_QUERIES = {"project": _PROJECT_QUERY, "work": _WORK_QUERY}


def _build_query(entity: Entity, offset: int, limit: Optional[int] = None) -> str:
    limit_clause = f"LIMIT {limit}" if limit is not None else ""
    return _QUERIES[entity].format(limit_clause=limit_clause, offset=offset)


def text_batches(
    con: duckdb.DuckDBPyConnection, entity: Entity, batch_size: int, offset_start: int = 0
) -> Generator[List[Tuple[int, str]], None, None]:
    """Yields (id, full_text) batches for `entity`, ordered by id, resumable via offset_start."""
    offset = offset_start
    while True:
        rows = con.execute(_build_query(entity, offset, limit=batch_size)).fetchall()
        if not rows:
            break
        yield rows
        offset += batch_size


def all_text_rows(con: duckdb.DuckDBPyConnection, entity: Entity, offset_start: int = 0) -> List[Tuple[int, str]]:
    """Loads every remaining (id, full_text) row for `entity` in one shot (no LIMIT)."""
    return con.execute(_build_query(entity, offset_start, limit=None)).fetchall()


def sample_texts(con: duckdb.DuckDBPyConnection, entity: Entity, sample_size: int) -> List[str]:
    """A sample of texts only (no ids) — e.g. for building a TF-IDF corpus."""
    rows = con.execute(_build_query(entity, offset=0, limit=sample_size)).fetchall()
    return [text for _, text in rows if text]
