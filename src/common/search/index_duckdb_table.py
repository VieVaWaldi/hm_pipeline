import duckdb
import numpy as np
import pandas as pd
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk as opensearch_bulk

from common.search.client import get_opensearch_client

DEFAULT_BATCH_SIZE = 1000


def index_duckdb_table_opensearch(
    con: duckdb.DuckDBPyConnection,
    table: str,
    index_name: str,
    id_field: str,
    mapping: dict,
    batch_size: int = DEFAULT_BATCH_SIZE,
    replace_all: bool = False,
    client: OpenSearch | None = None,
) -> int:
    """Loads every row of `table` into an OpenSearch index, paginated.

    OpenSearch needs an explicit mapping (field types) declared before the
    first document lands, and that mapping is largely immutable once set --
    so `mapping` is a required argument here, not an index-settings call
    made separately, and replace_all=True **deletes and recreates the
    index** (mapping included) rather than just clearing documents, since a
    table's column set changing between runs would otherwise leave a stale
    mapping behind. This is the simplest fit for this repo's CREATE OR
    REPLACE TABLE full-snapshot idiom -- a bigger dataset with a
    zero-downtime requirement would want a timestamped index + alias swap
    instead, not worth it for an experimental/dev-only index.

    client defaults to this repo's own dev OpenSearch instance
    (get_opensearch_client(), config/config.yaml's opensearch.host/port).

    Returns the number of documents indexed.
    """
    client = client or get_opensearch_client()

    if replace_all and client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
    if not client.indices.exists(index=index_name):
        client.indices.create(index=index_name, body={"mappings": mapping})

    total = 0
    offset = 0
    while True:
        df = con.execute(f'SELECT * FROM "{table}" LIMIT {batch_size} OFFSET {offset}').fetchdf()
        if df.empty:
            break
        documents = _dataframe_to_documents(df)
        actions = (
            {"_index": index_name, "_id": doc[id_field], "_source": doc}
            for doc in documents
        )
        opensearch_bulk(client, actions)
        total += len(df)
        offset += batch_size
    return total


def _dataframe_to_documents(df: pd.DataFrame) -> list[dict]:
    # astype(object) first: an all-NULL column in a given batch comes back as
    # float64, and a float64 Series can only hold NaN, not None — .where()
    # alone silently leaves those as NaN, which isn't valid JSON.
    df = df.astype(object).where(pd.notnull(df), None)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(lambda v: v.isoformat() if v is not None else None)
    # Sanitize on the plain dicts, not by reassigning into a DataFrame column:
    # a column of e.g. [1.0, None] round-tripped through Series.apply() gets
    # its dtype re-inferred on assignment, and pandas upcasts back to
    # float64 — silently turning None back into NaN, undoing the line above.
    return [{k: _to_json_safe(v) for k, v in record.items()} for record in df.to_dict(orient="records")]


def _to_json_safe(value):
    # DuckDB LIST/STRUCT columns come back from fetchdf() as numpy arrays
    # (elements possibly numpy scalars, or dicts for STRUCT — themselves
    # possibly containing numpy scalars), none of which json.dumps accepts
    # directly. Recurse so an arbitrarily nested LIST(STRUCT(...)) column
    # (e.g. known_subgroups) round-trips to plain lists/dicts/scalars.
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, list):
        return [_to_json_safe(v) for v in value]
    if isinstance(value, dict):
        return {k: _to_json_safe(v) for k, v in value.items()}
    return value
