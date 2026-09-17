import duckdb
import pandas as pd

from common.search.client import get_meilisearch_client

DEFAULT_BATCH_SIZE = 1000


def index_duckdb_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    index_name: str,
    primary_key: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> int:
    """Loads every row of `table` into a Meilisearch index, paginated.

    Meilisearch upserts by primary_key and creates the index on first write
    if it doesn't exist yet, so re-running this is idempotent — matches the
    rest of the pipeline's idempotence rule.

    Returns the number of documents indexed.
    """
    client = get_meilisearch_client()
    index = client.index(index_name)

    total = 0
    offset = 0
    while True:
        df = con.execute(f'SELECT * FROM "{table}" LIMIT {batch_size} OFFSET {offset}').fetchdf()
        if df.empty:
            break
        task = index.add_documents(_dataframe_to_documents(df), primary_key=primary_key)
        client.wait_for_task(task.task_uid)  # add_documents is async — block so callers can rely on "returned means indexed"
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
    return df.to_dict(orient="records")
