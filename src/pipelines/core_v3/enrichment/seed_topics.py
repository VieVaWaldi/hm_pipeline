"""
Seeds the OpenAlex topic taxonomy (topic + relation_topic tables) into
core_v3's staging duckdb. Run once before topic_modelling.py.

Usage:
    uv run python -m pipelines.core_v3.enrichment.seed_topics
"""

import logging

import duckdb
import numpy as np
import pandas as pd

from common.config.pipelines import get_pipeline_paths
from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging
from enrichment.topic_modelling.schema import CREATE_RELATION_TOPIC_SQL, CREATE_TOPIC_SQL


def seed_topics(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    df = df.replace({np.nan: None})
    records = [
        (
            int(row["topic_id"]),
            row["subfield_id"], row["field_id"], row["domain_id"],
            row["topic_name"], row["subfield_name"], row["field_name"], row["domain_name"],
            row["keywords"], row["summary"], row["wikipedia_url"],
            None, None,
        )
        for _, row in df.iterrows()
    ]
    con.executemany(
        "INSERT OR IGNORE INTO topic VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        records,
    )
    n = con.execute("SELECT count(*) FROM topic").fetchone()[0]
    logging.info(f"Topics in DB: {n}")


def main() -> None:
    setup_logging("enrichment-topic_modelling", "seed_topics")
    db_path = get_pipeline_paths()["core_v3"]["path_staging_duck"]
    logging.info(f"Seeding topics into {db_path}")

    con = duckdb.connect(db_path)
    con.execute(CREATE_TOPIC_SQL)
    con.execute(CREATE_RELATION_TOPIC_SQL)

    df = pd.read_csv(get_project_root_path() / "data/topics/openalex_topic_mapping.csv")
    logging.info(f"Loaded {len(df)} topics from CSV.")
    seed_topics(con, df)
    con.close()


if __name__ == "__main__":
    main()
