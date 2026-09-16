import logging

import duckdb
import numpy as np
import pandas as pd

from elt.core_v3.model.core_orm_model import CREATE_TOPIC_SQL, CREATE_RELATION_TOPIC_SQL
from utils.config.config_loader import get_project_root_path, get_query_config
from utils.logger.logger import setup_logging


def seed_topics(con: duckdb.DuckDBPyConnection, df: pd.DataFrame):
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


if __name__ == "__main__":
    setup_logging("enrichment-topic_modelling", "seed_topics")
    logging.info("Seeding topics into core_v3")

    config = get_query_config()["core_v3"]
    db_path = config["path_duck"]

    con = duckdb.connect(db_path)
    con.execute(CREATE_TOPIC_SQL)
    con.execute(CREATE_RELATION_TOPIC_SQL)

    df = pd.read_csv(get_project_root_path() / "data/topics/openalex_topic_mapping.csv")
    logging.info(f"Loaded {len(df)} topics from CSV.")
    seed_topics(con, df)
    con.close()
