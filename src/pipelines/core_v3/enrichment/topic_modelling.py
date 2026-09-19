"""
Runs TF-IDF topic classification against core_v3's project/work rows and
writes results to relation_topic. Duckdb-aware glue: reads text via
text_sources.py, calls the pure TfidfTopicClassifier, writes results back.

Usage:
    uv run python -m pipelines.core_v3.enrichment.seed_topics
    uv run python -m pipelines.core_v3.enrichment.topic_modelling
    uv run python -m pipelines.core_v3.enrichment.topic_modelling --entity work
"""

import argparse
import datetime
import logging
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import List

import duckdb
import pandas as pd
import psutil

from common.config.pipelines import get_pipeline_paths
from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging
from enrichment.topic_modelling.classifier import TfidfTopicClassifier, TopicPrediction
from enrichment.topic_modelling.schema import CREATE_RELATION_TOPIC_SQL, CREATE_TOPIC_SQL
from pipelines.core_v3.enrichment.text_sources import Entity, sample_texts, text_batches

MAX_WORKERS = psutil.cpu_count(logical=False)
BATCH_SIZE = 1024


def _classify_chunk(texts: List[str], classifier: TfidfTopicClassifier) -> List[TopicPrediction]:
    return classifier.enrich(texts)


def _write_predictions(
    con: duckdb.DuckDBPyConnection, entity: Entity, ids: List[int], predictions: List[TopicPrediction]
) -> None:
    records = [
        (entity, int(doc_id), p.topic_id, p.score)
        for doc_id, p in zip(ids, predictions)
        if p.topic_id != -1  # skip docs with no usable text
    ]
    if records:
        con.executemany(
            "INSERT OR IGNORE INTO relation_topic (type, source_id, topic_id, score) VALUES (?, ?, ?, ?)",
            records,
        )


def run(con: duckdb.DuckDBPyConnection, classifier: TfidfTopicClassifier, entity: Entity, offset: int = 0) -> None:
    if offset > 0:
        logging.info(f"Skipping {offset} rows.")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for idx, batch in enumerate(text_batches(con, entity, BATCH_SIZE, offset_start=offset)):
            start = datetime.datetime.now()
            ids = [row[0] for row in batch]
            texts = [row[1] or "" for row in batch]

            chunk_size = max(1, -(-len(texts) // MAX_WORKERS))  # ceil div
            chunks = [texts[i : i + chunk_size] for i in range(0, len(texts), chunk_size)]
            futures = [executor.submit(_classify_chunk, chunk, classifier) for chunk in chunks]
            predictions = [p for future in futures for p in future.result()]

            logging.info(
                f"[{entity}] batch #{idx} ({len(batch)} docs) took {datetime.datetime.now() - start}"
            )
            _write_predictions(con, entity, ids, predictions)


def load_or_build_classifier(con: duckdb.DuckDBPyConnection, model_path: Path) -> TfidfTopicClassifier:
    if model_path.exists():
        return TfidfTopicClassifier.load(model_path)
    logging.info("Building new TF-IDF model...")
    topics_df = pd.read_csv(get_project_root_path() / "data/topics/openalex_topic_mapping.csv")
    texts = sample_texts(con, "project", sample_size=50000)
    classifier = TfidfTopicClassifier.build(topics_df, texts)
    classifier.save(model_path)
    return classifier


def main() -> None:
    parser = argparse.ArgumentParser(description="Run TF-IDF topic classification against core_v3.")
    parser.add_argument("--entity", choices=["project", "work"], default="project")
    args = parser.parse_args()

    setup_logging("enrichment-topic_modelling", "tfidf")
    db_path = get_pipeline_paths()["core_v3"]["path_staging_duck"]
    logging.info(f"Starting TF-IDF topic enrichment against {db_path}")
    logging.info(f"CPUs available: {MAX_WORKERS}, batch size: {BATCH_SIZE}")

    con = duckdb.connect(db_path)
    con.execute(CREATE_TOPIC_SQL)
    con.execute(CREATE_RELATION_TOPIC_SQL)
    con.execute("SET memory_limit='160GB'")
    con.execute(f"SET threads={MAX_WORKERS}")

    try:
        model_path = get_project_root_path() / "data/models/tfidf_topic_model.pkl"
        classifier = load_or_build_classifier(con, model_path)

        already_done = con.execute(
            f"SELECT count(*) FROM relation_topic WHERE type = '{args.entity}'"
        ).fetchone()[0]
        offset = (already_done // BATCH_SIZE) * BATCH_SIZE
        if offset > 0:
            logging.info(
                f"Resuming {args.entity}s from offset {offset} ({already_done} already classified)."
            )

        logging.info(f"=== Enriching {args.entity}s ===")
        run(con, classifier, args.entity, offset=offset)
    finally:
        con.close()

    logging.info("Topic enrichment complete.")


if __name__ == "__main__":
    main()
