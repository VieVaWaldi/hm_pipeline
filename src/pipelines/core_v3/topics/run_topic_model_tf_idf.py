import datetime
import logging
import pickle
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Generator, List, Tuple

import duckdb
import numpy as np
import pandas as pd
import psutil
import spacy
from pandas import DataFrame
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from elt.core_v3.model.core_orm_model import CREATE_TOPIC_SQL, CREATE_RELATION_TOPIC_SQL
from utils.config.config_loader import get_project_root_path, get_query_config
from utils.logger.logger import setup_logging

# Module-level globals — set once on main process, pickled into workers
topic_vectors = None
vectorizer = None
topic_id_mapping = {}

max_workers = psutil.cpu_count(logical=False)
batch_size = 1024

try:
    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
except OSError:
    raise Exception(
        "Spacy model missing: python -m spacy download en_core_web_sm"
    )

# Text queries — CONCAT_WS ignores NULLs, so missing columns are silently skipped
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
    LIMIT {limit} OFFSET {offset}
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
    LIMIT {limit} OFFSET {offset}
"""


def batch_iter(
    con: duckdb.DuckDBPyConnection, entity: str, offset_start: int = 0
) -> Generator[List[Tuple], None, None]:
    query = _PROJECT_QUERY if entity == "project" else _WORK_QUERY
    offset = offset_start
    while True:
        rows = con.execute(query.format(limit=batch_size, offset=offset)).fetchall()
        if not rows:
            break
        yield rows
        offset += batch_size


def run_topic_modelling_tfidf(
    con: duckdb.DuckDBPyConnection, entity: str, offset: int = 0
):
    global vectorizer, topic_vectors, topic_id_mapping
    model_components = (vectorizer, topic_vectors, topic_id_mapping)

    if offset > 0:
        logging.info(f"Skipping {offset} rows.")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for idx, batch in enumerate(batch_iter(con, entity, offset_start=offset)):
            start = datetime.datetime.now()

            futures = [
                executor.submit(process_document_tfidf, doc, model_components)
                for doc in batch
            ]
            processed_batch = [future.result() for future in as_completed(futures)]

            logging.info(
                f"[{entity}] batch #{idx} ({len(batch)} docs) took {datetime.datetime.now() - start}"
            )
            upload_topics_to_db(con, entity, processed_batch)


def process_document_tfidf(
    doc_data: Tuple, model_components: Tuple
) -> Tuple[int, int, float]:
    doc_id, full_text = doc_data
    text = _normalise_text(full_text or "")
    topic_id, score = _assign_topic_tfidf(text, model_components)
    return doc_id, topic_id, score


def _assign_topic_tfidf(text: str, model_components: Tuple) -> Tuple[int, float]:
    vectorizer, topic_vectors, topic_id_mapping = model_components
    if not text.strip():
        return -1, 0.0
    doc_vector = vectorizer.transform([text])
    similarities = cosine_similarity(doc_vector, topic_vectors).flatten()
    best_idx = int(np.argmax(similarities))
    return topic_id_mapping[best_idx], float(similarities[best_idx])


def _normalise_text(text: str) -> str:
    if not text or not text.strip():
        return ""
    doc = nlp(text.lower()[:999_900])
    tokens = [
        token.lemma_
        for token in doc
        if not token.is_stop
        and not token.is_punct
        and not token.is_space
        and token.is_alpha
        and len(token.text) > 2
    ]
    return " ".join(tokens)


def build_tfidf_model(topics_df: DataFrame, sample_texts: List[str] = None):
    global vectorizer, topic_vectors, topic_id_mapping
    start = datetime.datetime.now()
    logging.info("Building TF-IDF model...")

    topic_texts = []
    topic_ids = []
    for _, row in topics_df.iterrows():
        topic_text = f"{row['keywords']} {row['summary']}"
        topic_texts.append(_normalise_text(topic_text))
        topic_ids.append(int(row["topic_id"]))

    all_texts = topic_texts.copy()
    if sample_texts:
        logging.info(f"Adding {len(sample_texts)} sample texts to corpus for IDF.")
        all_texts.extend([_normalise_text(t) for t in sample_texts])

    vectorizer = TfidfVectorizer(
        max_features=10000,
        min_df=2,
        max_df=0.8,
        ngram_range=(1, 2),
        stop_words="english",
    )
    vectorizer.fit(all_texts)
    topic_vectors = vectorizer.transform(topic_texts)
    topic_id_mapping = {i: tid for i, tid in enumerate(topic_ids)}

    logging.info(f"TF-IDF model built in {datetime.datetime.now() - start}")
    logging.info(f"Vocab size: {len(vectorizer.vocabulary_)}, topic vectors: {topic_vectors.shape}")


def save_tfidf_model(model_path: Path):
    model_data = {
        "vectorizer": vectorizer,
        "topic_vectors": topic_vectors,
        "topic_id_mapping": topic_id_mapping,
    }
    with open(model_path, "wb") as f:
        pickle.dump(model_data, f)
    logging.info(f"TF-IDF model saved to {model_path}")


def load_tfidf_model(model_path: Path):
    global vectorizer, topic_vectors, topic_id_mapping
    with open(model_path, "rb") as f:
        model_data = pickle.load(f)
    vectorizer = model_data["vectorizer"]
    topic_vectors = model_data["topic_vectors"]
    topic_id_mapping = model_data["topic_id_mapping"]
    logging.info(f"TF-IDF model loaded from {model_path}")


def get_sample_texts(
    con: duckdb.DuckDBPyConnection, sample_size: int = 50000
) -> List[str]:
    logging.info(f"Sampling {sample_size} project texts for IDF corpus...")
    rows = con.execute(
        _PROJECT_QUERY.format(limit=sample_size, offset=0)
    ).fetchall()
    return [text for _, text in rows if text]


def upload_topics_to_db(
    con: duckdb.DuckDBPyConnection, entity: str, batch: List[Tuple]
):
    records = [
        (entity, int(doc_id), int(topic_id), float(score))
        for doc_id, topic_id, score in batch
        if topic_id != -1  # skip docs with no usable text
    ]
    if records:
        con.executemany(
            "INSERT OR IGNORE INTO relation_topic (type, source_id, topic_id, score) VALUES (?, ?, ?, ?)",
            records,
        )


def load_topics_and_build_model(con: duckdb.DuckDBPyConnection):
    df = pd.read_csv(get_project_root_path() / "data/topics/openalex_topic_mapping.csv")
    model_path = get_project_root_path() / "data/models/tfidf_topic_model.pkl"

    if model_path.exists():
        logging.info("Loading existing TF-IDF model...")
        load_tfidf_model(model_path)
    else:
        logging.info("Building new TF-IDF model...")
        sample_texts = get_sample_texts(con, sample_size=50000)
        build_tfidf_model(df, sample_texts)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        save_tfidf_model(model_path)


if __name__ == "__main__":
    setup_logging("enrichment-topic_modelling", "tfidf")
    logging.info("Starting TF-IDF topic enrichment for core_v3")
    logging.info(f"CPUs available: {max_workers}, batch size: {batch_size}")

    config = get_query_config()["core_v3"]
    db_path = config["path_duck"]

    con = duckdb.connect(db_path)
    con.execute(CREATE_TOPIC_SQL)
    con.execute(CREATE_RELATION_TOPIC_SQL)
    con.execute("SET memory_limit='160GB'")
    con.execute(f"SET threads={max_workers}")

    try:
        load_topics_and_build_model(con)

        already_done = con.execute(
            "SELECT count(*) FROM relation_topic WHERE type = 'project'"
        ).fetchone()[0]
        project_offset = (already_done // batch_size) * batch_size
        if project_offset > 0:
            logging.info(f"Resuming projects from offset {project_offset} ({already_done} already classified).")
        logging.info("=== Enriching projects ===")
        run_topic_modelling_tfidf(con, "project", offset=project_offset)

        # logging.info("=== Enriching works ===")
        # run_topic_modelling_tfidf(con, "work", offset=0)
    finally:
        con.close()

    logging.info("Topic enrichment complete.")
