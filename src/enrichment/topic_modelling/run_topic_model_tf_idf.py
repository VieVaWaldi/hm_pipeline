import datetime
import logging
import pickle
from concurrent.futures import as_completed, ProcessPoolExecutor
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd
import psutil
import spacy
from pandas import DataFrame
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# TODO: core_orm_model.py was removed from enrichment_lib (no model lives outside the
# actual pipeline). Point this at the core_v4 model once it exists.
from enrichment_lib.core_orm_model import ResearchOutput, JResearchOutputTopicOA, TopicOA
from enrichment_lib.utils.batch_requester import BatchRequester
from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create
from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging

# Global variables
topic_vectors = None
vectorizer = None
topic_id_mapping = {}
max_workers = psutil.cpu_count(logical=False)
batch_size = 128

try:
    nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
except OSError as e:
    raise Exception(
        "Don't forget to download the dataset first: `python -m spacy download en_core_web_sm`."
    )


def run_topic_modelling_tfidf(batch_requester: BatchRequester, offset=0):
    """Main function to run TF-IDF based topic modeling"""
    global vectorizer, topic_vectors, topic_id_mapping

    if offset > 0:
        logging.info(f"Skipping {offset} rows.")

    # Prepare model components for worker processes
    model_components = (vectorizer, topic_vectors, topic_id_mapping)

    for idx, batch in enumerate(batch_requester.next_batch(offset_start=offset)):
        start = datetime.datetime.now()
        processed_batch = []

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    process_document_tfidf, doc, model_components
                )  # Add model_components here
                for doc in batch
            ]

            for future in as_completed(futures):
                doc_id, topic_id, score = future.result()
                processed_batch.append((doc_id, topic_id, score))

        logging.info(
            f"took {datetime.datetime.now() - start} seconds to process batch #{idx}"
        )
        upload_topics_to_db(processed_batch)


def process_document_tfidf(doc_data: Tuple[int, str], model_components: Tuple) -> Tuple[int, int, float]:
    """Process a single document using TF-IDF approach"""
    doc_id, full_text = doc_data
    text = _normalise_text(full_text)
    topic_id, score = _assign_topic_tfidf(text, model_components)  # Add model_components here
    return doc_id, topic_id, score


def _assign_topic_tfidf(text: str, model_components: Tuple) -> Tuple[int, float]:
    """Assign topic using TF-IDF cosine similarity"""
    vectorizer, topic_vectors, topic_id_mapping = model_components

    if not text.strip():
        return -1, 0.0

    # Transform the document text to TF-IDF vector
    doc_vector = vectorizer.transform([text])

    # Calculate cosine similarity with all topic vectors
    similarities = cosine_similarity(doc_vector, topic_vectors).flatten()

    # Find best matching topic
    best_idx = np.argmax(similarities)
    best_score = similarities[best_idx]
    best_topic_id = topic_id_mapping[best_idx]

    return best_topic_id, best_score


def build_tfidf_model(topics_df: DataFrame, sample_projects: List[str] = None):
    """Build TF-IDF model from topics and optionally sample projects"""
    global vectorizer, topic_vectors, topic_id_mapping

    start = datetime.datetime.now()
    logging.info("Building TF-IDF model...")

    # Prepare topic texts (keywords + summary)
    topic_texts = []
    topic_ids = []

    for idx, row in topics_df.iterrows():
        # Combine keywords and summary for richer topic representation
        topic_text = f"{row['keywords']} {row['summary']}"
        normalized_topic = _normalise_text(topic_text)
        topic_texts.append(normalized_topic)
        topic_ids.append(row['topic_id'])

    # If sample projects provided, include them in corpus for better IDF calculation
    all_texts = topic_texts.copy()
    if sample_projects:
        logging.info(f"Including {len(sample_projects)} sample projects in corpus")
        normalized_samples = [_normalise_text(text) for text in sample_projects]
        all_texts.extend(normalized_samples)

    # Build TF-IDF vectorizer
    vectorizer = TfidfVectorizer(
        max_features=10000,  # Limit vocabulary size
        min_df=2,           # Ignore terms appearing in fewer than 2 documents
        max_df=0.8,         # Ignore terms appearing in more than 80% of documents
        ngram_range=(1, 2), # Include both unigrams and bigrams
        stop_words='english'  # Additional stopword removal
    )

    # Fit vectorizer on all texts (topics + sample projects)
    vectorizer.fit(all_texts)

    # Transform only the topic texts to create topic vectors
    topic_vectors = vectorizer.transform(topic_texts)

    # Create mapping from vector index to topic ID
    topic_id_mapping = {idx: topic_id for idx, topic_id in enumerate(topic_ids)}

    logging.info(f"TF-IDF model built in {datetime.datetime.now() - start}")
    logging.info(f"Vocabulary size: {len(vectorizer.vocabulary_)}")
    logging.info(f"Topic vectors shape: {topic_vectors.shape}")

    return vectorizer, topic_vectors


def save_tfidf_model(model_path: Path):
    """Save TF-IDF model components for reuse"""
    model_data = {
        'vectorizer': vectorizer,
        'topic_vectors': topic_vectors,
        'topic_id_mapping': topic_id_mapping
    }

    with open(model_path, 'wb') as f:
        pickle.dump(model_data, f)
    logging.info(f"TF-IDF model saved to {model_path}")


def load_tfidf_model(model_path: Path):
    """Load pre-trained TF-IDF model"""
    global vectorizer, topic_vectors, topic_id_mapping

    with open(model_path, 'rb') as f:
        model_data = pickle.load(f)

    vectorizer = model_data['vectorizer']
    topic_vectors = model_data['topic_vectors']
    topic_id_mapping = model_data['topic_id_mapping']

    logging.info(f"TF-IDF model loaded from {model_path}")


def get_sample_projects(batch_requester: BatchRequester, sample_size: int = 1000) -> List[str]:
    """Get a sample of projects for building better TF-IDF model"""
    logging.info(f"Sampling {sample_size} projects for TF-IDF model...")

    sample_texts = []
    processed = 0

    for batch in batch_requester.next_batch():
        for doc_id, full_text in batch:
            if processed >= sample_size:
                break
            sample_texts.append(full_text)
            processed += 1
        if processed >= sample_size:
            break

    logging.info(f"Collected {len(sample_texts)} sample projects")
    return sample_texts


# Keep existing utility functions
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
           and len(token.text) > 2  # Filter very short tokens
    ]
    return " ".join(tokens)


def upload_topics_to_db(batch: List[Tuple[int, int, float]]):
    with create_db_session()() as session:
        junction_records = [
            JResearchOutputTopicOA(
                researchoutput_id=doc_id,  # Changed from researchoutput_id
                topic_id=topic_id,  # Changed from topic_openalex_tfidf_id
                score=score,
            )
            for doc_id, topic_id, score in batch
        ]
        session.add_all(junction_records)
        session.commit()


def load_topics(do_seed=False):
    """Load topics and build TF-IDF model"""
    start = datetime.datetime.now()
    df = pd.read_csv(get_project_root_path() / "data/topics/openalex_topic_mapping.csv")

    if do_seed:
        _seed_topics(df)

    # Check if pre-trained model exists
    model_path = get_project_root_path() / "data/models/tfidf_topic_model.pkl"

    if model_path.exists():
        logging.info("Loading existing TF-IDF model...")
        load_tfidf_model(model_path)
    else:
        logging.info("Building new TF-IDF model...")
        # Get sample projects for better IDF calculation
        sample_projects = get_sample_projects(BatchRequester(ResearchOutput), sample_size=50000)

        # Build and save model
        build_tfidf_model(df, sample_projects)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        save_tfidf_model(model_path)

    logging.info(f"took {datetime.datetime.now() - start} seconds to load the topics.")


def _seed_topics(df: DataFrame):
    # Same as original implementation
    df = df.replace({np.nan: None})
    with create_db_session()() as session:
        for idx, row in df.iterrows():
            fields = {
                "id": row["topic_id"],
                "subfield_id": row["subfield_id"],
                "field_id": row["field_id"],
                "domain_id": row["domain_id"],
                "topic_name": row["topic_name"],
                "subfield_name": row["subfield_name"],
                "field_name": row["field_name"],
                "domain_name": row["domain_name"],
                "keywords": row["keywords"],
                "summary": row["summary"],
                "wikipedia_url": row["wikipedia_url"],
            }
            get_or_create(
                session,
                TopicOA,
                unique_key={"id": row["topic_id"]},
                **fields,
            )
            if idx % 100 == 0:
                session.commit()
                session.expunge_all()
                print(f"Processed {idx} rows.")
        session.commit()


if __name__ == "__main__":
    setup_logging("enrichment-topic_modelling", "tfidf")
    logging.info(f"Starting TF-IDF topic enrichment.")
    logging.info(f"Got {max_workers} CPUs available.")
    logging.info(f"Using batch size {batch_size}.")

    load_topics(do_seed=False)
    run_topic_modelling_tfidf(BatchRequester(ResearchOutput), offset=500)