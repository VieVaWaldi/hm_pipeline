"""
TF-IDF topic classifier — pure Enricher, no duckdb or pipeline-table knowledge.

Classifies free text against a fixed OpenAlex topic taxonomy using cosine
similarity over TF-IDF vectors. Model build/save/load lives here too — it's
still just "text in, model out", no DB involved.
"""

import logging
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import numpy as np
import spacy
from pandas import DataFrame
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from enrichment.interface import Enricher

try:
    _nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])
except OSError:
    raise RuntimeError("Spacy model missing: python -m spacy download en_core_web_sm")


@dataclass
class TopicPrediction:
    topic_id: int  # -1 if the text was empty / had no usable content
    score: float


def normalise_text(text: str) -> str:
    if not text or not text.strip():
        return ""
    doc = _nlp(text.lower()[:999_900])
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


class TfidfTopicClassifier(Enricher[str, TopicPrediction]):
    """Enricher[str, TopicPrediction]. Build a fresh model with `.build()`, or
    reuse one via `.save()`/`.load()`. `.enrich(texts)` returns one prediction
    per input text, same order — safe to zip back against ids by the caller."""

    def __init__(self, vectorizer: TfidfVectorizer, topic_vectors, topic_id_mapping: dict):
        self._vectorizer = vectorizer
        self._topic_vectors = topic_vectors
        self._topic_id_mapping = topic_id_mapping

    def enrich(self, items: Iterable[str]) -> List[TopicPrediction]:
        return [self._classify_one(normalise_text(text)) for text in items]

    def _classify_one(self, text: str) -> TopicPrediction:
        if not text.strip():
            return TopicPrediction(topic_id=-1, score=0.0)
        doc_vector = self._vectorizer.transform([text])
        similarities = cosine_similarity(doc_vector, self._topic_vectors).flatten()
        best_idx = int(np.argmax(similarities))
        return TopicPrediction(
            topic_id=self._topic_id_mapping[best_idx],
            score=float(similarities[best_idx]),
        )

    @classmethod
    def build(
        cls, topics_df: DataFrame, sample_texts: Optional[List[str]] = None
    ) -> "TfidfTopicClassifier":
        """topics_df needs `topic_id`, `keywords`, `summary` columns (the
        OpenAlex topic taxonomy CSV). `sample_texts` are extra real-world
        documents mixed into the corpus purely to improve IDF weighting —
        they don't get classified themselves."""
        topic_texts, topic_ids = [], []
        for _, row in topics_df.iterrows():
            topic_texts.append(normalise_text(f"{row['keywords']} {row['summary']}"))
            topic_ids.append(int(row["topic_id"]))

        all_texts = topic_texts.copy()
        if sample_texts:
            logging.info(f"Adding {len(sample_texts)} sample texts to corpus for IDF.")
            all_texts.extend(normalise_text(t) for t in sample_texts)

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

        logging.info(
            f"Vocab size: {len(vectorizer.vocabulary_)}, topic vectors: {topic_vectors.shape}"
        )
        return cls(vectorizer, topic_vectors, topic_id_mapping)

    def save(self, model_path: Path) -> None:
        model_path.parent.mkdir(parents=True, exist_ok=True)
        with open(model_path, "wb") as f:
            pickle.dump(
                {
                    "vectorizer": self._vectorizer,
                    "topic_vectors": self._topic_vectors,
                    "topic_id_mapping": self._topic_id_mapping,
                },
                f,
            )
        logging.info(f"TF-IDF model saved to {model_path}")

    @classmethod
    def load(cls, model_path: Path) -> "TfidfTopicClassifier":
        with open(model_path, "rb") as f:
            data = pickle.load(f)
        logging.info(f"TF-IDF model loaded from {model_path}")
        return cls(data["vectorizer"], data["topic_vectors"], data["topic_id_mapping"])
