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
import scipy.sparse as sp
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


def normalise_texts(texts: Iterable[str], batch_size: int = 64) -> List[str]:
    """Same result as [normalise_text(t) for t in texts], but through nlp.pipe (batched
    tokenisation/lemmatisation instead of one nlp() call per text)."""
    texts = list(texts)
    out = [""] * len(texts)
    todo = [(i, t.lower()[:999_900]) for i, t in enumerate(texts) if t and t.strip()]
    docs = _nlp.pipe((t for _, t in todo), batch_size=batch_size)
    for (i, _), doc in zip(todo, docs):
        out[i] = " ".join(
            token.lemma_
            for token in doc
            if not token.is_stop
            and not token.is_punct
            and not token.is_space
            and token.is_alpha
            and len(token.text) > 2
        )
    return out


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

    def enrich_batch(self, items: Iterable[str]) -> List[TopicPrediction]:
        """Same predictions as enrich(), faster: nlp.pipe for the text normalisation and one sparse
        matrix product per batch instead of a cosine_similarity call per text."""
        topic_ids, scores = self.classify_batch(items)
        return [TopicPrediction(topic_id=int(t), score=float(s)) for t, s in zip(topic_ids, scores)]

    def classify_batch(self, items: Iterable[str]):
        """(topic_ids int64[n], scores float32[n]); topic_id -1 / score 0 for texts with no usable content.
        Document and topic vectors are both L2-normalised by the TfidfVectorizer, so cosine similarity
        is just the sparse product X @ T.T."""
        normalised = normalise_texts(items)
        n = len(normalised)
        topic_ids = np.full(n, -1, dtype=np.int64)
        scores = np.zeros(n, dtype=np.float32)
        usable = [i for i, t in enumerate(normalised) if t.strip()]
        if not usable:
            return topic_ids, scores
        doc_vectors = self._vectorizer.transform([normalised[i] for i in usable])
        similarities = sp.csr_matrix(doc_vectors @ self._topic_vectors.T)
        best_idx = np.asarray(similarities.argmax(axis=1)).ravel()
        best_score = similarities.max(axis=1).toarray().ravel()
        mapping = np.array([self._topic_id_mapping[i] for i in range(len(self._topic_id_mapping))], dtype=np.int64)
        topic_ids[usable] = mapping[best_idx]
        scores[usable] = best_score
        return topic_ids, scores

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
