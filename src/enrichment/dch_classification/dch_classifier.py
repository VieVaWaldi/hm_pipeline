"""
DCH (Digital Cultural Heritage) classifier — pure Enricher, no duckdb or
pipeline-table knowledge. Runs a fine-tuned BERT model over batches of text
and returns P(is DCH) per text, same order as the input.

Architecture (unchanged from before the split):
  - Background thread: tokenises in-memory texts into CPU tensors, feeds a queue
  - Main thread: pulls from queue, runs GPU inference (bf16 AMP, eager mode)
This overlaps CPU tokenisation with GPU inference within one `enrich()` call.
Resumability across process restarts is the caller's job (see run.py) — this
class has no notion of ids or persistence, just text in, probability out.
"""

import logging
import threading
from queue import Queue
from typing import Iterable, List

import numpy as np
import torch
from torch.amp import autocast
from transformers import BertForSequenceClassification, BertTokenizerFast

from enrichment.interface import Enricher

DEFAULT_BATCH_SIZE = 2048  # sequences per GPU forward pass — eager mode (no torch.compile) needs <10GB for FFN intermediate
DEFAULT_MAX_LENGTH = 512  # BERT max tokens
DEFAULT_PREFETCH = 4  # tokenised batches buffered ahead of GPU


class DchClassifier(Enricher[str, float]):
    """Enricher[str, float]. `.enrich(texts)` returns one P(is DCH) score per
    text, same order as input — safe to zip back against ids by the caller."""

    def __init__(
        self,
        model: BertForSequenceClassification,
        tokenizer: BertTokenizerFast,
        batch_size: int = DEFAULT_BATCH_SIZE,
        max_length: int = DEFAULT_MAX_LENGTH,
        prefetch: int = DEFAULT_PREFETCH,
    ):
        self._model = model
        self._tokenizer = tokenizer
        self._batch_size = batch_size
        self._max_length = max_length
        self._prefetch = prefetch
        self._device = next(model.parameters()).device
        self._amp_dtype = (
            torch.bfloat16
            if self._device.type == "cuda" and torch.cuda.is_bf16_supported()
            else torch.float16
        )

    @classmethod
    def load(cls, model_path, **kwargs) -> "DchClassifier":
        if not torch.cuda.is_available():
            raise RuntimeError("No CUDA GPU detected. Submit this job to a GPU node.")
        device = torch.device("cuda")
        props = torch.cuda.get_device_properties(0)
        logging.info(f"GPU: {props.name}  VRAM={props.total_memory / 1e9:.1f} GB  CUDA {torch.version.cuda}")

        logging.info("Loading tokenizer (bert-base-uncased)...")
        tokenizer = BertTokenizerFast.from_pretrained("bert-base-uncased")

        logging.info(f"Loading model from {model_path} ...")
        model = BertForSequenceClassification.from_pretrained(str(model_path))
        model.to(device)
        model.eval()

        return cls(model, tokenizer, **kwargs)

    def enrich(self, items: Iterable[str]) -> List[float]:
        """Blocks until every text in `items` has been scored. Truncates each
        text to 50k chars before tokenising (matches the original BERT-input
        cap — well above MAX_LENGTH tokens, just bounds tokeniser CPU cost)."""
        texts = [(text or "")[:50_000] for text in items]
        if not texts:
            return []

        queue: Queue = Queue(maxsize=self._prefetch)
        producer = threading.Thread(target=self._tokeniser_worker, args=(texts, queue), daemon=True)
        producer.start()

        probs: List[float] = [0.0] * len(texts)
        cursor = 0
        try:
            while True:
                item = queue.get()
                if item is None:
                    break
                batch_texts, encoded = item
                batch_probs = self._infer_batch(encoded)
                probs[cursor : cursor + len(batch_texts)] = batch_probs.tolist()
                cursor += len(batch_texts)
        finally:
            producer.join()

        return probs

    def _tokeniser_worker(self, texts: List[str], queue: Queue) -> None:
        try:
            for i in range(0, len(texts), self._batch_size):
                batch = texts[i : i + self._batch_size]
                encoded = self._tokenizer(
                    batch, padding=True, truncation=True, max_length=self._max_length, return_tensors="pt"
                )
                queue.put((batch, encoded))
        finally:
            queue.put(None)

    def _infer_batch(self, encoded) -> np.ndarray:
        input_ids = encoded["input_ids"].to(self._device, non_blocking=True)
        attention_mask = encoded["attention_mask"].to(self._device, non_blocking=True)
        with torch.no_grad():
            with autocast("cuda", dtype=self._amp_dtype):
                logits = self._model(input_ids, attention_mask=attention_mask).logits
            return torch.softmax(logits.float(), dim=1)[:, 1].cpu().numpy()
