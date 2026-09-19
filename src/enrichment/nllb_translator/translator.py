"""
NLLB-200 translation to English. Pure module: takes strings and FLORES source-language codes,
returns strings. No duckdb, no pipeline knowledge.

    tr = NllbTranslator("600M", backend="ctranslate2")
    tr.translate(["Ein Satz.", ...], ["deu_Latn", ...])   # -> ["A sentence.", ...]

Pipeline per call:
  1. truncate each text (`max_chars`; descriptions can be 5.18M characters), lower-case ALL-CAPS ones,
  2. split into sentences, pack sentences into chunks of <= `max_chunk_tokens` (~200) sentencepiece
     tokens (a single longer sentence is cut on token boundaries),
  3. sort the chunks by length and translate in token-budget batches (grouped by source language by
     default; the language is a prefix token, so batches may also mix languages: `group_by_language`),
  4. re-assemble the translated chunks of each text in order.

Two backends behind the same interface: CTranslate2 (fast, float16/int8) and Hugging Face
Transformers (fallback: the cluster's GPU driver is 530 / CUDA 12.1 and CTranslate2's CUDA build
compatibility with it was not known upfront; see README for what was measured).
"""

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

from enrichment.nllb_translator.language_id import is_shouting
from enrichment.nllb_translator.download import (
    DEFAULT_QUANTIZATION,
    NLLB_REPOS,
    hf_dir,
    require_model,
    spm_path,
)

TARGET_LANG = "eng_Latn"

DEFAULT_MAX_CHUNK_TOKENS = 200
DEFAULT_MAX_CHARS = 1500  # truncation of long texts (descriptions); titles are far shorter
DEFAULT_BATCH_TOKENS = 32768  # source tokens per translate batch (8192 -> 32768: +50% tokens/s, ~10 GB VRAM for 1.3B)
DEFAULT_BEAM_SIZE = 1  # greedy: 2.2x faster than beam 2 for -2 chrF (README)

# Sentence end (Latin, Greek question mark, Arabic, CJK, Devanagari danda) followed by whitespace,
# or a CJK/danda terminator that needs no whitespace after it.
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?…;؟۔।])\s+|(?<=[。！？])")


@dataclass
class TranslationStats:
    texts: int = 0
    chunks: int = 0
    source_tokens: int = 0
    seconds: float = 0.0
    by_lang: Dict[str, int] = field(default_factory=dict)

    @property
    def tokens_per_second(self) -> float:
        return self.source_tokens / self.seconds if self.seconds else 0.0

    @property
    def chunks_per_second(self) -> float:
        return self.chunks / self.seconds if self.seconds else 0.0


def truncate_text(text: str, max_chars: int) -> str:
    """Cuts to <= max_chars, preferably at a sentence end past the halfway point, else at whitespace."""
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    ends = [m.end() for m in re.finditer(r"[.!?。！？]\s|[。！？]", cut)]
    if ends and ends[-1] >= max_chars // 2:
        return cut[: ends[-1]].rstrip()
    space = cut.rfind(" ")
    return cut[:space] if space >= max_chars // 2 else cut


def split_sentences(text: str) -> List[str]:
    return [s for s in (p.strip() for p in _SENTENCE_SPLIT.split(text)) if s]


class _Tokenizer:
    """The NLLB sentencepiece model, used for token counting and (CTranslate2) tokenisation."""

    def __init__(self, model_key: str):
        import sentencepiece as spm

        self.sp = spm.SentencePieceProcessor(model_file=str(spm_path(model_key)))

    def pieces(self, text: str) -> List[str]:
        return self.sp.encode(text, out_type=str)

    def decode(self, pieces: List[str]) -> str:
        return self.sp.decode(pieces)

    def chunk(self, text: str, max_tokens: int) -> List[List[str]]:
        """Token-piece lists of <= max_tokens pieces each, sentence-aligned where possible."""
        chunks: List[List[str]] = []
        current: List[str] = []
        for sentence in split_sentences(text):
            pieces = self.pieces(sentence)
            while len(pieces) > max_tokens:  # one huge "sentence" (no punctuation): cut on tokens
                if current:
                    chunks.append(current)
                    current = []
                chunks.append(pieces[:max_tokens])
                pieces = pieces[max_tokens:]
            if len(current) + len(pieces) > max_tokens and current:
                chunks.append(current)
                current = []
            current = current + pieces
        if current:
            chunks.append(current)
        return chunks


class _CTranslate2Backend:
    name = "ctranslate2"

    def __init__(self, model_key: str, device: str, quantization: str, beam_size: int, batch_tokens: int, inter_threads: int = 1):
        import ctranslate2

        if device == "cuda":
            _preload_cuda_libs()
        self.translator = ctranslate2.Translator(
            str(require_model(model_key, "ctranslate2", quantization)), device=device, inter_threads=inter_threads
        )
        self.beam_size = beam_size
        self.batch_tokens = batch_tokens

    def translate(self, tokenizer: _Tokenizer, batch: List[List[str]], src_langs: List[str]) -> List[str]:
        sources = [[lang, *pieces, "</s>"] for lang, pieces in zip(src_langs, batch)]
        results = self.translator.translate_batch(
            sources,
            target_prefix=[[TARGET_LANG]] * len(sources),
            beam_size=self.beam_size,
            max_batch_size=self.batch_tokens,
            batch_type="tokens",
            max_decoding_length=256,
        )
        return [tokenizer.decode(r.hypotheses[0][1:]) for r in results]  # [0] is the target-language token


class _TransformersBackend:
    name = "transformers"

    def __init__(self, model_key: str, device: str, beam_size: int, batch_tokens: int):
        import torch
        from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

        path = str(require_model(model_key, "transformers"))
        self.torch = torch
        self.device = device
        self.hf_tokenizer = AutoTokenizer.from_pretrained(path)
        self.model = AutoModelForSeq2SeqLM.from_pretrained(
            path, torch_dtype=torch.float16 if device == "cuda" else torch.float32
        ).to(device).eval()
        self.target_id = self.hf_tokenizer.convert_tokens_to_ids(TARGET_LANG)
        self.beam_size = beam_size
        self.batch_tokens = batch_tokens

    def translate(self, tokenizer: _Tokenizer, batch: List[List[str]], src_langs: List[str]) -> List[str]:
        # `batch` is already sorted longest-first by the caller; cut it into token-budget mini-batches.
        out: List[str] = []
        start = 0
        while start < len(batch):
            longest = len(batch[start]) + 2
            size = max(1, self.batch_tokens // longest)
            mini = batch[start : start + size]
            mini_langs = src_langs[start : start + size]
            start += size
            ids = []
            for lang, pieces in zip(mini_langs, mini):
                ids.append(
                    [self.hf_tokenizer.convert_tokens_to_ids(lang)]
                    + self.hf_tokenizer.convert_tokens_to_ids(pieces)
                    + [self.hf_tokenizer.eos_token_id]
                )
            width = max(len(i) for i in ids)
            pad = self.hf_tokenizer.pad_token_id
            input_ids = self.torch.tensor([i + [pad] * (width - len(i)) for i in ids], device=self.device)
            attention = (input_ids != pad).long()
            with self.torch.inference_mode():
                generated = self.model.generate(
                    input_ids=input_ids,
                    attention_mask=attention,
                    forced_bos_token_id=self.target_id,
                    num_beams=self.beam_size,
                    max_new_tokens=256,
                )
            out.extend(self.hf_tokenizer.batch_decode(generated, skip_special_tokens=True))
        return out


def _preload_cuda_libs() -> None:
    """CTranslate2's wheel does not ship the CUDA runtime; the torch wheels bring nvidia-cublas/cudnn.
    Importing torch first makes those libraries loadable by CTranslate2."""
    try:
        import torch  # noqa: F401
    except ImportError:
        pass


class NllbTranslator:
    def __init__(
        self,
        model: str = "1.3B",
        backend: str = "ctranslate2",
        device: str = "cuda",
        quantization: str = DEFAULT_QUANTIZATION,
        max_chunk_tokens: int = DEFAULT_MAX_CHUNK_TOKENS,
        max_chars: int = DEFAULT_MAX_CHARS,
        batch_tokens: int = DEFAULT_BATCH_TOKENS,
        beam_size: int = DEFAULT_BEAM_SIZE,
        group_by_language: bool = True,
        inter_threads: int = 1,
    ):
        if model not in NLLB_REPOS:
            raise ValueError(f"unknown NLLB model {model!r}; choose from {sorted(NLLB_REPOS)}")
        self.model_key = model
        self.max_chunk_tokens = max_chunk_tokens
        self.max_chars = max_chars
        self.group_by_language = group_by_language
        self.tokenizer = _Tokenizer(model)
        if backend == "ctranslate2":
            self.backend = _CTranslate2Backend(model, device, quantization, beam_size, batch_tokens, inter_threads)
        elif backend == "transformers":
            self.backend = _TransformersBackend(model, device, beam_size, batch_tokens)
        else:
            raise ValueError(f"unknown backend {backend!r}")
        self.supported_languages = self._supported_languages()
        self.stats = TranslationStats()

    def _supported_languages(self) -> set:
        """FLORES codes the model knows (LID-218 knows a few more), from the HF tokenizer config."""
        import json

        cfg = json.loads((hf_dir(self.model_key) / "special_tokens_map.json").read_text())
        return {t for t in cfg.get("additional_special_tokens", []) if re.fullmatch(r"[a-z]{3}_[A-Z][a-z]{3}", t)}

    def translate(self, texts: Sequence[str], src_langs: Sequence[str], max_chars: Optional[int] = None) -> List[str]:
        """English translations, aligned with `texts`. `src_langs` are FLORES codes (must be supported)."""
        if len(texts) != len(src_langs):
            raise ValueError("texts and src_langs must have the same length")
        start = time.perf_counter()
        limit = max_chars or self.max_chars
        # chunk index: (text idx, chunk idx within text) -> pieces, grouped by language
        per_text: List[List[List[str]]] = []
        by_lang: Dict[str, List[tuple]] = {}
        for i, (text, lang) in enumerate(zip(texts, src_langs)):
            if lang not in self.supported_languages:
                raise ValueError(f"unsupported source language {lang!r}")
            text = truncate_text(text, limit)
            if is_shouting(text):  # NLLB copies ALL-CAPS input through untranslated; lower-case it first
                text = text.lower()
            chunks = self.tokenizer.chunk(text, self.max_chunk_tokens)
            per_text.append(chunks)
            for j, pieces in enumerate(chunks):
                by_lang.setdefault(lang, []).append((i, j, pieces))

        translated: List[List[Optional[str]]] = [[None] * len(c) for c in per_text]
        n_chunks = n_tokens = 0
        # The source language is only a prefix token per sequence, so one batch may mix languages;
        # grouping by language (the default) keeps batches homogeneous, ungrouped fills them better.
        groups = list(by_lang.items()) if self.group_by_language else [("*", [it for v in by_lang.values() for it in v])]
        for _, items in groups:
            items.sort(key=lambda it: len(it[2]), reverse=True)  # longest first: minimal padding, OOM early
            outs = self.backend.translate(self.tokenizer, [it[2] for it in items], [src_langs[it[0]] for it in items])
            for (i, j, pieces), out in zip(items, outs):
                translated[i][j] = out
                n_tokens += len(pieces)
            n_chunks += len(items)
        for lang, items in by_lang.items():
            self.stats.by_lang[lang] = self.stats.by_lang.get(lang, 0) + len(items)

        self.stats.texts += len(texts)
        self.stats.chunks += n_chunks
        self.stats.source_tokens += n_tokens
        self.stats.seconds += time.perf_counter() - start
        logging.debug(f"translated {len(texts)} texts ({n_chunks} chunks) in {time.perf_counter() - start:.1f}s")
        return [" ".join(c for c in chunks if c) for chunks in translated]
