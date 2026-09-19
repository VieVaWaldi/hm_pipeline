"""
Language identification with fastText LID-218 (facebook/fasttext-language-identification,
CC-BY-NC 4.0). Labels are FLORES-200 codes (deu_Latn, eng_Latn, ...), the same codes NLLB
uses, so a prediction feeds the translator directly.

We do NOT trust OpenAire's `language` label (33% of works are `und`, others are wrong):
every text of every row is identified. Pure module: no duckdb, no pipeline knowledge.
"""

import re
from typing import List, Optional, Sequence, Tuple

from enrichment.nllb_translator.download import require_lid

ENGLISH = "eng_Latn"

MIN_CHARS = 20  # shorter texts (acronym-like titles, "N/A") are not reliably identifiable: skipped
LID_MAX_CHARS = 1000  # only the head of a text is needed to identify its language
DEFAULT_MIN_PROB = 0.5  # translate only when P(label) >= this; see README "threshold" for the tuning

_WS = re.compile(r"\s+")


def clean_for_lid(text: Optional[str]) -> str:
    """fastText predicts one line: collapse all whitespace/newlines, cap the length. ALL-CAPS text is
    lower-cased: LID-218 labels upper-case English as yue_Hant/kor_Hang with probability ~1.0
    (measured: 3,039 of 60,000 sample titles), which would send English to the GPU as "Cantonese"."""
    if not text:
        return ""
    line = _WS.sub(" ", text[: LID_MAX_CHARS * 2]).strip()[:LID_MAX_CHARS]
    return line.lower() if is_shouting(line) else line


def is_shouting(text: str) -> bool:
    """Mostly upper-case letters (>= 8 letters, > 60% upper): ALL-CAPS titles."""
    letters = [c for c in text if c.isalpha()]
    return len(letters) >= 8 and sum(c.isupper() for c in letters) / len(letters) > 0.6


def latin_share(text: str) -> float:
    """Share of the alphabetic characters that are Latin script (Basic Latin .. Latin Extended-B)."""
    letters = [c for c in text if c.isalpha()]
    return sum(ord(c) < 0x250 for c in letters) / len(letters) if letters else 0.0


def script_consistent(label: str, text: str) -> bool:
    """A label whose script is not Latin must be backed by non-Latin letters in the text (and a Latin
    label by mostly Latin ones). Catches the LID's residual confident nonsense, e.g. English acronyms
    and Latin-script text labelled kor_Hang / yue_Hant."""
    share = latin_share(text)
    return share >= 0.5 if label.endswith("_Latn") else share <= 0.7


class LanguageIdentifier:
    def __init__(self, model_path: Optional[str] = None):
        import fasttext

        fasttext.FastText.eprint = lambda *a, **k: None  # silence the "deprecated load_model" warning
        self._model = fasttext.load_model(str(model_path or require_lid()))

    def predict(self, text: Optional[str]) -> Tuple[Optional[str], float]:
        """(FLORES label, probability), or (None, 0.0) for empty / shorter than MIN_CHARS texts."""
        line = clean_for_lid(text)
        if len(line) < MIN_CHARS:
            return None, 0.0
        # model.f.predict instead of model.predict: the python wrapper calls
        # np.array(..., copy=False), which raises under numpy >= 2.
        pairs = self._model.f.predict(line + "\n", 1, 0.0, "strict")
        if not pairs:
            return None, 0.0
        prob, label = pairs[0]
        label = label.removeprefix("__label__")
        if not script_consistent(label, line):
            return None, 0.0
        return label, min(float(prob), 1.0)

    def predict_batch(self, texts: Sequence[Optional[str]]) -> List[Tuple[Optional[str], float]]:
        return [self.predict(t) for t in texts]


def should_translate(label: Optional[str], prob: float, min_prob: float = DEFAULT_MIN_PROB) -> bool:
    return label is not None and label != ENGLISH and prob >= min_prob
