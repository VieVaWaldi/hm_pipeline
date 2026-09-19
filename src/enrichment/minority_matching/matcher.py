"""
Minority keyword matcher: Aho-Corasick over normalised text, no duckdb, no pipeline knowledge.

    groups = load_groups_from_duckdb(path)          # minorities_raw rows -> Group
    matcher = MinorityMatcher(groups, rules=load_rules())
    matcher.qids("Sámi joik and Romani music")      # ['Q48199', ...]
    matcher.find("...")                             # detailed Match objects (keyword, span, typo, strict)

Normalisation (both sides): NFKD accent stripping (plus a few letters NFKD keeps: ł ø đ ß æ),
lower-casing, apostrophe unification, whitespace collapsing.

Matching rules
  - whole word: the characters before and after a hit must not be alphanumeric;
  - typo tolerance: for keywords of >= MIN_TYPO_LEN (8) chars, every edit-distance-1 variant (delete, transpose,
    substitute, insert a-z; first letter fixed) also matches. A variant that equals an exact keyword, is a common
    English word (english_words(): offline, from spaCy's en_core_web_sm) or belongs to two different groups is
    dropped. The length floor and the word list exist because 6-7 character keywords sit one edit away from real
    words ("hazard" is a substitution away from the keyword Hazara);
  - ambiguity: keywords of <= 4 chars (unless in rules.always_match) and rules.common_words only
    match exactly capitalised ("Sami") in case-preserved text, and get no typo variants;
    rules.blocklist never matches;
  - a keyword shared by several groups yields all of their QIDs; the QID emitted is the
    group's canonical `qid` (merged_qids are the same group, used only to look groups up by any of
    their Wikidata ids in the override files).
"""

import logging
import re
import unicodedata
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, FrozenSet, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

import ahocorasick
import yaml

MIN_TYPO_LEN = 8  # normalised keywords shorter than this get no typo variants (6-7 chars: too many real words nearby)
MAX_AMBIGUOUS_LEN = 4
_ALPHABET = "abcdefghijklmnopqrstuvwxyz"
_EXTRA_TRANSLIT = str.maketrans({"ł": "l", "Ł": "L", "ø": "o", "Ø": "O", "đ": "d", "Đ": "D", "æ": "ae", "Æ": "AE"})
_APOSTROPHES = str.maketrans({"’": "'", "‘": "'", "`": "'", "´": "'", "ʼ": "'"})
_WS = re.compile(r"\s+")

RULES_PATH = Path(__file__).with_name("rules.yaml")


def strip_accents(text: str) -> str:
    text = text.translate(_EXTRA_TRANSLIT).replace("ß", "ss")
    return "".join(c for c in unicodedata.normalize("NFKD", text) if not unicodedata.combining(c))


def normalize(text: str, lower: bool = True) -> str:
    """Accent-stripped, whitespace-collapsed, optionally lower-cased (lower=False keeps case for
    the capitalised-only matching of ambiguous keywords)."""
    text = strip_accents(text.translate(_APOSTROPHES))
    if lower:
        text = text.lower()
    return _WS.sub(" ", text).strip()


@dataclass
class Group:
    qid: str
    keywords: List[str]
    name: str = ""
    merged_qids: List[str] = field(default_factory=list)


@dataclass
class Rules:
    common_words: Set[str] = field(default_factory=set)
    blocklist: Set[str] = field(default_factory=set)
    always_match: Set[str] = field(default_factory=set)  # short but distinctive: exempt from capitalised-only


@dataclass(frozen=True)
class Match:
    keyword: str  # the normalised keyword this hit belongs to (the exact form, also for typo hits)
    qids: Tuple[str, ...]
    start: int
    end: int  # exclusive; offsets into the normalised text of the matching pass
    typo: bool
    strict: bool  # matched through the capitalised-only pass


def load_rules(path: Path = RULES_PATH) -> Rules:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    return Rules(
        common_words={normalize(w) for w in raw.get("common_words") or []},
        blocklist={normalize(w) for w in raw.get("blocklist") or []},
        always_match={normalize(w) for w in raw.get("always_match") or []},
    )




@lru_cache(maxsize=1)
def english_words() -> FrozenSet[str]:
    """Common English words, offline: the lemma index and exception table of spaCy's en_core_web_sm (about 90,000 base
    forms and irregular inflections, WordNet-derived, includes place names and some proper nouns) plus spaCy's stop
    words. spaCy and en_core_web_sm are already dependencies (topic modelling), and nothing is downloaded. If the model
    is missing the stop words alone are used, and if spaCy is missing nothing (both with a warning): typo variants are
    then only filtered against the exact keywords."""
    words: Set[str] = set()
    try:
        from spacy.lang.en.stop_words import STOP_WORDS

        words.update(STOP_WORDS)
    except ImportError:
        logging.warning("spaCy is not installed: typo variants are not checked against an English word list")
        return frozenset()
    try:
        import spacy.util
        from spacy.lookups import Lookups

        lookups = Lookups().from_disk(next(spacy.util.get_package_path("en_core_web_sm").glob("*/lemmatizer/lookups")))
        for table_name in ("lemma_index", "lemma_exc"):
            for value in lookups.get_table(table_name).values():
                if isinstance(value, dict):
                    for inflected, lemmas in value.items():
                        words.add(inflected)
                        words.update(lemmas)
                else:
                    words.update(value)
    except Exception as e:  # missing model, changed spaCy layout: degrade, do not fail the run
        logging.warning(f"spaCy en_core_web_sm lemma tables unavailable ({e!r}): using only spaCy's stop words as the English word list")
    return frozenset(w.lower() for w in words if w.isalpha())


def is_common_word(word: str, words: FrozenSet[str]) -> bool:
    """Whether `word` (one normalised token) is in `words` or a plain inflection of a word in it."""
    if word in words:
        return True
    return any(base in words for base in _base_forms(word))


def _base_forms(word: str) -> List[str]:
    """Candidate base forms of a regular English inflection (plural, -ed, -ing, -ly), spelling rules respected:
    "gardens" -> garden, "boxes" -> box, "cities" -> city, "hoped" -> hope, but "gardenes" -> nothing."""
    out: List[str] = []
    if word.endswith("ies"):
        out.append(word[:-3] + "y")
    if word.endswith("es") and word[:-2].endswith(("s", "x", "z", "ch", "sh")):
        out.append(word[:-2])
    if word.endswith("s") and not word.endswith("ss"):
        out.append(word[:-1])
    if word.endswith("ed"):
        out += [word[:-2], word[:-1]]
    if word.endswith("ing"):
        out += [word[:-3], word[:-3] + "e"]
    if word.endswith("ly"):
        out.append(word[:-2])
    return out


def _variants(word: str) -> Set[str]:
    """Edit-distance-1 variants of `word`, first letter kept (typos rarely hit it)."""
    out: Set[str] = set()
    for i in range(1, len(word)):
        out.add(word[:i] + word[i + 1 :])  # deletion
        if i + 1 < len(word):
            out.add(word[:i] + word[i + 1] + word[i] + word[i + 2 :])  # transposition
        if word[i] != " ":
            for c in _ALPHABET:
                if c != word[i]:
                    out.add(word[:i] + c + word[i + 1 :])  # substitution
    for i in range(1, len(word) + 1):
        for c in _ALPHABET:
            out.add(word[:i] + c + word[i:])  # insertion
    out.discard(word)
    return {v for v in out if "  " not in v and not v.startswith(" ") and not v.endswith(" ")}


def _is_word_char(c: str) -> bool:
    return c.isalnum()


def _whole_word(text: str, start: int, end: int) -> bool:
    return (start == 0 or not _is_word_char(text[start - 1])) and (end == len(text) or not _is_word_char(text[end]))


class MinorityMatcher:
    def __init__(
        self,
        groups: Iterable[Group],
        rules: Optional[Rules] = None,
        typo: bool = True,
        known_words: Optional[Iterable[str]] = None,
    ):
        """`known_words`: the English words a typo variant must not equal (default: english_words(); pass an empty
        collection to switch the check off)."""
        self.rules = rules if rules is not None else Rules()
        self.typo = typo
        # normalised keyword -> QIDs
        exact: Dict[str, Set[str]] = {}
        for g in groups:
            for kw in [g.name, *g.keywords]:
                n = normalize(kw) if kw else ""
                if n:
                    exact.setdefault(n, set()).add(g.qid)
        self.blocked = sorted(k for k in exact if k in self.rules.blocklist)
        for k in self.blocked:
            del exact[k]
        self.keyword_qids: Dict[str, Tuple[str, ...]] = {k: tuple(sorted(v)) for k, v in exact.items()}
        self.ambiguous = {
            k
            for k in exact
            if (len(k) <= MAX_AMBIGUOUS_LEN and k not in self.rules.always_match) or k in self.rules.common_words
        }

        # pattern -> (keyword, qids, is_typo, pattern length)
        self._loose = ahocorasick.Automaton()
        self._strict = ahocorasick.Automaton()
        loose: Dict[str, tuple] = {}
        for k, qids in self.keyword_qids.items():
            if k in self.ambiguous:
                # capitalised form of the accent-stripped keyword: "sami" -> "Sami"
                cap = k[0].upper() + k[1:]
                self._strict.add_word(cap, (k, qids, False, len(cap)))
            else:
                loose[k] = (k, qids, False, len(k))
        if typo:
            words = english_words() if known_words is None else frozenset(known_words)
            variants: Dict[str, Optional[tuple]] = {}
            for k, qids in self.keyword_qids.items():
                if k in self.ambiguous or len(k) < MIN_TYPO_LEN:
                    continue
                for v in _variants(k):
                    if v in self.keyword_qids or v in self.rules.blocklist:
                        continue  # equals another exact keyword: keep that meaning
                    if is_common_word(v, words):
                        continue  # an ordinary English word one typo away from a keyword: not a typo
                    if v in variants and variants[v] is not None and variants[v][1] != qids:
                        variants[v] = None  # shared by different groups: ambiguous, drop
                    elif v not in variants:
                        variants[v] = (k, qids, True, len(v))
            for v, val in variants.items():
                if val is not None and v not in loose:
                    loose[v] = val
        for pattern, val in loose.items():
            self._loose.add_word(pattern, val)
        for auto in (self._loose, self._strict):
            if len(auto):
                auto.make_automaton()
        self.n_patterns = len(loose) + len(self._strict)

    # ---- matching ------------------------------------------------------------------------
    def find(self, text: str) -> List[Match]:
        if not text:
            return []
        out: List[Match] = []
        for auto, lower, strict in ((self._loose, True, False), (self._strict, False, True)):
            if not len(auto):
                continue
            norm = normalize(text, lower=lower)
            for end_idx, (kw, qids, is_typo, plen) in auto.iter(norm):
                end = end_idx + 1
                start = end - plen
                if _whole_word(norm, start, end):
                    out.append(Match(kw, qids, start, end, is_typo, strict))
        return out

    def qids(self, text: str) -> List[str]:
        return sorted({q for m in self.find(text) for q in m.qids})

    def qids_and_keywords(self, text: str) -> Tuple[List[str], List[str]]:
        matches = self.find(text)
        return sorted({q for m in matches for q in m.qids}), sorted({m.keyword for m in matches})



# ---- loading the group table and the override files --------------------------------------------
def load_groups(
    rows: Iterable[Tuple[str, Sequence[str], str, Sequence[str]]],
    term_overrides: Optional[Dict[str, List[str]]] = None,
    titular_majority: Optional[Set[str]] = None,
) -> List[Group]:
    """rows: (qid, merged_qids, group_name_en, search_keywords) from minorities_raw.

    term_overrides    qid -> extra terms (manual_term_overrides.csv); union-ed in, idempotent
                      with the loader, which already merged them into search_keywords
    titular_majority  QIDs flagged is_titular_majority in titular_majority_overrides.csv; a group
                      whose qid or any merged qid is in the set is dropped (the loader already
                      removes them, this guards against a stale table)
    """
    term_overrides = term_overrides or {}
    titular_majority = titular_majority or set()
    groups = []
    for qid, merged, name, keywords in rows:
        merged = list(merged or [qid])
        if titular_majority & {qid, *merged}:
            continue
        extra = [t for q in {qid, *merged} for t in term_overrides.get(q, [])]
        groups.append(Group(qid=qid, name=name or "", keywords=list(keywords or []) + extra, merged_qids=merged))
    return groups


def read_term_overrides(path: Path) -> Dict[str, List[str]]:
    import csv

    out: Dict[str, List[str]] = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            out.setdefault(row["qid"], []).append(row["term"])
    return out


def read_titular_majority(path: Path) -> Set[str]:
    import csv

    with open(path, newline="", encoding="utf-8") as f:
        return {row["qid"] for row in csv.DictReader(f) if row["is_titular_majority"].strip().lower() == "true"}


def load_groups_from_duckdb(
    db_path: Path,
    term_overrides_csv: Optional[Path] = None,
    titular_csv: Optional[Path] = None,
) -> List[Group]:
    import duckdb

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute("SELECT qid, merged_qids, group_name_en, search_keywords FROM minorities_raw").fetchall()
    finally:
        con.close()
    return load_groups(
        rows,
        read_term_overrides(term_overrides_csv) if term_overrides_csv else None,
        read_titular_majority(titular_csv) if titular_csv else None,
    )
