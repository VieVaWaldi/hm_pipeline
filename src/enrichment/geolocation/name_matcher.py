"""
Offline fuzzy name → coordinates matcher over reference records (e.g. ROR names
and aliases). Pure: reference data is passed in as plain records, this module
never touches duckdb or the network. Replaces what OpenAlex's institution
search used to give us — OpenAlex's own coordinates are GeoNames city points
via ROR, which we already hold locally.

Precision over recall: a wrong coordinate is worse than a null. token_sort_ratio
only (token_set_ratio gave confident nonsense on real data, e.g. matching any
name that shares a few tokens with a shorter one; WRatio's partial mode matched
character fragments like "Istemi" inside "SISTEMI"). Sub-units of a known parent
("Cairo University, Faculty of Arts, Department of ...") are recovered by also
trying the segment before the first comma/slash as its own query.
"""

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
from rapidfuzz import fuzz, process

from enrichment.geolocation.noise_words import normalize_institution_name

# Stricter without a country to narrow candidates: the whole reference set is in
# play, so more chance of a coincidental near-name match.
SCORE_CUTOFF_WITH_COUNTRY = 92
SCORE_CUTOFF_NO_COUNTRY = 96
MIN_NAME_LEN = 4  # shorter (acronyms, fragments) is too ambiguous to fuzzy-match
_ALL = "*"  # pool key holding every reference regardless of country
_QUERY_CHUNK = 64  # queries per cdist call — bounds the (queries × pool) score matrix
_HEAD_SPLIT = re.compile(r"\s*[,/;(]\s*|\s+-\s+")


@dataclass
class ReferenceLocation:
    name: str
    country: Optional[str]  # ISO 3166-1 alpha-2
    latitude: float
    longitude: float


@dataclass
class NameMatch:
    latitude: float
    longitude: float
    score: float
    matched_name: str


def _query_variants(name: str) -> List[str]:
    variants = []
    for candidate in (name, _HEAD_SPLIT.split(name, maxsplit=1)[0]):
        normalized = normalize_institution_name(candidate)
        if len(normalized) >= MIN_NAME_LEN and normalized not in variants:
            variants.append(normalized)
    return variants


class NameMatcher:
    def __init__(self, references: Iterable[ReferenceLocation]):
        self._names: Dict[str, List[str]] = defaultdict(list)
        self._locs: Dict[str, List[ReferenceLocation]] = defaultdict(list)
        for ref in references:
            normalized = normalize_institution_name(ref.name)
            if len(normalized) < MIN_NAME_LEN:
                continue
            self._names[_ALL].append(normalized)
            self._locs[_ALL].append(ref)
            if ref.country:
                country = ref.country.upper()
                self._names[country].append(normalized)
                self._locs[country].append(ref)

    def match(self, name: str, country: Optional[str] = None) -> Optional[NameMatch]:
        return self.match_many([(name, country)])[0]

    def match_many(self, queries: List[Tuple[str, Optional[str]]]) -> List[Optional[NameMatch]]:
        """One result per query, same order. Runs multi-core via rapidfuzz cdist."""
        results: List[Optional[NameMatch]] = [None] * len(queries)

        by_pool: Dict[str, List[Tuple[int, str]]] = defaultdict(list)
        for i, (name, country) in enumerate(queries):
            country = (country or "").upper()
            pool = country if country in self._names else _ALL
            for variant in _query_variants(name):
                by_pool[pool].append((i, variant))

        for pool, items in by_pool.items():
            cutoff = SCORE_CUTOFF_NO_COUNTRY if pool == _ALL else SCORE_CUTOFF_WITH_COUNTRY
            names, locs = self._names[pool], self._locs[pool]
            for start in range(0, len(items), _QUERY_CHUNK):
                chunk = items[start : start + _QUERY_CHUNK]
                scores = process.cdist(
                    [variant for _, variant in chunk],
                    names,
                    scorer=fuzz.token_sort_ratio,
                    dtype=np.uint8,  # truncates, so a 91.7 never sneaks past a cutoff of 92
                    score_cutoff=cutoff,
                    workers=-1,
                )
                best_idx = scores.argmax(axis=1)
                best = scores[np.arange(len(chunk)), best_idx]
                for (i, _), idx, score in zip(chunk, best_idx, best):
                    if score < cutoff:
                        continue
                    if results[i] is None or score > results[i].score:
                        ref = locs[idx]
                        results[i] = NameMatch(ref.latitude, ref.longitude, float(score), ref.name)
        return results
