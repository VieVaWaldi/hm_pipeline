"""
Institution geolocation — pure Enricher, no duckdb or pipeline-table knowledge.

Tiers, cheapest first:
  1. Offline fuzzy name match against reference records (NameMatcher — ROR
     names/aliases in practice). Free, no network.
  2. Mapbox forward geocoding, only for queries that carry a city + country
     (Mapbox is an address geocoder — a bare company name gives junk), only
     when a request budget was granted (default 0 = never spend), and always
     with permanent=true: Mapbox forbids storing temporary-geocoding results,
     and we store these in duckdb. Permanent is billed ($5 / 1,000 requests up
     to 500k, then $4 / 1,000 — mapbox.com/pricing).

OpenAlex is deliberately not queried: its institution coordinates are GeoNames
city points reached via ROR, which the offline tier already covers.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Iterable, List, Optional

from common.requests.requests import make_get_request
from enrichment.geolocation.name_matcher import NameMatcher
from enrichment.interface import Enricher

_MAPBOX_URL = "https://api.mapbox.com/search/geocode/v6/forward"
_HIGH_CONFIDENCE_SCORE = 97


@dataclass
class InstitutionQuery:
    id: int  # caller's row id — carried through to the result, never inspected here
    name: str
    city: Optional[str] = None
    country: Optional[str] = None
    postalcode: Optional[str] = None
    street: Optional[str] = None


@dataclass
class GeolocationResult:
    id: int
    latitude: float
    longitude: float
    source: str  # "RorNameMatch" | "Mapbox"
    confidence: str  # "high" | "medium"


class GeolocationEnricher(Enricher[InstitutionQuery, GeolocationResult]):
    """Enricher[InstitutionQuery, GeolocationResult]. Skips (doesn't emit a
    result for) any query it can't resolve — match results back to input on
    `.id`, not by position."""

    def __init__(
        self,
        matcher: Optional[NameMatcher] = None,
        mapbox_token: Optional[str] = None,
        max_mapbox_requests: int = 0,
        mapbox_workers: int = 4,  # ~12 req/s, under Mapbox's default 1,000 req/min
    ):
        self._matcher = matcher
        self._mapbox_token = mapbox_token if mapbox_token is not None else os.getenv("API_KEY_MAPBOX")
        self._mapbox_requests_left = max_mapbox_requests
        self._mapbox_workers = mapbox_workers

    def enrich(self, items: Iterable[InstitutionQuery]) -> List[GeolocationResult]:
        items = list(items)
        results: List[GeolocationResult] = []

        remaining = items
        if self._matcher is not None:
            remaining = []
            matches = self._matcher.match_many([(q.name, q.country) for q in items])
            for query, match in zip(items, matches):
                if match is None:
                    remaining.append(query)
                    continue
                confidence = "high" if match.score >= _HIGH_CONFIDENCE_SCORE else "medium"
                results.append(
                    GeolocationResult(query.id, match.latitude, match.longitude, "RorNameMatch", confidence)
                )

        results.extend(self._mapbox_many(remaining))
        return results

    def _mapbox_many(self, queries: List[InstitutionQuery]) -> List[GeolocationResult]:
        if not self._mapbox_token or self._mapbox_requests_left <= 0:
            return []
        eligible = [q for q in queries if q.city and q.country][: self._mapbox_requests_left]
        self._mapbox_requests_left -= len(eligible)
        if not eligible:
            return []

        with ThreadPoolExecutor(max_workers=self._mapbox_workers) as pool:
            found = list(pool.map(self._search_mapbox, eligible))
        return [r for r in found if r is not None]

    def _search_mapbox(self, query: InstitutionQuery) -> Optional[GeolocationResult]:
        try:
            params = {
                "place": query.city,
                "country": query.country,
                "access_token": self._mapbox_token,
                "permanent": "true",
                "limit": 1,
            }
            if query.postalcode:
                params["postcode"] = query.postalcode
            if query.street:
                params["street"] = query.street

            response = make_get_request(_MAPBOX_URL, params=params, can_fail=True)
            features = (response or {}).get("features") or []
            coordinates = ((features[0].get("geometry") or {}).get("coordinates")) if features else None
            if not coordinates:
                return None
            # Mapbox returns [lon, lat]
            return GeolocationResult(query.id, coordinates[1], coordinates[0], "Mapbox", "medium")
        except Exception as e:
            logging.warning(f"Mapbox API error for {query.name!r}: {e}")
            return None
