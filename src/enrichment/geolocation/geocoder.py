"""
Institution geolocation lookup — pure Enricher, no duckdb or pipeline-table
knowledge. Given a name (+ optional city/country hint), searches OpenAlex
first, then falls back to Mapbox (only usable when city+country are both
given — plain name search isn't reliable enough there).
"""

import logging
import os
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional

from common.requests.requests import make_get_request
from enrichment.geolocation.noise_words import normalize_institution_name
from enrichment.interface import Enricher

_OPENALEX_MAILTO = "walter.ehrenberger@uni-jena.de"


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
    source: str  # "OpenAlexNormalized" | "OpenAlex" | "Mapbox"
    confidence: str  # "high" | "medium"


class GeolocationEnricher(Enricher[InstitutionQuery, GeolocationResult]):
    """Enricher[InstitutionQuery, GeolocationResult]. Skips (doesn't emit a
    result for) any query it can't resolve — callers should not assume
    positional alignment with the input, match on `.id` instead."""

    def __init__(self, mapbox_token: Optional[str] = None, request_delay_s: float = 0.1):
        self._mapbox_token = mapbox_token if mapbox_token is not None else os.getenv("API_KEY_MAPBOX")
        self._request_delay_s = request_delay_s

    def enrich(self, items: Iterable[InstitutionQuery]) -> List[GeolocationResult]:
        results = []
        for item in items:
            result = self._resolve(item)
            if result is not None:
                results.append(result)
        return results

    def _resolve(self, item: InstitutionQuery) -> Optional[GeolocationResult]:
        found = self._search_openalex(item.name, normalized=True)
        if found is None:
            found = self._search_openalex(item.name, normalized=False)
        if found is None:
            found = self._search_mapbox(item)
        if found is None:
            return None
        source, lat, lon, confidence = found
        return GeolocationResult(id=item.id, latitude=lat, longitude=lon, source=source, confidence=confidence)

    def _search_openalex(self, name: str, normalized: bool) -> Optional[tuple]:
        try:
            search_name = normalize_institution_name(name) if normalized else name
            search_name = search_name.replace("!", "").replace("|", "")

            time.sleep(self._request_delay_s)
            response = make_get_request(
                "https://api.openalex.org/institutions",
                params={"search": search_name, "select": "id,display_name,geo", "mailto": _OPENALEX_MAILTO},
                can_fail=True,
            )
            if not response:
                return None

            results = response.get("results") or []
            if not results:
                return None
            geo = results[0].get("geo") or {}
            if geo.get("latitude") is None or geo.get("longitude") is None:
                return None

            source = "OpenAlexNormalized" if normalized else "OpenAlex"
            return source, geo["latitude"], geo["longitude"], "high"
        except Exception as e:
            logging.warning(f"OpenAlex API error for {name!r}: {e}")
            return None

    def _search_mapbox(self, item: InstitutionQuery) -> Optional[tuple]:
        if not self._mapbox_token or not (item.city and item.country):
            return None

        try:
            query_params = {"place": item.city, "country": item.country, "access_token": self._mapbox_token}
            if item.postalcode:
                query_params["postcode"] = item.postalcode
            if item.street:
                query_params["street"] = item.street

            response = make_get_request(
                "https://api.mapbox.com/search/geocode/v6/forward", params=query_params, can_fail=True
            )
            if not response:
                return None

            features = response.get("features") or []
            if not features:
                return None
            geometry = features[0].get("geometry") or {}
            coordinates = geometry.get("coordinates")
            if not coordinates:
                return None

            # Mapbox returns [lon, lat]
            return "Mapbox", coordinates[1], coordinates[0], "medium"
        except Exception as e:
            logging.warning(f"Mapbox API error for {item.name!r}: {e}")
            return None
