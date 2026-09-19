"""
Institution geolocation — pure Enricher, no pipeline-table knowledge.

Mapbox Geocoding v6 *batch* endpoint (POST /search/geocode/v6/batch, up to 1,000 queries per
request). Design, all of it driven by cost (permanent geocoding is $5 / 1,000 queries, $4 above
500k a month):

  * Only queries with a real address (street + city + country) are ever sent; a name alone never
    is (Mapbox is an address geocoder, a bare institution name gives junk).
  * Queries are structured fields (address_number, street, postcode, place, country), not a
    free-text `q`.
  * `permanent` is an option (default False = temporary geocoding, free tier 100k/month; True = permanent,
    $5/1,000, results may be stored). It is a URL parameter of the batch endpoint. Temporary results
    are stored anyway for now; every cache row records which mode fetched it (`permanent` column,
    and `source` = "mapbox_temporary" / "mapbox"), and `refresh_temporary=True` (with permanent=True)
    re-requests exactly the cached temporary *positive* rows for the later permanent backfill.
    There is no `fuzzyMatch` in v6 (removed from the API): approximate matching is always on, and its
    quality is reported as `properties.match_code.confidence` (exact > high > medium > low). Only
    medium-or-better is accepted; a low match is stored in the cache as a negative so it is never paid
    for twice. Mapbox omits match_code for street-only results (feature_type "street"): those are
    accepted, with confidence "street", when the returned place equals any part of the queried city
    ("LOUVAIN / LEUVEN" is split on / ; , and spaced dashes) or the returned postcode equals the queried one.
  * A persistent duckdb cache (data/cache/mapbox.duckdb) is checked before every request, negative
    results included, and written right after each HTTP call so a crash never loses paid results.
  * Identical addresses inside a run are sent once. `max_requests` (default 0 = never spend) caps
    billed queries; each query in a batch counts as one billed request.
  * 429 / 5xx / connection errors are retried with exponential backoff (honouring Retry-After).

OpenAlex is deliberately not queried: its institution coordinates are GeoNames city points
reached via ROR, which staging already carries.
"""

import hashlib
import logging
import os
import re
import time
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

import duckdb

from enrichment.interface import Enricher

MAPBOX_BATCH_URL = "https://api.mapbox.com/search/geocode/v6/batch"
MAX_BATCH_SIZE = 1000
# Default 1,000 requests/minute for the Geocoding API; each query in a batch counts, so this
# is enforced per query, not per HTTP call (docs are ambiguous, the conservative reading is used).
DEFAULT_RATE_LIMIT_PER_MIN = 1000

ACCEPTED_CONFIDENCE = ("exact", "high", "medium")  # best first; "low" is rejected
STREET_CONFIDENCE = "street"  # stored for street/address features without match_code whose place matches the query
STREET_FEATURE_TYPES = ("street", "address")
PLACE_MISMATCH = "street_place_mismatch"  # cached negative: a street result in another city
PRICE_PER_1000_USD = 5.0
PRICE_PER_1000_USD_ABOVE_500K = 4.0
# Temporary geocoding: 100k free per month, then tiered per 1,000 requests.
TEMP_TIERS = [(100_000, 0.0), (500_000, 0.75), (1_000_000, 0.60), (float("inf"), 0.45)]
SOURCE_PERMANENT = "mapbox"
SOURCE_TEMPORARY = "mapbox_temporary"


class MapboxError(RuntimeError):
    pass


@dataclass
class InstitutionQuery:
    id: int  # caller's row id: carried through to the result, never inspected here
    street: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None  # ISO alpha-2, already normalised by the caller
    postalcode: Optional[str] = None
    name: Optional[str] = None  # for log messages only, never sent


@dataclass
class GeolocationResult:
    id: int
    latitude: float
    longitude: float
    source: str  # "mapbox" (permanent) | "mapbox_temporary"
    confidence: str  # exact | high | medium | street (street-only match, city verified)


@dataclass
class Estimate:
    eligible: int  # queries that carry a real address
    unique: int  # distinct addresses among them
    cached: int  # of which already in the cache (positive or negative) and staying there: free
    to_request: int  # distinct addresses that would really be billed (incl. temporary rows a refresh replaces)
    cost_usd: float


@dataclass
class Stats:
    requests: int = 0  # billed queries sent
    http_calls: int = 0
    cache_hits: int = 0
    found: int = 0
    rejected: int = 0  # answered, but no result or below medium confidence
    deferred: int = 0  # not sent because the budget ran out
    ineligible: int = 0  # no real address


# ---- query construction ------------------------------------------------------------------------


def _clean(value: Optional[str]) -> str:
    return re.sub(r"\s+", " ", unicodedata.normalize("NFKC", value or "")).strip().strip(",").strip()


def is_eligible(q: InstitutionQuery) -> bool:
    """A real address: street + city + country. Never a name alone."""
    return bool(_clean(q.street) and _clean(q.city) and _clean(q.country))


_TRAILING_NUMBER = re.compile(r"^(?P<street>.*?[^\W\d_].*?)[\s,]+(?P<number>\d+[a-zA-Z]?)(?:\s*[-/].*)?$")
_LEADING_NUMBER = re.compile(r"^(?P<number>\d+[a-zA-Z]?)[\s,]+(?P<street>[^\W\d_].*)$")


def split_street(street: str) -> Tuple[Optional[str], str]:
    """(address_number, street name). European addresses put the number last ("Hauptstr. 1"),
    Anglo ones first ("10 Downing Street"); ranges keep their first number. If there is no clear
    number the whole string stays the street."""
    street = _clean(street)
    for pattern in (_TRAILING_NUMBER, _LEADING_NUMBER):
        m = pattern.match(street)
        if m and len(m.group("street")) >= 2:
            return m.group("number"), m.group("street").strip(" ,")
    return None, street


def query_key(q: InstitutionQuery) -> str:
    parts = [_clean(q.street), _clean(q.postalcode), _clean(q.city), _clean(q.country).upper()]
    return hashlib.sha1("\x1f".join(p.casefold() for p in parts).encode()).hexdigest()


def build_item(q: InstitutionQuery) -> dict:
    """One structured query of the batch body."""
    number, street = split_street(q.street)
    # Bilingual city ("LOUVAIN / LEUVEN"): only the first part is sent, one query is one billed request
    item = {"street": street, "place": (city_parts(q.city) or [_clean(q.city)])[0], "country": _clean(q.country).upper(), "limit": 1}
    if number:
        item["address_number"] = number
    if _clean(q.postalcode):
        item["postcode"] = _clean(q.postalcode)
    return item


def _fold(value: Optional[str]) -> str:
    """Case/diacritic-insensitive comparison key ("Zürich" == "zurich", "Köln " == "KOLN")."""
    decomposed = unicodedata.normalize("NFKD", _clean(value).casefold())
    return re.sub(r"[\W_]+", " ", "".join(c for c in decomposed if not unicodedata.combining(c))).strip()


_CITY_SEPARATORS = re.compile(r"\s*[/;,]\s*|\s+[-\u2013\u2014]\s+")


def city_parts(city: Optional[str]) -> List[str]:
    """Cordis cities can be bilingual ("LOUVAIN / LEUVEN"): split on "/", ";", "," and spaced dashes
    (a dash inside a name, "Aix-en-Provence", is kept)."""
    return [part for part in (_clean(p) for p in _CITY_SEPARATORS.split(_clean(city))) if part]


def _place_matches(feature: dict, city: Optional[str], postcode: Optional[str] = None) -> bool:
    """The feature's context place (v6: properties.context.place.name) equals ANY part of the queried
    city, or the queried postcode equals the context postcode (place names differ across languages)."""
    context = (feature.get("properties") or {}).get("context") or {}
    place = _fold((context.get("place") or {}).get("name"))
    if place and place in {_fold(part) for part in city_parts(city)}:
        return True
    wanted, got = _fold(postcode).replace(" ", ""), _fold((context.get("postcode") or {}).get("name")).replace(" ", "")
    return bool(wanted) and wanted == got


def parse_feature(
    response: dict, city: Optional[str] = None, postcode: Optional[str] = None
) -> Tuple[Optional[Tuple[float, float]], Optional[str]]:
    """((lat, lon) or None, confidence or None) from one entry of the batch response. Coordinates
    are only returned for medium-or-better matches, or for a street/address feature that carries no
    match_code (Mapbox omits it for street-only queries) and whose context place equals a part of
    `city` or whose context postcode equals `postcode` (confidence "street")."""
    features = (response or {}).get("features") or []
    if not features:
        return None, None
    feature = features[0]
    properties = feature.get("properties") or {}
    confidence = (properties.get("match_code") or {}).get("confidence")
    coordinates = (feature.get("geometry") or {}).get("coordinates")
    if not coordinates or len(coordinates) < 2:
        return None, confidence
    if confidence is None and properties.get("feature_type") in STREET_FEATURE_TYPES:
        if not _place_matches(feature, city, postcode):
            return None, PLACE_MISMATCH
        return (coordinates[1], coordinates[0]), STREET_CONFIDENCE
    if confidence not in ACCEPTED_CONFIDENCE:
        return None, confidence
    return (coordinates[1], coordinates[0]), confidence  # Mapbox returns [lon, lat]


def estimate_cost_usd(n_requests: int, permanent: bool = False, used_this_month: int = 0) -> float:
    """Cost of `n_requests` more billed queries. `used_this_month` = queries already billed this month
    (temporary tiers start with a 100k free allowance; permanent has no free tier)."""
    if permanent:
        low = min(n_requests, 500_000)
        high = max(n_requests - 500_000, 0)
        return low * PRICE_PER_1000_USD / 1000 + high * PRICE_PER_1000_USD_ABOVE_500K / 1000
    cost, start, left = 0.0, used_this_month, n_requests
    for upper, price in TEMP_TIERS:
        if start >= upper:
            continue
        n = min(left, upper - start)
        cost += n * price / 1000
        start, left = start + n, left - n
        if left <= 0:
            break
    return cost


# ---- persistent cache --------------------------------------------------------------------------


# (found, lat, lon, confidence, permanent)
Answer = Tuple[bool, Optional[float], Optional[float], Optional[str], bool]


class MapboxCache:
    """duckdb file holding every answer Mapbox ever gave us (found or not). It outlives any
    pipeline teardown, and answers are billed, so never delete it."""

    def __init__(self, path: Union[str, Path]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.path))
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS mapbox_cache (
                key        VARCHAR PRIMARY KEY,
                street     VARCHAR,
                postcode   VARCHAR,
                city       VARCHAR,
                country    VARCHAR,
                found      BOOLEAN NOT NULL,   -- FALSE: no result or below medium confidence
                lat        DOUBLE,
                lon        DOUBLE,
                confidence VARCHAR,
                queried_at TIMESTAMP DEFAULT current_timestamp,
                permanent  BOOLEAN DEFAULT FALSE  -- fetched with permanent=true (storable); FALSE: temporary
            )
            """
        )
        # databases created before the column existed: their rows were fetched temporarily
        self.con.execute("ALTER TABLE mapbox_cache ADD COLUMN IF NOT EXISTS permanent BOOLEAN DEFAULT FALSE")
        self.con.execute("UPDATE mapbox_cache SET permanent = FALSE WHERE permanent IS NULL")

    def get_many(self, keys: Sequence[str]) -> Dict[str, Answer]:
        """{key: (found, lat, lon, confidence, permanent)}"""
        if not keys:
            return {}
        rows = self.con.execute(
            "SELECT key, found, lat, lon, confidence, COALESCE(permanent, FALSE) FROM mapbox_cache"
            " WHERE list_contains(?, key)",
            [list(keys)],
        ).fetchall()
        return {k: (found, lat, lon, conf, perm) for k, found, lat, lon, conf, perm in rows}

    def put_many(self, rows: Iterable[tuple]) -> None:
        """rows: (key, street, postcode, city, country, found, lat, lon, confidence, permanent)"""
        rows = list(rows)
        if rows:
            self.con.executemany(
                "INSERT OR REPLACE INTO mapbox_cache (key, street, postcode, city, country, found, lat, lon, confidence, permanent)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows,
            )

    def close(self) -> None:
        self.con.close()


# ---- HTTP --------------------------------------------------------------------------------------

# post(url, params, json_body) -> (status_code, parsed_json, headers); injectable for tests.
HttpPost = Callable[[str, dict, list], Tuple[int, object, dict]]


def _requests_post(url: str, params: dict, body: list) -> Tuple[int, object, dict]:
    import requests

    r = requests.post(url, params=params, json=body, timeout=120)
    try:
        payload = r.json()
    except ValueError:
        payload = None
    return r.status_code, payload, dict(r.headers)


# ---- enricher ----------------------------------------------------------------------------------


class GeolocationEnricher(Enricher[InstitutionQuery, GeolocationResult]):
    """Skips (emits no result for) any query it cannot resolve: match results back to the input on
    `.id`, not by position."""

    def __init__(
        self,
        mapbox_token: Optional[str] = None,
        max_requests: int = 0,
        cache_path: Union[str, Path, None] = None,
        *,
        permanent: bool = False,  # False: temporary geocoding (free tier); True: permanent ($5/1,000), storable
        refresh_temporary: bool = False,  # with permanent=True: cached temporary positives count as not cached
        max_mapbox_requests: Optional[int] = None,  # old name, kept so core_v3's untouched glue still constructs
        http_post: Optional[HttpPost] = None,
        batch_size: int = MAX_BATCH_SIZE,
        rate_limit_per_min: int = DEFAULT_RATE_LIMIT_PER_MIN,
        max_retries: int = 5,
        backoff_base_s: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ):
        if not 1 <= batch_size <= MAX_BATCH_SIZE:
            raise ValueError(f"batch_size must be 1..{MAX_BATCH_SIZE}")
        self._token = mapbox_token if mapbox_token is not None else os.getenv("API_KEY_MAPBOX")
        self.permanent = permanent
        self.refresh_temporary = refresh_temporary
        self.max_requests = max_mapbox_requests if max_mapbox_requests is not None else max_requests
        self._cache_path = cache_path
        self._cache: Optional[MapboxCache] = None
        self._post = http_post or _requests_post
        self._batch_size = batch_size
        self._rate = rate_limit_per_min
        self._max_retries = max_retries
        self._backoff = backoff_base_s
        self._sleep = sleep
        self._clock = clock
        self.stats = Stats()

    @property
    def cache(self) -> MapboxCache:
        if self._cache is None:
            if self._cache_path is None:
                raise ValueError("GeolocationEnricher needs a cache_path: results must be cached before they are paid for")
            self._cache = MapboxCache(self._cache_path)
        return self._cache

    def close(self) -> None:
        if self._cache is not None:
            self._cache.close()
            self._cache = None

    @property
    def budget_left(self) -> int:
        return max(self.max_requests - self.stats.requests, 0)

    def plan(self, keys: Sequence[str]) -> Tuple[Dict[str, Answer], List[str]]:
        """(cached answers, keys to request). A key is requested when it is not cached, or, with
        permanent + refresh_temporary, when its cached row is a temporary *positive* (negatives carry no
        coordinates, so they have nothing to refresh). A refreshed row stays in the answers as a fallback
        (still usable if the budget runs out), it is replaced once re-requested."""
        answers = self.cache.get_many(list(keys))
        refresh = self.permanent and self.refresh_temporary
        todo = [k for k in keys if k not in answers or (refresh and answers[k][0] and not answers[k][4])]
        return answers, todo

    # -- dry run ---------------------------------------------------------------------------------
    def estimate(self, items: Iterable[InstitutionQuery]) -> Estimate:
        """Counts what would be billed, sending nothing."""
        unique: Dict[str, InstitutionQuery] = {}
        eligible = 0
        for q in items:
            if is_eligible(q):
                eligible += 1
                unique.setdefault(query_key(q), q)
        _, todo = self.plan(list(unique))
        to_request = len(todo)
        return Estimate(eligible, len(unique), len(unique) - to_request, to_request, estimate_cost_usd(to_request, self.permanent))

    # -- real run --------------------------------------------------------------------------------
    def enrich(self, items: Iterable[InstitutionQuery]) -> List[GeolocationResult]:
        items = list(items)
        by_key: Dict[str, List[InstitutionQuery]] = {}
        for q in items:
            if is_eligible(q):
                by_key.setdefault(query_key(q), []).append(q)
            else:
                self.stats.ineligible += 1

        if not by_key:
            return []
        answers, todo = self.plan(list(by_key))
        self.stats.cache_hits += len(by_key) - len(todo)
        allowed = todo[: self.budget_left] if self._token else []
        if len(allowed) < len(todo):
            self.stats.deferred += sum(len(by_key[k]) for k in todo[len(allowed) :])
            if not self._token and todo:
                logging.warning("API_KEY_MAPBOX is not set: nothing is sent")
        for start in range(0, len(allowed), self._batch_size):
            chunk = allowed[start : start + self._batch_size]
            answers.update(self._request(chunk, {k: by_key[k][0] for k in chunk}))

        results = []
        for key, queries in by_key.items():
            if key not in answers:
                continue
            found, lat, lon, confidence, permanent = answers[key]
            if not found:
                self.stats.rejected += len(queries)
                continue
            source = SOURCE_PERMANENT if permanent else SOURCE_TEMPORARY
            for q in queries:
                results.append(GeolocationResult(q.id, lat, lon, source, confidence))
            self.stats.found += len(queries)
        return results

    def _request(self, keys: List[str], queries: Dict[str, InstitutionQuery]) -> dict:
        """Sends `keys` as one batch (bisecting on a 422 so one malformed address cannot sink 1,000),
        caches every answer, returns {key: (found, lat, lon, confidence, permanent)}."""
        started = self._clock()
        body = [build_item(queries[k]) for k in keys]
        status, payload = self._post_with_retry(body)

        if status == 422 and len(keys) > 1:
            mid = len(keys) // 2
            out = self._request(keys[:mid], queries)
            out.update(self._request(keys[mid:], queries))
            return out
        if status == 422:  # a single address Mapbox refuses: remember it as unanswerable
            answers = {keys[0]: (False, None, None, "invalid", self.permanent)}
        else:
            batch = (payload or {}).get("batch") if isinstance(payload, dict) else None
            if status != 200 or not isinstance(batch, list) or len(batch) != len(keys):
                raise MapboxError(f"Mapbox batch failed: HTTP {status}, unexpected response for {len(keys)} queries")
            answers = {}
            for key, entry in zip(keys, batch):
                coords, confidence = parse_feature(entry, queries[key].city, queries[key].postalcode)
                answers[key] = (coords is not None, *(coords or (None, None)), confidence, self.permanent)

        self.stats.requests += len(keys)
        self.cache.put_many(
            (k, _clean(queries[k].street), _clean(queries[k].postalcode), _clean(queries[k].city), _clean(queries[k].country).upper(), *a)
            for k, a in answers.items()
        )
        # rate limit: each query counts as one request per minute
        wait = len(keys) * 60.0 / self._rate - (self._clock() - started)
        if wait > 0:
            self._sleep(wait)
        return answers

    def _post_with_retry(self, body: list) -> Tuple[int, object]:
        params = {"access_token": self._token, "permanent": "true" if self.permanent else "false"}
        for attempt in range(self._max_retries + 1):
            self.stats.http_calls += 1
            try:
                status, payload, headers = self._post(MAPBOX_BATCH_URL, params, body)
            except Exception as e:  # connection reset, timeout
                status, payload, headers = 0, None, {}
                logging.warning(f"Mapbox request error: {e}")
            if status == 429 or status >= 500 or status == 0:
                if attempt == self._max_retries:
                    raise MapboxError(f"Mapbox batch still failing after {self._max_retries} retries (HTTP {status})")
                delay = self._backoff * 2**attempt
                retry_after = {k.lower(): v for k, v in headers.items()}.get("retry-after")
                if retry_after and str(retry_after).replace(".", "", 1).isdigit():
                    delay = max(delay, float(retry_after))
                logging.warning(f"Mapbox HTTP {status}, retry {attempt + 1}/{self._max_retries} in {delay:.0f}s")
                self._sleep(delay)
                continue
            if status in (401, 403):
                raise MapboxError(f"Mapbox rejected the token (HTTP {status}); check API_KEY_MAPBOX")
            return status, payload
        raise AssertionError("unreachable")
