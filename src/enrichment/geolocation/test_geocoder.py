"""Geocoder tests: HTTP is always mocked (no test here may spend money).

The one live check is skipped unless MAPBOX_LIVE_OK=1 and API_KEY_MAPBOX are set; it sends at most 20
queries. Do not enable it without the owner's explicit OK.
"""

import os

import pytest

from enrichment.geolocation.geocoder import (
    MAPBOX_BATCH_URL,
    GeolocationEnricher,
    InstitutionQuery,
    MapboxError,
    build_item,
    estimate_cost_usd,
    is_eligible,
    query_key,
    split_street,
)


def feature(lon, lat, confidence="high"):
    return {"features": [{"geometry": {"coordinates": [lon, lat]}, "properties": {"match_code": {"confidence": confidence}}}]}


EMPTY = {"features": []}


class FakeMapbox:
    """Answers a batch in order from `answers` keyed by (street, place); records every call."""

    def __init__(self, answers=None, fail_first=(), invalid_streets=()):
        self.answers = answers or {}
        self.fail_first = list(fail_first)  # status codes to answer before behaving
        self.invalid = set(invalid_streets)
        self.calls = []

    def __call__(self, url, params, body):
        self.calls.append((url, params, body))
        if self.fail_first:
            return self.fail_first.pop(0), None, {}
        if any(item["street"] in self.invalid for item in body):
            return 422, {"message": "invalid"}, {}
        return 200, {"batch": [self.answers.get((i["street"], i["place"]), EMPTY) for i in body]}, {}

    @property
    def queried(self):
        return [item for _, _, body in self.calls for item in body]


def make(tmp_path, fake, budget=100, **kw):
    kw.setdefault("sleep", lambda s: None)
    return GeolocationEnricher("tok", max_requests=budget, cache_path=tmp_path / "cache.duckdb", http_post=fake, **kw)


BERLIN = InstitutionQuery(1, street="Hauptstr. 1", city="Berlin", country="DE", postalcode="10115")
OXFORD = InstitutionQuery(2, street="10 High Street", city="Oxford", country="GB")


def test_split_street():
    assert split_street("Hauptstr. 1") == ("1", "Hauptstr.")
    assert split_street("Rue de Rivoli 12a") == ("12a", "Rue de Rivoli")
    assert split_street("10 Downing Street") == ("10", "Downing Street")
    assert split_street("Karl-Marx-Allee 12-14") == ("12", "Karl-Marx-Allee")
    assert split_street("Piazza Navona") == (None, "Piazza Navona")
    assert split_street("1234") == (None, "1234")


def test_request_shape(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5)})
    make(tmp_path, fake).enrich([BERLIN])
    url, params, body = fake.calls[0]
    assert url == MAPBOX_BATCH_URL and url.endswith("/search/geocode/v6/batch")
    assert params == {"access_token": "tok", "permanent": "false"}  # default: temporary, free tier
    assert body == [{"street": "Hauptstr.", "address_number": "1", "postcode": "10115", "place": "Berlin", "country": "DE", "limit": 1}]


def test_permanent_option(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5)})
    make(tmp_path, fake, permanent=True).enrich([BERLIN])
    assert fake.calls[0][1]["permanent"] == "true"


def test_result_is_lat_lon_of_lonlat_coordinates(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5, "exact")})
    [r] = make(tmp_path, fake).enrich([BERLIN])
    assert (r.id, r.latitude, r.longitude, r.source, r.confidence) == (1, 52.5, 13.4, "Mapbox", "exact")


def test_never_queries_without_a_real_address(tmp_path):
    fake = FakeMapbox()
    enricher = make(tmp_path, fake)
    for q in (
        InstitutionQuery(1, name="ETH Zurich", country="CH"),  # name only
        InstitutionQuery(2, street="X 1", country="CH"),  # no city
        InstitutionQuery(3, city="Zurich", country="CH"),  # no street
        InstitutionQuery(4, street="Rämistr. 101", city="Zurich"),  # no country
        InstitutionQuery(5, street="  ", city="Zurich", country="CH"),
    ):
        assert not is_eligible(q)
    assert enricher.enrich([InstitutionQuery(1, name="ETH", country="CH"), InstitutionQuery(2, city="Z", country="CH")]) == []
    assert fake.calls == [] and enricher.stats.ineligible == 2


@pytest.mark.parametrize("confidence,accepted", [("exact", True), ("high", True), ("medium", True), ("low", False), (None, False)])
def test_only_medium_or_better(tmp_path, confidence, accepted):
    resp = feature(1, 2, confidence) if confidence else {"features": [{"geometry": {"coordinates": [1, 2]}, "properties": {}}]}
    results = make(tmp_path, FakeMapbox({("Hauptstr.", "Berlin"): resp})).enrich([BERLIN])
    assert bool(results) is accepted


def test_cache_hit_positive_and_negative_never_paid_twice(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5)})  # Oxford answers EMPTY: a negative
    first = make(tmp_path, fake)
    assert len(first.enrich([BERLIN, OXFORD])) == 1
    first.close()
    assert len(fake.queried) == 2

    second = make(tmp_path, fake)  # new process, same cache file
    assert len(second.enrich([BERLIN, OXFORD])) == 1  # BERLIN from cache
    assert len(fake.queried) == 2  # nothing re-sent, the negative included
    assert second.stats.cache_hits == 2 and second.stats.requests == 0


def test_low_confidence_is_cached_as_negative(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(1, 2, "low")})
    make(tmp_path, fake).enrich([BERLIN])
    assert make(tmp_path, fake).enrich([BERLIN]) == []
    assert len(fake.queried) == 1


def test_identical_addresses_are_sent_once(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5)})
    twin = InstitutionQuery(99, street=" hauptstr.  1 ", city="berlin", country="de", postalcode="10115")
    assert query_key(BERLIN) == query_key(twin)
    results = make(tmp_path, fake).enrich([BERLIN, twin])
    assert sorted(r.id for r in results) == [1, 99] and len(fake.queried) == 1


def test_budget_default_zero_spends_nothing(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5)})
    enricher = GeolocationEnricher("tok", cache_path=tmp_path / "c.duckdb", http_post=fake)  # default max_requests
    assert enricher.enrich([BERLIN]) == [] and fake.calls == [] and enricher.stats.deferred == 1


def test_budget_cap_counts_queries_and_defers_rest(tmp_path):
    fake = FakeMapbox()
    enricher = make(tmp_path, fake, budget=2)
    queries = [InstitutionQuery(i, street=f"Weg {i}", city="Berlin", country="DE") for i in range(5)]
    enricher.enrich(queries)
    assert len(fake.queried) == 2 and enricher.stats.requests == 2 and enricher.stats.deferred == 3
    enricher.enrich(queries)  # cached ones are free, the budget is used up for the rest
    assert len(fake.queried) == 2 and enricher.budget_left == 0


def test_batches_are_split_at_batch_size(tmp_path):
    fake = FakeMapbox()
    make(tmp_path, fake, batch_size=2).enrich([InstitutionQuery(i, street=f"Weg {i}", city="Bonn", country="DE") for i in range(5)])
    assert [len(body) for _, _, body in fake.calls] == [2, 2, 1]
    with pytest.raises(ValueError):
        make(tmp_path, fake, batch_size=1001)


def test_retry_with_backoff_on_429_and_5xx(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5)}, fail_first=[429, 503])
    sleeps = []
    enricher = make(tmp_path, fake, sleep=sleeps.append, backoff_base_s=2.0)
    assert len(enricher.enrich([BERLIN])) == 1
    assert len(fake.calls) == 3 and sleeps[:2] == [2.0, 4.0]
    assert enricher.stats.requests == 1  # retries are not billed queries


def test_retry_exhaustion_raises_and_caches_nothing(tmp_path):
    fake = FakeMapbox(fail_first=[500] * 10)
    enricher = make(tmp_path, fake, max_retries=2)
    with pytest.raises(MapboxError):
        enricher.enrich([BERLIN])
    assert len(fake.calls) == 3
    assert enricher.cache.get_many([query_key(BERLIN)]) == {}


def test_bad_token_fails_fast(tmp_path):
    fake = FakeMapbox(fail_first=[401])
    with pytest.raises(MapboxError, match="token"):
        make(tmp_path, fake).enrich([BERLIN])
    assert len(fake.calls) == 1


def test_422_bisects_down_to_the_bad_address(tmp_path):
    fake = FakeMapbox({("Weg", "Bonn"): feature(1, 1)}, invalid_streets={"Bad"})
    queries = [InstitutionQuery(1, street="Weg 1", city="Bonn", country="DE"), InstitutionQuery(2, street="Bad", city="Bonn", country="DE")]
    enricher = make(tmp_path, fake)
    assert [r.id for r in enricher.enrich(queries)] == [1]
    assert enricher.stats.requests == 2
    sent = len(fake.queried)
    assert [r.id for r in enricher.enrich(queries)] == [1]  # rerun is free, the invalid one is cached too
    assert len(fake.queried) == sent and enricher.stats.requests == 2


def test_progress_survives_a_crash_mid_run(tmp_path):
    """Batch 1 is answered and cached before batch 2 fails: paid results are never lost."""
    calls = []

    def flaky(url, params, body):
        calls.append(body)
        if len(calls) == 2:
            return 500, None, {}
        return 200, {"batch": [feature(1, 2) for _ in body]}, {}

    queries = [InstitutionQuery(i, street=f"Weg {i}", city="Bonn", country="DE") for i in range(4)]
    enricher = make(tmp_path, flaky, batch_size=2, max_retries=0)
    with pytest.raises(MapboxError):
        enricher.enrich(queries)
    enricher.close()
    assert len(make(tmp_path, flaky).cache.get_many([query_key(q) for q in queries])) == 2


def test_rate_limit_throttles_per_query(tmp_path):
    slept = []
    ticks = iter(range(0, 1000, 1))  # the clock advances 1s per reading
    enricher = make(tmp_path, FakeMapbox(), sleep=slept.append, clock=lambda: float(next(ticks)), rate_limit_per_min=60)
    enricher.enrich([InstitutionQuery(i, street=f"Weg {i}", city="Bonn", country="DE") for i in range(10)])
    assert slept and slept[0] == pytest.approx(10 * 60 / 60 - 1)  # 10 queries at 60/min = 10s, 1s already elapsed


def test_estimate_dry_run_sends_nothing(tmp_path):
    fake = FakeMapbox({("Hauptstr.", "Berlin"): feature(13.4, 52.5)})
    enricher = make(tmp_path, fake)
    enricher.enrich([BERLIN])  # now cached
    est = enricher.estimate([BERLIN, OXFORD, OXFORD, InstitutionQuery(9, name="x", country="DE")])
    assert (est.eligible, est.unique, est.cached, est.to_request) == (3, 2, 1, 1)
    assert est.cost_usd == 0.0  # temporary: inside the free tier
    assert len(fake.queried) == 1  # only the earlier real call


def test_cost_tiers():
    assert estimate_cost_usd(1000, permanent=True) == pytest.approx(5.0)
    assert estimate_cost_usd(600_000, permanent=True) == pytest.approx(500 * 5 + 100 * 4)


def test_temporary_cost_has_a_free_tier():
    assert estimate_cost_usd(16_000) == 0.0
    assert estimate_cost_usd(100_000) == 0.0
    assert estimate_cost_usd(200_000) == pytest.approx(100 * 0.75)
    assert estimate_cost_usd(1000, used_this_month=100_000) == pytest.approx(0.75)
    assert estimate_cost_usd(600_000) == pytest.approx(400 * 0.75 + 100 * 0.60)


def test_build_item_omits_missing_postcode_and_number():
    assert build_item(InstitutionQuery(1, street="Piazza Navona", city="Roma", country="it")) == {
        "street": "Piazza Navona", "place": "Roma", "country": "IT", "limit": 1,
    }


@pytest.mark.skipif(os.getenv("MAPBOX_LIVE_OK") != "1" or not os.getenv("API_KEY_MAPBOX"), reason="live Mapbox test spends money")
def test_live_smoke(tmp_path):  # at most 20 queries
    enricher = GeolocationEnricher(max_requests=20, cache_path=tmp_path / "live.duckdb")
    results = enricher.enrich([BERLIN, OXFORD])
    assert enricher.stats.requests <= 20 and results
