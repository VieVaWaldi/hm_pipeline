"""Glue tests against the tiny staging fixture; HTTP is mocked."""

import duckdb
import pytest

from enrichment.geolocation.geocoder import GeolocationEnricher
from pipelines.core_v4.enrichment.fixtures import make_staging_fixture
from pipelines.core_v4.enrichment.geolocation import candidates_sql, estimate, run
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput
from common.countries import register_country_macro


class Fake:
    def __init__(self):
        self.bodies = []

    def __call__(self, url, params, body):
        self.bodies.append(body)
        return 200, {"batch": [{"features": [{"geometry": {"coordinates": [8.0, 50.0]}, "properties": {"match_code": {"confidence": "high"}}}] } for _ in body]}, {}


@pytest.fixture
def staging(tmp_path):
    path = make_staging_fixture(tmp_path / "staging.duckdb")
    con = duckdb.connect(str(path), read_only=True)
    register_country_macro(con)
    return con


def make(tmp_path, fake, budget):
    return GeolocationEnricher("tok", max_requests=budget, cache_path=tmp_path / "cache.duckdb", http_post=fake, sleep=lambda s: None)


def test_candidates_need_null_geolocation_street_city_and_country(staging, tmp_path):
    rows = staging.execute(candidates_sql()).fetchall()
    assert sorted(r[4] for r in rows) == ["DE", "GB"]  # o3 has no address; UK normalised to GB
    staging.close()
    con = duckdb.connect(str(tmp_path / "staging.duckdb"))
    con.execute("UPDATE organization SET geolocation = {'lat': 1.0, 'lon': 2.0} WHERE legalName = 'Org 1'")  # ROR/Cordis coords
    con.close()
    con = duckdb.connect(str(tmp_path / "staging.duckdb"), read_only=True)
    register_country_macro(con)
    assert [r[4] for r in con.execute(candidates_sql()).fetchall()] == ["GB"]


def test_run_writes_side_output_and_resumes(staging, tmp_path):
    out = SideOutput(tmp_path / "enrich", "geolocation", "organization")
    fake = Fake()
    enricher = make(tmp_path, fake, budget=10)
    run(staging, enricher, out)
    assert out.is_complete()
    rows = duckdb.sql(f"SELECT lat, lon, geolocation_source, confidence FROM ({out.read_all_sql()})").fetchall()
    assert rows == [(50.0, 8.0, "Mapbox", "high")] * 2
    assert sum(len(b) for b in fake.bodies) == 2

    run(staging, make(tmp_path, fake, budget=10), out)  # resume: nothing left, nothing sent
    assert sum(len(b) for b in fake.bodies) == 2 and out.is_complete()


def test_default_budget_writes_nothing_and_is_not_complete(staging, tmp_path):
    out = SideOutput(tmp_path / "enrich", "geolocation", "organization")
    fake = Fake()
    run(staging, make(tmp_path, fake, budget=0), out)
    assert fake.bodies == [] and out.parts() == [] and not out.is_complete()


def test_test_mode_writes_nothing(staging, tmp_path):
    out = SideOutput(tmp_path / "enrich", "geolocation", "organization")
    fake = Fake()
    run(staging, make(tmp_path, fake, budget=0), out, limit=1, write=False)
    assert not out.dir.exists() and fake.bodies == []


def test_dry_run_estimate(staging, tmp_path):
    est = estimate(staging, make(tmp_path, Fake(), budget=0), candidates_sql())
    assert (est.eligible, est.unique, est.cached, est.to_request) == (2, 2, 0, 2)
    assert est.cost_usd == 0.0  # temporary: free tier
    perm = estimate(staging, GeolocationEnricher('tok', cache_path=tmp_path / 'cache.duckdb', permanent=True), candidates_sql())
    assert perm.cost_usd == pytest.approx(0.01)
