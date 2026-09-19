import csv

import duckdb
import pytest

from common.countries import VALID_CODES
from pipelines.core_v4.enrichment.fixtures import make_staging_fixture
from pipelines.core_v4.enrichment.regions import COUNTRY_REGIONS_CSV, run
from pipelines.core_v4.enrichment.side_outputs import SideOutput

ALLOWED = {"Northern Europe", "Central Europe", "Western Europe", "Eastern Europe", "Southern Europe", "Outside Europe"}


def test_csv_covers_every_normalisable_country():
    rows = list(csv.DictReader(open(COUNTRY_REGIONS_CSV)))
    assert {r["alpha2"] for r in rows} == VALID_CODES and len(rows) == len(VALID_CODES)
    assert {r["region"] for r in rows} <= ALLOWED
    by = {r["alpha2"]: r["region"] for r in rows}
    assert by["DE"] == "Central Europe" and by["FR"] == "Western Europe" and by["SE"] == "Northern Europe"
    assert by["PL"] == "Central Europe" and by["IT"] == "Southern Europe" and by["UA"] == "Eastern Europe"
    assert by["US"] == "Outside Europe" and by["JP"] == "Outside Europe"


def open_db(path):
    return duckdb.connect(str(path), read_only=True)


def region_of(out, con, name):
    return con.execute(
        f"SELECT r.region FROM ({out.read_all_sql()}) r JOIN organization o ON o.id = r.id WHERE o.legalName = ?", [name]
    ).fetchone()


def test_regions_side_output(tmp_path):
    path = make_staging_fixture(tmp_path / "s.duckdb")
    con = open_db(path)
    out = SideOutput(tmp_path / "e", "regions", "organization")
    assert run(con, out) == 2  # Org 3 has no country
    assert out.is_complete()
    assert region_of(out, con, "Org 1") == ("Central Europe",)  # DE
    assert region_of(out, con, "Org 2") == ("Northern Europe",)  # countryCode UK -> GB
    assert region_of(out, con, "Org 3") is None
    assert run(con, out) == 0  # resume: everything already done


def test_country_fallback_order(tmp_path):
    path = make_staging_fixture(tmp_path / "s.duckdb")
    w = duckdb.connect(str(path))
    w.execute("ALTER TABLE organization ADD COLUMN rorLocations JSON")
    w.execute("UPDATE organization SET countryCode = NULL")  # force the fallbacks
    w.execute("""UPDATE organization SET rorLocations = '[{"geonames_details": {"country_code": "FR"}}]' WHERE legalName = 'Org 1'""")
    w.close()
    con = open_db(path)
    out = SideOutput(tmp_path / "e", "regions", "organization")
    run(con, out)
    assert region_of(out, con, "Org 1") == ("Western Europe",)  # ROR country (FR) beats address_country (DE)
    assert region_of(out, con, "Org 2") == ("Northern Europe",)  # address_country UK -> GB


def test_test_mode_writes_nothing(tmp_path):
    con = open_db(make_staging_fixture(tmp_path / "s.duckdb"))
    out = SideOutput(tmp_path / "e", "regions", "organization")
    assert run(con, out, limit=1, write=False) == 1 and not out.dir.exists()
