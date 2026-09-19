import duckdb
import pytest

from common.countries import COUNTRY_MAPPING, ISO_ALPHA2, country_sql, normalize_country, register_country_macro

CASES = [
    ("DE", "DE"), ("de", "DE"), (" gr ", "GR"), ("UK", "GB"), ("EL", "GR"), ("QAT", "QA"), ("YU", "RS"),
    ("XK", "XK"), ("EU", None), ("ZZ", None), ("WORLD", None), ("", None), (None, None), ("XX", None),
]


@pytest.mark.parametrize("raw,expected", CASES)
def test_normalize_country(raw, expected):
    assert normalize_country(raw) == expected


def test_sql_matches_python():
    con = duckdb.connect()
    register_country_macro(con)
    for raw, expected in CASES:
        assert con.execute("SELECT norm_cc(?)", [raw]).fetchone()[0] == expected, raw
    assert con.execute(f"SELECT {country_sql(chr(39) + 'uk' + chr(39))}").fetchone()[0] == "GB"


def test_every_iso_code_is_stable_and_mapping_targets_are_valid():
    assert all(normalize_country(c) == c for c in ISO_ALPHA2)
    assert all(v is None or v in ISO_ALPHA2 for v in COUNTRY_MAPPING.values())
