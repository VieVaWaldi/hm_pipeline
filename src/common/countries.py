"""
One place that normalises country codes for every core_v4 step. Built from the
Phase 1 COUNTRY_MAPPING (src/pipelines/core_v4/investigation/phase1_measurements.py,
README section 5); after it nothing is left unmapped in OpenAire org.countryCode,
Cordis institution.country, ROR country_code or work.countries.

Python: normalize_country(code). SQL: country_sql(expr) builds the equivalent
CASE expression, or register_country_macro(con) defines norm_cc(c) on a duckdb
connection so both stay in step.
"""

from typing import Optional

import duckdb

# Non-ISO codes seen in the data -> ISO alpha-2. None = not a country, drop to NULL.
# EL/UK: EU institutional codes (Cordis). YU/CS -> RS and AN -> CW are judgement calls.
COUNTRY_MAPPING = {
    "EL": "GR",
    "UK": "GB",
    "ZR": "CD",
    "YU": "RS",
    "CS": "RS",
    "AN": "CW",
    "QAT": "QA",  # alpha-3 leaked into work.countries
    "LIE": "LI",
    "ZZ": None,
    "EU": None,
    "DC": None,
    "DD": None,
    "OC": None,
    "EUROPE": None,
    "WORLD": None,
}

# Not ISO 3166-1, but user-assigned and used by the EU/ROR: kept as its own code.
KEEP_NON_ISO = {"XK"}

# ISO 3166-1 alpha-2, 249 officially assigned codes.
ISO_ALPHA2 = set(
    """AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ
    CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR
    GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO
    JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR
    MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO
    RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV
    TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW""".split()
)
VALID_CODES = ISO_ALPHA2 | KEEP_NON_ISO


def normalize_country(code: Optional[str]) -> Optional[str]:
    """Upper-cased ISO alpha-2 (or XK) for `code`; None for empty, non-countries
    (EU, ZZ, ...) and anything that is still not a valid code after mapping."""
    if code is None:
        return None
    key = code.strip().upper()
    if not key:
        return None
    if key in COUNTRY_MAPPING:
        key = COUNTRY_MAPPING[key]
    return key if key in VALID_CODES else None


def country_sql(expr: str) -> str:
    """SQL expression equivalent to normalize_country(<expr>). Anything not a
    valid code after mapping becomes NULL, exactly like the Python function."""
    whens = " ".join(f"WHEN '{k}' THEN {'NULL' if v is None else repr(v)}" for k, v in COUNTRY_MAPPING.items())
    valid = ", ".join(f"'{c}'" for c in sorted(VALID_CODES))
    return (
        f"(CASE WHEN upper(trim({expr})) IN ({valid}) THEN upper(trim({expr})) "
        f"ELSE CASE upper(trim({expr})) {whens} ELSE NULL END END)"
    )


def register_country_macro(con: duckdb.DuckDBPyConnection, name: str = "norm_cc") -> None:
    """Defines `<name>(c)` on `con` (idempotent); same semantics as normalize_country."""
    con.execute(f"CREATE OR REPLACE TEMP MACRO {name}(c) AS {country_sql('c')}")
