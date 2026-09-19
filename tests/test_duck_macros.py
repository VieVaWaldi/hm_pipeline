"""
Equivalence test: DuckDB sanitize_* macros must match the Python parse_* functions.

Run:  uv run python tests/test_duck_macros.py      (or pytest, if installed)
"""

import random

import duckdb

from common.sanitizers.duck_macros import register_sanitizer_macros
from common.sanitizers.parse_text import (
    parse_content,
    parse_names_and_identifiers,
    parse_string,
    parse_titles_and_labels,
    parse_web_resources,
)

PAIRS = {
    "sanitize_string": parse_string,
    "sanitize_name": parse_names_and_identifiers,
    "sanitize_title": parse_titles_and_labels,
    "sanitize_content": parse_content,
    "sanitize_url": parse_web_resources,
}

# Edge-case alphabet: ASCII/Unicode whitespace, BOM + zero-width, NUL, dashes,
# non-ASCII letters, C0/C1 separators.
ALPHABET = [
    "a", "B", "é", "-", "–", "—", "", " ", " ", "\t", "\n", "\r", "\x00",
    "\x0b", "\x0c", "\x1f", "\x85", "\xa0", " ", " ", "　",
    "​", "﻿", "￾",
]  # fmt: skip


def _cases(n: int = 200_000) -> list:
    rng = random.Random(1)
    rows = ["".join(rng.choices(ALPHABET, k=rng.randint(0, 12))) for _ in range(n)]
    return rows + [None, "", " ", "﻿", "  a  b  ", "a\rb", "l1\r\n\r\n l2\t x "]


def _connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    register_sanitizer_macros(con)
    return con


def test_macros_match_python():
    con = _connection()
    con.execute("CREATE TABLE t(s VARCHAR)")
    con.executemany("INSERT INTO t VALUES (?)", [(r,) for r in _cases()])
    for macro, fn in PAIRS.items():
        got = con.execute(f"SELECT s, {macro}(s) FROM t").fetchall()
        bad = [(s, g, fn(s)) for s, g in got if g != fn(s)]
        assert not bad, f"{macro}: {len(bad)} mismatches, e.g. {bad[:3]!r}"


def test_macros_work_inside_list_transform():
    """Staging uses list_filter(list_transform(col, x -> sanitize_*(x)), ...)."""
    con = _connection()
    got = con.execute(
        "SELECT list_filter(list_transform(['  a  b ', '﻿', NULL], "
        "x -> sanitize_name(x)), x -> x IS NOT NULL)"
    ).fetchone()[0]
    assert got == ["a b"], got


if __name__ == "__main__":
    test_macros_match_python()
    test_macros_work_inside_list_transform()
    print("OK: all sanitize_* macros match parse_text.py")
