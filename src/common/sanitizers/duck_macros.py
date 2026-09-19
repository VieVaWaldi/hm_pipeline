"""
DuckDB SQL macro versions of the sanitizers in parse_text.py.

Python UDFs run under the GIL, so at OpenAIRE scale (200M+ works) they cap a
whole query at ~1 core. These macros are plain SQL, so DuckDB parallelises
them across all threads (~9x faster in a benchmark on abstract-sized strings).

parse_text.py stays the source of truth for row-by-row callers (API loaders,
meta_heritage, ...). These macros must produce IDENTICAL output; that is
enforced by tests/test_duck_macros.py.

Macro          mirrors
-------------  ------------------------------
sanitize_string    parse_string
sanitize_name      parse_names_and_identifiers
sanitize_title     parse_titles_and_labels
sanitize_content   parse_content
sanitize_url       parse_web_resources

All return NULL for NULL / empty / whitespace-only input.
"""

from duckdb import DuckDBPyConnection

# Same set as parse_text._BOM_CHARS.
_BOM_CHARS = "﻿​‌‍￾"

# Python's str.split()/str.strip() whitespace: RE2 \s (ASCII) plus Unicode
# separators (\p{Z}), VT, NEL and the C0 separators 0x1c-0x1f.
_WS = r"[\s\p{Z}\x{0b}\x{85}\x{1c}-\x{1f}]"

_MACROS = f"""
-- Strip BOM/zero-width chars at both ends; '' -> NULL.
CREATE OR REPLACE MACRO _san_ensure(s) AS
    NULLIF(trim(s, '{_BOM_CHARS}'), '');

-- str.strip(): remove leading/trailing whitespace.
CREATE OR REPLACE MACRO _san_wstrip(s) AS
    regexp_replace(s, '^{_WS}+|{_WS}+$', '', 'g');

-- " ".join(s.split()): collapse whitespace runs to one space, trim ends.
CREATE OR REPLACE MACRO _san_squash(s) AS
    _san_wstrip(regexp_replace(s, '{_WS}+', ' ', 'g'));

CREATE OR REPLACE MACRO sanitize_string(v) AS
    NULLIF(_san_squash(_san_ensure(v)), '');

CREATE OR REPLACE MACRO sanitize_name(v) AS
    NULLIF(_san_squash(replace(_san_ensure(v), chr(13), '')), '');

CREATE OR REPLACE MACRO sanitize_title(v) AS
    NULLIF(
        _san_squash(
            replace(replace(replace(replace(
                _san_ensure(v),
                chr(13), ''), chr(10), ' '), '–', '-'), '—', '-')
        ),
        ''
    );

-- Keeps paragraph structure: squash each line, drop empty lines, join with \\n.
CREATE OR REPLACE MACRO sanitize_content(v) AS
    NULLIF(
        array_to_string(
            list_filter(
                list_transform(
                    string_split(
                        replace(replace(_san_ensure(v), chr(13), ''), chr(0), ''),
                        chr(10)
                    ),
                    p -> _san_squash(p)
                ),
                p -> p <> ''
            ),
            chr(10)
        ),
        ''
    );

CREATE OR REPLACE MACRO sanitize_url(v) AS
    NULLIF(
        replace(replace(replace(
            _san_wstrip(_san_ensure(v)),
            chr(13), ''), chr(10), ''), chr(9), ''),
        ''
    );
"""


def register_sanitizer_macros(con: DuckDBPyConnection) -> None:
    """Create the sanitize_* macros on `con` (idempotent, persisted in the DB file)."""
    con.execute(_MACROS)
