"""
Staging fingerprint: how a side output says which staging it was computed against.

Side outputs are never cleared when the staging duckdb is rebuilt (a rebuild with another `--limit` or
`--work-cap` leaves the old rows behind). Each enrichment therefore records, in its `_SUCCESS` file, a
fingerprint of the id set it processed, and assemble compares it with the current staging.

A stamp is `{"n": row count, "sum": sum of the ids, "xor": xor of the ids}` (ids are UBIGINT hashes, so
this is a cheap, order-independent digest of the id set: one scan of one 8-byte column, a second or two
for 50M works). Stamps of disjoint id sets combine (`combine`), which is how the stamp of "all works" is
the combination of the tier-0 and tier-1 stamps. `work` can be scoped to a tier (`link_tier`), so tier-0
side outputs stay valid when only tier 1 changed.

Not covered: a rebuild that keeps every id but changes a text (a resumable enrichment would skip the row).
A new OpenAire dump changes the ids, so the usual cases are caught.
"""

from typing import Dict, Optional

import duckdb

STAMP_KEYS = ("n", "sum", "xor")
Stamp = Dict[str, object]


class StagingTierError(RuntimeError):
    pass


def staging_stamp(
    con: duckdb.DuckDBPyConnection, entity: str, tier: Optional[int] = None, catalog: Optional[str] = None
) -> Stamp:
    """Stamp of `entity`'s ids in staging (`catalog` = the ATTACH alias when the staging is attached), optionally
    only the works of one link tier."""
    table = f"{catalog}.{entity}" if catalog else entity
    where = ""
    if tier is not None:
        if entity != "work":
            raise StagingTierError(f"--tier only applies to works, not {entity}")
        cols = {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}
        if "link_tier" not in cols:
            raise StagingTierError(
                f"{table} has no link_tier column: rebuild the staging with the current transformation to use --tier"
            )
        where = f" WHERE link_tier = {int(tier)}"
    n, total, xor = con.execute(f"SELECT count(*), sum(id::HUGEINT), bit_xor(id) FROM {table}{where}").fetchone()
    return {"n": int(n), "sum": str(int(total or 0)), "xor": str(int(xor or 0))}


def combine(a: Optional[Stamp], b: Optional[Stamp]) -> Optional[Stamp]:
    """Stamp of the union of two disjoint id sets; None when either is unknown."""
    if not a or not b:
        return None
    return {
        "n": int(a["n"]) + int(b["n"]),
        "sum": str(int(a["sum"]) + int(b["sum"])),
        "xor": str(int(a["xor"]) ^ int(b["xor"])),
    }


def same(a: Optional[Stamp], b: Optional[Stamp]) -> bool:
    return bool(a) and bool(b) and all(str(a.get(k)) == str(b.get(k)) for k in STAMP_KEYS)


def describe(stamp: Optional[Stamp]) -> str:
    return "no fingerprint recorded" if not stamp else f"{int(stamp['n']):,} ids"
