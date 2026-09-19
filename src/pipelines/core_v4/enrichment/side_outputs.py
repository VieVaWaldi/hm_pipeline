"""
Side parquet outputs: how every core_v4 enrichment stores its results.

An enrichment never writes into the staging duckdb (opened read-only). Results
land in
    <enrichment_dir>/<name>/<entity>/part-<shard>-<n>.parquet
keyed by id, and a `_SUCCESS` file marks the directory complete. Phase 3
assembles the final tables from these files.

    out = SideOutput(enrichment_dir, "dch", "work", shard=Shard.parse("1/4"))
    out.begin()                                # drops stale tmp files / stale _SUCCESS
    for batch in ...:
        out.write(pa.table({...}))             # tmp file, then atomic rename
    out.finish()                               # this shard's marker; _SUCCESS once all N shards finished

Resume: anti-join against `out.done_ids_sql()` (parts already on disk).
Sharding: `Shard.sql("id")` is a predicate that splits ids into N disjoint sets,
so shards can run in parallel on separate nodes.

Sparse outputs (theme, minorities, pillars) only hold rows that matched, so
their own ids cannot say what was processed. Either recompute them wholesale
(pure SQL/CPU, cheap) or write a companion `<name>/seen` output.

Tiers (works only). `SideOutput(..., tier=0|1)` is a run over the works of one `link_tier` (0 = project-linked,
1 = org-only). Both tiers write into the SAME directory, so every reader (resume anti-joins, text_sources,
assemble) is unchanged, ids being disjoint between tiers. What differs is the file naming and the markers:
    part-t0-<shard>-<n>.parquet        tier-scoped parts (untiered: part-<shard>-<n>.parquet), so a shard
                                       reset or resume only ever touches its own tier's parts
    _SUCCESS.tier0, _SUCCESS.tier1     that tier is complete: tier 0 can be used long before tier 1 runs
    _SUCCESS                           everything is complete (an untiered run, or both tier markers)
Every marker file holds the staging stamp (fingerprint.py) it was computed against.
"""

import json
import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import NamedTuple, Optional, Union

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.core_v4.enrichment.fingerprint import Stamp, combine

SUCCESS_FILE = "_SUCCESS"
TIERS = (0, 1)
_PART_RE = re.compile(r"^part-(?:t(\d+)-)?(\d+)-(\d+)\.parquet$")  # groups: tier, shard, part number


class Completion(NamedTuple):
    """The marker that says a side output is complete for some scope."""

    path: Path
    tier: Optional[int]  # None = the marker covers every tier
    stamp: Optional[Stamp]  # staging fingerprint recorded by the run; None for a legacy (empty) marker


class ShardStampMismatch(RuntimeError):
    pass


def _read_stamp(path: Path) -> Optional[Stamp]:
    try:
        stamp = json.loads(path.read_text() or "null")
    except (OSError, ValueError):
        return None
    return stamp if isinstance(stamp, dict) and "n" in stamp else None


def _write_stamp(path: Path, stamp: Optional[Stamp]) -> None:
    path.write_text(json.dumps(stamp) if stamp else "")  # a few bytes: no tmp + rename needed


@dataclass(frozen=True)
class Shard:
    """Shard `index` of `count`, from a "--shard I/N" flag (0-based I < N)."""

    index: int = 0
    count: int = 1

    def __post_init__(self):
        if self.count < 1 or not 0 <= self.index < self.count:
            raise ValueError(f"invalid shard {self.index}/{self.count}: need 0 <= I < N")

    @classmethod
    def parse(cls, value: Optional[str]) -> "Shard":
        if not value:
            return cls()
        index, _, count = value.partition("/")
        return cls(int(index), int(count))

    def sql(self, id_expr: str = "id") -> Optional[str]:
        """Predicate selecting this shard's ids (ids are UBIGINT hashes); None when unsharded."""
        if self.count == 1:
            return None
        return f"({id_expr} % {self.count}) = {self.index}"

    def __str__(self) -> str:
        return f"{self.index}/{self.count}"


def add_shard_arg(parser) -> None:
    parser.add_argument(
        "--shard",
        default="0/1",
        help="I/N: process only ids with id %% N == I, so N jobs can run in parallel on separate nodes.",
    )


# Column contract of every side output (Phase 2 shared contract). ids are the UBIGINT hashes
# used by project/work/organization.
SCHEMAS = {
    "nllb": pa.schema(
        [("id", pa.uint64()), ("field", pa.string()), ("text_en", pa.string()), ("src_lang", pa.string())]
    ),
    "nllb/seen": pa.schema(
        [("id", pa.uint64()), ("field", pa.string()), ("src_lang", pa.string()), ("translated", pa.bool_())]
    ),
    "topics": pa.schema([("id", pa.uint64()), ("topic_id", pa.int32()), ("score", pa.float32())]),
    "theme": pa.schema([("id", pa.uint64()), ("theme", pa.string())]),
    "dch": pa.schema([("id", pa.uint64()), ("is_ch", pa.bool_()), ("pred", pa.float32())]),
    "minorities": pa.schema([("id", pa.uint64()), ("minority_qid", pa.list_(pa.string()))]),
    "pillars": pa.schema([("id", pa.uint64()), ("pillars", pa.uint8())]),
    "geolocation": pa.schema(
        [
            ("id", pa.uint64()),
            ("lat", pa.float64()),
            ("lon", pa.float64()),
            ("geolocation_source", pa.string()),
            ("confidence", pa.string()),
        ]
    ),
    "regions": pa.schema([("id", pa.uint64()), ("region", pa.string())]),
}

# The pillars bitmask, lowest bit first.
PILLARS = ["inclusive", "sustainable", "resilient", "innovative", "global"]


def _sql_str(value: Union[str, Path]) -> str:
    return "'" + str(value).replace("'", "''") + "'"


class SideOutput:
    def __init__(
        self,
        enrichment_dir: Union[str, Path],
        name: str,
        entity: str,
        shard: Shard = Shard(),
        schema: Optional[pa.Schema] = None,
        tier: Optional[int] = None,
    ):
        if tier is not None and tier not in TIERS:
            raise ValueError(f"tier must be one of {TIERS} or None, got {tier!r}")
        self.dir = Path(enrichment_dir) / name / entity
        self.name = name
        self.entity = entity
        self.shard = shard
        self.schema = schema if schema is not None else SCHEMAS.get(name)
        self.tier = tier

    # ---- paths ---------------------------------------------------------------------------
    @property
    def success_path(self) -> Path:
        return self.dir / SUCCESS_FILE

    def tier_success_path(self, tier: int) -> Path:
        return self.dir / f"{SUCCESS_FILE}.tier{tier}"

    @property
    def tag(self) -> str:
        """File-name tag of this run's tier ("" untiered, "t0-", "t1-")."""
        return "" if self.tier is None else f"t{self.tier}-"

    def _shard_marker(self, index: int) -> Path:
        tag = "" if self.tier is None else f"t{self.tier}."
        return self.dir / f"{SUCCESS_FILE}.{tag}{index}-of-{self.shard.count}"

    def parts(self) -> list:
        """Every part of the directory, all tiers and shards."""
        if not self.dir.is_dir():
            return []
        return sorted(p for p in self.dir.iterdir() if _PART_RE.match(p.name))

    def _own_parts(self) -> list:
        """The parts this run's (tier, shard) wrote: what a reset drops and what numbering continues."""
        own = []
        for p in self.parts():
            m = _PART_RE.match(p.name)
            tier = int(m.group(1)) if m.group(1) is not None else None
            if tier == self.tier and int(m.group(2)) == self.shard.index:
                own.append((p, int(m.group(3))))
        return own

    @property
    def glob(self) -> str:
        return str(self.dir / "part-*.parquet")

    # ---- lifecycle -----------------------------------------------------------------------
    def begin(self, reset: bool = False) -> None:
        """Call at the start of a run: removes half-written tmp files and the completion markers
        (this run may add rows). `reset` also deletes this (tier, shard)'s existing parts.
        A tier run only invalidates its own tier marker, so tier 0 stays usable while tier 1 runs."""
        self.dir.mkdir(parents=True, exist_ok=True)
        for tmp in self.dir.glob("*.tmp"):
            tmp.unlink()
        self.success_path.unlink(missing_ok=True)
        for t in TIERS if self.tier is None else (self.tier,):
            self.tier_success_path(t).unlink(missing_ok=True)
        self._shard_marker(self.shard.index).unlink(missing_ok=True)
        if reset:
            for part, _ in self._own_parts():
                part.unlink()

    def _next_part_index(self) -> int:
        return max((n for _, n in self._own_parts()), default=-1) + 1

    def write(self, data) -> Optional[Path]:
        """Appends one part file (pyarrow Table/RecordBatch or pandas DataFrame). Written to a tmp
        file, then renamed, so a kill never leaves a torn part. Empty input writes nothing."""
        if not isinstance(data, (pa.Table, pa.RecordBatch)):
            data = pa.Table.from_pandas(data, preserve_index=False)
        table = pa.Table.from_batches([data]) if isinstance(data, pa.RecordBatch) else data
        if table.num_rows == 0:
            return None
        if self.schema is not None:
            table = table.select(self.schema.names).cast(self.schema)
        self.dir.mkdir(parents=True, exist_ok=True)
        final = self.dir / f"part-{self.tag}{self.shard.index:03d}-{self._next_part_index():06d}.parquet"
        tmp = final.with_name(final.name + ".tmp")
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, final)
        return final

    def finish(self, stamp: Optional[Stamp] = None) -> bool:
        """Marks this shard done (recording the staging `stamp` it ran against); once every one of the N shards
        has finished, writes `_SUCCESS` (untiered run) or `_SUCCESS.tier<T>` (tier run; plus `_SUCCESS` when the
        other tier is complete too). Returns whether this run's whole scope is now complete.
        Shards that ran against different stagings never complete (ShardStampMismatch)."""
        self.dir.mkdir(parents=True, exist_ok=True)
        _write_stamp(self._shard_marker(self.shard.index), stamp)
        markers = [self._shard_marker(i) for i in range(self.shard.count)]
        if not all(m.exists() for m in markers):
            logging.info(f"{self.name}/{self.entity}: shard {self.shard} finished, waiting for the other shards")
            return False
        stamps = [_read_stamp(m) for m in markers]
        if any(s != stamps[0] for s in stamps):
            raise ShardStampMismatch(
                f"{self.name}/{self.entity}: the shards ran against different stagings ({[s and s['n'] for s in stamps]} ids); "
                "rerun them against the current staging"
            )
        for m in markers:
            m.unlink(missing_ok=True)
        if self.tier is None:
            _write_stamp(self.success_path, stamps[0])
        else:
            _write_stamp(self.tier_success_path(self.tier), stamps[0])
            others = [t for t in TIERS if t != self.tier]
            if all(self.tier_success_path(t).exists() for t in others):
                total = stamps[0]
                for t in others:
                    total = combine(total, _read_stamp(self.tier_success_path(t)))
                _write_stamp(self.success_path, total)
        logging.info(f"{self.name}/{self.entity}: complete{'' if self.tier is None else f' for tier {self.tier}'} ({self.dir})")
        return True

    def completion(self, tier: Optional[int] = None) -> Optional[Completion]:
        """The marker that covers `tier` (None = every tier): `_SUCCESS`, else that tier's own marker; None when
        the output is not complete for it."""
        if self.success_path.exists():
            return Completion(self.success_path, None, _read_stamp(self.success_path))
        if tier is not None and self.tier_success_path(tier).exists():
            path = self.tier_success_path(tier)
            return Completion(path, tier, _read_stamp(path))
        return None

    def is_complete(self, tier: Optional[int] = None) -> bool:
        """Complete for `tier` (default: this output's own tier; None = everything)."""
        return self.completion(self.tier if tier is None else tier) is not None

    # ---- reading -------------------------------------------------------------------------
    def _empty_sql(self) -> str:
        if self.schema is None:
            raise ValueError(f"{self.name}/{self.entity}: no parts yet and no schema to build an empty relation from")
        cols = ", ".join(f"NULL::{_duck_type(f.type)} AS {f.name}" for f in self.schema)
        return f"SELECT {cols} WHERE false"

    def read_all_sql(self) -> str:
        """SELECT over every part (union_by_name), or an empty relation with the schema when there are none."""
        if not self.parts():
            return self._empty_sql()
        return f"SELECT * FROM read_parquet({_sql_str(self.glob)}, union_by_name=true)"

    def read_all(self, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        return con.sql(self.read_all_sql())

    def done_ids_sql(self, id_col: str = "id") -> str:
        """SELECT DISTINCT <id_col> over the parts already written, for resume anti-joins:
            ... WHERE NOT EXISTS (SELECT 1 FROM (<done_ids_sql>) d WHERE d.id = s.id)"""
        if not self.parts():
            return f"SELECT NULL::UBIGINT AS {id_col} WHERE false"
        return f"SELECT DISTINCT {id_col} FROM read_parquet({_sql_str(self.glob)}, union_by_name=true)"

    def done_ids(self, con: duckdb.DuckDBPyConnection) -> duckdb.DuckDBPyRelation:
        """The done-ids relation; anti-join with `rel.set_alias("d")` or use done_ids_sql()."""
        return con.sql(self.done_ids_sql())


def _duck_type(t: pa.DataType) -> str:
    if pa.types.is_uint64(t):
        return "UBIGINT"
    if pa.types.is_uint8(t):
        return "UTINYINT"
    if pa.types.is_int32(t):
        return "INTEGER"
    if pa.types.is_float32(t):
        return "FLOAT"
    if pa.types.is_float64(t):
        return "DOUBLE"
    if pa.types.is_boolean(t):
        return "BOOLEAN"
    if pa.types.is_string(t):
        return "VARCHAR"
    if pa.types.is_list(t):
        return _duck_type(t.value_type) + "[]"
    raise ValueError(f"unsupported type {t}")
