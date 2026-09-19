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
"""

import logging
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

SUCCESS_FILE = "_SUCCESS"
_PART_RE = re.compile(r"^part-(\d+)-(\d+)\.parquet$")


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
    ):
        self.dir = Path(enrichment_dir) / name / entity
        self.name = name
        self.entity = entity
        self.shard = shard
        self.schema = schema if schema is not None else SCHEMAS.get(name)

    # ---- paths ---------------------------------------------------------------------------
    @property
    def success_path(self) -> Path:
        return self.dir / SUCCESS_FILE

    def _shard_marker(self, index: int) -> Path:
        return self.dir / f"{SUCCESS_FILE}.{index}-of-{self.shard.count}"

    def parts(self) -> list:
        if not self.dir.is_dir():
            return []
        return sorted(p for p in self.dir.iterdir() if _PART_RE.match(p.name))

    @property
    def glob(self) -> str:
        return str(self.dir / "part-*.parquet")

    # ---- lifecycle -----------------------------------------------------------------------
    def begin(self, reset: bool = False) -> None:
        """Call at the start of a run: removes half-written tmp files and the completion markers
        (this run may add rows). `reset` also deletes this shard's existing parts."""
        self.dir.mkdir(parents=True, exist_ok=True)
        for tmp in self.dir.glob("*.tmp"):
            tmp.unlink()
        self.success_path.unlink(missing_ok=True)
        self._shard_marker(self.shard.index).unlink(missing_ok=True)
        if reset:
            for part in self.parts():
                if int(_PART_RE.match(part.name).group(1)) == self.shard.index:
                    part.unlink()

    def _next_part_index(self) -> int:
        used = [int(m.group(2)) for p in self.parts() if (m := _PART_RE.match(p.name)) and int(m.group(1)) == self.shard.index]
        return max(used, default=-1) + 1

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
        final = self.dir / f"part-{self.shard.index:03d}-{self._next_part_index():06d}.parquet"
        tmp = final.with_name(final.name + ".tmp")
        pq.write_table(table, tmp, compression="zstd")
        os.replace(tmp, final)
        return final

    def finish(self) -> bool:
        """Marks this shard done; writes `_SUCCESS` once every one of the N shards has finished.
        Returns whether the whole output is now complete."""
        self.dir.mkdir(parents=True, exist_ok=True)
        self._shard_marker(self.shard.index).touch()
        if all(self._shard_marker(i).exists() for i in range(self.shard.count)):
            self.success_path.touch()
            for i in range(self.shard.count):
                self._shard_marker(i).unlink(missing_ok=True)
            logging.info(f"{self.name}/{self.entity}: complete ({self.dir})")
            return True
        logging.info(f"{self.name}/{self.entity}: shard {self.shard} finished, waiting for the other shards")
        return False

    def is_complete(self) -> bool:
        return self.success_path.exists()

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
