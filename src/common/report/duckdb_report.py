"""Profiles a DuckDB file: per-table row counts, and per-column type,
non-null count and a sample value. Read-only, no writes to the file.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, List

import duckdb


@dataclass
class ColumnReport:
    name: str
    type: str
    non_null_count: int
    row_count: int
    sample: Any

    @property
    def non_null_pct(self) -> float:
        return (self.non_null_count / self.row_count * 100) if self.row_count else 0.0


@dataclass
class TableReport:
    name: str
    row_count: int
    columns: List[ColumnReport] = field(default_factory=list)


@dataclass
class DatabaseReport:
    name: str
    path: Path
    size_mb: float
    tables: List[TableReport] = field(default_factory=list)


def build_database_report(name: str, path: Path) -> DatabaseReport:
    con = duckdb.connect(str(path), read_only=True)
    try:
        table_names = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        tables = [_build_table_report(con, table) for table in table_names]
    finally:
        con.close()
    return DatabaseReport(
        name=name,
        path=path,
        size_mb=path.stat().st_size / (1024**2),
        tables=tables,
    )


def _build_table_report(con: duckdb.DuckDBPyConnection, table: str) -> TableReport:
    described = con.execute(f'DESCRIBE "{table}"').fetchall()
    col_names = [row[0] for row in described]
    col_types = {row[0]: row[1] for row in described}

    # One aggregate query for all counts instead of N+1 — these files can be
    # hundreds of GB (openaire/openalex dumps), so a query per column would
    # mean a full extra table scan per column.
    count_exprs = ", ".join(f'COUNT("{c}") AS "{c}"' for c in col_names)
    row_count, *non_null_counts = con.execute(
        f'SELECT COUNT(*), {count_exprs} FROM "{table}"'
    ).fetchone()
    non_null_by_col = dict(zip(col_names, non_null_counts))

    columns = [
        ColumnReport(
            name=col,
            type=col_types[col],
            non_null_count=non_null_by_col[col],
            row_count=row_count,
            sample=_first_non_null_sample(con, table, col),
        )
        for col in col_names
    ]
    return TableReport(name=table, row_count=row_count, columns=columns)


def _first_non_null_sample(con: duckdb.DuckDBPyConnection, table: str, col: str) -> Any:
    row = con.execute(f'SELECT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL LIMIT 1').fetchone()
    return row[0] if row else None
