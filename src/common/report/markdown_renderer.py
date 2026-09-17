"""Renders a DatabaseReport as a Markdown document."""

import json
from datetime import datetime
from typing import List

from common.file_handling.path_utils import get_project_root_path
from common.report.duckdb_report import ColumnReport, DatabaseReport, TableReport

SAMPLE_MAX_LEN = 200


def render_database_report(report: DatabaseReport) -> str:
    lines = [
        f"# {report.name} - Last updated: {datetime.now().strftime('%d-%m-%Y')}",
        "",
        f"- **Path:** `{_display_path(report.path)}`",
        f"- **Size:** {report.size_mb:.1f} MB",
        f"- **Tables:** {len(report.tables)}",
        "",
    ]
    lines.extend(_render_tldr(report.tables))
    for table in report.tables:
        lines.extend(_render_table(table))
    return "\n".join(lines) + "\n"


def _display_path(path) -> str:
    """Repo-relative path when possible (dev) — falls back to the absolute
    path when it lives outside the repo (prod, under settings.hpc_root)."""
    try:
        return str(path.relative_to(get_project_root_path()))
    except ValueError:
        return str(path)


def _render_tldr(tables: List[TableReport]) -> List[str]:
    lines = [
        "## TL;DR",
        "",
        "| Table | Rows | Columns |",
        "|---|---|---|",
    ]
    lines.extend(f"| {table.name} | {table.row_count:,} | {len(table.columns)} |" for table in tables)
    lines.append("")
    return lines


def _render_table(table: TableReport) -> List[str]:
    lines = [
        f"## **{table.name}** - {table.row_count:,} rows",
        "",
        "| Column | Type | NotNull | Sample |",
        "|---|---|---|---|",
    ]
    lines.extend(_render_column_row(col) for col in table.columns)
    lines.append("")
    return lines


def _render_column_row(col: ColumnReport) -> str:
    not_null = f"{col.non_null_count:,} ({col.non_null_pct:.0f}%)"
    return f"| {col.name} | {col.type} | {not_null} | {_format_sample(col.sample)} |"


def _format_sample(value) -> str:
    if value is None:
        return ""
    text = json.dumps(value, default=str) if isinstance(value, (dict, list)) else str(value)
    text = text.replace("|", "\\|").replace("\n", " ")
    if len(text) > SAMPLE_MAX_LEN:
        text = text[:SAMPLE_MAX_LEN] + "…"
    return text
