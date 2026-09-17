"""Generates a Markdown data-profile report for every configured DuckDB file.

Mirrors the config -> src/sources / src/pipelines structure under reports/:
    reports/sources/dumps/<name>[_<extra>].md      <- config/dumps.yaml
    reports/sources/external/<name>[_<extra>].md   <- config/external.yaml
    reports/sources/apis/<source>/<query_id>.md    <- config/api_runner.yaml
    reports/pipelines/<pipeline>/<stage>.md        <- config/pipelines.yaml

A duckdb file that doesn't exist yet (that source/stage hasn't been run in
this environment) is skipped rather than treated as an error.
"""

import argparse
import logging
from pathlib import Path
from typing import Dict, Iterator, Optional, Tuple

from common.config.api_runner import get_query_settings
from common.config.dumps import get_dumps_paths
from common.config.external import get_external_paths
from common.config.pipelines import get_pipeline_paths
from common.config.source_paths import SourcePaths
from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging
from common.report.duckdb_report import build_database_report
from common.report.markdown_renderer import render_database_report

REPORTS_ROOT = get_project_root_path() / "reports"


def _duck_label(key: str) -> str:
    """"path_duck" -> "", "path_duck_staging_2" -> "staging_2",
    "path_staging_duck" -> "staging" — config files don't agree on whether the
    duck-ness is a prefix or suffix, so strip both forms."""
    label = key.replace("path_duck", "").replace("path_", "").replace("duck", "")
    return label.strip("_")


def _iter_duck_paths(paths_by_name: Dict[str, SourcePaths]) -> Iterator[Tuple[str, str, Path]]:
    """Yields (source_name, label, duck_path) for every path_duck* entry."""
    for name, paths in paths_by_name.items():
        for key, value in paths.root.items():
            if "duck" not in key:
                continue
            yield name, _duck_label(key), Path(value)


def _iter_dumps() -> Iterator[Tuple[Path, Path]]:
    """Dump reports are versioned: a source's "version" key (e.g. a dump date)
    is appended to the report name, so switching a dump to a newer version
    doesn't clobber the report generated against the previous one."""
    paths_by_name = get_dumps_paths()
    for name, label, duck_path in _iter_duck_paths(paths_by_name):
        report_name = f"{name}_{label}" if label else name
        version = paths_by_name[name].get("version")
        if version:
            report_name = f"{report_name}_{version}"
        yield duck_path, REPORTS_ROOT / "sources" / "dumps" / f"{report_name}.md"


def _iter_external() -> Iterator[Tuple[Path, Path]]:
    for name, label, duck_path in _iter_duck_paths(get_external_paths()):
        report_name = f"{name}_{label}" if label else name
        yield duck_path, REPORTS_ROOT / "sources" / "external" / f"{report_name}.md"


def _iter_apis() -> Iterator[Tuple[Path, Path]]:
    for source, source_cfg in get_query_settings().items():
        for query_id, query in source_cfg.queries.items():
            if not query.path_duck:
                continue
            yield Path(query.path_duck), REPORTS_ROOT / "sources" / "apis" / source / f"{query_id}.md"


def _iter_pipelines() -> Iterator[Tuple[Path, Path]]:
    for name, label, duck_path in _iter_duck_paths(get_pipeline_paths()):
        stage = label or "main"
        yield duck_path, REPORTS_ROOT / "pipelines" / name / f"{stage}.md"


def generate_all_reports(only: Optional[str] = None) -> None:
    for duck_path, out_path in [*_iter_dumps(), *_iter_external(), *_iter_apis(), *_iter_pipelines()]:
        if only and only not in str(out_path):
            continue
        if not duck_path.exists():
            logging.info(f"Skipping {duck_path} (not built yet)")
            continue

        report = build_database_report(name=out_path.stem, path=duck_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(render_database_report(report))
        logging.info(f"Wrote {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate DuckDB data-profile reports")
    parser.add_argument(
        "--only",
        help="Only regenerate reports whose output path contains this substring "
        "(e.g. --only ror, --only pipelines/core_v3)",
    )
    args = parser.parse_args()

    setup_logging("report", "generate_reports")
    generate_all_reports(only=args.only)
