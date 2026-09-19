"""
Fills in `organization.geolocation` for core_v3 rows that don't already have
one — duckdb-aware glue around GeolocationEnricher (geocoder.py).

Targets organizations ROR couldn't resolve (no rorId match, so no ROR-derived
geolocation from transformation.py's merge step) using legalName + countryCode
via OpenAlex/Mapbox search instead. Since this only ever touches rows where
geolocation IS NULL, there's no "is the new value different enough to bother"
distance check to make — every result found here is new information.

Usage:
    uv run python -m pipelines.core_v3.enrichment.geolocation
"""

import logging
from typing import List

import duckdb

from common.config.pipelines import get_pipeline_paths
from common.log.logger import setup_logging
from enrichment.geolocation.geocoder import GeolocationEnricher, GeolocationResult, InstitutionQuery

BATCH_SIZE = 32

_UNRESOLVED_ORG_QUERY = """
    SELECT id, legalName, countryCode
    FROM organization
    WHERE geolocation IS NULL AND legalName IS NOT NULL
    ORDER BY id
    LIMIT {limit} OFFSET {offset}
"""


def _batch_iter(con: duckdb.DuckDBPyConnection, offset_start: int = 0):
    offset = offset_start
    while True:
        rows = con.execute(_UNRESOLVED_ORG_QUERY.format(limit=BATCH_SIZE, offset=offset)).fetchall()
        if not rows:
            break
        yield rows
        offset += BATCH_SIZE


def _write_results(con: duckdb.DuckDBPyConnection, results: List[GeolocationResult]) -> None:
    if not results:
        return
    records = [([r.latitude, r.longitude], r.id) for r in results]
    con.executemany("UPDATE organization SET geolocation = ? WHERE id = ?", records)


def run(con: duckdb.DuckDBPyConnection, enricher: GeolocationEnricher, offset: int = 0) -> None:
    stats = {"processed": 0, "updated": 0}
    if offset > 0:
        logging.info(f"Skipping {offset} rows.")

    for idx, batch in enumerate(_batch_iter(con, offset_start=offset)):
        queries = [InstitutionQuery(id=row[0], name=row[1], country=row[2]) for row in batch]
        results = enricher.enrich(queries)

        stats["processed"] += len(batch)
        stats["updated"] += len(results)
        _write_results(con, results)

        logging.info(
            f"batch #{idx}: {len(batch)} orgs, {len(results)} resolved "
            f"(total processed={stats['processed']:,}, updated={stats['updated']:,})"
        )

    logging.info(f"Done. processed={stats['processed']:,} updated={stats['updated']:,}")


def main() -> None:
    setup_logging("enrichment-geolocation", "run")
    db_path = get_pipeline_paths()["core_v3"]["path_staging_duck"]
    logging.info(f"Starting geolocation enrichment against {db_path}")

    con = duckdb.connect(db_path)
    try:
        enricher = GeolocationEnricher()
        run(con, enricher)
    finally:
        con.close()


if __name__ == "__main__":
    main()
