"""
Fills in `organization.geolocation` for core_v3 rows that don't already have
one — duckdb-aware glue around GeolocationEnricher (geocoder.py).

Targets organizations ROR's id-join couldn't resolve (no rorId, so no
ROR-derived geolocation from transformation.py's merge step). The only tier is
Mapbox: off unless you grant a request budget, and only fires for rows with
city + country, which core_v3's organization table doesn't have — so today it's
inert here; it's wired for core_v4. NOT part of the `core_v3` Snakemake target —
run by name (against core_v3_staging_2) if needed.

Since this only touches rows where geolocation IS NULL there's no "is the new
value different enough" distance check — every result is new information.

Usage:
    uv run python -m pipelines.core_v3.enrichment.geolocation
    uv run python -m pipelines.core_v3.enrichment.geolocation --max-mapbox-requests 1000   # spends money
"""

import argparse
import logging
from typing import Iterator, List

import duckdb

from common.config.pipelines import get_pipeline_paths
from common.log.logger import setup_logging
from enrichment.geolocation.geocoder import GeolocationEnricher, GeolocationResult, InstitutionQuery

BATCH_SIZE = 256

_UNRESOLVED_ORG_QUERY = """
    SELECT id, legalName, countryCode
    FROM organization
    WHERE geolocation IS NULL AND legalName IS NOT NULL AND id > {last_id}
    ORDER BY id
    LIMIT {limit}
"""


def _batch_iter(con: duckdb.DuckDBPyConnection) -> Iterator[list]:
    # Keyset pagination (id > last_id), not OFFSET: rows resolved in earlier
    # batches leave the `geolocation IS NULL` set, so an offset would skip
    # unprocessed rows.
    last_id = -1
    while True:
        rows = con.execute(_UNRESOLVED_ORG_QUERY.format(limit=BATCH_SIZE, last_id=last_id)).fetchall()
        if not rows:
            break
        yield rows
        last_id = rows[-1][0]


def _write_results(con: duckdb.DuckDBPyConnection, results: List[GeolocationResult]) -> None:
    if not results:
        return
    records = [([r.latitude, r.longitude], r.id) for r in results]
    con.executemany("UPDATE organization SET geolocation = ? WHERE id = ?", records)


def run(con: duckdb.DuckDBPyConnection, enricher: GeolocationEnricher) -> None:
    stats = {"processed": 0, "updated": 0}

    for idx, batch in enumerate(_batch_iter(con)):
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
    parser = argparse.ArgumentParser(description="Fill in organization.geolocation for core_v3.")
    parser.add_argument(
        "--max-mapbox-requests",
        type=int,
        default=0,
        help="Hard cap on paid Mapbox permanent-geocoding requests ($5/1,000). 0 = never spend.",
    )
    args = parser.parse_args()

    setup_logging("enrichment-geolocation", "run")
    db_path = get_pipeline_paths()["core_v3"]["path_duck_staging_2"]
    logging.info(f"Starting geolocation enrichment against {db_path}")
    logging.info(f"Mapbox budget: {args.max_mapbox_requests} requests")

    enricher = GeolocationEnricher(max_mapbox_requests=args.max_mapbox_requests)

    con = duckdb.connect(db_path)
    try:
        run(con, enricher)
    finally:
        con.close()


if __name__ == "__main__":
    main()
