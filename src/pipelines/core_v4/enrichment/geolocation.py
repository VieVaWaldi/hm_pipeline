"""
Fills in coordinates for organizations that still have none after ROR and Cordis. Duckdb-aware glue
around GeolocationEnricher (src/enrichment/geolocation/geocoder.py); writes the `geolocation` side
output (id, lat, lon, geolocation_source, confidence), never the staging duckdb.
geolocation_source is "mapbox" (fetched permanent) or "mapbox_temporary"; confidence is
exact | high | medium, or "street" (street-only match whose city was verified).

Tiers: ROR, then Cordis, then Mapbox. ROR and Cordis coordinates already arrive in the staging
table with geolocation_source set (there is no fuzzy ROR tier), so this step only handles orgs with
`geolocation IS NULL` and a real address (street + city + country). Phase 3 lets a Mapbox row fill
exactly those gaps. Only Mapbox answers are written; an address Mapbox could not resolve (or matched
below medium confidence) is remembered in the persistent cache (data/cache/mapbox.duckdb), so a rerun
never pays for it again.

Runs last in the enrichment graph because it spends money. `--max-requests` defaults to 0: nothing is
sent, cached answers are still written. The cache is a single-writer duckdb file: run this step
unsharded, one process at a time.

Usage:
    uv run python -m pipelines.core_v4.enrichment.geolocation --dry-run              # count + est. cost
    uv run python -m pipelines.core_v4.enrichment.geolocation --max-requests 20000   # temporary geocoding, free tier
    uv run python -m pipelines.core_v4.enrichment.geolocation --max-requests 20000 --permanent   # $5/1,000
    uv run python -m pipelines.core_v4.enrichment.geolocation --dry-run --permanent --refresh-temporary
        # later backfill: re-requests only cached temporary hits; the side output is rewritten
    uv run python -m pipelines.core_v4.enrichment.geolocation --test 50              # no HTTP, no writes
"""

import argparse
import logging
from pathlib import Path
from typing import Iterator, List, Optional

import duckdb
import pyarrow as pa

from common.config.settings import get_settings
from common.countries import register_country_macro
from common.log.logger import setup_logging
from enrichment.geolocation.geocoder import Estimate, GeolocationEnricher, InstitutionQuery
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.fingerprint import staging_stamp
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

READ_BATCH = 5_000

_CANDIDATES = """
    SELECT e.id, e.address_street, e.address_postalcode, e.address_city,
           COALESCE(norm_cc(e.address_country), norm_cc(e.countryCode)) AS country
    FROM organization e
    WHERE e.geolocation IS NULL
      AND nullif(trim(e.address_street), '') IS NOT NULL
      AND nullif(trim(e.address_city), '') IS NOT NULL
      AND COALESCE(norm_cc(e.address_country), norm_cc(e.countryCode)) IS NOT NULL
"""


def candidates_sql(shard: Shard = Shard(), exclude_ids_sql: Optional[str] = None, limit: Optional[int] = None) -> str:
    sql = _CANDIDATES
    if shard.sql("e.id"):
        sql += f" AND {shard.sql('e.id')}"
    if exclude_ids_sql:
        sql += f" AND NOT EXISTS (SELECT 1 FROM ({exclude_ids_sql}) d WHERE d.id = e.id)"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql


def _batches(con: duckdb.DuckDBPyConnection, sql: str) -> Iterator[pa.RecordBatch]:
    cursor = con.cursor()  # a cursor is its own connection: temp macros are per connection
    register_country_macro(cursor)
    try:
        yield from cursor.execute(sql).to_arrow_reader(READ_BATCH)
    finally:
        cursor.close()


def _queries(batch: pa.RecordBatch) -> List[InstitutionQuery]:
    cols = {n: batch.column(n).to_pylist() for n in batch.schema.names}
    return [
        InstitutionQuery(id=i, street=s, postalcode=p, city=c, country=cc)
        for i, s, p, c, cc in zip(cols["id"], cols["address_street"], cols["address_postalcode"], cols["address_city"], cols["country"])
    ]


def estimate(con: duckdb.DuckDBPyConnection, enricher: GeolocationEnricher, sql: str) -> Estimate:
    """Whole-run dry-run count: eligible orgs, distinct addresses, cached vs. to-be-billed, est. cost."""
    total = Estimate(0, 0, 0, 0, 0.0)
    seen_keys = set()
    from enrichment.geolocation.geocoder import estimate_cost_usd, query_key

    pending = 0
    for batch in _batches(con, sql):
        queries = _queries(batch)
        total.eligible += len(queries)
        fresh = {query_key(q): q for q in queries if query_key(q) not in seen_keys}
        seen_keys.update(fresh)
        _, todo = enricher.plan(list(fresh))  # only rows that would really be requested
        total.cached += len(fresh) - len(todo)
        pending += len(todo)
    total.unique = len(seen_keys)
    total.to_request = pending
    total.cost_usd = estimate_cost_usd(pending, enricher.permanent)
    return total


def run(
    con: duckdb.DuckDBPyConnection,
    enricher: GeolocationEnricher,
    out: SideOutput,
    *,
    limit: Optional[int] = None,
    write: bool = True,
) -> None:
    register_country_macro(con)
    # a refresh rewrites the whole side output: orgs already written as temporary must be redone, and
    # rows that are not re-requested (budget) fall back to their cached answer, so nothing is lost
    refresh = enricher.permanent and enricher.refresh_temporary
    sql = candidates_sql(out.shard, out.done_ids_sql() if write and not refresh else None, limit)
    if write:
        out.begin(reset=refresh)
    written = 0
    for batch in _batches(con, sql):
        results = enricher.enrich(_queries(batch))
        if results and write:
            out.write(
                pa.table(
                    {
                        "id": pa.array([r.id for r in results], pa.uint64()),
                        "lat": [r.latitude for r in results],
                        "lon": [r.longitude for r in results],
                        "geolocation_source": [r.source for r in results],
                        "confidence": [r.confidence for r in results],
                    }
                )
            )
        written += len(results)
        s = enricher.stats
        logging.info(
            f"geolocation: {written:,} resolved | billed={s.requests:,} cached={s.cache_hits:,} "
            f"rejected={s.rejected:,} deferred(budget)={s.deferred:,}"
        )
    if not write:
        logging.info(f"dry run (--test): {written:,} rows would be written; nothing was written")
    elif enricher.stats.deferred == 0:
        out.finish(staging_stamp(con, "organization"))
    else:
        logging.warning(
            f"{enricher.stats.deferred:,} orgs deferred: the request budget ({enricher.max_requests:,}) ran out, "
            "so no _SUCCESS; rerun with a larger --max-requests (cached answers are free)"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Mapbox geolocation for orgs without ROR/Cordis coordinates.")
    add_common_args(parser, text=False, entities=False)
    parser.add_argument("--max-requests", type=int, default=0, help="hard cap on billed Mapbox queries ($5/1,000); 0 = never spend")
    parser.add_argument("--permanent", action="store_true", help="permanent geocoding ($5/1,000, results storable); default temporary (free tier)")
    parser.add_argument(
        "--refresh-temporary",
        action="store_true",
        help="with --permanent: re-request cached temporary hits (billed as permanent), rewrite the side output",
    )
    parser.add_argument("--dry-run", action="store_true", help="print the number of queries and the estimated cost, send nothing")
    args = parser.parse_args()

    setup_logging("core_v4-geolocation", "run")
    get_settings()  # loads .env (API_KEY_MAPBOX)
    res = resolve(args)
    if args.refresh_temporary and not args.permanent:
        raise SystemExit("--refresh-temporary only makes sense with --permanent")
    if res.shard.count != 1:
        raise SystemExit("geolocation shares one single-writer cache file: run it unsharded")

    out = SideOutput(res.enrichment_dir, "geolocation", "organization", res.shard)
    con = duckdb.connect(res.db, read_only=True)
    register_country_macro(con)
    # --test never sends anything: a budget of 0 answers only from the cache
    enricher = GeolocationEnricher(
        max_requests=0 if res.dry_run else args.max_requests,
        permanent=args.permanent,
        refresh_temporary=args.refresh_temporary,
        cache_path=Path(res.cache_dir) / "mapbox.duckdb",
    )
    try:
        if args.dry_run:
            done = None if args.refresh_temporary else out.done_ids_sql()
            est = estimate(con, enricher, candidates_sql(res.shard, done, res.limit))
            print(
                f"eligible orgs: {est.eligible:,} | distinct addresses: {est.unique:,} | already cached: {est.cached:,} "
                f"| to request: {est.to_request:,} | estimated cost: ${est.cost_usd:,.2f} "
                + ("($5/1,000 up to 500k requests a month, $4/1,000 above" + ("; counts only temporary hits to refresh" if args.refresh_temporary else "") + ")" if args.permanent
                   else "(temporary: first 100k requests/month free, then $0.75/1,000 up to 500k; ignores usage already spent this month)")
            )
            return
        logging.info(f"Mapbox budget: {enricher.max_requests:,} requests")
        run(con, enricher, out, limit=res.limit, write=not res.dry_run)
    finally:
        enricher.close()
        con.close()


if __name__ == "__main__":
    main()
