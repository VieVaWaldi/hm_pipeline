"""
European region per organization, from its country. Pure SQL against staging (read-only), writes the
`regions` side output (id, region). Rows without any country get no row.

Country: COALESCE(countryCode, ROR country, address_country), each normalised through
common.countries (EL->GR, UK->GB, ...). "ROR country" is read from `rorCountryCode` if staging has
that column, else from `rorLocations` (JSON, first location's geonames_details.country_code) if
present, else skipped.

Region table: src/enrichment/regions/country_regions.csv.

Usage:
    uv run python -m pipelines.core_v4.enrichment.regions
    uv run python -m pipelines.core_v4.enrichment.regions --shard 0/4 --test 20
"""

import argparse
import logging
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow as pa

from common.countries import register_country_macro
from common.log.logger import setup_logging
from pipelines.core_v4.enrichment.cli import add_common_args, resolve
from pipelines.core_v4.enrichment.fingerprint import staging_stamp
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

COUNTRY_REGIONS_CSV = Path(__file__).resolve().parents[3] / "enrichment" / "regions" / "country_regions.csv"
BATCH_ROWS = 100_000


def _ror_country_expr(con: duckdb.DuckDBPyConnection) -> str:
    cols = {r[0]: r[1] for r in con.execute(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'organization'"
    ).fetchall()}
    if "rorCountryCode" in cols:
        return "e.rorCountryCode"
    if "rorLocations" in cols:
        return "json_extract_string(e.rorLocations::JSON, '$[0].geonames_details.country_code')"
    return "NULL"


def regions_sql(con: duckdb.DuckDBPyConnection, shard: Shard = Shard(), exclude_ids_sql: Optional[str] = None, limit: Optional[int] = None) -> str:
    country = f"COALESCE(norm_cc(e.countryCode), norm_cc({_ror_country_expr(con)}), norm_cc(e.address_country))"
    where = ["c.region IS NOT NULL"]
    if shard.sql("e.id"):
        where.append(shard.sql("e.id"))
    if exclude_ids_sql:
        where.append(f"NOT EXISTS (SELECT 1 FROM ({exclude_ids_sql}) d WHERE d.id = e.id)")
    sql = (
        f"SELECT e.id, c.region FROM organization e "
        f"LEFT JOIN read_csv('{COUNTRY_REGIONS_CSV}', header=true) c ON c.alpha2 = {country} "
        f"WHERE {' AND '.join(where)}"
    )
    return sql + (f" LIMIT {int(limit)}" if limit is not None else "")


def run(con: duckdb.DuckDBPyConnection, out: SideOutput, *, limit: Optional[int] = None, write: bool = True) -> int:
    if write:
        out.begin()
    sql = regions_sql(con, out.shard, out.done_ids_sql() if write else None, limit)
    total = 0
    cursor = con.cursor()  # a cursor is its own connection: temp macros are per connection
    register_country_macro(cursor)
    for batch in cursor.execute(sql).to_arrow_reader(BATCH_ROWS):
        total += batch.num_rows
        if write:
            out.write(pa.Table.from_batches([batch]))
    cursor.close()
    if write:
        out.finish(staging_stamp(con, "organization"))
    logging.info(f"regions: {total:,} rows {'written' if write else 'computed (--test, nothing written)'}")
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description="Assign a European region to every organization with a country.")
    add_common_args(parser, text=False, entities=False)
    args = parser.parse_args()
    setup_logging("core_v4-regions", "run")
    res = resolve(args)
    out = SideOutput(res.enrichment_dir, "regions", "organization", res.shard)
    con = duckdb.connect(res.db, read_only=True)
    try:
        run(con, out, limit=res.limit, write=not res.dry_run)
    finally:
        con.close()


if __name__ == "__main__":
    main()
