"""
Core v4 Transformation — seeds core_v4's staging duckdb from OpenAire staging v4
(trimming works to a cap while seeding), then merges in ROR + Cordis columns.

Like core_v3 (src/pipelines/core_v3/transformation.py) but:
  - works are trimmed AT SEED TIME to `work_cap` (never copy 218M works and delete after)
  - country codes are normalised (common.countries)
  - Cordis is matched PIC-first (name + country only as a strict fallback) and also
    fills organization address columns and (when ROR has none) geolocation
See READ_TRANSFORMATION.md for the design and the expected yield.

The target (`core_v4.path_duck_staging`, or the `core_v4_limit` one) is deleted and
rebuilt from scratch on every run. Persistent caches (path_cache_dir) and enrichment
side outputs (path_enrichment_dir) are never touched.

Seed (tables copied from OpenAire staging v4, `openaire_dump.path_duck_staging_v4`):
  - organization, project: whole
  - relation: cascaded to the kept works, and rows whose project or organization endpoint is missing from
    `project` / `organization` are dropped (logged)
  - work: only the `work_cap` best works, see below; `link_tier` SMALLINT (0 = project-linked, 1 = org-only) is kept
    on every work, so the enrichments can run tier 0 first (`--tier 0`) and assemble can build a tier-0 file

Work trim: tier 0 = produced by a project (relation project-produces->product), tier 1 =
authored by an organization (product-hasAuthorInstitution->organization); works with
no relation are dropped. Keep order: tier, publicationDate DESC (NULLs last; dates in
the future are set to NULL first), has a description, id. The last key makes the cut
deterministic when many works share the cutoff date.

Merge (adds columns to the seeded tables, no new tables created):
  - organization: rorStatus, rorEstablished, rorTypes, rorLocations, rorRelationships,
                  geolocation (DOUBLE[] = [lat, lng]), geolocation_source ('ror' | 'cordis' | 'core_v2'),
                  address_street, address_postalcode, address_city, address_country, nuts3
  - relation:     cordis_ec_contribution, cordis_type
  Geolocation tiers, in order, each only where geolocation is still NULL: ROR, Cordis (project-scoped match, then an
  org-level PIC pass), core_v2 (legacy harvested Cordis coordinates, optional file).

Usage:
    uv run python -m pipelines.core_v4.transformation                       # full run
    uv run python -m pipelines.core_v4.transformation --work-cap 10000000   # smaller work set
    uv run python -m pipelines.core_v4.transformation --mem-mb 64000 --threads 8
    uv run python -m pipelines.core_v4.transformation --limit 500           # dev sample (variant limit)

--limit N implies `--variant limit`, which writes ONLY to the `core_v4_limit` paths, so a
limit run can never overwrite a full run. The sample is FK-respecting: N projects
(preferring ones whose grantId exists in Cordis), their relations, the works they
produce, the orgs they touch, plus up to 5*N org-only works so the trim really runs; the
cap in limit mode is min(work_cap, max(3*N, produced + N)). ROR and Cordis are still joined in full.

Prerequisites:
  - OpenAire staging v4 (`python -m sources.dumps.openaire.staging --target v4`)
  - Close all notebooks/kernels that have the duckdb files open (DuckDB is single-writer).
"""

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import duckdb

from common.config.api_runner import get_query_settings
from common.config.dumps import get_dumps_paths
from common.config.pipelines import get_pipeline_paths
from common.countries import register_country_macro
from common.log.logger import setup_logging
from common.log.timer import log_run_time
from pipelines.core_v3.resources import add_resource_args, apply_duckdb_limits

DEFAULT_WORK_CAP = 50_000_000
CORDIS_DB_ENV = "CORE_V4_CORDIS_DB"  # dev only: read this Cordis duckdb instead of the configured full_projects_no_pdfs one
DEFAULT_LIMIT = 1000  # `--variant limit` without --limit
LIMIT_CAP_FACTOR = 3  # limit mode: cap = min(work_cap, max(3 * N, produced works + N)) ...
LIMIT_EXTRA_FACTOR = 5  # ... and up to 5 * N org-only works are added to the sample so the cap binds
TIER_NAMES = {0: "project-linked", 1: "org-only"}


@dataclass
class Paths:
    staging: Path  # OpenAire staging v4 (input)
    ror: Path
    cordis: Path
    out: Path  # core_v4 staging (output, rebuilt from scratch)
    work_cap: int
    core_v2_geo: Optional[Path] = None  # legacy geolocations (optional; skipped when missing)
    core_v2_pic: Optional[Path] = None  # legacy institution_pic, a SEPARATE file (optional; name + country only without it)
    strict_columns: bool = False  # the configured (real) staging must have project.doi and work.countries, see check_staging_columns


def resolve_paths(
    variant: str,
    staging_db: Optional[str] = None,
    work_cap: Optional[int] = None,
    core_v2_geo: Optional[str] = None,
    core_v2_pic: Optional[str] = None,
    cordis_db: Optional[str] = None,
) -> Paths:
    """Config paths for `variant` ('full' or 'limit'). The limit variant only ever gets the
    `core_v4_limit` output path; the cap comes from core_v4.work_cap (a quoted string)."""
    pipelines = get_pipeline_paths()
    if variant == "limit":
        out = pipelines["core_v4_limit"]["path_duck_staging_limit"]
    else:
        out = pipelines["core_v4"]["path_duck_staging"]
    block = pipelines["core_v4_limit" if variant == "limit" else "core_v4"]
    core_v2 = core_v2_geo or block.get("path_core_v2_geolocations")
    core_v2_pics = core_v2_pic or block.get("path_core_v2_institution_pic")
    cap = work_cap if work_cap is not None else int(pipelines["core_v4"].get("work_cap", DEFAULT_WORK_CAP))
    return Paths(
        # a --staging-db override (tests, ad-hoc runs) or the limit variant stays tolerant about missing columns
        strict_columns=variant == "full" and staging_db is None,
        staging=Path(staging_db or get_dumps_paths()["openaire_dump"]["path_duck_staging_v4"]),
        ror=Path(get_dumps_paths()["ror_dump"]["path_duck"]),
        # dev override (`--cordis-db`, or the env var so a Snakemake run picks it up): the full Cordis db is empty locally
        cordis=Path(
            cordis_db
            or os.environ.get(CORDIS_DB_ENV)
            or get_query_settings()["cordis"].queries["full_projects_no_pdfs"].path_duck
        ),
        out=Path(out),
        work_cap=cap,
        core_v2_geo=Path(core_v2) if core_v2 else None,
        core_v2_pic=Path(core_v2_pics) if core_v2_pics else None,
    )


def _reset_outputs(out_db: Path) -> None:
    """Tear down and rebuild: removes the staging duckdb (and a leftover WAL) of a previous run."""
    for path in (out_db, out_db.with_name(out_db.name + ".wal")):
        if path.exists():
            logging.info(f"Removing previous {path}")
            path.unlink()


def _has_column(con: duckdb.DuckDBPyConnection, database: Optional[str], table: str, column: str) -> bool:
    """database None = the target (the connection's own database)."""
    return bool(
        con.execute(
            "SELECT count(*) FROM duckdb_columns() "
            "WHERE database_name = coalesce(?, current_database()) AND table_name = ? AND column_name = ?",
            [database, table, column],
        ).fetchone()[0]
    )


def _one(con: duckdb.DuckDBPyConnection, sql: str) -> Any:
    return con.execute(sql).fetchone()[0]


# Columns of staging v4 that later steps use; an older staging built before they were added would lose them silently.
REQUIRED_STAGING_COLUMNS = {
    ("project", "doi"): "the DOI fallback of the Cordis project match (Cordis projects without a grantId match are lost)",
    ("work", "countries"): "the normalisation of work.countries (common.countries)",
}


def check_staging_columns(con: duckdb.DuckDBPyConnection, strict: bool, allow_missing: bool = False) -> list:
    """Checks the attached `openaire` staging for REQUIRED_STAGING_COLUMNS. With `strict` (the real full run) a missing
    column aborts with the rebuild command, unless `allow_missing`; otherwise it is a WARNING naming the skipped step.
    Returns the missing (table, column) pairs."""
    missing = [tc for tc in REQUIRED_STAGING_COLUMNS if not _has_column(con, "openaire", *tc)]
    if not missing:
        return []
    what = "; ".join(f"openaire.{t}.{c}: {REQUIRED_STAGING_COLUMNS[(t, c)]}" for t, c in missing)
    if strict and not allow_missing:
        raise RuntimeError(
            f"the OpenAire staging v4 lacks column(s) that would be silently skipped ({what}). It was built before they were "
            "added: rebuild it (`python -m sources.dumps.openaire.staging --target v4`, rule stage_openaire_dump_v4), or pass "
            "--allow-missing-columns to run without them."
        )
    for t, c in missing:
        logging.warning(f"OpenAire staging has no {t}.{c}: skipping {REQUIRED_STAGING_COLUMNS[(t, c)]}")
    return missing


# ---------------------------------------------------------------------------
# Sources: whole OpenAire staging (full) or an FK-respecting sample (limit)
# ---------------------------------------------------------------------------
def _define_sources(con: duckdb.DuckDBPyConnection, limit: Optional[int]) -> Optional[int]:
    """Defines temp views _src_{organization,project,work,relation} that the seed reads. In limit mode
    returns the number of works the sample projects produce (None in full mode)."""
    if not limit:
        for table in ("organization", "project", "work", "relation"):
            con.execute(f"CREATE OR REPLACE TEMP VIEW _src_{table} AS SELECT * FROM openaire.{table}")
        return None

    # Projects: prefer ones whose grantId exists in Cordis, then ones that produce works, then by id
    # (ids are hashes, so id order is a fixed pseudo-random order).
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _lim_project AS
            SELECT p.id FROM openaire.project p
            LEFT JOIN (SELECT DISTINCT id_original FROM cordis.project) c ON c.id_original = p.grantId
            LEFT JOIN (SELECT DISTINCT source AS id FROM openaire.relation
                       WHERE sourceType = 'project' AND targetType = 'product') pr ON pr.id = p.id
            ORDER BY (c.id_original IS NULL), (pr.id IS NULL), p.id
            LIMIT {limit}""")
    # Works: everything the sample projects produce, plus org-only works of the orgs they touch.
    con.execute("""CREATE OR REPLACE TEMP TABLE _lim_produced AS
           SELECT DISTINCT r.target AS id FROM openaire.relation r
           WHERE r.sourceType = 'project' AND r.targetType = 'product'
             AND r.source IN (SELECT id FROM _lim_project)
             AND r.target IN (SELECT id FROM openaire.work)""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _lim_work AS
            SELECT id FROM _lim_produced
            UNION
            (SELECT DISTINCT r.source AS id FROM openaire.relation r
             WHERE r.sourceType = 'product' AND r.targetType = 'organization'
               AND r.target IN (SELECT r2.target FROM openaire.relation r2
                                WHERE r2.sourceType = 'project' AND r2.targetType = 'organization'
                                  AND r2.source IN (SELECT id FROM _lim_project))
               AND r.source NOT IN (SELECT id FROM _lim_produced)
               AND r.source IN (SELECT id FROM openaire.work)
             ORDER BY r.source
             LIMIT {LIMIT_EXTRA_FACTOR * limit})""")
    con.execute("""CREATE OR REPLACE TEMP TABLE _lim_relation AS
           SELECT * FROM openaire.relation
           WHERE source IN (SELECT id FROM _lim_project)
              OR (sourceType = 'product' AND source IN (SELECT id FROM _lim_work))""")
    con.execute("""CREATE OR REPLACE TEMP TABLE _lim_org AS
           SELECT DISTINCT target AS id FROM _lim_relation WHERE targetType = 'organization'""")
    con.execute("CREATE OR REPLACE TEMP VIEW _src_relation AS SELECT * FROM _lim_relation")
    con.execute(
        "CREATE OR REPLACE TEMP VIEW _src_project AS "
        "SELECT * FROM openaire.project WHERE id IN (SELECT id FROM _lim_project)"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW _src_work AS SELECT * FROM openaire.work WHERE id IN (SELECT id FROM _lim_work)"
    )
    con.execute(
        "CREATE OR REPLACE TEMP VIEW _src_organization AS "
        "SELECT * FROM openaire.organization WHERE id IN (SELECT id FROM _lim_org)"
    )
    logging.info(
        f"LIMIT sample: {_one(con, 'SELECT count(*) FROM _lim_project'):,} projects, "
        f"{_one(con, 'SELECT count(*) FROM _lim_work'):,} candidate works "
        f"({_one(con, 'SELECT count(*) FROM _lim_produced'):,} produced by the projects), "
        f"{_one(con, 'SELECT count(*) FROM _lim_org'):,} orgs, {_one(con, 'SELECT count(*) FROM _lim_relation'):,} relations"
    )
    return _one(con, "SELECT count(*) FROM _lim_produced")


# ---------------------------------------------------------------------------
# 0. Seed, trimming works
# ---------------------------------------------------------------------------
def _trim_works(con: duckdb.DuckDBPyConnection, work_cap: int) -> Dict[str, Any]:
    """Builds _work_keep (id, tier, d, has_desc): the works to keep. Only narrow tables
    (ids, tier, date, flag) are sorted; the wide work table is joined afterwards."""
    con.execute("""CREATE OR REPLACE TEMP TABLE _work_link AS
           SELECT id, min(tier)::INTEGER AS tier FROM (
               SELECT target AS id, 0 AS tier FROM _src_relation
               WHERE sourceType = 'project' AND targetType = 'product' AND relType.name = 'produces'
               UNION ALL
               SELECT source AS id, 1 AS tier FROM _src_relation
               WHERE sourceType = 'product' AND targetType = 'organization' AND relType.name = 'hasAuthorInstitution'
           ) GROUP BY id""")
    # d = publicationDate, NULL when in the future (bad dates, e.g. 2550-2999)
    con.execute("""CREATE OR REPLACE TEMP TABLE _work_rank AS
           SELECT l.id, l.tier,
                  COALESCE(len(w.descriptions) > 0, false)                       AS has_desc,
                  CASE WHEN w.publicationDate <= current_date THEN w.publicationDate END AS d
           FROM _work_link l JOIN _src_work w ON w.id = l.id""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _work_keep AS
            SELECT id, tier, d, has_desc FROM _work_rank
            ORDER BY tier, d DESC NULLS LAST, has_desc DESC, id
            LIMIT {work_cap}""")

    total = _one(con, "SELECT count(*) FROM _src_work")
    linked = _one(con, "SELECT count(*) FROM _work_rank")
    kept = _one(con, "SELECT count(*) FROM _work_keep")
    future = _one(
        con,
        "SELECT count(*) FROM _src_work w SEMI JOIN _work_link l ON l.id = w.id WHERE w.publicationDate > current_date",
    )
    per_tier = con.execute("""SELECT r.tier, count(*) AS total, count(k.id) AS kept
           FROM _work_rank r LEFT JOIN _work_keep k ON k.id = r.id GROUP BY r.tier ORDER BY r.tier""").fetchall()
    logging.info(
        f"works in staging: {total:,} | linked (kept candidates): {linked:,} | "
        f"unlinked, dropped: {total - linked:,} | future-dated (set to NULL): {future:,}"
    )
    tiers = {}
    for tier, n_total, n_kept in per_tier:
        tiers[tier] = {"total": n_total, "kept": n_kept}
        logging.info(f"  tier {tier} ({TIER_NAMES.get(tier, '?')}): {n_total:,} works, {n_kept:,} kept")
    cutoff = None
    if kept < linked:
        last_tier = _one(con, "SELECT max(tier) FROM _work_keep")
        undated, cut_date = con.execute(
            "SELECT count(*) FILTER (WHERE d IS NULL), min(d) FROM _work_keep WHERE tier = ?", [last_tier]
        ).fetchone()
        cut_date = None if undated else cut_date
        on_date, kept_on_date = con.execute(
            """SELECT count(*), count(k.id) FROM _work_rank r LEFT JOIN _work_keep k ON k.id = r.id
               WHERE r.tier = ? AND r.d IS NOT DISTINCT FROM ?""",
            [last_tier, cut_date],
        ).fetchone()
        cutoff = {"tier": last_tier, "date": cut_date, "on_date": on_date, "kept_on_date": kept_on_date}
        logging.info(
            f"cap {work_cap:,} binds: cutoff in tier {last_tier} ({TIER_NAMES.get(last_tier, '?')}), "
            f"date {cut_date if cut_date is not None else 'undated'}: kept {kept_on_date:,} of {on_date:,} works "
            "on that date (tie-break: has description, then id)"
        )
    else:
        logging.info(f"cap {work_cap:,} does not bind: all {linked:,} linked works kept")
    return {"total": total, "linked": linked, "kept": kept, "tiers": tiers, "cutoff": cutoff, "future": future}


def _seed(con: duckdb.DuckDBPyConnection, work_cap: int) -> Dict[str, Any]:
    stats = _trim_works(con, work_cap)

    con.execute("CREATE OR REPLACE TABLE project AS SELECT * FROM _src_project")
    con.execute(
        "CREATE OR REPLACE TABLE organization AS "
        "SELECT * REPLACE (norm_cc(countryCode) AS countryCode) FROM _src_organization"
    )

    # Join the kept ids to the wide table (no sort of the wide table). Future dates -> NULL,
    # country codes normalised.
    if not _has_column(con, "openaire", "work", "countries"):
        logging.warning("OpenAire staging has no work.countries: work.countries stays as it is (no normalisation)")
    countries = (
        "countries"
        if not _has_column(con, "openaire", "work", "countries")
        else "CASE WHEN countries IS NULL THEN NULL "
        "ELSE list_sort(list_distinct(list_transform(countries, x -> norm_cc(x)))) END"
    )
    replace = "CASE WHEN w.publicationDate <= current_date THEN w.publicationDate END AS publicationDate"
    if _has_column(con, "openaire", "work", "countries"):
        replace += f", {countries.replace('countries', 'w.countries')} AS countries"
    # link_tier (0 = project-linked, 1 = org-only) is the tier the trim ranked the work by; kept as the LAST column
    con.execute(f"""CREATE OR REPLACE TABLE work AS
            SELECT w.* REPLACE ({replace}), k.tier::SMALLINT AS link_tier
            FROM _src_work w JOIN _work_keep k ON k.id = w.id""")

    # Relation cascade: product-side relations only for kept works; all project -> organization. Rows whose project or
    # organization endpoint is not in `project` / `organization` are dropped as well (dangling: nothing to serve them from).
    con.execute("""CREATE OR REPLACE TEMP VIEW _relation_cascade AS
           SELECT * FROM _src_relation WHERE sourceType = 'project' AND targetType = 'organization'
           UNION ALL
           SELECT * FROM _src_relation WHERE sourceType = 'project' AND targetType = 'product'
             AND target IN (SELECT id FROM _work_keep)
           UNION ALL
           SELECT * FROM _src_relation WHERE sourceType = 'product' AND targetType = 'organization'
             AND source IN (SELECT id FROM _work_keep)""")
    no_project = """(sourceType = 'project' AND source NOT IN (SELECT id FROM project))
                    OR (targetType = 'project' AND target NOT IN (SELECT id FROM project))"""
    no_org = """(sourceType = 'organization' AND source NOT IN (SELECT id FROM organization))
                OR (targetType = 'organization' AND target NOT IN (SELECT id FROM organization))"""
    total, n_no_project, n_no_org, n_dropped = con.execute(
        f"""SELECT count(*), count(*) FILTER (WHERE {no_project}), count(*) FILTER (WHERE {no_org}),
                   count(*) FILTER (WHERE ({no_project}) OR ({no_org}))
            FROM _relation_cascade"""
    ).fetchone()
    stats["dangling_endpoints"] = {"project": n_no_project, "organization": n_no_org, "dropped": n_dropped}
    logging.info(
        f"relation rows after the works cascade: {total:,}; dropped because the project is missing: {n_no_project:,}, "
        f"the organization is missing: {n_no_org:,} ({n_dropped:,} rows in all)"
    )
    con.execute(
        f"""CREATE OR REPLACE TABLE relation AS
            SELECT * FROM _relation_cascade WHERE NOT (({no_project}) OR ({no_org}))"""
    )
    for table in ("organization", "project", "work", "relation"):
        logging.info(f"  {table}: {_one(con, f'SELECT count(*) FROM {table}'):,} rows")
    return stats


# ---------------------------------------------------------------------------
# 1. ROR -> organization
# ---------------------------------------------------------------------------
def _merge_ror(con: duckdb.DuckDBPyConnection) -> int:
    for col, typ in (
        ("rorStatus", "VARCHAR"),
        ("rorEstablished", "INTEGER"),
        ("rorTypes", "VARCHAR[]"),
        ("rorLocations", "JSON"),
        ("geolocation", "DOUBLE[]"),  # [lat, lng], same order as core_v3
        ("geolocation_source", "VARCHAR"),
        ("rorRelationships", "JSON"),
    ):
        con.execute(f"ALTER TABLE organization ADD COLUMN IF NOT EXISTS {col} {typ}")

    # geolocation stays NULL (not [NULL, NULL] like core_v3) when ROR has no coordinates.
    con.execute("""UPDATE organization o
           SET
               rorStatus          = r.status,
               rorEstablished     = r.established,
               rorTypes           = r.types,
               rorLocations       = r.locations::JSON,
               geolocation        = CASE WHEN r.locations[1].geonames_details.lat IS NOT NULL
                                          AND r.locations[1].geonames_details.lng IS NOT NULL
                                         THEN [r.locations[1].geonames_details.lat,
                                               r.locations[1].geonames_details.lng] END,
               geolocation_source = CASE WHEN r.locations[1].geonames_details.lat IS NOT NULL
                                          AND r.locations[1].geonames_details.lng IS NOT NULL
                                         THEN 'ror' END,
               rorRelationships   = r.relationships::JSON
           FROM ror.organizations r
           WHERE o.rorId = r.id""")
    enriched = _one(con, "SELECT COUNT(*) FROM organization WHERE rorStatus IS NOT NULL")
    with_geo = _one(con, "SELECT COUNT(*) FROM organization WHERE geolocation_source = 'ror'")
    logging.info(f"organization rows enriched with ROR: {enriched:,} (with coordinates: {with_geo:,})")
    return enriched


# ---------------------------------------------------------------------------
# 2. Cordis -> relation + organization
# ---------------------------------------------------------------------------
def _merge_cordis(con: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    stats: Dict[str, Any] = {}
    for col, typ in (
        ("cordis_ec_contribution", "DOUBLE"),
        ("cordis_type", "VARCHAR"),
    ):
        con.execute(f"ALTER TABLE relation ADD COLUMN IF NOT EXISTS {col} {typ}")
    for col in ("address_street", "address_postalcode", "address_city", "address_country", "nuts3"):
        con.execute(f"ALTER TABLE organization ADD COLUMN IF NOT EXISTS {col} VARCHAR")

    # (a) project match: grantId = id_original; Cordis projects still unmatched -> DOI (lower-cased,
    # without the resolver prefix). A Cordis project can match several OpenAire projects (same grantId).
    con.execute("""CREATE OR REPLACE TEMP TABLE _pm AS
           SELECT cp.id AS cproj, op.id AS oproj, 'grant' AS method
           FROM cordis.project cp JOIN project op ON cp.id_original = op.grantId""")
    if _has_column(con, None, "project", "doi"):
        con.execute("""INSERT INTO _pm
               SELECT cp.id, op.id, 'doi'
               FROM cordis.project cp JOIN project op ON norm_doi(cp.doi) = norm_doi(op.doi)
               WHERE norm_doi(cp.doi) IS NOT NULL
                 AND cp.id NOT IN (SELECT cproj FROM _pm)""")
    else:
        logging.warning("project has no doi column: skipping the DOI fallback")
    n_cp = _one(con, "SELECT count(*) FROM cordis.project")
    by = dict(con.execute("SELECT method, count(DISTINCT cproj) FROM _pm GROUP BY method").fetchall())
    matched_projects = _one(con, "SELECT count(DISTINCT cproj) FROM _pm")
    stats["projects"] = {
        "cordis": n_cp,
        "grant": by.get("grant", 0),
        "doi": by.get("doi", 0),
        "matched": matched_projects,
    }
    logging.info(
        f"Cordis projects matched: {matched_projects:,} / {n_cp:,} ({_pct(matched_projects, n_cp)}) "
        f"| grantId: {by.get('grant', 0):,} | DOI fallback: {by.get('doi', 0):,} "
        "(Phase 1 grantId only: 87,439 / 142,773 = 61.2%)"
    )

    # (b) org match inside matched projects. A triplet = (Cordis project, Cordis institution).
    con.execute("""CREATE OR REPLACE TEMP TABLE _inst AS
           SELECT id,
                  lower(trim(legal_name))                       AS name_key,
                  norm_cc(country)                              AS cc,
                  nullif(trim(street), '')                      AS street,
                  nullif(trim(postalcode), '')                  AS postalcode,
                  nullif(trim(city), '')                        AS city,
                  nullif(trim(nuts_level_3), '')                AS nuts3,
                  -- Cordis stores [lon, lat] (verified against the data); the literal 'null' means missing
                  TRY_CAST(json_extract_string(geolocation, '$[0]') AS DOUBLE) AS lon,
                  TRY_CAST(json_extract_string(geolocation, '$[1]') AS DOUBLE) AS lat
           FROM cordis.institution""")
    con.execute("ALTER TABLE _inst ADD COLUMN has_geo BOOLEAN")
    con.execute("UPDATE _inst SET has_geo = COALESCE(lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180, false)")
    con.execute("""CREATE OR REPLACE TEMP TABLE _trip AS
           SELECT DISTINCT pm.cproj, pm.oproj, jpi.institution_id AS inst_id, nullif(trim(jpi.organization_id), '') AS pic,
                  jpi.ec_contribution, jpi.type
           FROM _pm pm JOIN cordis.j_project_institution jpi ON jpi.project_id = pm.cproj""")
    con.execute("""CREATE OR REPLACE TEMP TABLE _org_pic AS
           SELECT DISTINCT o.id AS org_id, trim(p.value) AS pic
           FROM organization o, UNNEST(o.pids) AS u(p) WHERE p.scheme = 'PIC' AND p.value IS NOT NULL""")
    # PIC first
    con.execute("""CREATE OR REPLACE TEMP TABLE _trip_org AS
           SELECT t.*, op.org_id, 'pic' AS method
           FROM _trip t JOIN _org_pic op ON op.pic = t.pic""")
    # fallback for rows without a PIC match: lower(trim(name)) + normalised country, both sides non-null.
    # Never name only (2.5 pairs per triplet in Phase 1).
    con.execute("""INSERT INTO _trip_org
           SELECT t.*, o.id, 'name_country'
           FROM _trip t
           JOIN _inst i ON i.id = t.inst_id
           JOIN organization o ON lower(trim(o.legalName)) = i.name_key AND i.cc IS NOT NULL AND o.countryCode = i.cc
           WHERE NOT EXISTS (SELECT 1 FROM _trip_org m WHERE m.cproj = t.cproj AND m.inst_id = t.inst_id
                                                          AND m.method = 'pic')""")
    n_trip = _one(con, "SELECT count(DISTINCT (cproj, inst_id)) FROM _trip")
    per_method = {
        m: n
        for m, n in con.execute(
            "SELECT method, count(DISTINCT (cproj, inst_id)) FROM _trip_org GROUP BY method"
        ).fetchall()
    }
    n_matched = _one(con, "SELECT count(DISTINCT (cproj, inst_id)) FROM _trip_org")
    stats["triplets"] = {"total": n_trip, "matched": n_matched, **per_method}
    logging.info(
        f"triplets in matched projects: {n_trip:,} | matched: {n_matched:,} ({_pct(n_matched, n_trip)}) "
        f"| pic: {per_method.get('pic', 0):,} | name_country fallback: {per_method.get('name_country', 0):,} "
        "(Phase 1: PIC 430,591 = 89.7%, name+country 249,755 = 52.0% of 480,133)"
    )

    # (c) relation columns: only project -> organization rows that already exist. One value per
    # (project, org): PIC before name+country, then a known contribution, then the lowest ids.
    con.execute("""CREATE OR REPLACE TEMP TABLE _rel_val AS
           SELECT oproj, org_id, ec_contribution, type FROM _trip_org
           QUALIFY row_number() OVER (PARTITION BY oproj, org_id
                                      ORDER BY (method <> 'pic'), (ec_contribution IS NULL), inst_id, cproj) = 1""")
    con.execute("""UPDATE relation r
           SET cordis_ec_contribution = v.ec_contribution,
               cordis_type            = v.type
           FROM _rel_val v
           WHERE r.sourceType = 'project' AND r.targetType = 'organization'
             AND r.source = v.oproj AND r.target = v.org_id""")
    enriched_rel = _one(con, "SELECT COUNT(*) FROM relation WHERE cordis_type IS NOT NULL")
    stats["relations_enriched"] = enriched_rel
    logging.info(f"relation rows enriched with Cordis: {enriched_rel:,}")

    # (d) addresses for matched orgs: one institution per org. PIC-matched first, then real coordinates,
    # then complete street + city, then highest institution id.
    con.execute(
        """CREATE OR REPLACE TEMP TABLE _org_inst AS
           SELECT m.org_id, m.inst_id, m.method, i.cc, i.street, i.postalcode, i.city, i.nuts3, i.lon, i.lat, i.has_geo
           FROM (SELECT org_id, inst_id, CASE WHEN bool_or(method = 'pic') THEN 'pic' ELSE 'name_country' END AS method
                 FROM _trip_org GROUP BY org_id, inst_id) m
           JOIN _inst i ON i.id = m.inst_id
           QUALIFY row_number() OVER (PARTITION BY m.org_id
                                      ORDER BY (m.method <> 'pic'), NOT i.has_geo,
                                               NOT (i.street IS NOT NULL AND i.city IS NOT NULL), m.inst_id DESC) = 1"""
    )
    # (e) Cordis coordinates only where ROR gave none (inside the helper)
    geo_before = _one(con, "SELECT count(*) FROM organization WHERE geolocation_source = 'cordis'")
    _apply_institutions(con, "_org_inst")
    n_orgs = _one(con, "SELECT count(*) FROM _org_inst")
    n_geo_project = _one(con, "SELECT count(*) FROM organization WHERE geolocation_source = 'cordis'") - geo_before
    stats["orgs_matched"] = n_orgs
    stats["geolocation_cordis_project_pass"] = n_geo_project
    logging.info(
        f"pass 1 (project-scoped): organizations matched: {n_orgs:,} | geolocation adopted from Cordis "
        f"(ROR had none): {n_geo_project:,}"
    )

    # (f) org-level PIC pass: orgs the project-scoped merge did not match, by PIC alone over ALL Cordis
    # j_project_institution rows (the Cordis project need not be matched). Same one-institution rule.
    # PIC only; no name fallback here.
    con.execute("""CREATE OR REPLACE TEMP TABLE _org_inst_pic AS
           SELECT m.org_id, m.inst_id, 'pic' AS method, i.cc, i.street, i.postalcode, i.city, i.nuts3, i.lon, i.lat, i.has_geo
           FROM (SELECT DISTINCT op.org_id, jpi.institution_id AS inst_id
                 FROM _org_pic op
                 JOIN cordis.j_project_institution jpi ON nullif(trim(jpi.organization_id), '') = op.pic
                 WHERE op.org_id NOT IN (SELECT org_id FROM _org_inst)) m
           JOIN _inst i ON i.id = m.inst_id
           QUALIFY row_number() OVER (PARTITION BY m.org_id
                                      ORDER BY NOT i.has_geo, NOT (i.street IS NOT NULL AND i.city IS NOT NULL),
                                               m.inst_id DESC) = 1""")
    geo_before = _one(con, "SELECT count(*) FROM organization WHERE geolocation_source = 'cordis'")
    _apply_institutions(con, "_org_inst_pic")
    n_orgs_pic = _one(con, "SELECT count(*) FROM _org_inst_pic")
    n_geo_pic = _one(con, "SELECT count(*) FROM organization WHERE geolocation_source = 'cordis'") - geo_before
    stats["orgs_matched_org_pic"] = n_orgs_pic
    stats["geolocation_cordis_org_pic_pass"] = n_geo_pic
    logging.info(
        f"pass 2 (org-level PIC): organizations added: {n_orgs_pic:,} | geolocation adopted from Cordis "
        f"(ROR had none): {n_geo_pic:,} "
        "(Phase 1, PIC at org level over both passes: 68,125 orgs, 43,557 with Cordis coordinates and no ROR coordinates)"
    )
    n_addr = _one(
        con, "SELECT count(*) FROM organization WHERE address_street IS NOT NULL AND address_city IS NOT NULL"
    )
    stats["orgs_with_address"] = n_addr
    stats["geolocation_cordis"] = n_geo_project + n_geo_pic
    logging.info(f"organizations with street + city: {n_addr:,}")
    return stats


def _apply_institutions(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Writes the chosen Cordis institution of each org in `table` (org_id, cc, street, postalcode, city, nuts3,
    lon, lat, has_geo) into the address columns, and its coordinates as [lat, lon] when the org has none yet."""
    con.execute(f"""UPDATE organization o
           SET address_street     = c.street,
               address_postalcode = c.postalcode,
               address_city       = c.city,
               address_country    = c.cc,
               nuts3              = c.nuts3
           FROM {table} c WHERE o.id = c.org_id""")
    con.execute(f"""UPDATE organization o
           SET geolocation = [c.lat, c.lon], geolocation_source = 'cordis'
           FROM {table} c
           WHERE o.id = c.org_id AND o.geolocation IS NULL AND c.has_geo""")


# ---------------------------------------------------------------------------
# 3. core_v2 legacy geolocations (free tier after ROR and Cordis)
# ---------------------------------------------------------------------------
def _merge_core_v2(
    con: duckdb.DuckDBPyConnection, core_v2_db: Optional[Path], core_v2_pic_db: Optional[Path] = None
) -> Dict[str, Any]:
    """Coordinates from the harvested core_v2 Cordis institutions, only for orgs whose geolocation is still NULL.
    Both files are optional: no geolocations file = the tier is skipped with a warning (prod must still run); no
    institution_pic file = name + country only."""
    if core_v2_db is None or not core_v2_db.exists():
        logging.warning(f"core_v2 geolocations not found ({core_v2_db}): skipping the core_v2 tier")
        return {"skipped": True}
    con.execute(f"ATTACH '{core_v2_db}' AS core_v2 (READ_ONLY)")
    stats: Dict[str, Any] = {"skipped": False}
    no_geo_before = _one(con, "SELECT count(*) FROM organization WHERE geolocation IS NULL")

    # geolocation is DOUBLE[] = [lon, lat] like every Cordis source: flipped to [lat, lng] on write.
    con.execute("""CREATE OR REPLACE TEMP TABLE _v2_inst AS
           SELECT id, lower(trim(legal_name)) AS name_key, norm_cc(country) AS cc,
                  geolocation[1] AS lon, geolocation[2] AS lat
           FROM core_v2.institution
           WHERE len(geolocation) = 2 AND geolocation[1] BETWEEN -180 AND 180 AND geolocation[2] BETWEEN -90 AND 90""")
    n_inst = _one(con, "SELECT count(*) FROM _v2_inst")

    # Institution PICs live in a SEPARATE duckdb (table institution_pic; institution_id = institution.id, the 32-hex
    # hash). Usable rows: a standard (9 digit) PIC that belongs to exactly one institution. One institution may carry
    # several usable PICs and matches through each. PICs are compared digits only.
    con.execute("CREATE OR REPLACE TEMP TABLE _v2_pic (pic VARCHAR, inst_id VARCHAR)")
    if core_v2_pic_db is None or not core_v2_pic_db.exists():
        logging.warning(f"core_v2 institution_pic not found ({core_v2_pic_db}): name + country path only")
    else:
        con.execute(f"ATTACH '{core_v2_pic_db}' AS core_v2_pic (READ_ONLY)")
        needed = ("institution_id", "pic", "pic_is_standard", "pic_has_multiple_institutions")
        if not all(_has_column(con, "core_v2_pic", "institution_pic", c) for c in needed):
            logging.warning(f"core_v2 institution_pic lacks a table or one of {needed}: name + country path only")
        else:
            con.execute("""INSERT INTO _v2_pic
                   SELECT DISTINCT nullif(regexp_replace(ip.pic::VARCHAR, '[^0-9]', '', 'g'), ''), ip.institution_id::VARCHAR
                   FROM core_v2_pic.institution_pic ip
                   WHERE ip.pic_is_standard AND NOT ip.pic_has_multiple_institutions
                     AND nullif(regexp_replace(ip.pic::VARCHAR, '[^0-9]', '', 'g'), '') IS NOT NULL""")
            n_pic_rows = _one(con, "SELECT count(*) FROM _v2_pic")
            n_resolved = _one(con, "SELECT count(*) FROM _v2_pic p JOIN core_v2.institution i ON i.id = p.inst_id")
            stats["pic_rows_usable"] = n_pic_rows
            logging.info(f"core_v2 institution_pic: {n_pic_rows:,} usable rows, {n_resolved:,} resolve to an institution.id")
            if n_pic_rows and not n_resolved:
                logging.warning("core_v2 institution_pic.institution_id matches no institution.id: PIC path finds nothing")
    con.execute("""CREATE OR REPLACE TEMP TABLE _v2_org_pic AS
           SELECT DISTINCT o.id AS org_id, nullif(regexp_replace(p.value, '[^0-9]', '', 'g'), '') AS pic
           FROM organization o, UNNEST(o.pids) AS u(p)
           WHERE p.scheme = 'PIC' AND p.value IS NOT NULL AND o.geolocation IS NULL""")

    # PIC first; then name + normalised country, only for institutions without a usable PIC row, never name only.
    con.execute("""CREATE OR REPLACE TEMP TABLE _v2_match AS
           SELECT org_id, inst_id, method, lat, lon FROM (
               SELECT op.org_id, i.id AS inst_id, 'pic' AS method, i.lat, i.lon
               FROM _v2_org_pic op JOIN _v2_pic p ON p.pic = op.pic JOIN _v2_inst i ON i.id = p.inst_id
               UNION ALL
               SELECT o.id, i.id, 'name_country', i.lat, i.lon
               FROM organization o
               JOIN _v2_inst i ON lower(trim(o.legalName)) = i.name_key AND i.cc IS NOT NULL AND o.countryCode = i.cc
               WHERE o.geolocation IS NULL AND i.id NOT IN (SELECT inst_id FROM _v2_pic)
           )
           QUALIFY row_number() OVER (PARTITION BY org_id ORDER BY (method <> 'pic'), inst_id) = 1""")
    con.execute("""UPDATE organization o
           SET geolocation = [m.lat, m.lon], geolocation_source = 'core_v2'
           FROM _v2_match m WHERE o.id = m.org_id AND o.geolocation IS NULL""")

    by_method = dict(con.execute("SELECT method, count(*) FROM _v2_match GROUP BY method").fetchall())
    no_geo_after = _one(con, "SELECT count(*) FROM organization WHERE geolocation IS NULL")
    stats.update(
        institutions=n_inst,
        pic=by_method.get("pic", 0),
        name_country=by_method.get("name_country", 0),
        without_coordinates_before=no_geo_before,
        without_coordinates_after=no_geo_after,
    )
    logging.info(
        f"core_v2 tier ({n_inst:,} usable institutions): orgs added by pic: {stats['pic']:,} | "
        f"name_country: {stats['name_country']:,} | orgs without coordinates before: {no_geo_before:,}, "
        f"after: {no_geo_after:,}"
    )
    return stats


def _pct(a: int, b: int) -> str:
    return f"{100.0 * a / b:.1f}%" if b else "n/a"


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def build(
    paths: Paths,
    *,
    limit: Optional[int] = None,
    mem_mb: Optional[int] = None,
    threads: Optional[int] = None,
    allow_missing_columns: bool = False,
) -> Dict[str, Any]:
    """Rebuilds `paths.out` from scratch. Returns the headline numbers (also logged)."""
    if paths.work_cap < 1:
        raise ValueError(f"work_cap must be >= 1, got {paths.work_cap}")

    # before the previous output is removed: an abort here leaves the last good staging alone
    probe = duckdb.connect()
    try:
        probe.execute(f"ATTACH '{paths.staging}' AS openaire (READ_ONLY)")
        check_staging_columns(probe, paths.strict_columns, allow_missing_columns)
    finally:
        probe.close()

    paths.out.parent.mkdir(parents=True, exist_ok=True)
    _reset_outputs(paths.out)
    con = duckdb.connect(str(paths.out))
    if mem_mb is not None and threads is not None:
        apply_duckdb_limits(con, mem_mb, threads)

    try:
        con.execute(f"ATTACH '{paths.staging}' AS openaire (READ_ONLY)")
        con.execute(f"ATTACH '{paths.ror}'     AS ror     (READ_ONLY)")
        con.execute(f"ATTACH '{paths.cordis}'  AS cordis  (READ_ONLY)")
    except Exception as e:
        con.close()
        raise RuntimeError(
            f"Failed to attach a source database: {e}. Close all notebooks/kernels that have these files open."
        ) from e

    register_country_macro(con)  # norm_cc(c)
    con.execute(r"""CREATE OR REPLACE TEMP MACRO norm_doi(d) AS
            nullif(lower(trim(regexp_replace(trim(d), '^(https?://)?(dx\.)?doi\.org/', '', 'i'))), '')""")

    total_start = datetime.now()
    result: Dict[str, Any] = {}
    try:
        logging.info("--- Seeding core tables from OpenAire staging ---")
        t = datetime.now()
        produced = _define_sources(con, limit)
        work_cap = paths.work_cap
        if limit:
            # keeps all works the sample projects produce plus ~N org-only ones, so the cap really binds
            work_cap = min(work_cap, max(LIMIT_CAP_FACTOR * limit, produced + limit))
        result["work_cap"] = work_cap
        logging.info(f"work cap: {work_cap:,}")
        result["seed"] = _seed(con, work_cap)
        log_run_time(t)

        logging.info("--- Merging ROR into organization ---")
        t = datetime.now()
        result["ror_enriched"] = _merge_ror(con)
        log_run_time(t)

        logging.info("--- Merging Cordis into relation and organization ---")
        t = datetime.now()
        result["cordis"] = _merge_cordis(con)
        log_run_time(t)

        logging.info("--- Merging core_v2 legacy geolocations (only where geolocation is still NULL) ---")
        t = datetime.now()
        result["core_v2"] = _merge_core_v2(con, paths.core_v2_geo, paths.core_v2_pic)
        log_run_time(t)

        logging.info("--- Final verification ---")
        org = con.execute("""SELECT COUNT(*), COUNT(rorStatus), COUNT(geolocation), COUNT(address_city),
                      COUNT(*) FILTER (WHERE geolocation_source = 'ror'), COUNT(*) FILTER (WHERE geolocation_source = 'cordis'),
                      COUNT(*) FILTER (WHERE geolocation_source = 'core_v2')
               FROM organization""").fetchone()
        logging.info(
            f"organization — total: {org[0]:,} | rorStatus: {org[1]:,} | geolocation: {org[2]:,} "
            f"(ror {org[4]:,}, cordis {org[5]:,}, core_v2 {org[6]:,}) | address_city: {org[3]:,}"
        )
        rel = con.execute("SELECT COUNT(*), COUNT(cordis_ec_contribution), COUNT(cordis_type) FROM relation").fetchone()
        logging.info(f"relation — total: {rel[0]:,} | cordis_ec_contribution: {rel[1]:,} | cordis_type: {rel[2]:,}")
        dangling = _one(
            con,
            """SELECT count(*) FROM relation r
               WHERE (r.targetType = 'product' AND r.target NOT IN (SELECT id FROM work))
                  OR (r.sourceType = 'product' AND r.source NOT IN (SELECT id FROM work))""",
        )
        logging.info(f"relation rows pointing at a dropped work (must be 0): {dangling:,}")
        result["dangling_relations"] = dangling
        dangling_endpoints = _one(
            con,
            """SELECT count(*) FROM relation r
               WHERE (r.sourceType = 'project' AND r.source NOT IN (SELECT id FROM project))
                  OR (r.targetType = 'organization' AND r.target NOT IN (SELECT id FROM organization))""",
        )
        logging.info(f"relation rows pointing at a missing project or organization (must be 0): {dangling_endpoints:,}")
        result["relation_missing_endpoints"] = dangling_endpoints
        log_run_time(total_start)
        logging.info(f"Database: {paths.out}")
    finally:
        con.close()
    return result


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = argparse.ArgumentParser(
        description="Core v4 transformation: seed from OpenAire staging v4 (trimming works), merge in ROR + Cordis."
    )
    parser.add_argument(
        "--variant",
        choices=["full", "limit"],
        default=None,
        help="config block for the OUTPUT path: core_v4 or core_v4_limit (default: limit when --limit is given, else full)",
    )
    parser.add_argument(
        "--staging-db",
        default=None,
        help="OpenAire staging v4 duckdb (default: config openaire_dump.path_duck_staging_v4)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="dev sample: N projects, their relations, produced works, touched orgs, plus org-only works "
        f"(cap = min(work_cap, max({LIMIT_CAP_FACTOR}*N, produced works + N))). Implies --variant limit; ROR/Cordis are still joined in full.",
    )
    parser.add_argument(
        "--core-v2-geo",
        default=None,
        help="legacy core_v2 geolocations duckdb (default: config path_core_v2_geolocations); skipped when missing",
    )
    parser.add_argument(
        "--core-v2-pic",
        default=None,
        help="legacy core_v2 institution_pic duckdb, a separate file (default: config path_core_v2_institution_pic); "
        "name + country only when missing",
    )
    parser.add_argument(
        "--cordis-db",
        default=None,
        help=f"Cordis duckdb to join (default: config cordis full_projects_no_pdfs, or ${CORDIS_DB_ENV}); dev override, "
        "e.g. the heritage subset when the full db is not loaded locally",
    )
    parser.add_argument(
        "--allow-missing-columns",
        action="store_true",
        help="run although the OpenAire staging lacks project.doi / work.countries (their steps are skipped with a WARNING). "
        "Without it the full run aborts: rebuild the staging (stage_openaire_dump_v4) instead.",
    )
    parser.add_argument(
        "--work-cap",
        type=int,
        default=None,
        help=f"max works to keep (default: config core_v4.work_cap, {DEFAULT_WORK_CAP:,})",
    )
    add_resource_args(parser, default_threads=32)
    args = parser.parse_args(argv)

    if args.limit is not None and args.variant == "full":
        parser.error(
            "--limit is a dev sample and writes to the core_v4_limit paths; it cannot be combined with --variant full"
        )
    variant = args.variant or ("limit" if args.limit else "full")
    limit = args.limit if args.limit else (DEFAULT_LIMIT if variant == "limit" else None)

    setup_logging("transformation", "core_v4")
    paths = resolve_paths(variant, args.staging_db, args.work_cap, args.core_v2_geo, args.core_v2_pic, args.cordis_db)

    logging.info(f"CORE V4 TRANSFORMATION ({variant})")
    logging.info(f"Target:              {paths.out}")
    logging.info(f"OpenAire staging v4: {paths.staging}")
    logging.info(f"ROR:                 {paths.ror}")
    logging.info(f"Cordis:              {paths.cordis}")
    logging.info(f"core_v2 geolocation: {paths.core_v2_geo}")
    logging.info(f"core_v2 institution_pic: {paths.core_v2_pic}")
    logging.info(f"Work cap:            {paths.work_cap:,} (config / --work-cap)")
    logging.info(f"Resources:           {args.mem_mb:,} MB, {args.threads} threads")
    if limit:
        logging.info(f"LIMIT mode: {limit} projects")

    try:
        build(paths, limit=limit, mem_mb=args.mem_mb, threads=args.threads, allow_missing_columns=args.allow_missing_columns)
    except RuntimeError as e:
        logging.error(str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
