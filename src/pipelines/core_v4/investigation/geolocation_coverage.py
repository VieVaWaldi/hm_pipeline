"""core_v4 geolocation coverage (read-only diagnostic).

Question: how many organisations get coordinates from each FREE tier of the core_v4 transformation
(ROR -> Cordis -> core_v2), and above all how many PROJECTS have participating organisations with
coordinates? A collaboration on a map needs at least two geolocated participants per project.

The coordinate tiers of src/pipelines/core_v4/transformation.py are reproduced at organisation level,
WITHOUT running the transformation (no work trim, no relation writes, no big copies). The SQL below is
copied from transformation.py (and READ_TRANSFORMATION.md sections 3 to 5); the comments name the function it
comes from. It deliberately does NOT import transformation.py, so it keeps working while that file changes.
If the matching rules there change, mirror them here.

Tiers, in order, each only for organisations that still have no coordinates:
  ror                   organization.rorId = ror.organizations.id, locations[1].geonames_details lat/lng
  cordis_project        Cordis project-scoped merge (_merge_cordis pass 1): Cordis project matched to an OpenAire
                        project on grantId (then DOI), Cordis institution matched to the org by PIC first,
                        name + normalised country as a strict fallback, one chosen institution per org
  cordis_pic            Cordis org-level PIC pass (_merge_cordis pass 2): orgs pass 1 did not match, PIC only,
                        over ALL Cordis j_project_institution rows
  core_v2_pic           core_v2 legacy harvest (_merge_core_v2), usable PIC rows only
  core_v2_name_country  core_v2 legacy harvest, lower(trim(name)) + normalised country, only for institutions
                        without a usable PIC row; never name only

Inputs (every database is attached READ_ONLY; nothing is written into them):
  OpenAire staging   default dumps.yaml openaire_dump.path_duck_staging_2 (organization, project, relation);
                     the v4 staging works too (--staging-db). Without project.doi the DOI fallback of the
                     project match is skipped (a v4 staging has it, staging_2 does not).
  ROR                dumps.yaml ror_dump.path_duck, table organizations
  Cordis             config cordis full_projects_no_pdfs (or $CORE_V4_CORDIS_DB, or --cordis-db)
  core_v2 (optional) pipelines.yaml core_v4.path_core_v2_geolocations / path_core_v2_institution_pic
                     (a missing geolocations file skips both core_v2 tiers, a missing pic file leaves name + country)

Metrics (markdown tables to stdout, the same numbers as JSON next to this file, override with --out):
  1. organisations: cumulative coordinates by tier, incremental gain, still without
  2. the same for organisations that participate in at least one project (relation hasParticipant)
  3. projects (the key result): >=1 / >=2 participants, >=1 / >=2 geolocated, >=2 at distinct coordinates,
     cumulative by tier, also as a share of the projects with >=2 participants
  4. Mapbox headroom: participants without coordinates that have a real Cordis address (street + city + country),
     and the collaboration-capable projects that geocoding all of them would add (an upper bound; nothing is called)
  5. sanity: orgs matched to several Cordis institutions, PIC conflicts, organisations without a country code

Heavy part: the relation table (~300M rows on prod) is filtered to relType.name = 'hasParticipant' first and
aggregated in SQL only. Intermediate tables go to a temp DuckDB in --tmp-dir (removed at the end), never to an
input. Run it on SLURM:

  sbatch --partition=fat,standard,long --cpus-per-task=16 --mem=128G --time=02:00:00 \\
    --wrap "cd /vast/lu72hip/hm_pipeline && ENV=prod uv run python -m pipelines.core_v4.investigation.geolocation_coverage \\
    --mem-mb 128000 --threads 16 > data/logs/geolocation_coverage.log 2>&1"
"""

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import date
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

import duckdb

from common.countries import register_country_macro

CORDIS_DB_ENV = (
    "CORE_V4_CORDIS_DB"  # same env var as transformation.py: read this Cordis duckdb instead of the configured one
)
COORD_DECIMALS = 4  # "distinct coordinates" compares lat/lon rounded to 4 decimals (about 10 m)

# (key, label) in the order the tiers are applied; rank = position + 1
TIERS = [
    ("ror", "ROR"),
    ("cordis_project", "Cordis, project-scoped merge"),
    ("cordis_pic", "Cordis, org-level PIC pass"),
    ("core_v2_pic", "core_v2, PIC"),
    ("core_v2_name_country", "core_v2, name + country"),
]

OUT: Dict[str, Any] = {}


# ---------------------------------------------------------------------------
# small helpers (same style as phase1_measurements.py)
# ---------------------------------------------------------------------------
def md_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        lines.append("| " + " | ".join(fmt(c) for c in r) + " |")
    return "\n".join(lines)


def fmt(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        return f"{v:,.2f}"
    return str(v)


def pct(a, b):
    return round(100.0 * a / b, 2) if b else None


def section(title):
    print(f"\n## {title}\n", flush=True)


def warn(msg: str) -> None:
    print(f"WARNING: {msg}", file=sys.stderr, flush=True)


def progress(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", file=sys.stderr, flush=True)


def _one(con, sql: str, params=None) -> Any:
    return con.execute(sql, params or []).fetchone()[0]


def _has_column(con, database: str, table: str, column: str) -> bool:
    return bool(
        con.execute(
            "SELECT count(*) FROM duckdb_columns() WHERE database_name = ? AND table_name = ? AND column_name = ?",
            [database, table, column],
        ).fetchone()[0]
    )


# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------
def resolve_inputs(args: argparse.Namespace) -> Dict[str, Optional[str]]:
    """Config paths with CLI overrides. The config helpers are only imported when a default is needed, so a
    fully overridden run (tests) does not depend on the configured paths."""
    staging, ror, cordis = args.staging_db, args.ror_db, args.cordis_db or os.environ.get(CORDIS_DB_ENV)
    if not (staging and ror):
        from common.config.dumps import get_dumps_paths

        dumps = get_dumps_paths()
        staging = staging or dumps["openaire_dump"]["path_duck_staging_2"]
        ror = ror or dumps["ror_dump"]["path_duck"]
    if not cordis:
        from common.config.api_runner import get_query_settings

        cordis = get_query_settings()["cordis"].queries["full_projects_no_pdfs"].path_duck
    v2_geo, v2_pic = args.core_v2_geo, args.core_v2_pic
    if not (v2_geo and v2_pic):
        from common.config.pipelines import get_pipeline_paths

        block = get_pipeline_paths()["core_v4"]
        v2_geo = v2_geo or block.get("path_core_v2_geolocations")
        v2_pic = v2_pic or block.get("path_core_v2_institution_pic")
    return {
        "staging_db": str(staging),
        "ror_db": str(ror),
        "cordis_db": str(cordis),
        "core_v2_geo": str(v2_geo) if v2_geo else None,
        "core_v2_pic": str(v2_pic) if v2_pic else None,
    }


# ---------------------------------------------------------------------------
# Intermediate tables (all in the temp DuckDB, inputs are attached read-only)
# ---------------------------------------------------------------------------
def build_tables(con, inputs: Dict[str, Optional[str]]) -> Dict[str, Any]:
    """Builds, in the temp db: org_geo (one row per organisation: tier, lat, lon, has_addr), part (distinct
    project/organisation pairs of hasParticipant) and the sanity tables. Returns notes about skipped steps."""
    notes: Dict[str, Any] = {"doi_fallback": False, "core_v2_geo": False, "core_v2_pic": False}
    register_country_macro(con)  # norm_cc(c), the same macro transformation.py uses
    # copied from transformation.py build()
    con.execute(r"""CREATE OR REPLACE TEMP MACRO norm_doi(d) AS
            nullif(lower(trim(regexp_replace(trim(d), '^(https?://)?(dx\.)?doi\.org/', '', 'i'))), '')""")

    # -- organisations: narrow columns only ---------------------------------------------------------------
    progress("organisations")
    con.execute("""CREATE OR REPLACE TABLE org AS
           SELECT id, lower(trim(legalName)) AS name_key, norm_cc(countryCode) AS cc,
                  nullif(trim(countryCode), '') AS cc_raw, rorId
           FROM oa.organization""")
    # PIC pids: an org can carry several, a PIC can sit on several orgs (_org_pic in _merge_cordis)
    con.execute("""CREATE OR REPLACE TABLE org_pic AS
           SELECT DISTINCT o.id AS org_id, trim(p.value) AS pic, p.value AS pic_raw
           FROM oa.organization o, UNNEST(o.pids) AS u(p) WHERE p.scheme = 'PIC' AND p.value IS NOT NULL""")

    # -- tier 1: ROR (_merge_ror) -------------------------------------------------------------------------
    progress("tier ror")
    con.execute("""CREATE OR REPLACE TABLE org_ror AS
           SELECT o.id AS org_id,
                  r.locations[1].geonames_details.lat AS lat, r.locations[1].geonames_details.lng AS lon
           FROM org o JOIN ror.organizations r ON o.rorId = r.id
           WHERE r.locations[1].geonames_details.lat IS NOT NULL AND r.locations[1].geonames_details.lng IS NOT NULL
           QUALIFY row_number() OVER (PARTITION BY o.id ORDER BY r.id) = 1""")

    # -- tiers 2 and 3: Cordis (_merge_cordis) ------------------------------------------------------------
    progress("tier cordis: project match")
    # (a) project match: grantId = id_original, then DOI for the Cordis projects still unmatched
    con.execute("""CREATE OR REPLACE TABLE pm AS
           SELECT cp.id AS cproj, op.id AS oproj, 'grant' AS method
           FROM cordis.project cp JOIN oa.project op ON cp.id_original = op.grantId""")
    if _has_column(con, "oa", "project", "doi"):
        notes["doi_fallback"] = True
        con.execute("""INSERT INTO pm
               SELECT cp.id, op.id, 'doi'
               FROM cordis.project cp JOIN oa.project op ON norm_doi(cp.doi) = norm_doi(op.doi)
               WHERE norm_doi(cp.doi) IS NOT NULL AND cp.id NOT IN (SELECT cproj FROM pm)""")
    else:
        warn("the OpenAire staging has no project.doi: skipping the DOI fallback of the Cordis project match")

    # (b) institutions: [lon, lat] JSON, the literal 'null' means missing (_inst)
    con.execute("""CREATE OR REPLACE TABLE inst AS
           SELECT id, lower(trim(legal_name)) AS name_key, norm_cc(country) AS cc,
                  nullif(trim(street), '') AS street, nullif(trim(city), '') AS city,
                  TRY_CAST(json_extract_string(geolocation, '$[0]') AS DOUBLE) AS lon,
                  TRY_CAST(json_extract_string(geolocation, '$[1]') AS DOUBLE) AS lat
           FROM cordis.institution""")
    con.execute("ALTER TABLE inst ADD COLUMN has_geo BOOLEAN")
    con.execute("UPDATE inst SET has_geo = COALESCE(lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180, false)")
    con.execute("""CREATE OR REPLACE TABLE trip AS
           SELECT DISTINCT pm.cproj, jpi.institution_id AS inst_id, nullif(trim(jpi.organization_id), '') AS pic
           FROM pm JOIN cordis.j_project_institution jpi ON jpi.project_id = pm.cproj""")
    # PIC first
    con.execute("""CREATE OR REPLACE TABLE trip_org AS
           SELECT t.cproj, t.inst_id, op.org_id, 'pic' AS method
           FROM trip t JOIN (SELECT DISTINCT org_id, pic FROM org_pic) op ON op.pic = t.pic""")
    # strict fallback for triplets without a PIC match: name + normalised country, both non-null; never name only
    con.execute("""INSERT INTO trip_org
           SELECT t.cproj, t.inst_id, o.id, 'name_country'
           FROM trip t
           JOIN inst i ON i.id = t.inst_id
           JOIN org o ON o.name_key = i.name_key AND i.cc IS NOT NULL AND o.cc = i.cc
           WHERE NOT EXISTS (SELECT 1 FROM trip_org m WHERE m.cproj = t.cproj AND m.inst_id = t.inst_id
                                                           AND m.method = 'pic')""")

    # pass 1: one institution per org: PIC-matched first, then real coordinates, then street + city, then highest id
    con.execute("""CREATE OR REPLACE TABLE cand1 AS
           SELECT org_id, inst_id, CASE WHEN bool_or(method = 'pic') THEN 'pic' ELSE 'name_country' END AS method
           FROM trip_org GROUP BY org_id, inst_id""")
    con.execute(
        """CREATE OR REPLACE TABLE chosen1 AS
           SELECT m.org_id, m.inst_id, i.lat, i.lon, i.has_geo,
                  (i.street IS NOT NULL AND i.city IS NOT NULL AND i.cc IS NOT NULL) AS has_addr
           FROM cand1 m JOIN inst i ON i.id = m.inst_id
           QUALIFY row_number() OVER (PARTITION BY m.org_id
                                      ORDER BY (m.method <> 'pic'), NOT i.has_geo,
                                               NOT (i.street IS NOT NULL AND i.city IS NOT NULL), m.inst_id DESC) = 1"""
    )

    # pass 2: orgs pass 1 did not match, PIC only, over ALL Cordis rows (the project need not be matched)
    progress("tier cordis: org-level PIC pass")
    con.execute("""CREATE OR REPLACE TABLE cand2 AS
           SELECT DISTINCT op.org_id, jpi.institution_id AS inst_id
           FROM (SELECT DISTINCT org_id, pic FROM org_pic) op
           JOIN cordis.j_project_institution jpi ON nullif(trim(jpi.organization_id), '') = op.pic
           WHERE op.org_id NOT IN (SELECT org_id FROM chosen1)""")
    con.execute("""CREATE OR REPLACE TABLE chosen2 AS
           SELECT m.org_id, m.inst_id, i.lat, i.lon, i.has_geo,
                  (i.street IS NOT NULL AND i.city IS NOT NULL AND i.cc IS NOT NULL) AS has_addr
           FROM cand2 m JOIN inst i ON i.id = m.inst_id
           QUALIFY row_number() OVER (PARTITION BY m.org_id
                                      ORDER BY NOT i.has_geo, NOT (i.street IS NOT NULL AND i.city IS NOT NULL),
                                               m.inst_id DESC) = 1""")

    # coordinates after ROR and Cordis: ROR is never overwritten, the chosen institution's coordinates only
    # count when it has real ones (_apply_institutions)
    con.execute("""CREATE OR REPLACE TABLE org_geo1 AS
           SELECT o.id,
                  CASE WHEN r.org_id IS NOT NULL THEN 'ror'
                       WHEN c1.has_geo THEN 'cordis_project'
                       WHEN c2.has_geo THEN 'cordis_pic' END AS tier,
                  CASE WHEN r.org_id IS NOT NULL THEN r.lat WHEN c1.has_geo THEN c1.lat WHEN c2.has_geo THEN c2.lat END AS lat,
                  CASE WHEN r.org_id IS NOT NULL THEN r.lon WHEN c1.has_geo THEN c1.lon WHEN c2.has_geo THEN c2.lon END AS lon,
                  -- the address columns come from the chosen institution of pass 1, else of pass 2
                  COALESCE(c1.has_addr, c2.has_addr, false) AS has_addr
           FROM org o
           LEFT JOIN org_ror r ON r.org_id = o.id
           LEFT JOIN chosen1 c1 ON c1.org_id = o.id
           LEFT JOIN chosen2 c2 ON c2.org_id = o.id""")

    # -- tiers 4 and 5: core_v2 (_merge_core_v2), only for orgs still without coordinates ------------------
    con.execute(
        "CREATE OR REPLACE TABLE v2_cand (org_id UBIGINT, inst_id VARCHAR, method VARCHAR, lat DOUBLE, lon DOUBLE)"
    )
    con.execute("CREATE OR REPLACE TABLE v2_pic (pic VARCHAR, inst_id VARCHAR)")
    v2_geo, v2_pic = inputs["core_v2_geo"], inputs["core_v2_pic"]
    if v2_geo is None or not Path(v2_geo).exists():
        warn(f"core_v2 geolocations not found ({v2_geo}): skipping both core_v2 tiers")
    else:
        progress("tier core_v2")
        notes["core_v2_geo"] = True
        con.execute(f"ATTACH '{v2_geo}' AS core_v2 (READ_ONLY)")
        # geolocation is [lon, lat]
        con.execute(
            """CREATE OR REPLACE TABLE v2_inst AS
               SELECT id, lower(trim(legal_name)) AS name_key, norm_cc(country) AS cc,
                      geolocation[1] AS lon, geolocation[2] AS lat
               FROM core_v2.institution
               WHERE len(geolocation) = 2 AND geolocation[1] BETWEEN -180 AND 180 AND geolocation[2] BETWEEN -90 AND 90"""
        )
        if v2_pic is None or not Path(v2_pic).exists():
            warn(f"core_v2 institution_pic not found ({v2_pic}): name + country path only")
        else:
            con.execute(f"ATTACH '{v2_pic}' AS core_v2_pic (READ_ONLY)")
            needed = ("institution_id", "pic", "pic_is_standard", "pic_has_multiple_institutions")
            if not all(_has_column(con, "core_v2_pic", "institution_pic", c) for c in needed):
                warn(f"core_v2 institution_pic lacks a table or one of {needed}: name + country path only")
            else:
                notes["core_v2_pic"] = True
                # usable rows: a standard PIC that belongs to exactly one institution, digits only
                con.execute("""INSERT INTO v2_pic
                       SELECT DISTINCT nullif(regexp_replace(ip.pic::VARCHAR, '[^0-9]', '', 'g'), ''), ip.institution_id::VARCHAR
                       FROM core_v2_pic.institution_pic ip
                       WHERE ip.pic_is_standard AND NOT ip.pic_has_multiple_institutions
                         AND nullif(regexp_replace(ip.pic::VARCHAR, '[^0-9]', '', 'g'), '') IS NOT NULL""")
        con.execute("""CREATE OR REPLACE TABLE v2_todo AS SELECT id FROM org_geo1 WHERE tier IS NULL""")
        con.execute("""INSERT INTO v2_cand
               SELECT op.org_id, i.id, 'pic', i.lat, i.lon
               FROM (SELECT DISTINCT org_id, nullif(regexp_replace(pic_raw, '[^0-9]', '', 'g'), '') AS pic FROM org_pic
                     WHERE org_id IN (SELECT id FROM v2_todo)) op
               JOIN v2_pic p ON p.pic = op.pic JOIN v2_inst i ON i.id = p.inst_id""")
        # name + country only for institutions without a usable PIC row; never name only
        con.execute("""INSERT INTO v2_cand
               SELECT o.id, i.id, 'name_country', i.lat, i.lon
               FROM org o
               JOIN v2_inst i ON o.name_key = i.name_key AND i.cc IS NOT NULL AND o.cc = i.cc
               WHERE o.id IN (SELECT id FROM v2_todo)
                 AND i.id NOT IN (SELECT inst_id FROM v2_pic WHERE inst_id IS NOT NULL)""")
    con.execute("""CREATE OR REPLACE TABLE v2_match AS
           SELECT org_id, method, lat, lon FROM v2_cand
           QUALIFY row_number() OVER (PARTITION BY org_id ORDER BY (method <> 'pic'), inst_id) = 1""")

    # -- final organisation table: tier, rank (1..5), coordinates, address flag --------------------------------
    rank_case = "CASE tier " + " ".join(f"WHEN '{k}' THEN {i}" for i, (k, _) in enumerate(TIERS, 1)) + " END"
    con.execute(f"""CREATE OR REPLACE TABLE org_geo AS
           SELECT id, tier, {rank_case} AS tier_rank, lat, lon, has_addr FROM (
               SELECT g.id,
                      COALESCE(g.tier, CASE v.method WHEN 'pic' THEN 'core_v2_pic' WHEN 'name_country' THEN 'core_v2_name_country' END) AS tier,
                      COALESCE(g.lat, v.lat) AS lat, COALESCE(g.lon, v.lon) AS lon, g.has_addr
               FROM org_geo1 g LEFT JOIN v2_match v ON v.org_id = g.id AND g.tier IS NULL)""")

    # -- participation: hasParticipant only, project and organisation must exist (the core_v4 relation cascade) ----
    progress("participation (relation hasParticipant)")
    con.execute("""CREATE OR REPLACE TABLE part AS
           SELECT DISTINCT r.source AS project_id, r.target AS org_id
           FROM oa.relation r
           WHERE r.relType.name = 'hasParticipant' AND r.sourceType = 'project' AND r.targetType = 'organization'
             AND r.source IN (SELECT id FROM oa.project) AND r.target IN (SELECT id FROM org)""")
    return notes


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
def tier_counts(con, only_participating: bool) -> Dict[str, Any]:
    """Cumulative coordinates by tier for all orgs, or only for those in `part`."""
    where = "WHERE id IN (SELECT org_id FROM part)" if only_participating else ""
    total = _one(con, f"SELECT count(*) FROM org_geo {where}")
    by_tier = dict(con.execute(f"SELECT tier, count(*) FROM org_geo {where} GROUP BY tier").fetchall())
    rows, cumulative = [], 0
    for key, label in TIERS:
        n = by_tier.get(key, 0)
        cumulative += n
        rows.append(
            {
                "tier": key,
                "label": label,
                "incremental": n,
                "cumulative": cumulative,
                "cumulative_pct": pct(cumulative, total),
                "without_coordinates": total - cumulative,
            }
        )
    return {"total": total, "tiers": rows, "without_coordinates": total - cumulative}


def print_tier_table(res: Dict[str, Any]) -> None:
    print(f"Organisations: {res['total']:,}\n")
    rows = [
        [t["label"], t["incremental"], t["cumulative"], t["cumulative_pct"], t["without_coordinates"]]
        for t in res["tiers"]
    ]
    print(
        md_table(["tier", "gained by this tier", "cumulative with coordinates", "cumulative %", "still without"], rows)
    )


def project_counts(con) -> Dict[str, Any]:
    """The key result: per-project counts of geolocated participants, cumulative by tier."""
    progress("projects")
    per_tier = []
    for k in range(1, len(TIERS) + 1):
        per_tier.append(
            f"count(*) FILTER (WHERE g.tier_rank <= {k}) AS g{k}, "
            f"count(DISTINCT (round(g.lat, {COORD_DECIMALS}), round(g.lon, {COORD_DECIMALS}))) "
            f"FILTER (WHERE g.tier_rank <= {k}) AS d{k}"
        )
    con.execute(f"""CREATE OR REPLACE TABLE proj_stats AS
           SELECT p.project_id, count(*) AS n_orgs, {', '.join(per_tier)},
                  count(*) FILTER (WHERE g.tier_rank IS NULL AND g.has_addr) AS h
           FROM part p JOIN org_geo g ON g.id = p.org_id
           GROUP BY p.project_id""")
    sel = ["count(*)", "count(*) FILTER (WHERE n_orgs >= 2)"]
    for k in range(1, len(TIERS) + 1):
        sel += [
            f"count(*) FILTER (WHERE g{k} >= 1)",
            f"count(*) FILTER (WHERE g{k} >= 2)",
            f"count(*) FILTER (WHERE d{k} >= 2)",
        ]
    n_last = len(TIERS)
    sel += [f"count(*) FILTER (WHERE g{n_last} + h >= 2)", f"count(*) FILTER (WHERE d{n_last} + h >= 2)"]
    vals = con.execute(f"SELECT {', '.join(sel)} FROM proj_stats").fetchone()
    n_projects_total = _one(con, "SELECT count(*) FROM oa.project")
    with_part, ge2 = vals[0], vals[1]
    rows, prev = [], 0
    for i, (key, label) in enumerate(TIERS):
        ge1_geo, ge2_geo, ge2_dist = vals[2 + 3 * i : 5 + 3 * i]
        rows.append(
            {
                "tier": key,
                "label": label,
                "ge1_geolocated": ge1_geo,
                "ge2_geolocated": ge2_geo,
                "ge2_geolocated_distinct": ge2_dist,
                "ge2_geolocated_gain": ge2_geo - prev,
                "ge2_geolocated_pct_of_ge2_participants": pct(ge2_geo, ge2),
                "ge2_distinct_pct_of_ge2_participants": pct(ge2_dist, ge2),
                "ge1_geolocated_pct_of_with_participant": pct(ge1_geo, with_part),
            }
        )
        prev = ge2_geo
    after, after_distinct = vals[2 + 3 * len(TIERS) :]
    return {
        "projects_total": n_projects_total,
        "with_participant": with_part,
        "with_ge2_participants": ge2,
        "tiers": rows,
        "headroom_ge2_geolocated_after": after,
        "headroom_ge2_distinct_after": after_distinct,
    }


def headroom(con, projects: Dict[str, Any]) -> Dict[str, Any]:
    """Mapbox upper bound: participants without coordinates that have a real Cordis address."""
    n_part_orgs = _one(con, "SELECT count(DISTINCT org_id) FROM part")
    without = _one(con, "SELECT count(*) FROM org_geo WHERE tier IS NULL AND id IN (SELECT org_id FROM part)")
    with_addr = _one(
        con, "SELECT count(*) FROM org_geo WHERE tier IS NULL AND has_addr AND id IN (SELECT org_id FROM part)"
    )
    last = projects["tiers"][-1]
    return {
        "participating_orgs": n_part_orgs,
        "participating_orgs_without_coordinates": without,
        "with_cordis_address": with_addr,
        "without_coordinates_and_without_address": without - with_addr,
        "projects_ge2_geolocated_now": last["ge2_geolocated"],
        "projects_ge2_geolocated_upper_bound": projects["headroom_ge2_geolocated_after"],
        "projects_ge2_geolocated_added": projects["headroom_ge2_geolocated_after"] - last["ge2_geolocated"],
        "projects_ge2_distinct_now": last["ge2_geolocated_distinct"],
        "projects_ge2_distinct_upper_bound": projects["headroom_ge2_distinct_after"],
        "projects_ge2_distinct_added": projects["headroom_ge2_distinct_after"] - last["ge2_geolocated_distinct"],
    }


def sanity(con, notes: Dict[str, Any]) -> Dict[str, Any]:
    """Counts that show how ambiguous the matching is, plus organisations without a country code."""
    s: Dict[str, Any] = {}
    n_cp = _one(con, "SELECT count(*) FROM cordis.project")
    by = dict(con.execute("SELECT method, count(DISTINCT cproj) FROM pm GROUP BY method").fetchall())
    s["cordis_projects"] = {
        "total": n_cp,
        "matched_grantid": by.get("grant", 0),
        "matched_doi_fallback": by.get("doi", 0),
        "doi_fallback_active": notes["doi_fallback"],
    }
    for name, cand, chosen in (("pass1_project_scoped", "cand1", "chosen1"), ("pass2_org_pic", "cand2", "chosen2")):
        s[name] = {
            "orgs_matched": _one(con, f"SELECT count(*) FROM {chosen}"),
            "orgs_with_coordinates": _one(con, f"SELECT count(*) FROM {chosen} WHERE has_geo"),
            "orgs_matched_to_several_cordis_institutions": _one(
                con,
                f"SELECT count(*) FROM (SELECT org_id FROM {cand} GROUP BY org_id HAVING count(DISTINCT inst_id) > 1)",
            ),
            "of_which_with_different_coordinates": _one(
                con,
                f"""SELECT count(*) FROM (SELECT c.org_id FROM {cand} c JOIN inst i ON i.id = c.inst_id WHERE i.has_geo
                    GROUP BY c.org_id HAVING count(DISTINCT (round(i.lat, {COORD_DECIMALS}), round(i.lon, {COORD_DECIMALS}))) > 1)""",
            ),
        }
    s["pass1_project_scoped"]["orgs_matched_by_pic"] = _one(
        con, "SELECT count(DISTINCT org_id) FROM cand1 WHERE method = 'pic'"
    )
    s["pass1_project_scoped"]["orgs_matched_by_name_country_only"] = _one(
        con,
        "SELECT count(DISTINCT org_id) FROM cand1 WHERE org_id NOT IN (SELECT org_id FROM cand1 WHERE method = 'pic')",
    )
    s["pic_conflicts"] = {
        "orgs_with_several_pic_pids": _one(
            con, "SELECT count(*) FROM (SELECT org_id FROM org_pic GROUP BY org_id HAVING count(DISTINCT pic) > 1)"
        ),
        "pics_on_several_orgs": _one(
            con, "SELECT count(*) FROM (SELECT pic FROM org_pic GROUP BY pic HAVING count(DISTINCT org_id) > 1)"
        ),
        "orgs_sharing_a_pic_with_another_org": _one(
            con,
            """SELECT count(DISTINCT org_id) FROM org_pic
               WHERE pic IN (SELECT pic FROM org_pic GROUP BY pic HAVING count(DISTINCT org_id) > 1)""",
        ),
        "cordis_pics_on_several_institutions": _one(
            con,
            """SELECT count(*) FROM (SELECT nullif(trim(organization_id), '') AS pic FROM cordis.j_project_institution
               WHERE nullif(trim(organization_id), '') IS NOT NULL
               GROUP BY 1 HAVING count(DISTINCT institution_id) > 1)""",
        ),
        "core_v2_orgs_matched_to_several_institutions": _one(
            con, "SELECT count(*) FROM (SELECT org_id FROM v2_cand GROUP BY org_id HAVING count(DISTINCT inst_id) > 1)"
        ),
    }
    s["country_code"] = {
        "orgs_without_raw_country_code": _one(con, "SELECT count(*) FROM org WHERE cc_raw IS NULL"),
        "orgs_without_valid_country_code": _one(con, "SELECT count(*) FROM org WHERE cc IS NULL"),
        "participating_orgs_without_valid_country_code": _one(
            con, "SELECT count(*) FROM org WHERE cc IS NULL AND id IN (SELECT org_id FROM part)"
        ),
    }
    s["core_v2"] = {
        "geolocations_file_found": notes["core_v2_geo"],
        "pic_file_used": notes["core_v2_pic"],
        "usable_pic_rows": _one(con, "SELECT count(*) FROM v2_pic"),
        "usable_institutions": _one(con, "SELECT count(*) FROM v2_inst") if notes["core_v2_geo"] else 0,
    }
    return s


def print_projects(pr: Dict[str, Any]) -> None:
    print(
        md_table(
            ["projects", "n"],
            [
                ["in project table", pr["projects_total"]],
                ["with >= 1 participating org", pr["with_participant"]],
                ["with >= 2 participating orgs", pr["with_ge2_participants"]],
            ],
        )
    )
    print("\nCumulative by tier (orgs counted once per project):\n")
    rows = [
        [
            t["label"],
            t["ge1_geolocated"],
            t["ge2_geolocated"],
            t["ge2_geolocated_gain"],
            t["ge2_geolocated_pct_of_ge2_participants"],
            t["ge2_geolocated_distinct"],
            t["ge2_distinct_pct_of_ge2_participants"],
        ]
        for t in pr["tiers"]
    ]
    print(
        md_table(
            [
                "tier",
                ">= 1 geolocated",
                ">= 2 geolocated (collaboration-capable)",
                "gain of this tier",
                "% of projects with >= 2 participants",
                f">= 2 at distinct coordinates ({COORD_DECIMALS} decimals)",
                "% of projects with >= 2 participants",
            ],
            rows,
        )
    )


def print_sanity(s: Dict[str, Any]) -> None:
    rows = [["Cordis projects", s["cordis_projects"]["total"]]]
    rows += [["  matched on grantId", s["cordis_projects"]["matched_grantid"]]]
    rows += [["  matched by the DOI fallback", s["cordis_projects"]["matched_doi_fallback"]]]
    for name in ("pass1_project_scoped", "pass2_org_pic"):
        for k, v in s[name].items():
            rows.append([f"{name}: {k.replace('_', ' ')}", v])
    for group in ("pic_conflicts", "country_code", "core_v2"):
        for k, v in s[group].items():
            rows.append([f"{group}: {k.replace('_', ' ')}", v])
    print(md_table(["check", "value"], rows))


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def default_tmp_dir() -> str:
    from common.config.settings import get_settings

    hpc_root = get_settings().hpc_root
    if hpc_root:
        return str(Path(hpc_root) / "duckdb_tmp" / "geolocation_coverage")
    return tempfile.mkdtemp(prefix="geolocation_coverage_")


def main(argv: Optional[Sequence[str]] = None) -> Dict[str, Any]:
    ap = argparse.ArgumentParser(
        description="core_v4 geolocation coverage: orgs and projects with coordinates by tier (read-only)"
    )
    ap.add_argument("--mem-mb", type=int, default=128_000, help="SLURM mem_mb; DuckDB gets this minus 20 GB headroom")
    ap.add_argument("--threads", type=int, default=16)
    ap.add_argument("--out", default=str(Path(__file__).with_name("geolocation_coverage.json")))
    ap.add_argument(
        "--tmp-dir",
        default=None,
        help="temp DuckDB + spill dir (default: <hpc_root>/duckdb_tmp/geolocation_coverage, else a temp dir)",
    )
    ap.add_argument(
        "--keep-tmp", action="store_true", help="keep the temp DuckDB with the intermediate tables (debugging)"
    )
    ap.add_argument(
        "--staging-db",
        help="OpenAire staging (default: dumps.yaml openaire_dump.path_duck_staging_2; the v4 staging works too)",
    )
    ap.add_argument("--ror-db", help="ROR duckdb (default: dumps.yaml ror_dump.path_duck)")
    ap.add_argument(
        "--cordis-db", help=f"Cordis duckdb (default: ${CORDIS_DB_ENV}, else config cordis full_projects_no_pdfs)"
    )
    ap.add_argument(
        "--core-v2-geo", help="core_v2 geolocations duckdb (default: pipelines.yaml core_v4.path_core_v2_geolocations)"
    )
    ap.add_argument(
        "--core-v2-pic",
        help="core_v2 institution_pic duckdb (default: pipelines.yaml core_v4.path_core_v2_institution_pic)",
    )
    args = ap.parse_args(argv)

    inputs = resolve_inputs(args)
    tmp_dir = Path(args.tmp_dir or default_tmp_dir())
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_db = tmp_dir / "geolocation_coverage_tmp.duckdb"
    for stale in (tmp_db, tmp_db.with_name(tmp_db.name + ".wal")):
        stale.unlink(missing_ok=True)

    con = duckdb.connect(str(tmp_db))  # the only file that is written; every input is attached READ_ONLY
    con.execute(f"SET memory_limit='{max(args.mem_mb - 20_000, 2_000)}MB'")
    con.execute(f"SET threads={args.threads}")
    con.execute(f"SET temp_directory='{tmp_dir / 'spill'}'")
    try:
        for alias, key in [("oa", "staging_db"), ("ror", "ror_db"), ("cordis", "cordis_db")]:
            con.execute(f"ATTACH '{inputs[key]}' AS {alias} (READ_ONLY)")

        OUT.clear()
        OUT["meta"] = {"run_date": date.today().isoformat(), **inputs, "tmp_db": str(tmp_db)}
        print("# core_v4 geolocation coverage\n")
        print(md_table(["source", "path"], [[k, v] for k, v in OUT["meta"].items() if k not in ("run_date", "tmp_db")]))

        t0 = time.time()
        notes = build_tables(con, inputs)
        OUT["meta"]["skipped_or_missing"] = {
            "doi_fallback_active": notes["doi_fallback"],
            "core_v2_geolocations_used": notes["core_v2_geo"],
            "core_v2_pic_used": notes["core_v2_pic"],
        }
        print(
            f"\n_(tables built: {time.time() - t0:.0f}s; DOI fallback {'on' if notes['doi_fallback'] else 'OFF'}, "
            f"core_v2 geolocations {'used' if notes['core_v2_geo'] else 'MISSING'}, "
            f"core_v2 pic {'used' if notes['core_v2_pic'] else 'not used'})_"
        )

        section("1. Organisations by coordinate tier (cumulative, in tier order)")
        OUT["orgs_all"] = tier_counts(con, only_participating=False)
        print_tier_table(OUT["orgs_all"])

        section("2. Organisations that participate in at least one project (hasParticipant)")
        OUT["orgs_participating"] = tier_counts(con, only_participating=True)
        print_tier_table(OUT["orgs_participating"])

        section("3. Projects with geolocated participants (the map needs >= 2 per project)")
        OUT["projects"] = project_counts(con)
        print_projects(OUT["projects"])

        section("4. Mapbox headroom (upper bound, nothing is geocoded here)")
        OUT["headroom"] = headroom(con, OUT["projects"])
        h = OUT["headroom"]
        print(
            md_table(
                ["metric", "n"],
                [
                    ["participating orgs", h["participating_orgs"]],
                    ["participating orgs still without coordinates", h["participating_orgs_without_coordinates"]],
                    ["  of which with a Cordis address (street + city + country)", h["with_cordis_address"]],
                    ["  of which without such an address", h["without_coordinates_and_without_address"]],
                    ["projects with >= 2 geolocated participants now", h["projects_ge2_geolocated_now"]],
                    ["  if all address orgs geocoded (upper bound)", h["projects_ge2_geolocated_upper_bound"]],
                    ["  added", h["projects_ge2_geolocated_added"]],
                    ["projects with >= 2 distinct coordinates now", h["projects_ge2_distinct_now"]],
                    [
                        "  if all address orgs geocoded, each at a new point (upper bound)",
                        h["projects_ge2_distinct_upper_bound"],
                    ],
                    ["  added", h["projects_ge2_distinct_added"]],
                ],
            )
        )

        section("5. Sanity")
        OUT["sanity"] = sanity(con, notes)
        print_sanity(OUT["sanity"])
    finally:
        con.close()
        if not args.keep_tmp:
            shutil.rmtree(tmp_dir / "spill", ignore_errors=True)
            for f in (tmp_db, tmp_db.with_name(tmp_db.name + ".wal")):
                f.unlink(missing_ok=True)

    Path(args.out).write_text(json.dumps(OUT, indent=2, default=str))
    print(f"\nWrote {args.out}")
    return OUT


if __name__ == "__main__":
    main()
