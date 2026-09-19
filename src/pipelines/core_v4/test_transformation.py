"""
Tests for core_v4 transformation against tiny duckdb fixtures in tmp_path (OpenAire staging v4, ROR,
Cordis). Never touches real data or the configured paths.

    uv run python -m pytest src/pipelines/core_v4/test_transformation.py
"""

import itertools
from datetime import date
from types import SimpleNamespace

import duckdb
import pytest

from pipelines.core_v4 import transformation
from pipelines.core_v4.transformation import Paths, build, main

# id ranges: projects 1.., works 100.., orgs 500.. (UBIGINT like the hashed real ids)
PROJ, WORK, ORG = 0, 100, 500


def _insert(con, table, rows):
    for row in rows:
        con.execute(f"INSERT INTO {table} VALUES ({', '.join('?' * len(row))})", list(row))


def make_staging(path, *, orgs=(), projects=(), works=(), relations=()):
    """orgs: (n, name, country, pics=[]); projects: (n, grantId, doi); works: (n, date|None, has_desc, countries);
    relations: (source, sourceType, target, targetType, relType name)."""
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE organization (id UBIGINT, openaireId VARCHAR, legalName VARCHAR, countryCode VARCHAR,"
        " rorId VARCHAR, pids STRUCT(scheme VARCHAR, value VARCHAR)[])"
    )
    con.execute("CREATE TABLE project (id UBIGINT, openaireId VARCHAR, grantId VARCHAR, title VARCHAR, doi VARCHAR)")
    con.execute(
        "CREATE TABLE work (id UBIGINT, openaireId VARCHAR, title VARCHAR, publicationDate DATE,"
        " descriptions VARCHAR[], countries VARCHAR[])"
    )
    con.execute(
        "CREATE TABLE relation (source UBIGINT, sourceType VARCHAR, target UBIGINT, targetType VARCHAR,"
        ' relType STRUCT("name" VARCHAR, "type" VARCHAR), validated BOOLEAN)'
    )
    for o in orgs:
        n, name, cc = o[:3]
        pics = o[3] if len(o) > 3 else []
        ror = o[4] if len(o) > 4 else None
        _insert(con, "organization", [(ORG + n, f"o{n}", name, cc, ror, [{"scheme": "PIC", "value": p} for p in pics])])
    for n, grant, doi in projects:
        _insert(con, "project", [(PROJ + n, f"p{n}", grant, f"Project {n}", doi)])
    for n, d, has_desc, countries in works:
        _insert(
            con,
            "work",
            [(WORK + n, f"w{n}", f"Work {n}", d, ["a description"] if has_desc else [], countries)],
        )
    for s, st, t, tt, rel in relations:
        _insert(con, "relation", [(s, st, t, tt, {"name": rel, "type": "x"}, True)])
    con.close()
    return path


def produces(p, w):
    return (PROJ + p, "project", WORK + w, "product", "produces")


def authored(w, o):
    return (WORK + w, "product", ORG + o, "organization", "hasAuthorInstitution")


def participant(p, o):
    return (PROJ + p, "project", ORG + o, "organization", "hasParticipant")


def make_ror(path, rows=()):
    """rows: (id, lat|None, lng|None)."""
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE organizations (id VARCHAR, status VARCHAR, established BIGINT, types VARCHAR[],"
        " locations STRUCT(geonames_id BIGINT, geonames_details STRUCT(country_code VARCHAR, lat DOUBLE, lng DOUBLE))[],"
        " relationships STRUCT(type VARCHAR, label VARCHAR, id VARCHAR)[])"
    )
    for rid, lat, lng in rows:
        loc = [{"geonames_id": 1, "geonames_details": {"country_code": "DE", "lat": lat, "lng": lng}}]
        _insert(con, "organizations", [(rid, "active", 1900, ["education"], loc, [])])
    con.close()
    return path


def make_cordis(path, *, projects=(), institutions=(), links=()):
    """projects: (id, id_original, doi); institutions: (id, name, country, street, city, geolocation_json, nuts3);
    links: (project_id, institution_id, ec_contribution, type, pic)."""
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE project (id INTEGER, id_original VARCHAR, doi VARCHAR)")
    con.execute(
        "CREATE TABLE institution (id INTEGER, legal_name VARCHAR, street VARCHAR, postalcode VARCHAR, city VARCHAR,"
        " country VARCHAR, geolocation JSON, nuts_level_3 VARCHAR)"
    )
    con.execute(
        "CREATE TABLE j_project_institution (project_id INTEGER, institution_id INTEGER, ec_contribution FLOAT,"
        " type VARCHAR, organization_id VARCHAR)"
    )
    _insert(con, "project", projects)
    for i, name, cc, street, city, geo, nuts in institutions:
        _insert(con, "institution", [(i, name, street, "12345", city, cc, geo, nuts)])
    _insert(con, "j_project_institution", links)
    con.close()
    return path


def make_core_v2(path, institutions=(), pics=None):
    """institutions: (id, source_id, legal_name, country, lon, lat). pics: None = no institution_pic file, else rows
    (institution.id, pic[, standard, multi_institutions]) written to the SEPARATE file <path>.pic.duckdb like the real
    layout; `standard` / `multi_institutions` default to what the real file would say (9 digits / PIC on 2+ institutions).
    Returns (geolocations path, pic path or None)."""
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE institution (id VARCHAR, source_id VARCHAR, legal_name VARCHAR, street VARCHAR, city VARCHAR,"
        " country VARCHAR, geolocation DOUBLE[])"
    )
    for i, src, name, cc, lon, lat in institutions:
        _insert(con, "institution", [(i, src, name, "Street 1", "City", cc, [lon, lat])])
    con.close()
    if pics is None:
        return path, None
    pic_path = path.with_name(path.stem + ".pic.duckdb")
    con = duckdb.connect(str(pic_path))
    con.execute(
        "CREATE TABLE institution_pic (institution_id VARCHAR, institution_source_id VARCHAR, pic VARCHAR,"
        " n_projects BIGINT, institution_has_multiple_pics BOOLEAN, pic_has_multiple_institutions BOOLEAN,"
        " pic_is_standard BOOLEAN)"
    )
    owners = {}
    for row in pics:
        owners.setdefault(row[1], set()).add(row[0])
    for row in pics:
        inst, pic = row[0], row[1]
        standard = row[2] if len(row) > 2 else len(pic) == 9 and pic.isdigit()
        multi = row[3] if len(row) > 3 else len(owners[pic]) > 1
        _insert(con, "institution_pic", [(inst, None, pic, 1, False, multi, standard)])
    con.close()
    return path, pic_path


def make_paths(tmp_path, staging, ror=(), cordis=None, *, cap=1000, core_v2=None):
    tmp_path.mkdir(exist_ok=True)
    v2_geo, v2_pic = make_core_v2(tmp_path / "core_v2.duckdb", **core_v2) if core_v2 is not None else (None, None)
    return Paths(
        core_v2_geo=v2_geo,
        core_v2_pic=v2_pic,
        staging=make_staging(tmp_path / "staging.duckdb", **staging),
        ror=make_ror(tmp_path / "ror.duckdb", ror),
        cordis=make_cordis(tmp_path / "cordis.duckdb", **(cordis or {})),
        out=tmp_path / "out" / "core_v4_staging.duckdb",
        work_cap=cap,
    )


_runs = itertools.count()


def run(tmp_path, staging, ror=(), cordis=None, *, cap=1000, limit=None, core_v2=None):
    """Builds core_v4 from the fixtures in a fresh sub directory; returns (result, read-only connection to the output)."""
    paths = make_paths(tmp_path / f"run{next(_runs)}", staging, ror, cordis, cap=cap, core_v2=core_v2)
    result = build(paths, limit=limit)
    return result, duckdb.connect(str(paths.out), read_only=True)


def ids(con, table):
    return sorted(r[0] for r in con.execute(f"SELECT id FROM {table}").fetchall())


def works_kept(con):
    return sorted(i - WORK for i in ids(con, "work"))


# ---------------------------------------------------------------------------
# Work trim
# ---------------------------------------------------------------------------
def test_trim_tiers_and_unlinked_dropped(tmp_path):
    staging = dict(
        orgs=[(1, "Org", "DE")],
        projects=[(1, "g1", None)],
        works=[(1, date(2000, 1, 1), True, None), (2, date(2020, 1, 1), True, None), (3, date(2021, 1, 1), True, None)],
        relations=[produces(1, 1), authored(2, 1)],  # w3 has no relation
    )
    result, con = run(tmp_path, staging)
    assert works_kept(con) == [1, 2]  # unlinked work 3 never copied
    assert result["seed"]["tiers"] == {0: {"total": 1, "kept": 1}, 1: {"total": 1, "kept": 1}}
    assert result["seed"]["cutoff"] is None  # cap does not bind


def test_cap_prefers_project_linked_over_newer_org_only(tmp_path):
    staging = dict(
        orgs=[(1, "Org", "DE")],
        projects=[(1, "g1", None)],
        works=[
            (1, date(1995, 1, 1), False, None),  # old but project-linked
            (2, date(2024, 1, 1), True, None),
            (3, date(2023, 1, 1), True, None),
            (4, date(2020, 1, 1), True, None),
            (5, None, True, None),  # undated org-only: last
        ],
        relations=[produces(1, 1), authored(2, 1), authored(3, 1), authored(4, 1), authored(5, 1)],
    )
    result, con = run(tmp_path, staging, cap=3)
    assert works_kept(con) == [1, 2, 3]
    cut = result["seed"]["cutoff"]
    assert (cut["tier"], cut["date"]) == (1, date(2023, 1, 1))


def test_undated_kept_after_dated_when_cap_reaches_them(tmp_path):
    staging = dict(
        orgs=[(1, "Org", "DE")],
        works=[(1, None, True, None), (2, date(2001, 1, 1), True, None)],
        relations=[authored(1, 1), authored(2, 1)],
    )
    result, con = run(tmp_path, staging, cap=1)
    assert works_kept(con) == [2]
    result, con = run(tmp_path, staging, cap=2)
    assert works_kept(con) == [1, 2]


def test_tie_break_at_cutoff_date_is_deterministic(tmp_path):
    # 6 org-only works on the same date; descriptions first, then lowest id
    d = date(2018, 5, 1)
    works = [(n, d, n in (2, 4, 6), None) for n in range(1, 7)]
    relations = [authored(n, 1) for n in range(1, 7)]
    base = dict(orgs=[(1, "Org", "DE")], works=works, relations=relations)
    _, con = run(tmp_path, base, cap=4)
    first = works_kept(con)
    assert first == [1, 2, 4, 6]  # 2, 4, 6 have descriptions; 1 is the lowest id among the rest
    # same data inserted in reverse order -> same kept set
    shuffled = dict(orgs=base["orgs"], works=list(reversed(works)), relations=list(reversed(relations)))
    result, con = run(tmp_path, shuffled, cap=4)
    assert works_kept(con) == first
    cut = result["seed"]["cutoff"]
    assert (cut["on_date"], cut["kept_on_date"]) == (6, 4)


def test_future_dates_nulled_and_ordered_as_undated(tmp_path):
    staging = dict(
        orgs=[(1, "Org", "DE")],
        works=[(1, date(2999, 1, 1), True, None), (2, date(2001, 1, 1), True, None)],
        relations=[authored(1, 1), authored(2, 1)],
    )
    _, con = run(tmp_path, staging, cap=1)
    assert works_kept(con) == [2]  # the 2999 work is not "newest"
    _, con = run(tmp_path, staging, cap=10)
    dates = dict((i - WORK, d) for i, d in con.execute("SELECT id, publicationDate FROM work").fetchall())
    assert dates == {1: None, 2: date(2001, 1, 1)}


def test_relation_cascade(tmp_path):
    staging = dict(
        orgs=[(1, "Org", "DE"), (2, "Other", "DE")],
        projects=[(1, "g1", None)],
        works=[(1, date(2020, 1, 1), True, None), (2, date(2010, 1, 1), True, None), (3, date(2019, 1, 1), True, None)],
        relations=[
            produces(1, 1),
            authored(1, 1),
            authored(2, 1),
            authored(2, 2),
            authored(3, 2),
            participant(1, 1),
            participant(1, 2),
        ],
    )
    result, con = run(tmp_path, staging, cap=2)  # keeps w1 (tier 0), w3 (2019)
    assert works_kept(con) == [1, 3]
    rels = sorted(con.execute("SELECT sourceType, source, targetType, target FROM relation").fetchall())
    assert rels == sorted(
        [
            ("product", WORK + 1, "organization", ORG + 1),
            ("product", WORK + 3, "organization", ORG + 2),
            ("project", PROJ + 1, "organization", ORG + 1),  # all hasParticipant stay
            ("project", PROJ + 1, "organization", ORG + 2),
            ("project", PROJ + 1, "product", WORK + 1),
        ]
    )
    assert result["dangling_relations"] == 0
    assert ids(con, "organization") == [ORG + 1, ORG + 2]  # orgs and projects are never trimmed
    assert ids(con, "project") == [PROJ + 1]


def test_country_codes_normalised(tmp_path):
    staging = dict(
        orgs=[(1, "A", "UK"), (2, "B", "EU"), (3, "C", "de"), (4, "D", None)],
        works=[(1, date(2020, 1, 1), True, ["UK", "GB", "EL", "EU", "QAT", "DE"]), (2, date(2020, 1, 1), True, None)],
        relations=[authored(1, 1), authored(2, 1)],
    )
    _, con = run(tmp_path, staging)
    cc = dict((i - ORG, c) for i, c in con.execute("SELECT id, countryCode FROM organization").fetchall())
    assert cc == {1: "GB", 2: None, 3: "DE", 4: None}
    countries = dict((i - WORK, c) for i, c in con.execute("SELECT id, countries FROM work").fetchall())
    assert countries == {1: ["DE", "GB", "GR", "QA"], 2: None}


def test_output_rebuilt_from_scratch(tmp_path):
    staging = dict(orgs=[(1, "Org", "DE")], works=[(1, date(2020, 1, 1), True, None)], relations=[authored(1, 1)])
    paths = make_paths(tmp_path / "r", staging)
    build(paths)
    stale = duckdb.connect(str(paths.out))
    stale.execute("CREATE TABLE leftover (x INTEGER)")
    stale.close()
    build(paths)
    con = duckdb.connect(str(paths.out), read_only=True)
    assert not con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name = 'leftover'").fetchone()[0]


# ---------------------------------------------------------------------------
# ROR
# ---------------------------------------------------------------------------
def test_ror_merge_geolocation_lat_lng(tmp_path):
    staging = dict(
        orgs=[(1, "A", "DE", [], "https://ror.org/a"), (2, "B", "DE", [], "https://ror.org/b"), (3, "C", "DE")]
    )
    _, con = run(tmp_path, staging, ror=[("https://ror.org/a", 52.5, 13.4), ("https://ror.org/b", None, None)])
    rows = {
        i - ORG: r
        for i, *r in con.execute(
            "SELECT id, rorStatus, geolocation, geolocation_source, rorTypes FROM organization"
        ).fetchall()
    }
    assert rows[1] == ["active", [52.5, 13.4], "ror", ["education"]]  # [lat, lng]
    assert (
        rows[2][0] == "active" and rows[2][1] is None and rows[2][2] is None
    )  # no coordinates: NULL, not [NULL, NULL]
    assert rows[3][0] is None and rows[3][1] is None and rows[3][2] is None


# ---------------------------------------------------------------------------
# Cordis
# ---------------------------------------------------------------------------
def cordis_case(**overrides):
    """One matched project (grant g1), participants: org1 (PIC 111), org2 (name + country only)."""
    staging = dict(
        orgs=[
            (1, "Totally Different Name", "DE", ["111"]),
            (2, "University of Twente", "GB"),
            (3, "University of Twente", "NL"),  # same name, other country
            (4, "Nameless Country Org", None),  # no country: never matched by name
            (5, "Unrelated", "DE"),
        ],
        projects=[(1, "g1", None)],
        works=[(1, date(2020, 1, 1), True, None)],
        relations=[participant(1, 1), participant(1, 2), participant(1, 3), participant(1, 4), authored(1, 5)],
    )
    cordis = dict(
        projects=[(10, "g1", None)],
        institutions=[
            (20, "Uni Berlin Cordis", "DE", "Street 1", "Berlin", "[13.4, 52.5]", "DE300"),
            (21, "University of Twente", "UK", "Drienerlolaan 5", "Enschede", "[6.85, 52.24]", "NL213"),
            (22, "NAMELESS COUNTRY ORG", "DE", "S", "C", "null", None),
        ],
        links=[
            (10, 20, 100.0, "coordinator", "111"),
            (10, 21, 50.0, "participant", None),
            (10, 22, 5.0, "participant", None),
        ],
    )
    staging.update(overrides.pop("staging", {}))
    cordis.update(overrides.pop("cordis", {}))
    return staging, cordis


def test_pic_first_then_name_country_fallback_never_name_only(tmp_path):
    staging, cordis = cordis_case()
    result, con = run(tmp_path, staging, cordis=cordis)
    rel = {
        (t - ORG): (ec, ty)
        for t, ec, ty in con.execute(
            "SELECT target, cordis_ec_contribution, cordis_type FROM relation WHERE sourceType = 'project'"
        ).fetchall()
    }
    assert rel[1] == (100.0, "coordinator")  # PIC, names differ
    assert rel[2] == (50.0, "participant")  # name + country (UK -> GB)
    assert rel[3] == (None, None)  # same name, wrong country: never name only
    assert rel[4] == (None, None)  # org without country: no name match
    m = result["cordis"]["triplets"]
    assert (m["total"], m["matched"], m["pic"], m["name_country"]) == (3, 2, 1, 1)


def test_pic_match_wins_over_name_match_for_the_same_institution(tmp_path):
    # institution 20 has a PIC that matches org1; it also has the same name + country as org5. The PIC wins:
    # a PIC match means the name fallback is not tried for that row.
    staging, cordis = cordis_case(
        staging={"orgs": [(1, "X", "DE", ["111"]), (5, "Uni Berlin Cordis", "DE")]},
    )
    staging["relations"] = [participant(1, 1), participant(1, 5)]
    result, con = run(tmp_path, staging, cordis=cordis)
    matched = dict(
        con.execute("SELECT id - 500, address_city FROM organization WHERE address_city IS NOT NULL").fetchall()
    )
    assert matched == {1: "Berlin"}


def test_relation_columns_only_on_existing_relations(tmp_path):
    # org1 matches by PIC but has no hasParticipant relation to the project: no relation row is created or updated
    staging, cordis = cordis_case()
    staging["relations"] = [participant(1, 2), authored(1, 5)]
    _, con = run(tmp_path, staging, cordis=cordis)
    rows = con.execute("SELECT count(*), count(cordis_type) FROM relation").fetchone()
    assert rows == (2, 1)
    # ... but org1's address still comes from the (matched) institution
    assert con.execute("SELECT address_city FROM organization WHERE id = ?", [ORG + 1]).fetchone()[0] == "Berlin"


def test_relation_columns_not_set_on_product_direction(tmp_path):
    staging, cordis = cordis_case()
    _, con = run(tmp_path, staging, cordis=cordis)
    assert con.execute("SELECT count(cordis_type) FROM relation WHERE sourceType = 'product'").fetchone()[0] == 0


def test_doi_fallback_only_for_unmatched_cordis_projects(tmp_path):
    staging, cordis = cordis_case(
        staging={"projects": [(1, "no-such-grant", "10.3030/999"), (2, "g2", "10.3030/888")]},
        cordis={
            "projects": [(10, "g1", "https://doi.org/10.3030/999"), (11, "g2", "10.3030/777")],
            "links": [(10, 20, 100.0, "coordinator", "111")],
        },
    )
    staging["relations"] = [participant(1, 1)]
    result, con = run(tmp_path, staging, cordis=cordis)
    p = result["cordis"]["projects"]
    assert (p["cordis"], p["grant"], p["doi"], p["matched"]) == (2, 1, 1, 2)  # 11 matches on grantId g2, 10 via DOI
    assert con.execute("SELECT cordis_type FROM relation WHERE target = ?", [ORG + 1]).fetchone()[0] == "coordinator"


def test_doi_fallback_does_not_add_second_match_for_grant_matched_project(tmp_path):
    staging, cordis = cordis_case(staging={"projects": [(1, "g1", "10.1/x"), (2, "other", "10.1/x")]})
    result, _ = run(tmp_path, staging, cordis=cordis)
    assert result["cordis"]["projects"]["doi"] == 0


def test_addresses_only_on_matched_orgs(tmp_path):
    staging, cordis = cordis_case()
    _, con = run(tmp_path, staging, cordis=cordis)
    rows = {
        i - ORG: r
        for i, *r in con.execute(
            "SELECT id, address_street, address_postalcode, address_city, address_country, nuts3 FROM organization"
        ).fetchall()
    }
    assert rows[1] == ["Street 1", "12345", "Berlin", "DE", "DE300"]
    assert rows[2] == ["Drienerlolaan 5", "12345", "Enschede", "GB", "NL213"]  # UK normalised to GB
    for unmatched in (3, 4, 5):
        assert rows[unmatched] == [None] * 5
    # org4 has no country -> not matched by name, but the country-less name+country match never happens
    assert con.execute("SELECT count(*) FROM organization WHERE address_city IS NOT NULL").fetchone()[0] == 2


def test_cordis_coordinates_only_when_geolocation_null(tmp_path):
    staging, cordis = cordis_case(
        staging={
            "orgs": [
                (1, "Totally Different Name", "DE", ["111"], "https://ror.org/one"),  # has ROR coordinates
                (2, "University of Twente", "GB"),  # no ROR: adopts Cordis
                (3, "Null Geo Org", "DE", ["333"]),  # Cordis coordinates are the literal 'null'
            ]
        },
        cordis={
            "institutions": [
                (20, "Uni Berlin Cordis", "DE", "Street 1", "Berlin", "[13.4, 52.5]", "DE300"),
                (21, "University of Twente", "UK", "Drienerlolaan 5", "Enschede", "[6.85, 52.24]", "NL213"),
                (22, "Nullgeo", "DE", "S", "C", "null", None),
            ],
            "links": [
                (10, 20, 100.0, "coordinator", "111"),
                (10, 21, 50.0, "participant", None),
                (10, 22, 5.0, "participant", "333"),
            ],
        },
    )
    staging["relations"] = [participant(1, 1), participant(1, 2), participant(1, 3)]
    _, con = run(tmp_path, staging, ror=[("https://ror.org/one", 1.5, 2.5)], cordis=cordis)
    geo = {
        i - ORG: (g, s)
        for i, g, s in con.execute("SELECT id, geolocation, geolocation_source FROM organization").fetchall()
    }
    assert geo[1] == ([1.5, 2.5], "ror")  # ROR kept, Cordis (Berlin) not adopted
    assert geo[2] == ([52.24, 6.85], "cordis")  # Cordis [lon, lat] -> [lat, lon]
    assert geo[3] == (None, None)  # 'null' is missing, not a coordinate


def test_institution_choice_pic_first_then_coordinates_then_address(tmp_path):
    # org1 (PIC 111) and its name both match institutions: the PIC one wins even without coordinates
    staging = dict(
        orgs=[(1, "Same Name", "DE", ["111"]), (2, "Pair Name", "DE")],
        projects=[(1, "g1", None)],
        works=[(1, date(2020, 1, 1), True, None)],
        relations=[participant(1, 1), participant(1, 2), authored(1, 1)],
    )
    cordis = dict(
        projects=[(10, "g1", None)],
        institutions=[
            (20, "Other Name", "DE", "Pic Street", "PicCity", "null", None),  # PIC 111, no coordinates
            (21, "Same Name", "DE", "Name Street", "NameCity", "[1.0, 2.0]", None),  # name match with coordinates
            (30, "Pair Name", "DE", None, None, "null", None),  # name only matches; incomplete address
            (31, "Pair Name", "DE", "Street", "City", "null", None),  # complete address wins ...
            (32, "Pair Name", "DE", "Street", "City", "[3.0, 4.0]", None),  # ... but coordinates come first
        ],
        links=[
            (10, 20, 1.0, "coordinator", "111"),
            (10, 21, 1.0, "participant", None),
            (10, 30, 1.0, "participant", None),
            (10, 31, 1.0, "participant", None),
            (10, 32, 1.0, "participant", None),
        ],
    )
    _, con = run(tmp_path, staging, cordis=cordis)
    rows = {
        i - ORG: (c, g) for i, c, g in con.execute("SELECT id, address_city, geolocation FROM organization").fetchall()
    }
    assert rows[1] == ("PicCity", None)  # PIC-matched institution chosen; it has no coordinates -> stays NULL
    assert rows[2] == ("City", [4.0, 3.0])


def test_cordis_merge_ignores_projects_without_match(tmp_path):
    # no matched project: no relation columns, and no name fallback anywhere; only the org-level PIC pass (org1) applies
    staging, cordis = cordis_case(cordis={"projects": [(10, "not-in-openaire", None)]})
    result, con = run(tmp_path, staging, cordis=cordis)
    assert result["cordis"]["projects"]["matched"] == 0
    assert con.execute("SELECT count(cordis_type) FROM relation").fetchone()[0] == 0
    assert dict(
        con.execute("SELECT id - 500, address_city FROM organization WHERE address_city IS NOT NULL").fetchall()
    ) == {1: "Berlin"}


def test_org_level_pic_pass_adds_orgs_without_matched_project(tmp_path):
    staging, cordis = cordis_case(
        staging={
            "orgs": [
                (1, "Totally Different Name", "DE", ["111"]),  # matched in the project pass
                (6, "Elsewhere", "DE", ["666"]),  # PIC in a Cordis project OpenAire does not have
                (7, "Has ROR Coords", "DE", ["777"], "https://ror.org/seven"),
                (8, "Cordis Name Twin", "DE"),  # same name + country as institution 43, but no PIC: never matched here
            ]
        },
        cordis={
            "projects": [(10, "g1", None), (11, "unknown-grant", None)],
            "institutions": [
                (20, "Uni Berlin Cordis", "DE", "Street 1", "Berlin", "[13.4, 52.5]", "DE300"),
                (41, "Far Away", "EL", "Far St", "Athens", "[23.7, 37.9]", "EL301"),
                (42, "Seven", "DE", "Sev St", "Bonn", "[7.1, 50.7]", None),
                (43, "Cordis Name Twin", "DE", "Twin St", "Twinville", "[1.0, 2.0]", None),
                (44, "Other Seven", "DE", "Old St", "Cologne", "null", None),
            ],
            "links": [
                (10, 20, 100.0, "coordinator", "111"),
                (11, 41, 1.0, "participant", "666"),
                (11, 42, 1.0, "participant", "777"),
                (11, 43, 1.0, "participant", None),
                (11, 44, 1.0, "participant", "111"),  # same PIC as org1: org1 is already matched, must not change
            ],
        },
    )
    staging["relations"] = [participant(1, 1)]
    result, con = run(tmp_path, staging, ror=[("https://ror.org/seven", 9.0, 9.5)], cordis=cordis)
    c = result["cordis"]
    assert (c["orgs_matched"], c["orgs_matched_org_pic"]) == (1, 2)
    assert (c["geolocation_cordis_project_pass"], c["geolocation_cordis_org_pic_pass"]) == (1, 1)
    rows = {
        i - ORG: r
        for i, *r in con.execute(
            "SELECT id, address_city, address_country, geolocation, geolocation_source FROM organization"
        ).fetchall()
    }
    assert rows[1] == ["Berlin", "DE", [52.5, 13.4], "cordis"]  # project pass, not overwritten by inst 44 (Cologne)
    assert rows[6] == ["Athens", "GR", [37.9, 23.7], "cordis"]  # org-level PIC pass, EL -> GR
    assert rows[7] == ["Bonn", "DE", [9.0, 9.5], "ror"]  # address filled, ROR coordinates kept
    assert rows[8] == [None, None, None, None]  # no PIC: no name fallback at org level


# ---------------------------------------------------------------------------
# core_v2 legacy geolocation tier
# ---------------------------------------------------------------------------
def geo_of(con):
    return {
        i - ORG: (g, src)
        for i, g, src in con.execute("SELECT id, geolocation, geolocation_source FROM organization").fetchall()
    }


V2_ORGS = [
    (1, "Alpha University", "DE"),
    (2, "Beta Institute", "GR"),
    (3, "Gamma Lab", "DE"),
]


def v2_staging(orgs=V2_ORGS):
    return dict(orgs=orgs, works=[(1, date(2020, 1, 1), True, None)], relations=[authored(1, 1)])


def test_core_v2_name_country_flips_lon_lat_and_normalises_country(tmp_path):
    v2 = dict(
        institutions=[
            ("a", "1", "ALPHA UNIVERSITY ", "DE", 13.4, 52.5),
            ("b", "2", "beta institute", "EL", 23.7, 37.9),  # EL -> GR
            ("c", "3", "Gamma Lab", "AT", 16.3, 48.2),  # same name, other country
        ]
    )
    result, con = run(tmp_path, v2_staging(), core_v2=v2)
    assert geo_of(con) == {1: ([52.5, 13.4], "core_v2"), 2: ([37.9, 23.7], "core_v2"), 3: (None, None)}
    v = result["core_v2"]
    assert (v["pic"], v["name_country"]) == (0, 2)
    assert (v["without_coordinates_before"], v["without_coordinates_after"]) == (3, 1)


def test_core_v2_only_where_geolocation_null(tmp_path):
    staging, cordis = cordis_case(
        staging={
            "orgs": [
                (1, "Totally Different Name", "DE", ["111"], "https://ror.org/one"),  # ROR
                (2, "University of Twente", "GB"),  # Cordis
                (3, "Only V2", "DE"),
            ]
        }
    )
    staging["relations"] = [participant(1, 1), participant(1, 2)]
    v2 = dict(
        institutions=[
            ("a", "1", "Totally Different Name", "DE", 1.0, 1.0),
            ("b", "2", "University of Twente", "GB", 2.0, 2.0),
            ("c", "3", "Only V2", "DE", 3.0, 4.0),
        ]
    )
    _, con = run(tmp_path, staging, ror=[("https://ror.org/one", 1.5, 2.5)], cordis=cordis, core_v2=v2)
    geo = geo_of(con)
    assert geo[1] == ([1.5, 2.5], "ror")
    assert geo[2] == ([52.24, 6.85], "cordis")
    assert geo[3] == ([4.0, 3.0], "core_v2")


def pic_orgs():
    return [
        (1, "Different A", "DE", [" 999 000 001 "]),  # PIC with whitespace on the OpenAire side
        (2, "Name Twin", "DE", ["555000555"]),  # PIC match must win over the name match below
        (3, "Ambiguous Pic Org", "DE", ["777000777"]),
        (4, "Multi Pic Org", "DE", ["301000301"]),
        (5, "Has Pic Inst", "DE"),  # its institution only has a non-standard PIC: name fallback still allowed
        (6, "No Pic Inst", "DE"),
        (7, "Pic Owner", "DE"),  # same name + country as an institution that HAS a usable PIC: not matched by name
    ]


def pic_v2():
    return dict(
        institutions=[
            ("a", "1", "Old Name A", "DE", 1.0, 10.0),  # PIC 999000001
            ("b", "2", "Name Twin", "DE", 2.0, 20.0),  # name twin of org2, no PIC
            ("c", "3", "Pic Owner", "DE", 3.0, 30.0),  # PIC 555000555, org2's PIC
            ("d", "4", "Ambiguous Pic Org", "DE", 4.0, 40.0),  # PIC 777000777, shared with e: not usable
            ("e", "5", "Other Ambiguous", "DE", 5.0, 50.0),  # PIC 777000777
            ("f", "6", "Old Name Multi", "DE", 6.0, 60.0),  # PICs 300000300 and 301000301
            ("g", "7", "Has Pic Inst", "DE", 7.0, 70.0),  # only a non-standard PIC (5 digits)
            ("h", "8", "No Pic Inst", "DE", 8.0, 80.0),
        ],
        pics=[
            ("a", "999000001"),
            ("c", "555000555"),
            ("d", "777000777"),
            ("e", "777000777"),
            ("f", "300000300"),
            ("f", "301000301"),
            ("g", "12345"),
        ],
    )


def test_core_v2_pic_first_digits_only_standard_and_unambiguous_only(tmp_path):
    result, con = run(tmp_path, v2_staging(pic_orgs()), core_v2=pic_v2())
    geo = geo_of(con)
    assert geo[1] == ([10.0, 1.0], "core_v2")  # PIC, names differ, whitespace ignored
    assert geo[2] == ([30.0, 3.0], "core_v2")  # PIC wins over the name twin
    # PIC 777000777 sits on two institutions (pic_has_multiple_institutions): not usable, so institution d has no
    # usable PIC row and the strict name + country fallback applies
    assert geo[3] == ([40.0, 4.0], "core_v2")
    assert geo[4] == ([60.0, 6.0], "core_v2")  # institution with several PICs matches via 301000301
    assert geo[5] == ([70.0, 7.0], "core_v2")  # non-standard PIC is not usable: name fallback
    assert geo[6] == ([80.0, 8.0], "core_v2")  # institution without PIC: name + country
    assert geo[7] == (None, None)  # institution c has a usable PIC: no name fallback
    v = result["core_v2"]
    assert (v["pic"], v["name_country"]) == (3, 3)
    assert v["pic_rows_usable"] == 4  # a, c, f, f


def test_core_v2_pic_flags_decide_not_the_pic_format(tmp_path):
    v2 = pic_v2()
    v2["pics"] = [("a", "999000001", False, False), ("c", "555000555", True, True), ("f", "301000301", True, False)]
    result, con = run(tmp_path, v2_staging(pic_orgs()), core_v2=v2)
    geo = geo_of(con)
    assert geo[1] == (None, None)  # flagged non-standard: never matched by PIC (and no name match for org 1 either)
    assert geo[2] == ([20.0, 2.0], "core_v2")  # PIC of c is ambiguous: name twin b instead
    assert geo[4] == ([60.0, 6.0], "core_v2")
    assert geo[7] == ([30.0, 3.0], "core_v2")  # c has no usable PIC row now: name + country
    assert result["core_v2"]["pic_rows_usable"] == 1


def test_core_v2_without_institution_pic_file_uses_name_country_only(tmp_path, caplog):
    v2 = pic_v2()
    v2["pics"] = None
    result, con = run(tmp_path, v2_staging(pic_orgs()), core_v2=v2)
    geo = geo_of(con)
    assert geo[1] == (None, None)  # no PIC path
    assert geo[2] == ([20.0, 2.0], "core_v2")  # name twin
    assert geo[5] == ([70.0, 7.0], "core_v2") and geo[6] == ([80.0, 8.0], "core_v2")
    assert geo[7] == ([30.0, 3.0], "core_v2")
    assert (result["core_v2"]["pic"], result["core_v2"]["name_country"]) == (0, 5)  # orgs 2, 3, 5, 6, 7 by name
    assert "name + country path only" in caplog.text


def test_core_v2_pic_file_without_the_flag_columns_is_ignored(tmp_path, caplog):
    v2 = pic_v2()
    geo_path, pic_path = make_core_v2(tmp_path / "old.duckdb", **v2)
    con = duckdb.connect(str(pic_path))
    con.execute("ALTER TABLE institution_pic DROP COLUMN pic_is_standard")
    con.close()
    paths = make_paths(tmp_path / "noflags", v2_staging(pic_orgs()))
    paths.core_v2_geo, paths.core_v2_pic = geo_path, pic_path
    result = build(paths)
    assert (result["core_v2"]["pic"], result["core_v2"]["name_country"]) == (0, 5)
    assert "name + country path only" in caplog.text


def test_core_v2_missing_file_is_skipped(tmp_path, caplog):
    result, con = run(tmp_path, v2_staging())  # no core_v2 file configured
    assert result["core_v2"] == {"skipped": True}
    assert "skipping the core_v2 tier" in caplog.text
    paths = make_paths(tmp_path / "gone", v2_staging())
    paths.core_v2_geo = tmp_path / "does_not_exist.duckdb"
    assert build(paths)["core_v2"] == {"skipped": True}


def test_core_v2_config_key_and_cli_override(tmp_path, monkeypatch):
    cfg = _patch_config(monkeypatch, tmp_path)
    cfg["core_v4"]["path_core_v2_geolocations"] = "full_v2.duckdb"
    cfg["core_v4_limit"]["path_core_v2_geolocations"] = "limit_v2.duckdb"
    cfg["core_v4"]["path_core_v2_institution_pic"] = "full_pic.duckdb"
    cfg["core_v4_limit"]["path_core_v2_institution_pic"] = "limit_pic.duckdb"
    assert str(transformation.resolve_paths("full").core_v2_geo) == "full_v2.duckdb"
    assert str(transformation.resolve_paths("limit").core_v2_geo) == "limit_v2.duckdb"
    assert str(transformation.resolve_paths("full").core_v2_pic) == "full_pic.duckdb"
    assert str(transformation.resolve_paths("limit").core_v2_pic) == "limit_pic.duckdb"
    assert str(transformation.resolve_paths("full", core_v2_geo="x.duckdb").core_v2_geo) == "x.duckdb"
    assert str(transformation.resolve_paths("full", core_v2_pic="y.duckdb").core_v2_pic) == "y.duckdb"
    v2, v2_pic = make_core_v2(tmp_path / "v2.duckdb", institutions=[("a", "1", "Org 1", "DE", 5.0, 6.0)], pics=[])
    main(["--core-v2-geo", str(v2), "--core-v2-pic", str(v2_pic)])
    con = duckdb.connect(str(tmp_path / "full_staging.duckdb"), read_only=True)
    assert con.execute(
        "SELECT geolocation, geolocation_source FROM organization WHERE legalName = 'Org 1'"
    ).fetchone() == (
        [6.0, 5.0],
        "core_v2",
    )


# ---------------------------------------------------------------------------
# Limit variant
# ---------------------------------------------------------------------------
def big_staging(n_projects=12):
    """n_projects projects; each produces 2 works and has 1 participant org; org-only works on top."""
    orgs = [(n, f"Org {n}", "DE") for n in range(1, n_projects + 1)]
    projects = [(n, f"g{n}", None) for n in range(1, n_projects + 1)]
    works, relations = [], []
    w = 0
    for n in range(1, n_projects + 1):
        relations.append(participant(n, n))
        for _ in range(2):
            w += 1
            works.append((w, date(2020, 1, 1), True, ["DE"]))
            relations += [produces(n, w), authored(w, n)]
    for _ in range(80):  # org-only works of org 1..3
        w += 1
        works.append((w, date(2015, 1, 1), False, None))
        relations.append(authored(w, 1 + w % 3))
    for _ in range(10):  # works nothing links to
        w += 1
        works.append((w, date(2015, 1, 1), False, None))
    return dict(orgs=orgs, projects=projects, works=works, relations=relations)


def test_limit_sample_is_fk_respecting_and_cap_binds(tmp_path):
    cordis = dict(projects=[(10, "g9", None), (11, "g4", None)])  # projects whose grantId is in Cordis come first
    result, con = run(tmp_path, big_staging(), cordis=cordis, cap=10_000, limit=3)
    assert result["work_cap"] == 9  # min(work_cap, 3 * N)
    projects = ids(con, "project")
    assert len(projects) == 3 and {PROJ + 9, PROJ + 4} <= set(projects)
    assert len(ids(con, "work")) == 9  # cap binds: 6 produced + org-only works, capped
    assert result["seed"]["tiers"][0] == {"total": 6, "kept": 6}
    assert result["seed"]["tiers"][1]["total"] > result["seed"]["tiers"][1]["kept"] == 3
    assert result["dangling_relations"] == 0
    # every relation endpoint exists
    dangling = con.execute("""SELECT count(*) FROM relation r WHERE
              (r.sourceType = 'project' AND r.source NOT IN (SELECT id FROM project))
           OR (r.targetType = 'organization' AND r.target NOT IN (SELECT id FROM organization))""").fetchone()[0]
    assert dangling == 0


def test_limit_cap_never_above_work_cap(tmp_path):
    result, con = run(tmp_path, big_staging(), cap=4, limit=3)
    assert result["work_cap"] == 4 and len(ids(con, "work")) == 4


def _patch_config(monkeypatch, tmp_path):
    staging = make_staging(tmp_path / "staging.duckdb", **big_staging())
    ror = make_ror(tmp_path / "ror.duckdb")
    cordis = make_cordis(tmp_path / "cordis.duckdb")
    cfg = {
        "core_v4": {"path_duck_staging": str(tmp_path / "full_staging.duckdb"), "work_cap": "5"},
        "core_v4_limit": {"path_duck_staging_limit": str(tmp_path / "limit_staging.duckdb")},
    }
    monkeypatch.setattr(transformation, "get_pipeline_paths", lambda: cfg)
    monkeypatch.setattr(
        transformation,
        "get_dumps_paths",
        lambda: {"openaire_dump": {"path_duck_staging_v4": str(staging)}, "ror_dump": {"path_duck": str(ror)}},
    )
    monkeypatch.setattr(
        transformation,
        "get_query_settings",
        lambda: {"cordis": SimpleNamespace(queries={"full_projects_no_pdfs": SimpleNamespace(path_duck=str(cordis))})},
    )
    return cfg


def test_limit_variant_never_writes_full_paths(tmp_path, monkeypatch):
    cfg = _patch_config(monkeypatch, tmp_path)
    full = tmp_path / "full_staging.duckdb"
    full.write_bytes(b"precious full run")
    main(["--limit", "3"])
    assert full.read_bytes() == b"precious full run"
    limit_out = tmp_path / "limit_staging.duckdb"
    assert limit_out.exists()
    con = duckdb.connect(str(limit_out), read_only=True)
    assert con.execute("SELECT count(*) FROM work").fetchone()[0] == 5  # config work_cap 5 < 3 * 3
    con.close()
    assert transformation.resolve_paths("limit").out == limit_out
    assert transformation.resolve_paths("full").out == full


def test_full_variant_uses_config_cap_and_full_path(tmp_path, monkeypatch):
    _patch_config(monkeypatch, tmp_path)
    main([])
    assert not (tmp_path / "limit_staging.duckdb").exists()
    con = duckdb.connect(str(tmp_path / "full_staging.duckdb"), read_only=True)
    assert con.execute("SELECT count(*) FROM work").fetchone()[0] == 5
    assert con.execute("SELECT count(*) FROM project").fetchone()[0] == 12  # not sampled
    con.close()
    main(["--work-cap", "7"])
    con = duckdb.connect(str(tmp_path / "full_staging.duckdb"), read_only=True)
    assert con.execute("SELECT count(*) FROM work").fetchone()[0] == 7


def test_limit_with_explicit_full_variant_is_rejected(tmp_path, monkeypatch):
    _patch_config(monkeypatch, tmp_path)
    with pytest.raises(SystemExit):
        main(["--variant", "full", "--limit", "3"])


def test_cordis_db_override_flag_and_env_var(tmp_path, monkeypatch):
    _patch_config(monkeypatch, tmp_path)
    configured = str(tmp_path / "cordis.duckdb")
    monkeypatch.delenv(transformation.CORDIS_DB_ENV, raising=False)
    assert str(transformation.resolve_paths("full").cordis) == configured
    monkeypatch.setenv(transformation.CORDIS_DB_ENV, "from_env.duckdb")
    assert str(transformation.resolve_paths("full").cordis) == "from_env.duckdb"
    assert str(transformation.resolve_paths("full", cordis_db="flag.duckdb").cordis) == "flag.duckdb"  # flag beats env


# ---------------------------------------------------------------------------
# link_tier, dangling relations, required staging columns (Phase 3F)
# ---------------------------------------------------------------------------
def test_link_tier_kept_on_work_as_last_smallint_column(tmp_path):
    staging = dict(
        orgs=[(1, "Org", "DE")],
        projects=[(1, "g1", None)],
        works=[(1, date(2000, 1, 1), True, None), (2, date(2020, 1, 1), True, None), (3, date(2021, 1, 1), True, None)],
        relations=[produces(1, 1), authored(1, 1), authored(2, 1)],  # w1 is both: tier 0 wins
    )
    _, con = run(tmp_path, staging)
    assert dict((i - WORK, t) for i, t in con.execute("SELECT id, link_tier FROM work").fetchall()) == {1: 0, 2: 1}
    cols = con.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'work' ORDER BY ordinal_position").fetchall()
    assert cols[-1] == ("link_tier", "SMALLINT")


def test_link_tier_follows_the_trim_when_the_cap_binds(tmp_path):
    staging = dict(
        orgs=[(1, "Org", "DE")],
        projects=[(1, "g1", None)],
        works=[(1, date(1995, 1, 1), False, None), (2, date(2024, 1, 1), True, None), (3, date(2023, 1, 1), True, None)],
        relations=[produces(1, 1), authored(2, 1), authored(3, 1)],
    )
    _, con = run(tmp_path, staging, cap=2)
    assert dict((i - WORK, t) for i, t in con.execute("SELECT id, link_tier FROM work").fetchall()) == {1: 0, 2: 1}


def test_dangling_relations_to_missing_projects_and_orgs_are_dropped(tmp_path, caplog):
    staging = dict(
        orgs=[(1, "Org", "DE")],  # org 2 is not in organization
        projects=[(1, "g1", None)],  # project 2 is not in project
        works=[(1, date(2020, 1, 1), True, None), (2, date(2019, 1, 1), True, None)],
        relations=[
            participant(1, 1),  # fine
            participant(1, 2),  # org 2 missing
            participant(2, 1),  # project 2 missing
            participant(2, 2),  # both missing: counted once as dropped, in both endpoint counts
            produces(1, 1),  # fine
            produces(2, 2),  # project 2 missing (the work exists)
            authored(1, 1),  # fine
            authored(2, 2),  # org 2 missing
        ],
    )
    with caplog.at_level("INFO"):
        result, con = run(tmp_path, staging)
    rels = sorted(con.execute("SELECT sourceType, source - 0, targetType, target FROM relation").fetchall())
    assert rels == sorted(
        [
            ("project", PROJ + 1, "organization", ORG + 1),
            ("project", PROJ + 1, "product", WORK + 1),
            ("product", WORK + 1, "organization", ORG + 1),
        ]
    )
    assert result["seed"]["dangling_endpoints"] == {"project": 3, "organization": 3, "dropped": 5}
    assert result["relation_missing_endpoints"] == 0
    assert "the project is missing: 3" in caplog.text and "the organization is missing: 3" in caplog.text
    # work 2 stays (it is a kept work); only its relations to nothing are gone
    assert works_kept(con) == [1, 2]


def test_no_dangling_relations_when_everything_exists(tmp_path):
    staging = dict(orgs=[(1, "O", "DE")], projects=[(1, "g", None)], works=[(1, date(2020, 1, 1), True, None)],
                   relations=[participant(1, 1), produces(1, 1), authored(1, 1)])
    result, con = run(tmp_path, staging)
    assert result["seed"]["dangling_endpoints"] == {"project": 0, "organization": 0, "dropped": 0}
    assert con.execute("SELECT count(*) FROM relation").fetchone()[0] == 3


def _drop_columns(staging_path, *pairs):
    con = duckdb.connect(str(staging_path))
    for table, column in pairs:
        con.execute(f"ALTER TABLE {table} DROP COLUMN {column}")
    con.close()


def test_full_run_aborts_when_the_staging_lacks_doi_or_countries(tmp_path, monkeypatch, caplog):
    _patch_config(monkeypatch, tmp_path)
    previous = tmp_path / "full_staging.duckdb"
    previous.write_bytes(b"last good staging")
    _drop_columns(tmp_path / "staging.duckdb", ("project", "doi"), ("work", "countries"))
    with caplog.at_level("ERROR"), pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 1
    assert "stage_openaire_dump_v4" in caplog.text and "project.doi" in caplog.text and "work.countries" in caplog.text
    assert "--allow-missing-columns" in caplog.text
    assert previous.read_bytes() == b"last good staging"  # the abort happens before the old output is removed


def test_full_run_with_allow_missing_columns_warns_and_skips(tmp_path, monkeypatch, caplog):
    _patch_config(monkeypatch, tmp_path)
    _drop_columns(tmp_path / "staging.duckdb", ("project", "doi"))
    with caplog.at_level("WARNING"):
        main(["--allow-missing-columns"])
    assert "no project.doi: skipping the DOI fallback" in caplog.text
    con = duckdb.connect(str(tmp_path / "full_staging.duckdb"), read_only=True)
    assert con.execute("SELECT count(*) FROM work").fetchone()[0] == 5


def test_full_run_with_a_complete_staging_needs_no_flag(tmp_path, monkeypatch):
    _patch_config(monkeypatch, tmp_path)
    main([])  # both columns present: strict check passes


def test_limit_variant_and_staging_db_override_stay_tolerant_but_warn(tmp_path, monkeypatch, caplog):
    _patch_config(monkeypatch, tmp_path)
    staging = tmp_path / "staging.duckdb"
    _drop_columns(staging, ("project", "doi"), ("work", "countries"))
    with caplog.at_level("WARNING"):
        main(["--limit", "3"])
    assert "no project.doi" in caplog.text and "no work.countries" in caplog.text
    caplog.clear()
    with caplog.at_level("WARNING"):
        main(["--staging-db", str(staging)])  # explicit override in a full run: tolerant too
    assert "no project.doi" in caplog.text and "no work.countries" in caplog.text


def test_check_staging_columns_direct(tmp_path):
    con = duckdb.connect()
    path = make_staging(tmp_path / "s.duckdb", orgs=[(1, "O", "DE")])
    con.execute(f"ATTACH '{path}' AS openaire (READ_ONLY)")
    assert transformation.check_staging_columns(con, strict=True) == []
    con.execute("DETACH openaire")
    _drop_columns(path, ("work", "countries"))
    con.execute(f"ATTACH '{path}' AS openaire (READ_ONLY)")
    assert transformation.check_staging_columns(con, strict=False) == [("work", "countries")]
    with pytest.raises(RuntimeError, match="stage_openaire_dump_v4"):
        transformation.check_staging_columns(con, strict=True)
    assert transformation.check_staging_columns(con, strict=True, allow_missing=True) == [("work", "countries")]
