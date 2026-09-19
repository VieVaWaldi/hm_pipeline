"""Tests for geolocation_coverage.py against tiny hand-built OpenAire staging, ROR, Cordis and core_v2 fixtures
with known answers (every number below was worked out by hand from the fixture), plus one smoke test against the
real local core_v2 files.

    ENV=dev uv run python -m pytest src/pipelines/core_v4/investigation/test_geolocation_coverage.py
"""

import hashlib
import runpy
from pathlib import Path

import duckdb
import pytest

SCRIPT = Path(__file__).with_name("geolocation_coverage.py")
REPO = Path(__file__).resolve().parents[4]
REAL_V2_GEO = REPO / "data" / "pile" / "core_v2_geolocation" / "core_v2_geolocations.duckdb"
REAL_V2_PIC = REPO / "data" / "pile" / "core_v2_geolocation" / "core_v2_institution_pic.duckdb"

PROJ, WORK, ORG = 0, 100, 500  # id ranges (UBIGINT like the hashed real ids)


def load():
    return runpy.run_path(str(SCRIPT), run_name="geolocation_coverage_under_test")


# ---------------------------------------------------------------------------
# fixture builders
# ---------------------------------------------------------------------------
def _insert(con, table, rows):
    for row in rows:
        con.execute(f"INSERT INTO {table} VALUES ({', '.join('?' * len(row))})", list(row))


def make_staging(path, *, orgs=(), projects=(), relations=(), with_doi=True):
    """orgs: (n, name, country, pics, rorId|None); projects: (n, grantId, doi);
    relations: (source, sourceType, target, targetType, relType name)."""
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE organization (id UBIGINT, openaireId VARCHAR, legalName VARCHAR, countryCode VARCHAR,"
        " rorId VARCHAR, pids STRUCT(scheme VARCHAR, value VARCHAR)[])"
    )
    con.execute(
        "CREATE TABLE project (id UBIGINT, openaireId VARCHAR, grantId VARCHAR, title VARCHAR"
        + (", doi VARCHAR)" if with_doi else ")")
    )
    con.execute(
        "CREATE TABLE relation (source UBIGINT, sourceType VARCHAR, target UBIGINT, targetType VARCHAR,"
        ' relType STRUCT("name" VARCHAR, "type" VARCHAR), validated BOOLEAN)'
    )
    for n, name, cc, pics, ror in orgs:
        _insert(con, "organization", [(ORG + n, f"o{n}", name, cc, ror, [{"scheme": "PIC", "value": p} for p in pics])])
    for n, grant, doi in projects:
        row = (PROJ + n, f"p{n}", grant, f"Project {n}") + ((doi,) if with_doi else ())
        _insert(con, "project", [row])
    for s, st, t, tt, rel in relations:
        _insert(con, "relation", [(s, st, t, tt, {"name": rel, "type": "x"}, True)])
    con.close()
    return path


def participant(p, o):
    return (PROJ + p, "project", ORG + o, "organization", "hasParticipant")


def make_ror(path, rows=()):
    """rows: (id, lat|None, lng|None)."""
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE organizations (id VARCHAR,"
        " locations STRUCT(geonames_id BIGINT, geonames_details STRUCT(country_code VARCHAR, lat DOUBLE, lng DOUBLE))[])"
    )
    for rid, lat, lng in rows:
        loc = [{"geonames_id": 1, "geonames_details": {"country_code": "DE", "lat": lat, "lng": lng}}]
        _insert(con, "organizations", [(rid, loc)])
    con.close()
    return path


def make_cordis(path, *, projects=(), institutions=(), links=()):
    """projects: (id, id_original, doi); institutions: (id, name, country, street, city, geolocation_json);
    links: (project_id, institution_id, pic)."""
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE project (id INTEGER, id_original VARCHAR, doi VARCHAR)")
    con.execute(
        "CREATE TABLE institution (id INTEGER, legal_name VARCHAR, street VARCHAR, city VARCHAR,"
        " country VARCHAR, geolocation JSON)"
    )
    con.execute(
        "CREATE TABLE j_project_institution (project_id INTEGER, institution_id INTEGER, organization_id VARCHAR)"
    )
    _insert(con, "project", projects)
    _insert(con, "institution", [(i, name, street, city, cc, geo) for i, name, cc, street, city, geo in institutions])
    _insert(con, "j_project_institution", links)
    con.close()
    return path


def make_core_v2(path, institutions=(), pics=None):
    """institutions: (id, name, country, lon, lat); pics: None = no pic file, else rows
    (institution.id, pic, pic_is_standard, pic_has_multiple_institutions) in the SEPARATE file <stem>.pic.duckdb."""
    con = duckdb.connect(str(path))
    con.execute("CREATE TABLE institution (id VARCHAR, legal_name VARCHAR, country VARCHAR, geolocation DOUBLE[])")
    for i, name, cc, lon, lat in institutions:
        _insert(con, "institution", [(i, name, cc, [lon, lat])])
    con.close()
    if pics is None:
        return path, None
    pic_path = path.with_name(path.stem + ".pic.duckdb")
    con = duckdb.connect(str(pic_path))
    con.execute(
        "CREATE TABLE institution_pic (institution_id VARCHAR, pic VARCHAR, pic_is_standard BOOLEAN,"
        " pic_has_multiple_institutions BOOLEAN)"
    )
    _insert(con, "institution_pic", pics)
    con.close()
    return path, pic_path


# The scenario. Organisations (n, name, country, pics, rorId) and where their coordinates come from:
#  1  ror                  R1 (lat 48, lng 2); also carries PIC 111111111 like o2 (a PIC on two orgs); ROR is never overwritten
#  2  cordis_project       PIC 111111111 -> Cordis institutions 1 (coords) and 8 (no coords) in a matched project; the
#                          one WITH real coordinates is chosen -> lat 50, lon 5
#  3  cordis_pic           PIC 222222222 only in a Cordis project OpenAire does not have -> org-level pass -> lat 51, lon 6
#  4  cordis_project       name + country ("Foo University", DE) of Cordis institutions 2 and 7 (different coordinates);
#                          the highest institution id wins -> lat 52.6, lon 13.5
#  5  core_v2_name_country "Bar Institute", UK (core_v2 says GB: both normalise to GB) -> lat 51.5, lon 0.5
#  6  core_v2_pic          PIC 333333333 -> core_v2 institution v2b -> lat 48, lon 9
#  7  none, address        PIC 444444444 -> Cordis institution 5 with street + city but coordinates 'null' (Mapbox candidate)
#  8  none, no address
#  9  none                 name matches a Cordis institution but the org has no country: never name only
# 10  ror                  same rorId as o1 (duplicate coordinates)
# 11  cordis_project       only via the DOI fallback of the project match (cordis_pic without project.doi)
# 12  core_v2_name_country its only PIC row is shared by 2 institutions (unusable) -> name fallback; lat 48.8, lon 2.3
# 13  none                 name matches core_v2 institution v2d, which HAS a usable PIC row -> no name fallback
# 14  none                 core_v2 institution has out of range coordinates
# 15  ror                  does not participate in any project
ORGS = [
    (1, "Org One", "FR", ["111111111"], "https://ror.org/r1"),
    (2, "Alpha Org renamed", "NL", ["111111111"], None),
    (3, "Gamma renamed", "BE", ["222222222"], None),
    (4, "Foo University", "DE", [], None),
    (5, "Bar Institute", "UK", [], None),
    (6, "Zeta renamed", "DE", ["333333333"], None),
    (7, "Delta renamed", "NL", ["444444444"], None),
    (8, "Nothing Ltd", "ES", [], None),
    (9, "Baz Lab", None, [], None),
    (10, "Org Ten", "FR", [], "https://ror.org/r1"),
    (11, "Epsilon renamed", "IT", ["555555555"], None),
    (12, "Multi Inst", "FR", ["777777777"], None),
    (13, "Has Pic Inst", "FR", [], None),
    (14, "Bad Coord", "DE", [], None),
    (15, "Not participating", "ES", [], "https://ror.org/r2"),
]
# Projects (n, grantId, doi) and their participants:
#  1 pA: o1, o10 (same coordinates)    2 pB: o2, o3      3 pC: o4, o5      4 pD: o6, o7 (o7 is the Mapbox headroom)
#  5 pE: o8 alone                      6 pF: o1, o9, o8  7 pG: nobody      8 pH: o11, o8
#  9 pI: o12, o13, o14                10 pJ: o1, o2   (o1 is in pA, pF and pJ)
PROJECTS = [
    (1, None, None),
    (2, "g1", None),
    (3, "g2", None),
    (4, None, "10.1/x"),  # DOI-only match to Cordis project 3 (written as a resolver URL there)
    (5, None, None),
    (6, None, None),
    (7, None, None),
    (8, None, None),
    (9, None, None),
    (10, None, None),
]
PARTICIPANTS = {
    1: [1, 10],
    2: [2, 3],
    3: [4, 5],
    4: [6, 7],
    5: [8],
    6: [1, 9, 8],
    8: [11, 8],
    9: [12, 13, 14],
    10: [1, 2],
}
EXPECTED_TIER = {
    1: "ror", 2: "cordis_project", 3: "cordis_pic", 4: "cordis_project", 5: "core_v2_name_country",
    6: "core_v2_pic", 7: None, 8: None, 9: None, 10: "ror", 11: "cordis_project", 12: "core_v2_name_country",
    13: None, 14: None, 15: "ror",
}  # fmt: skip
EXPECTED_LATLON = {1: (48.0, 2.0), 2: (50.0, 5.0), 3: (51.0, 6.0), 4: (52.6, 13.5), 5: (51.5, 0.5), 6: (48.0, 9.0),
                   10: (48.0, 2.0), 11: (41.9, 12.5), 12: (48.8, 2.3), 15: (60.0, 10.0)}  # fmt: skip


def build_inputs(tmp_path, *, with_doi=True, with_core_v2=True, with_pic=True):
    relations = [participant(p, o) for p, orgs in PARTICIPANTS.items() for o in orgs]
    relations += [
        participant(1, 1),  # duplicate row
        participant(1, 99),  # organisation that does not exist
        (WORK + 1, "product", ORG + 13, "organization", "hasAuthorInstitution"),  # not a participation
        (PROJ + 7, "project", WORK + 1, "product", "produces"),
    ]
    staging = make_staging(
        tmp_path / "staging.duckdb", orgs=ORGS, projects=PROJECTS, relations=relations, with_doi=with_doi
    )
    ror = make_ror(tmp_path / "ror.duckdb", [("https://ror.org/r1", 48.0, 2.0), ("https://ror.org/r2", 60.0, 10.0)])
    cordis = make_cordis(
        tmp_path / "cordis.duckdb",
        projects=[(1, "g1", None), (2, "g2", None), (3, "g3", "https://doi.org/10.1/X"), (99, "g99", None)],
        institutions=[
            (1, "Alpha Org", "NL", "S1", "Utrecht", "[5.0, 50.0]"),
            (2, "Foo University", "DE", "S2", "Berlin", "[13.4, 52.5]"),
            (3, "Baz Lab", "DE", "S3", "Bonn", "[10.0, 53.0]"),
            (4, "Gamma", "BE", "S4", "Gent", "[6.0, 51.0]"),
            (5, "Delta", "NL", "Street 5", "Delft", "null"),
            (6, "Epsilon", "IT", "S6", "Rome", "[12.5, 41.9]"),
            (7, "Foo University", "DE", "S7", "Potsdam", "[13.5, 52.6]"),
            (8, "Alpha Org 2", "NL", "S8", "Leiden", "null"),
        ],
        links=[
            (1, 1, "111111111"), (1, 8, "111111111"),
            (2, 2, None), (2, 3, None), (2, 7, None),
            (99, 4, "222222222"), (99, 5, "444444444"),
            (3, 6, "555555555"),
        ],
    )  # fmt: skip
    v2_geo = v2_pic = None
    if with_core_v2:
        v2_geo, v2_pic = make_core_v2(
            tmp_path / "core_v2_geolocations.duckdb",
            institutions=[
                ("v2a", "Bar Institute", "GB", 0.5, 51.5),
                ("v2b", "Zeta Org", "DE", 9.0, 48.0),
                ("v2c", "Multi Inst", "FR", 2.3, 48.8),
                ("v2d", "Has Pic Inst", "FR", 3.0, 45.0),
                ("v2e", "Bad Coord", "DE", 200.0, 100.0),
            ],
            pics=(
                [
                    ("v2b", "333333333", True, False),
                    ("v2c", "777777777", True, True),  # a PIC shared by 2 institutions: not usable
                    ("v2d", "888888888", True, False),
                ]
                if with_pic
                else None
            ),
        )
    return staging, ror, cordis, v2_geo, v2_pic


def run(tmp_path, staging, ror, cordis, v2_geo, v2_pic, capsys=None):
    """Runs main() with the temp db kept; returns (JSON dict, read-only connection to the temp db, stdout)."""
    mod = load()
    out = tmp_path / "out.json"
    tmp_dir = tmp_path / "tmp"
    argv = [
        "--staging-db", str(staging), "--ror-db", str(ror), "--cordis-db", str(cordis),
        "--core-v2-geo", str(v2_geo or tmp_path / "missing_geo.duckdb"),
        "--core-v2-pic", str(v2_pic or tmp_path / "missing_pic.duckdb"),
        "--out", str(out), "--tmp-dir", str(tmp_dir), "--keep-tmp", "--mem-mb", "4000", "--threads", "2",
    ]  # fmt: skip
    result = mod["main"](argv)
    con = duckdb.connect(str(tmp_dir / "geolocation_coverage_tmp.duckdb"), read_only=True)
    return result, con


@pytest.fixture
def scenario(tmp_path):
    result, con = run(tmp_path, *build_inputs(tmp_path))
    yield result, con
    con.close()


def tiers_of(rows):
    return {t["tier"]: t for t in rows["tiers"]}


# ---------------------------------------------------------------------------
# organisations
# ---------------------------------------------------------------------------
def test_each_org_gets_the_expected_tier_and_coordinates(scenario):
    _, con = scenario
    got = {
        oid - ORG: (tier, lat, lon)
        for oid, tier, lat, lon in con.execute("SELECT id, tier, lat, lon FROM org_geo").fetchall()
    }
    for n, tier in EXPECTED_TIER.items():
        assert got[n][0] == tier, f"org {n}: {got[n]}"
        if tier is not None:
            assert got[n][1:] == pytest.approx(EXPECTED_LATLON[n]), f"org {n}"  # [lat, lon], flipped from [lon, lat]
        else:
            assert got[n][1:] == (None, None)


def test_org_tier_table_all_and_participating(scenario):
    result, _ = scenario
    allo = result["orgs_all"]
    assert allo["total"] == 15 and allo["without_coordinates"] == 5
    t = tiers_of(allo)
    assert [
        t[k]["incremental"] for k in ("ror", "cordis_project", "cordis_pic", "core_v2_pic", "core_v2_name_country")
    ] == [3, 3, 1, 1, 2]
    assert [t[k]["cumulative"] for k in t] == [3, 6, 7, 8, 10]
    assert t["core_v2_name_country"]["without_coordinates"] == 5
    assert t["core_v2_name_country"]["cumulative_pct"] == pytest.approx(66.67)

    part = result["orgs_participating"]  # o15 does not participate; the hasAuthorInstitution row does not count
    assert part["total"] == 14 and part["without_coordinates"] == 5
    t = tiers_of(part)
    assert [t[k]["incremental"] for k in t] == [2, 3, 1, 1, 2]
    assert t["core_v2_name_country"]["cumulative"] == 9


def test_org_chosen_by_pic_overrides_name_and_ror_is_never_overwritten(scenario):
    _, con = scenario
    # o1 matched Cordis (PIC 111111111) but keeps its ROR coordinates; o2 gets the institution with real coordinates
    assert con.execute("SELECT tier FROM org_geo WHERE id = ?", [ORG + 1]).fetchone() == ("ror",)
    assert con.execute("SELECT inst_id FROM chosen1 WHERE org_id = ?", [ORG + 2]).fetchone() == (1,)
    assert con.execute("SELECT inst_id FROM chosen1 WHERE org_id = ?", [ORG + 4]).fetchone() == (7,)  # highest id


# ---------------------------------------------------------------------------
# projects (the key result)
# ---------------------------------------------------------------------------
def test_project_counts_by_tier(scenario):
    result, _ = scenario
    p = result["projects"]
    assert p["projects_total"] == 10
    assert p["with_participant"] == 9  # pG has nobody; the missing org o99 and the authorship row do not count
    assert p["with_ge2_participants"] == 8  # all but pE (an org listed twice in pA counts once)
    t = tiers_of(p)
    # cumulative: projects with >=1 / >=2 geolocated participants / >=2 at distinct coordinates
    assert [(t[k]["ge1_geolocated"], t[k]["ge2_geolocated"], t[k]["ge2_geolocated_distinct"]) for k in t] == [
        (3, 1, 0),  # ror: pA has o1 + o10 at the SAME point (2 geolocated, 1 distinct); o1 is also alone in pF and pJ
        (6, 2, 1),  # + cordis_project: pB, pC, pH get one, pJ gets a second (o1 + o2)
        (6, 3, 2),  # + cordis_pic: o3 makes pB collaboration-capable
        (7, 3, 2),  # + core_v2_pic: pD gets one
        (8, 4, 3),  # + core_v2_name_country: pC gets a second, pI gets one
    ]
    assert [t[k]["ge2_geolocated_gain"] for k in t] == [1, 1, 1, 0, 1]
    assert t["core_v2_name_country"]["ge2_geolocated_pct_of_ge2_participants"] == 50.0
    assert t["core_v2_name_country"]["ge2_distinct_pct_of_ge2_participants"] == 37.5


def test_org_in_several_projects_counts_in_each(scenario):
    _, con = scenario
    per_project = dict(con.execute("SELECT project_id - ?, g1 FROM proj_stats", [PROJ]).fetchall())
    assert per_project[1] == 2 and per_project[6] == 1 and per_project[10] == 1  # o1 (ROR) sits in pA, pF and pJ
    assert 7 not in per_project  # no participant


def test_headroom_upper_bound(scenario):
    result, _ = scenario
    h = result["headroom"]
    assert h["participating_orgs"] == 14
    assert h["participating_orgs_without_coordinates"] == 5  # o7, o8, o9, o13, o14
    assert h["with_cordis_address"] == 1  # only o7 (street + city + country, coordinates 'null')
    assert h["without_coordinates_and_without_address"] == 4
    assert h["projects_ge2_geolocated_now"] == 4
    assert h["projects_ge2_geolocated_added"] == 1  # pD: o6 + o7
    assert h["projects_ge2_geolocated_upper_bound"] == 5
    assert h["projects_ge2_distinct_added"] == 1


# ---------------------------------------------------------------------------
# sanity block
# ---------------------------------------------------------------------------
def test_sanity_counts(scenario):
    result, _ = scenario
    s = result["sanity"]
    assert s["cordis_projects"] == {
        "total": 4, "matched_grantid": 2, "matched_doi_fallback": 1, "doi_fallback_active": True,
    }  # fmt: skip
    p1, p2 = s["pass1_project_scoped"], s["pass2_org_pic"]
    assert (p1["orgs_matched"], p1["orgs_with_coordinates"]) == (4, 4)  # o1, o2, o4, o11
    assert (p1["orgs_matched_by_pic"], p1["orgs_matched_by_name_country_only"]) == (3, 1)
    assert p1["orgs_matched_to_several_cordis_institutions"] == 3  # o1, o2 (inst 1 + 8), o4 (inst 2 + 7)
    assert p1["of_which_with_different_coordinates"] == 1  # only o4
    assert (p2["orgs_matched"], p2["orgs_with_coordinates"]) == (2, 1)  # o3, o7
    assert p2["orgs_matched_to_several_cordis_institutions"] == 0
    c = s["pic_conflicts"]
    assert c["pics_on_several_orgs"] == 1 and c["orgs_sharing_a_pic_with_another_org"] == 2  # 111111111 on o1 and o2
    assert c["cordis_pics_on_several_institutions"] == 1 and c["orgs_with_several_pic_pids"] == 0
    assert s["country_code"]["orgs_without_raw_country_code"] == 1  # o9
    assert s["country_code"]["participating_orgs_without_valid_country_code"] == 1
    assert s["core_v2"]["usable_pic_rows"] == 2  # the shared PIC of v2c is not usable
    assert s["core_v2"]["usable_institutions"] == 4  # v2e has out of range coordinates


# ---------------------------------------------------------------------------
# degraded inputs, read-only guarantees, output
# ---------------------------------------------------------------------------
def test_without_project_doi_column_the_doi_fallback_is_skipped(tmp_path):
    result, con = run(tmp_path, *build_inputs(tmp_path, with_doi=False))
    assert result["sanity"]["cordis_projects"]["doi_fallback_active"] is False
    assert result["sanity"]["cordis_projects"]["matched_doi_fallback"] == 0
    # o11 is then found by the org-level PIC pass instead of the project-scoped merge
    assert con.execute("SELECT tier FROM org_geo WHERE id = ?", [ORG + 11]).fetchone() == ("cordis_pic",)
    t = tiers_of(result["orgs_all"])
    assert (t["cordis_project"]["incremental"], t["cordis_pic"]["incremental"]) == (2, 2)
    con.close()


def test_missing_core_v2_files_skip_the_tier(tmp_path, capsys):
    result, con = run(tmp_path, *build_inputs(tmp_path, with_core_v2=False))
    t = tiers_of(result["orgs_all"])
    assert t["core_v2_pic"]["incremental"] == 0 and t["core_v2_name_country"]["incremental"] == 0
    assert t["core_v2_name_country"]["cumulative"] == 7
    assert result["meta"]["skipped_or_missing"]["core_v2_geolocations_used"] is False
    assert "core_v2 geolocations not found" in capsys.readouterr().err
    con.close()


def test_missing_pic_file_leaves_name_and_country_only(tmp_path):
    result, con = run(tmp_path, *build_inputs(tmp_path, with_pic=False))
    t = tiers_of(result["orgs_all"])
    assert t["core_v2_pic"]["incremental"] == 0  # o6 has only a PIC
    # without any pic rows no institution has a usable PIC, so o13 (Has Pic Inst, FR) now matches by name + country too
    assert t["core_v2_name_country"]["incremental"] == 3  # o5, o12, o13
    assert result["meta"]["skipped_or_missing"]["core_v2_pic_used"] is False
    con.close()


def test_inputs_untouched_temp_db_removed_and_json_written(tmp_path):
    staging, ror, cordis, v2_geo, v2_pic = build_inputs(tmp_path)
    inputs = [staging, ror, cordis, v2_geo, v2_pic]

    def digest(p):
        return hashlib.sha256(Path(p).read_bytes()).hexdigest()

    before = {str(p): digest(p) for p in inputs}
    mod = load()
    out = tmp_path / "res" / "cov.json"
    out.parent.mkdir()
    tmp_dir = tmp_path / "scratch"
    mod["main"](
        ["--staging-db", str(staging), "--ror-db", str(ror), "--cordis-db", str(cordis), "--core-v2-geo", str(v2_geo),
         "--core-v2-pic", str(v2_pic), "--out", str(out), "--tmp-dir", str(tmp_dir), "--mem-mb", "4000", "--threads", "2"]
    )  # fmt: skip
    assert before == {str(p): digest(p) for p in inputs}
    assert not (tmp_dir / "geolocation_coverage_tmp.duckdb").exists()  # no --keep-tmp
    import json

    data = json.loads(out.read_text())
    assert {"meta", "orgs_all", "orgs_participating", "projects", "headroom", "sanity"} <= set(data)


def test_markdown_tables_are_printed(tmp_path, capsys):
    run(tmp_path, *build_inputs(tmp_path))[1].close()
    text = capsys.readouterr().out
    for heading in ("## 1.", "## 2.", "## 3.", "## 4.", "## 5."):
        assert heading in text
    assert "| Cordis, org-level PIC pass |" in text


def test_works_against_a_v4_staging_with_extra_columns(tmp_path):
    staging, ror, cordis, v2_geo, v2_pic = build_inputs(tmp_path)
    con = duckdb.connect(str(staging))
    con.execute("ALTER TABLE organization ADD COLUMN extra VARCHAR")
    con.execute("CREATE TABLE work (id UBIGINT, countries VARCHAR[])")
    con.close()
    result, con = run(tmp_path, staging, ror, cordis, v2_geo, v2_pic)
    assert result["orgs_all"]["total"] == 15
    con.close()


# ---------------------------------------------------------------------------
# real local core_v2 files (skipped when they are not on this machine)
# ---------------------------------------------------------------------------
@pytest.mark.skipif(not (REAL_V2_GEO.exists() and REAL_V2_PIC.exists()), reason="local core_v2 files missing")
def test_real_core_v2_files_pic_and_name_tiers(tmp_path):
    from common.countries import normalize_country

    src = duckdb.connect()
    src.execute(f"ATTACH '{REAL_V2_GEO}' AS g (READ_ONLY)")
    src.execute(f"ATTACH '{REAL_V2_PIC}' AS p (READ_ONLY)")
    src.execute(
        "CREATE TEMP TABLE usable AS SELECT * FROM p.institution_pic WHERE pic_is_standard AND NOT pic_has_multiple_institutions"
    )
    pic, inst_id = src.execute("SELECT pic, institution_id FROM usable ORDER BY pic LIMIT 1").fetchone()
    lon, lat = src.execute(
        "SELECT geolocation[1], geolocation[2] FROM g.institution WHERE id = ?", [inst_id]
    ).fetchone()
    cand = src.execute(
        """SELECT legal_name, country FROM g.institution WHERE id NOT IN (SELECT institution_id FROM usable)
           AND len(geolocation) = 2 AND geolocation[1] BETWEEN -180 AND 180 AND geolocation[2] BETWEEN -90 AND 90
           ORDER BY id LIMIT 200"""
    ).fetchall()
    name, country = next((n, c) for n, c in cand if normalize_country(c))
    orgs = [
        (1, "Unrelated", "NL", [pic], None),
        (2, "  " + name.upper() + " ", country, [], None),
        (3, name.upper(), None, [], None),
    ]
    staging = make_staging(
        tmp_path / "s.duckdb",
        orgs=orgs,
        projects=[(1, None, None)],
        relations=[participant(1, 1), participant(1, 2), participant(1, 3)],
    )
    ror = make_ror(tmp_path / "ror.duckdb")
    cordis = make_cordis(tmp_path / "cordis.duckdb")
    result, con = run(tmp_path, staging, ror, cordis, REAL_V2_GEO, REAL_V2_PIC)
    got = {
        oid - ORG: (tier, lat_, lon_)
        for oid, tier, lat_, lon_ in con.execute("SELECT id, tier, lat, lon FROM org_geo").fetchall()
    }
    assert got[1][0] == "core_v2_pic" and got[1][1:] == pytest.approx((lat, lon))
    assert got[2][0] == "core_v2_name_country"  # name trimmed and lower-cased, normalised country
    assert got[3] == (None, None, None)  # name only, no country: never matches
    p = result["projects"]
    assert p["with_ge2_participants"] == 1 and tiers_of(p)["core_v2_name_country"]["ge2_geolocated"] == 1
    con.close()
