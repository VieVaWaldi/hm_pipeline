from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

from pipelines.core_v4 import assemble as asm
from pipelines.core_v4.assemble import AssembleError, assemble
from pipelines.core_v4.enrichment.fixtures import make_staging_fixture, project_id, work_id
from pipelines.core_v4.enrichment.fingerprint import staging_stamp
from pipelines.core_v4.enrichment.side_outputs import SCHEMAS, SideOutput

MEM = 4000  # small dev budget: memory_limit = max(mem - headroom, mem / 2)


def org_id(i: int) -> int:
    return duckdb.sql(f"SELECT hash('o{i}')").fetchone()[0]


def stamp_of(staging_path, entity, tier=None):
    con = duckdb.connect(str(staging_path), read_only=True)
    try:
        return staging_stamp(con, entity, tier)
    finally:
        con.close()


def side(root, name, entity, rows, complete=True, tier=None, stamp="staging"):
    """Writes a fake side output: rows are dicts of the schema's columns. Its `_SUCCESS` carries the fingerprint of the
    staging next to the side-output root (tmp_path/staging.duckdb), like a real enrichment run; stamp=None: legacy marker."""
    out = SideOutput(root, name, entity, tier=tier)
    out.begin()
    if rows:
        cols = {f.name: pa.array([r.get(f.name) for r in rows], f.type) for f in SCHEMAS[name]}
        out.write(pa.table(cols))
    if stamp == "staging":
        stamp = stamp_of(Path(root).parent / "staging.duckdb", entity, tier)
    if complete:
        out.finish(stamp)
    return out


@pytest.fixture
def staging(tmp_path):
    path = make_staging_fixture(tmp_path / "staging.duckdb")
    con = duckdb.connect(str(path))
    # work 1 has a second description that must survive the overwrite of element 1
    con.execute("UPDATE work SET descriptions = [descriptions[1], 'Second description'] WHERE openaireId = 'w1'")
    con.execute("UPDATE organization SET geolocation = {'lat': 1.0, 'lon': 2.0}, geolocation_source = 'ror' WHERE legalName = 'Org 1'")
    con.execute(
        "CREATE TABLE relation (source UBIGINT, sourceType VARCHAR, target UBIGINT, targetType VARCHAR,"
        " relType STRUCT(name VARCHAR, type VARCHAR), provenance STRUCT(provenance VARCHAR, trust VARCHAR),"
        " validated BOOLEAN, cordis_ec_contribution DOUBLE, cordis_type VARCHAR)"
    )
    rels = [
        (project_id(1), "project", org_id(1), "organization", "hasParticipant"),
        (project_id(2), "project", org_id(2), "organization", "hasParticipant"),
        (project_id(1), "project", work_id(1), "product", "produces"),
        (work_id(1), "product", org_id(1), "organization", "hasAuthorInstitution"),
        (work_id(2), "product", org_id(2), "organization", "hasAuthorInstitution"),
    ]
    for s, st, t, tt, name in rels:
        con.execute(
            "INSERT INTO relation VALUES (?, ?, ?, ?, {'name': ?, 'type': 'x'}, NULL, NULL, NULL, NULL)", [s, st, t, tt, name]
        )
    con.close()
    return path


@pytest.fixture
def oa_topics(tmp_path):
    path = tmp_path / "oa_topics_raw.duckdb"
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE oa_topics_raw (topic_id BIGINT, topic_name VARCHAR, subfield_id BIGINT, subfield_name VARCHAR,"
        " field_id BIGINT, field_name VARCHAR, domain_id BIGINT, domain_name VARCHAR, keywords JSON, summary VARCHAR,"
        " wikipedia_url VARCHAR)"
    )
    con.execute("INSERT INTO oa_topics_raw VALUES (10001, 'Geology', 1901, 'Geochem', 19, 'Earth', 3, 'Physical', '[\"a\",\"b\"]', 's', 'u')")
    con.execute("INSERT INTO oa_topics_raw VALUES (10002, 'Chemistry', 1902, 'Phys chem', 19, 'Earth', 3, 'Physical', NULL, NULL, NULL)")
    con.close()
    return path


@pytest.fixture
def enrich(tmp_path):
    """A complete set of fake side outputs for both entities and the organization enrichments."""
    root = tmp_path / "enrich"
    p1, p2, p3 = project_id(1), project_id(2), project_id(3)
    w1, w2 = work_id(1), work_id(2)
    side(root, "nllb", "project", [
        {"id": p1, "field": "title", "text_en": "Digital Heritage Platform", "src_lang": "deu_Latn"},
        {"id": p1, "field": "summary", "text_en": "A project about digital archives.", "src_lang": "deu_Latn"},
        {"id": p3, "field": "title", "text_en": "Title only", "src_lang": "fra_Latn"},
    ])
    side(root, "nllb/seen", "project", [])
    side(root, "nllb", "work", [
        {"id": w1, "field": "title", "text_en": "The future of museums", "src_lang": "deu_Latn"},
        {"id": w1, "field": "description", "text_en": "Museums in the digital age.", "src_lang": "deu_Latn"},
        {"id": w2, "field": "subjects", "text_en": "Roma", "src_lang": "deu_Latn"},  # a non-overwritten field still marks is_translated
    ])
    side(root, "nllb/seen", "work", [])
    side(root, "topics", "project", [
        {"id": p1, "topic_id": 10001, "score": 0.9},
        {"id": p1, "topic_id": 10002, "score": 0.3},
        {"id": p2, "topic_id": 10002, "score": 0.5},
        {"id": project_id(99), "topic_id": 10001, "score": 0.1},  # not in staging
    ])
    side(root, "topics", "work", [{"id": w1, "topic_id": 10001, "score": 0.7}])
    side(root, "theme", "project", [{"id": p1, "theme": "Digital"}])
    side(root, "theme", "work", [])
    side(root, "dch", "project", [{"id": p1, "is_ch": True, "pred": 0.95}, {"id": p2, "is_ch": False, "pred": 0.1}])
    side(root, "dch", "work", [{"id": w2, "is_ch": True, "pred": 0.8}])
    side(root, "minorities", "project", [{"id": p2, "minority_qid": ["Q1", "Q2"]}])
    side(root, "minorities", "work", [{"id": w2, "minority_qid": ["Q3"]}])
    side(root, "pillars", "project", [{"id": p1, "pillars": 0b10101}])
    side(root, "pillars", "work", [])
    side(root, "geolocation", "organization", [
        {"id": org_id(1), "lat": 50.0, "lon": 60.0, "geolocation_source": "mapbox"},  # staging has coordinates: ignored
        {"id": org_id(2), "lat": 51.5, "lon": -0.1, "geolocation_source": "mapbox_temporary"},
    ])
    side(root, "regions", "organization", [{"id": org_id(1), "region": "Central Europe"}, {"id": org_id(2), "region": "Western Europe"}])
    return root


def run(entity, staging, enrich, oa_topics, out, **kw):
    kw.setdefault("mem_mb", MEM)
    kw.setdefault("threads", 2)
    return assemble(entity, staging, enrich, out, oa_topics_db=oa_topics, **kw)


def rows(path, sql):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def tables(path):
    return {r[0] for r in rows(path, "SHOW TABLES")}


# ---- projects ------------------------------------------------------------------------------
def test_project_file_contents(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "gold" / "projects.duckdb"
    stats = run("project", staging, enrich, oa_topics, out)
    assert tables(out) == {"project", "organization", "relation", "topic", "relation_topic"}
    assert stats["project"]["rows"] == 4

    got = {
        r[0]: r[1:]
        for r in rows(out, "SELECT openaireId, title, summary, is_translated, is_ch, pred, minority_qid, pillars, theme FROM project")
    }
    assert got["p1"] == ("Digital Heritage Platform", "A project about digital archives.", True, True, pytest.approx(0.95), [], 0b10101, "Digital")
    # p2: nothing translated, keeps the original text; dch + minorities but no pillars/theme
    assert got["p2"] == ("Sustainable inclusive cities", "Resilient urban planning for everyone.", False, False, pytest.approx(0.1), ["Q1", "Q2"], 0, None)
    # p3: only the title was translated, summary is NULL and stays NULL; no dch row: NULL
    assert got["p3"] == ("Title only", None, True, None, None, [], 0, None)
    # p4: untouched by every enrichment: defaults
    assert got["p4"] == (None, None, False, None, None, [], 0, None)
    # untouched staging columns keep their position and values
    cols = [r[0] for r in rows(out, "SELECT column_name FROM information_schema.columns WHERE table_name = 'project' ORDER BY ordinal_position")]
    assert cols[:8] == ["id", "openaireId", "title", "summary", "acronym", "keywords", "subjects", "doi"]
    assert cols[8:] == ["is_translated", "is_ch", "pred", "minority_qid", "pillars", "theme"]
    types = dict(rows(out, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'project'"))
    assert types["pillars"] == "UTINYINT" and types["pred"] == "FLOAT" and types["minority_qid"] == "VARCHAR[]"
    assert rows(out, "SELECT acronym FROM project WHERE openaireId = 'p1'") == [("DKP",)]


def test_no_work_rows_in_project_file_and_vice_versa(tmp_path, staging, enrich, oa_topics):
    pout, wout = tmp_path / "projects.duckdb", tmp_path / "works.duckdb"
    run("project", staging, enrich, oa_topics, pout)
    run("work", staging, enrich, oa_topics, wout)
    assert "work" not in tables(pout) and "project" not in tables(wout)
    assert tables(wout) == {"work", "relation", "relation_topic"}  # topic + organization live in the projects file

    # project file: project<->organization only; works file: everything a product touches
    prel = rows(pout, "SELECT sourceType, targetType FROM relation ORDER BY 1, 2")
    assert prel == [("project", "organization")] * 2
    wrel = rows(wout, "SELECT sourceType, targetType FROM relation ORDER BY 1, 2")
    assert wrel == [("product", "organization")] * 2 + [("project", "product")]

    assert {r[0] for r in rows(pout, "SELECT DISTINCT type FROM relation_topic")} == {"project"}
    assert {r[0] for r in rows(wout, "SELECT DISTINCT type FROM relation_topic")} == {"work"}


def test_relation_topic_and_topic_table(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "projects.duckdb"
    run("project", staging, enrich, oa_topics, out)
    got = sorted(rows(out, "SELECT s.openaireId, rt.topic_id, rt.score, rt.type, rt.created_at IS NOT NULL FROM relation_topic rt JOIN project s ON s.id = rt.source_id"))
    assert got == [("p1", 10001, pytest.approx(0.9), "project", True), ("p1", 10002, pytest.approx(0.3), "project", True),
                   ("p2", 10002, pytest.approx(0.5), "project", True)]
    # the topic id of a project that is not in staging was dropped
    assert rows(out, "SELECT count(*) FROM relation_topic") == [(3,)]

    assert rows(out, "SELECT id, subfield_id, field_id, domain_id, topic_name, keywords FROM topic ORDER BY id") == [
        (10001, "1901", "19", "3", "Geology", '["a","b"]'),
        (10002, "1902", "19", "3", "Chemistry", None),
    ]
    types = dict(rows(out, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'topic'"))
    assert types["id"] == "INTEGER" and types["created_at"] == "TIMESTAMP"


def test_organization_region_and_geolocation_overlay_only_where_null(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "projects.duckdb"
    stats = run("project", staging, enrich, oa_topics, out)
    got = {r[0]: r[1:] for r in rows(out, "SELECT legalName, geolocation, geolocation_source, region FROM organization")}
    # Org 1 had ROR coordinates in staging: the Mapbox row must not touch them
    assert got["Org 1"] == ({"lat": 1.0, "lon": 2.0}, "ror", "Central Europe")
    # Org 2 had none: filled, and the source string is kept verbatim
    assert got["Org 2"] == ({"lat": 51.5, "lon": -0.1}, "mapbox_temporary", "Western Europe")
    # Org 3: nothing anywhere
    assert got["Org 3"] == (None, None, None)
    assert stats["organization"] == {"rows": 3, "region": 2, "geolocation_overlay": 1}


def test_organization_with_list_geolocation_and_no_region_column(tmp_path, staging, enrich, oa_topics):
    """core_v3-style staging: geolocation DOUBLE[] = [lat, lon], no geolocation_source / region columns."""
    con = duckdb.connect(str(staging))
    con.execute("ALTER TABLE organization DROP COLUMN geolocation")
    con.execute("ALTER TABLE organization DROP COLUMN geolocation_source")
    con.execute("ALTER TABLE organization DROP COLUMN region")
    con.execute("ALTER TABLE organization ADD COLUMN geolocation DOUBLE[]")
    con.execute("UPDATE organization SET geolocation = [1.0, 2.0] WHERE legalName = 'Org 1'")
    con.close()
    out = tmp_path / "projects.duckdb"
    run("project", staging, enrich, oa_topics, out)
    got = {r[0]: r[1:] for r in rows(out, "SELECT legalName, geolocation, geolocation_source, region FROM organization")}
    assert got["Org 1"] == ([1.0, 2.0], None, "Central Europe")
    assert got["Org 2"] == ([51.5, -0.1], "mapbox_temporary", "Western Europe")


# ---- works ---------------------------------------------------------------------------------
def test_work_text_overwrite_replaces_description_one_only(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "works.duckdb"
    run("work", staging, enrich, oa_topics, out)
    got = {r[0]: r[1:] for r in rows(out, "SELECT openaireId, title, descriptions, is_translated, is_ch, pred, minority_qid, pillars, theme FROM work")}
    assert got["w1"][:3] == ("The future of museums", ["Museums in the digital age.", "Second description"], True)
    # w2: only `subjects` was translated: text untouched, but is_translated is true
    assert got["w2"] == ("Roma communities in Europe", ["A study of Romani minorities."], True, True, pytest.approx(0.8), ["Q3"], 0, None)
    # w3: nothing in the side outputs
    assert got["w3"] == ("Only a title", [], False, None, None, [], 0, None)
    assert rows(out, "SELECT count(*) FROM work") == [(4,)]


def test_work_sharding_matches_single_pass(tmp_path, staging, enrich, oa_topics):
    a, b = tmp_path / "a.duckdb", tmp_path / "b.duckdb"
    run("work", staging, enrich, oa_topics, a, shards=1)
    run("work", staging, enrich, oa_topics, b, shards=5)
    q = "SELECT * FROM work ORDER BY id"
    assert rows(a, q) == rows(b, q)
    q = "SELECT * FROM relation_topic ORDER BY source_id, topic_id"
    assert [r[:4] for r in rows(a, q)] == [r[:4] for r in rows(b, q)]


def test_duplicate_side_output_rows_do_not_multiply_rows(tmp_path, staging, enrich, oa_topics):
    # a rerun that left a second part with the same keys (begin() only resets the marker, not the parts)
    side(enrich, "dch", "project", [{"id": project_id(1), "is_ch": True, "pred": 0.95}])
    side(enrich, "topics", "project", [{"id": project_id(1), "topic_id": 10001, "score": 0.9}] * 2)
    dst = tmp_path / "projects.duckdb"
    run("project", staging, enrich, oa_topics, dst)
    assert rows(dst, "SELECT count(*), count(DISTINCT id) FROM project") == [(4, 4)]
    # p1 has topics 10001 (three copies now) and 10002: one row each
    assert rows(dst, "SELECT topic_id FROM relation_topic WHERE source_id = %d ORDER BY 1" % project_id(1)) == [(10001,), (10002,)]


# ---- skip / _SUCCESS -----------------------------------------------------------------------
def test_missing_success_blocks_assembly_and_names_the_output(tmp_path, staging, enrich, oa_topics):
    (enrich / "dch" / "project" / "_SUCCESS").unlink()
    out = tmp_path / "projects.duckdb"
    with pytest.raises(AssembleError, match=r"dch/project"):
        run("project", staging, enrich, oa_topics, out)
    assert not out.exists()
    # the works file does not need the project outputs
    run("work", staging, enrich, oa_topics, tmp_path / "works.duckdb")


def test_nllb_seen_success_is_required_too(tmp_path, staging, enrich, oa_topics):
    (enrich / "nllb" / "seen" / "work" / "_SUCCESS").unlink()
    with pytest.raises(AssembleError, match=r"seen"):
        run("work", staging, enrich, oa_topics, tmp_path / "works.duckdb")


def test_skip_keeps_schema_with_defaults_even_without_any_side_output(tmp_path, staging, enrich, oa_topics):
    full = tmp_path / "full.duckdb"
    run("project", staging, enrich, oa_topics, full)
    empty = tmp_path / "empty_enrich"  # nothing was ever run
    out = tmp_path / "skipped.duckdb"
    run("project", staging, empty, oa_topics, out, skip=sorted(asm.SKIPPABLE))

    q = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s' ORDER BY table_name, ordinal_position"
    for t in ("project", "organization", "relation_topic", "topic"):
        assert rows(out, q % t) == rows(full, q % t)
    assert rows(out, "SELECT count(*) FROM relation_topic") == [(0,)]
    assert rows(out, "SELECT count(*) FROM project WHERE is_translated OR is_ch IS NOT NULL OR pred IS NOT NULL OR len(minority_qid) > 0 OR pillars <> 0 OR theme IS NOT NULL") == [(0,)]
    orig = {r[0]: r[1:] for r in rows(staging, "SELECT openaireId, title, summary FROM project")}
    assert {r[0]: r[1:] for r in rows(out, "SELECT openaireId, title, summary FROM project")} == orig
    # region stays NULL, staging geolocation untouched, no overlay
    assert rows(out, "SELECT count(region), count(geolocation), count(geolocation_source) FROM organization") == [(0, 1, 1)]


def test_skip_ignores_existing_parts_and_only_blocks_missing_ones(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "projects.duckdb"
    (enrich / "nllb" / "project" / "_SUCCESS").unlink()
    run("project", staging, enrich, oa_topics, out, skip=["nllb"])
    # nllb parts exist on disk but were skipped: not applied
    assert rows(out, "SELECT count(*) FROM project WHERE is_translated") == [(0,)]
    assert rows(out, "SELECT title FROM project WHERE openaireId = 'p1'") == [("Digitale Kulturerbe Plattform",)]
    assert rows(out, "SELECT count(*) FROM project WHERE is_ch IS NOT NULL") == [(2,)]  # other outputs still applied


def test_unknown_skip_name(tmp_path, staging, enrich, oa_topics):
    with pytest.raises(AssembleError, match="unknown --skip"):
        run("project", staging, enrich, oa_topics, tmp_path / "x.duckdb", skip=["nope"])


# ---- atomic output -------------------------------------------------------------------------
def test_failure_leaves_no_file_behind(tmp_path, staging, enrich):
    bad_topics = tmp_path / "bad_topics.duckdb"
    duckdb.connect(str(bad_topics)).close()  # exists but has no oa_topics_raw: fails after project/organization are built
    out = tmp_path / "gold" / "projects.duckdb"
    with pytest.raises(duckdb.Error):
        run("project", staging, enrich, bad_topics, out)
    assert not out.exists()
    assert sorted(p.name for p in out.parent.iterdir()) == []  # no .tmp, .wal or spill dir


def test_failure_keeps_the_previous_good_output(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "projects.duckdb"
    run("project", staging, enrich, oa_topics, out)
    before = rows(out, "SELECT count(*) FROM project")
    bad_topics = tmp_path / "bad_topics.duckdb"
    duckdb.connect(str(bad_topics)).close()
    with pytest.raises(duckdb.Error):
        run("project", staging, enrich, bad_topics, out)
    assert rows(out, "SELECT count(*) FROM project") == before
    assert sorted(p.name for p in tmp_path.iterdir() if p.name.startswith("projects")) == ["projects.duckdb"]


def test_rebuild_replaces_the_output_and_never_touches_staging(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "projects.duckdb"
    run("project", staging, enrich, oa_topics, out)
    stg_before = rows(staging, "SELECT count(*) FROM project")
    duckdb.connect(str(out)).execute("CREATE TABLE junk AS SELECT 1").close()
    run("project", staging, enrich, oa_topics, out)
    assert "junk" not in tables(out)
    assert rows(staging, "SELECT count(*) FROM project") == stg_before
    assert "is_translated" not in {r[0] for r in rows(staging, "SELECT column_name FROM information_schema.columns WHERE table_name = 'project'")}


# ---- cli -----------------------------------------------------------------------------------
def test_cli_with_explicit_paths(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "cli_works.duckdb"
    asm.main([
        "--entity", "work", "--staging-db", str(staging), "--enrichment-dir", str(enrich), "--out", str(out),
        "--skip", "theme, pillars", "--mem-mb", str(MEM), "--threads", "2", "--shards", "2",
    ])
    assert rows(out, "SELECT count(*) FROM work") == [(4,)]


def test_cli_missing_success_exits_with_message(tmp_path, staging, oa_topics):
    with pytest.raises(SystemExit, match="_SUCCESS"):
        asm.main([
            "--entity", "project", "--staging-db", str(staging), "--enrichment-dir", str(tmp_path / "none"),
            "--out", str(tmp_path / "o.duckdb"), "--oa-topics-db", str(oa_topics), "--mem-mb", str(MEM),
        ])
    assert not (tmp_path / "o.duckdb").exists()


# ---- tiers ---------------------------------------------------------------------------------
WORK_SIDES = [n for n in asm.ENTITY_ENRICHMENTS] + ["nllb/seen"]


def write_work_tier(root, tier, w_ids, complete=True):
    """Fake work side outputs of one tier: a dch row for every id, an nllb title for the first one."""
    for name in WORK_SIDES:
        rows = []
        if name == "dch":
            rows = [{"id": i, "is_ch": True, "pred": 0.5} for i in w_ids]
        elif name == "nllb" and w_ids:
            rows = [{"id": w_ids[0], "field": "title", "text_en": f"EN {w_ids[0]}", "src_lang": "deu_Latn"}]
        elif name == "nllb/seen" and w_ids:
            rows = [{"id": i, "field": "title", "src_lang": "deu_Latn", "translated": True} for i in w_ids]
        elif name == "topics" and w_ids:
            rows = [{"id": w_ids[0], "topic_id": 10001, "score": 0.5}]
        side(root, name, "work", rows, complete=complete, tier=tier)


@pytest.fixture
def tiered(tmp_path, staging, oa_topics):
    """Staging with a tier-1 work that has its own relation; no work side outputs at all yet."""
    con = duckdb.connect(str(staging))
    con.execute("INSERT INTO relation VALUES (?, 'product', ?, 'organization', {'name': 'hasAuthorInstitution', 'type': 'x'}, NULL, NULL, NULL, NULL)", [work_id(3), org_id(2)])
    con.close()
    return tmp_path / "enrich"


def test_link_tier_passes_through_to_the_work_table(tmp_path, staging, enrich, oa_topics):
    out = tmp_path / "works.duckdb"
    run("work", staging, enrich, oa_topics, out)
    assert dict(rows(out, "SELECT openaireId, link_tier FROM work")) == {"w1": 0, "w2": 0, "w3": 1, "w4": 1}
    types = dict(rows(out, "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'work'"))
    assert types["link_tier"] == "SMALLINT"


def test_tier0_assemble_from_tier0_side_outputs_only(tmp_path, staging, tiered, oa_topics):
    write_work_tier(tiered, 0, [work_id(1), work_id(2)])
    out = tmp_path / "linked.duckdb"
    stats = run("work", staging, tiered, oa_topics, out, tier=0)
    assert stats["work"]["rows"] == 2
    assert dict(rows(out, "SELECT openaireId, link_tier FROM work")) == {"w1": 0, "w2": 0}
    assert rows(out, "SELECT title FROM work WHERE openaireId = 'w1'") == [(f"EN {work_id(1)}",)]  # tier-0 nllb applied
    # the relation of the tier-1 work is absent, the tier-0 ones are there
    assert sorted(rows(out, "SELECT sourceType, targetType FROM relation")) == [("product", "organization")] * 2 + [("project", "product")]
    assert rows(out, "SELECT count(*) FROM relation_topic") == [(1,)]
    # the full file is not possible yet: only the tier-0 markers exist
    with pytest.raises(AssembleError, match="without _SUCCESS"):
        run("work", staging, tiered, oa_topics, tmp_path / "full.duckdb")


def test_full_assemble_after_both_tiers_and_tier1_run_keeps_tier0_usable(tmp_path, staging, tiered, oa_topics):
    write_work_tier(tiered, 0, [work_id(1), work_id(2)])
    # a tier-1 run in progress (begin() ran, no finish yet) does not invalidate the tier-0 markers
    for name in WORK_SIDES:
        SideOutput(tiered, name, "work", tier=1).begin()
    run("work", staging, tiered, oa_topics, tmp_path / "linked.duckdb", tier=0)
    write_work_tier(tiered, 1, [work_id(3), work_id(4)])
    assert all(SideOutput(tiered, n, "work").is_complete() for n in WORK_SIDES)  # both tiers: _SUCCESS, with a combined stamp
    full = tmp_path / "full.duckdb"
    run("work", staging, tiered, oa_topics, full)
    assert dict(rows(full, "SELECT openaireId, link_tier FROM work")) == {"w1": 0, "w2": 0, "w3": 1, "w4": 1}
    assert rows(full, "SELECT count(*) FROM relation") == [(4,)]
    assert rows(full, "SELECT count(*) FROM work WHERE is_ch") == [(4,)]
    # tier 0 rows are the same in both files
    q = "SELECT * FROM work WHERE link_tier = 0 ORDER BY id"
    assert rows(tmp_path / "linked.duckdb", q) == rows(full, q)


def test_tier_only_for_works_and_needs_the_column(tmp_path, staging, enrich, oa_topics):
    with pytest.raises(AssembleError, match="only for --entity work"):
        run("project", staging, enrich, oa_topics, tmp_path / "p.duckdb", tier=0)
    con = duckdb.connect(str(staging))
    con.execute("ALTER TABLE work DROP COLUMN link_tier")
    con.close()
    with pytest.raises(AssembleError, match="link_tier"):
        run("work", staging, enrich, oa_topics, tmp_path / "w.duckdb", tier=0)


def test_cli_tier0_default_out_key(tmp_path, staging, tiered, oa_topics):
    write_work_tier(tiered, 0, [work_id(1), work_id(2)])
    out = tmp_path / "cli_linked.duckdb"
    asm.main(["--entity", "work", "--tier", "0", "--staging-db", str(staging), "--enrichment-dir", str(tiered),
              "--out", str(out), "--mem-mb", str(MEM), "--threads", "2"])
    assert rows(out, "SELECT count(*) FROM work") == [(2,)]


# ---- staging fingerprint -------------------------------------------------------------------
def rebuild_staging(staging):
    """A staging rebuild that dropped a project and a work (like a different --limit)."""
    con = duckdb.connect(str(staging))
    con.execute("DELETE FROM project WHERE openaireId = 'p3'")
    con.execute("DELETE FROM work WHERE openaireId = 'w4'")
    con.close()


def test_stale_side_outputs_block_assembly_with_a_clear_message(tmp_path, staging, enrich, oa_topics):
    rebuild_staging(staging)
    out = tmp_path / "projects.duckdb"
    with pytest.raises(AssembleError, match=r"different staging.*dch/project.*computed against 4 ids, staging has 3 ids"):
        run("project", staging, enrich, oa_topics, out)
    assert not out.exists()
    with pytest.raises(AssembleError, match="--allow-stale"):
        run("work", staging, enrich, oa_topics, tmp_path / "works.duckdb")


def test_allow_stale_lets_named_outputs_through(tmp_path, staging, enrich, oa_topics, caplog):
    rebuild_staging(staging)
    out = tmp_path / "projects.duckdb"
    every = sorted(asm.SKIPPABLE)
    with pytest.raises(AssembleError, match="different staging"):
        run("project", staging, enrich, oa_topics, out, allow_stale=[n for n in every if n != "dch"])  # one is still stale
    with caplog.at_level("WARNING"):
        run("project", staging, enrich, oa_topics, out, allow_stale=every)
    assert "STALE side output used" in caplog.text
    # orphan rows (side outputs of the vanished p3 / of project 99) are dropped by the join, the rest is applied
    assert rows(out, "SELECT count(*) FROM project") == [(3,)]
    assert rows(out, "SELECT title FROM project WHERE openaireId = 'p1'") == [("Digital Heritage Platform",)]
    with pytest.raises(AssembleError, match="unknown --skip / --allow-stale"):
        run("project", staging, enrich, oa_topics, out, allow_stale=["nope"])


def test_allow_stale_nllb_applies_cached_translations_only_to_ids_that_still_exist(tmp_path, staging, enrich, oa_topics):
    rebuild_staging(staging)  # p3 had a cached translation ("Title only"); it is gone from staging
    out = tmp_path / "projects.duckdb"
    others = [n for n in asm.SKIPPABLE if n != "nllb"]
    run("project", staging, enrich, oa_topics, out, skip=others, allow_stale=["nllb"])
    assert dict(rows(out, "SELECT openaireId, is_translated FROM project")) == {"p1": True, "p2": False, "p4": False}
    assert rows(out, "SELECT title FROM project WHERE openaireId = 'p1'") == [("Digital Heritage Platform",)]


def test_legacy_success_without_fingerprint_is_stale(tmp_path, staging, enrich, oa_topics):
    side(enrich, "dch", "project", [{"id": project_id(1), "is_ch": True, "pred": 0.9}], stamp=None)  # an older run: empty _SUCCESS
    with pytest.raises(AssembleError, match=r"dch/project: computed against no fingerprint recorded"):
        run("project", staging, enrich, oa_topics, tmp_path / "p.duckdb")
    run("project", staging, enrich, oa_topics, tmp_path / "p.duckdb", allow_stale=["dch"])


def test_tier0_outputs_stay_valid_when_only_tier1_changed(tmp_path, staging, tiered, oa_topics):
    write_work_tier(tiered, 0, [work_id(1), work_id(2)])
    con = duckdb.connect(str(staging))  # staging rebuilt with a different tier 1
    con.execute("DELETE FROM work WHERE openaireId = 'w4'")
    con.close()
    run("work", staging, tiered, oa_topics, tmp_path / "linked.duckdb", tier=0)  # tier-0 stamp unchanged: fine
    con = duckdb.connect(str(staging))
    con.execute("DELETE FROM work WHERE openaireId = 'w2'")  # ... but a tier-0 change is stale
    con.close()
    with pytest.raises(AssembleError, match="different staging"):
        run("work", staging, tiered, oa_topics, tmp_path / "linked.duckdb", tier=0)
