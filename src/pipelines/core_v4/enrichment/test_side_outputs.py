import pyarrow as pa
import pytest
import duckdb

from pipelines.core_v4.enrichment.side_outputs import SCHEMAS, Shard, SideOutput


def dch_table(ids):
    return pa.table({"id": pa.array(ids, pa.uint64()), "is_ch": [True] * len(ids), "pred": [0.9] * len(ids)})


def test_shard_parse_and_sql():
    assert Shard.parse(None) == Shard(0, 1)
    assert Shard.parse("2/4") == Shard(2, 4)
    assert Shard(0, 1).sql("id") is None
    assert Shard(2, 4).sql("e.id") == "(e.id % 4) = 2"
    for bad in ("4/4", "-1/2", "0/0"):
        with pytest.raises(ValueError):
            Shard.parse(bad)


def test_write_is_atomic_and_numbered(tmp_path):
    out = SideOutput(tmp_path, "dch", "work")
    out.begin()
    p0 = out.write(dch_table([1, 2]))
    p1 = out.write(dch_table([3]))
    assert p0.name == "part-000-000000.parquet" and p1.name == "part-000-000001.parquet"
    assert not list(out.dir.glob("*.tmp"))
    assert out.write(dch_table([])) is None  # empty writes nothing
    con = duckdb.connect()
    assert out.read_all(con).aggregate("count(*)").fetchone()[0] == 3


def test_ids_above_int64_survive(tmp_path):
    big = 2**64 - 1
    out = SideOutput(tmp_path, "dch", "work")
    out.begin()
    out.write(dch_table([big]))
    assert duckdb.sql(f"SELECT id FROM ({out.read_all_sql()})").fetchone()[0] == big


def test_begin_removes_stale_tmp_and_success(tmp_path):
    out = SideOutput(tmp_path, "dch", "work")
    out.begin()
    out.write(dch_table([1]))
    out.finish()
    assert out.is_complete()
    (out.dir / "part-000-000009.parquet.tmp").write_bytes(b"torn")
    out.begin()  # a new run may add rows: not complete any more, torn tmp gone
    assert not out.is_complete()
    assert not list(out.dir.glob("*.tmp"))
    assert len(out.parts()) == 1  # existing parts are kept for resume


def test_done_ids_resume_anti_join(tmp_path):
    out = SideOutput(tmp_path, "dch", "work")
    assert duckdb.sql(out.done_ids_sql()).fetchall() == []  # no parts yet: empty relation
    out.begin()
    out.write(dch_table([1, 2]))
    out.write(dch_table([2, 3]))
    con = duckdb.connect()
    con.execute("CREATE TABLE src AS SELECT range::UBIGINT AS id FROM range(1, 6)")
    todo = con.execute(
        f"SELECT id FROM src s WHERE NOT EXISTS (SELECT 1 FROM ({out.done_ids_sql()}) d WHERE d.id = s.id) ORDER BY id"
    ).fetchall()
    assert todo == [(4,), (5,)]


def test_empty_output_reads_with_schema(tmp_path):
    out = SideOutput(tmp_path, "minorities", "project")
    out.begin()
    out.finish()
    con = duckdb.connect()
    rel = out.read_all(con)
    assert rel.columns == ["id", "minority_qid"]
    assert rel.fetchall() == []


def test_schemas_cover_every_contract_output():
    assert set(SCHEMAS) == {"nllb", "nllb/seen", "topics", "theme", "dch", "minorities", "pillars", "geolocation", "regions"}
    for name in SCHEMAS:  # each schema builds an empty relation
        out = SideOutput("/nonexistent", name, "project")
        duckdb.sql(out.read_all_sql())


def test_nested_name_path(tmp_path):
    out = SideOutput(tmp_path, "nllb/seen", "work")
    out.begin()
    out.write(pa.table({"id": pa.array([1], pa.uint64()), "field": ["title"], "src_lang": ["deu_Latn"], "translated": [True]}))
    assert (tmp_path / "nllb" / "seen" / "work" / "part-000-000000.parquet").exists()


def test_shards_write_disjoint_parts_and_success_after_last(tmp_path):
    outs = [SideOutput(tmp_path, "dch", "work", shard=Shard(i, 3)) for i in range(3)]
    for i, out in enumerate(outs):
        out.begin()
        out.write(dch_table([i]))
    assert [o.finish() for o in outs[:2]] == [False, False]
    assert not outs[0].is_complete()
    assert outs[2].finish() is True
    assert outs[0].is_complete()
    names = sorted(p.name for p in outs[0].parts())
    assert names == ["part-000-000000.parquet", "part-001-000000.parquet", "part-002-000000.parquet"]
    assert not list(outs[0].dir.glob("_SUCCESS.*"))


def test_reset_only_drops_own_shard(tmp_path):
    a, b = SideOutput(tmp_path, "dch", "work", shard=Shard(0, 2)), SideOutput(tmp_path, "dch", "work", shard=Shard(1, 2))
    for o in (a, b):
        o.begin()
        o.write(dch_table([1]))
    a.begin(reset=True)
    assert [p.name for p in a.parts()] == ["part-001-000000.parquet"]


# ---- tiers and staging stamps --------------------------------------------------------------
STAMP0 = {"n": 2, "sum": "10", "xor": "6"}
STAMP1 = {"n": 3, "sum": "100", "xor": "5"}


def test_tier_parts_are_tagged_and_shard_reset_is_tier_scoped(tmp_path):
    t0 = SideOutput(tmp_path, "dch", "work", shard=Shard(0, 2), tier=0)
    t1 = SideOutput(tmp_path, "dch", "work", shard=Shard(0, 4), tier=1)  # same shard index, another tier and count
    for o, i in ((t0, 1), (t1, 2)):
        o.begin()
        o.write(dch_table([i]))
    assert t0.parts()[0].name == "part-t0-000-000000.parquet"
    assert [p.name for p in t1.parts()] == ["part-t0-000-000000.parquet", "part-t1-000-000000.parquet"]  # readers see both tiers
    t1.begin(reset=True)  # a tier-1 reset must not delete tier-0 rows
    assert [p.name for p in t1.parts()] == ["part-t0-000-000000.parquet"]
    t1.write(dch_table([3]))
    assert {r[0] for r in duckdb.sql(f"SELECT id FROM ({t1.read_all_sql()})").fetchall()} == {1, 3}


def test_tier_completion_markers(tmp_path):
    outs = {t: SideOutput(tmp_path, "dch", "work", tier=t) for t in (0, 1)}
    outs[0].begin()
    outs[0].write(dch_table([1]))
    assert outs[0].finish(STAMP0) is True
    assert (outs[0].dir / "_SUCCESS.tier0").exists() and not outs[0].success_path.exists()
    assert outs[0].is_complete() and outs[0].is_complete(0) and not outs[0].is_complete(1)
    plain = SideOutput(tmp_path, "dch", "work")
    assert plain.completion(0).stamp == STAMP0 and plain.completion(0).tier == 0
    assert not plain.is_complete()  # "everything" is not complete yet

    outs[1].begin()  # tier 1 starting must not invalidate tier 0
    assert outs[0].is_complete()
    outs[1].write(dch_table([2]))
    assert outs[1].finish(STAMP1) is True
    done = plain.completion(None)
    assert done.path.name == "_SUCCESS" and done.tier is None
    assert done.stamp == {"n": 5, "sum": "110", "xor": "3"}  # combined stamp of both tiers
    assert plain.is_complete(0) and plain.completion(0).path.name == "_SUCCESS"

    outs[0].begin()  # rerunning tier 0 drops its marker and the combined one, keeps tier 1's
    assert not outs[0].is_complete() and not plain.is_complete()
    assert outs[1].is_complete(1) and not (outs[0].dir / "_SUCCESS").exists()


def test_untiered_run_clears_both_tier_markers_and_writes_one_success(tmp_path):
    t0 = SideOutput(tmp_path, "dch", "work", tier=0)
    t0.begin()
    t0.finish(STAMP0)
    full = SideOutput(tmp_path, "dch", "work")
    full.begin()
    assert not t0.is_complete()
    full.finish(STAMP1)
    assert full.is_complete() and not (full.dir / "_SUCCESS.tier0").exists()
    assert full.completion(0).stamp == STAMP1


def test_stamp_is_stored_and_legacy_marker_has_none(tmp_path):
    out = SideOutput(tmp_path, "dch", "work")
    out.begin()
    out.finish()  # no stamp: the empty marker of an older run
    assert out.completion().stamp is None and out.is_complete()
    out.begin()
    out.finish(STAMP0)
    assert out.completion().stamp == STAMP0


def test_shards_with_different_stamps_never_complete(tmp_path):
    from pipelines.core_v4.enrichment.side_outputs import ShardStampMismatch

    a, b = (SideOutput(tmp_path, "dch", "work", shard=Shard(i, 2), tier=0) for i in range(2))
    a.begin()
    b.begin()
    assert a.finish(STAMP0) is False
    with pytest.raises(ShardStampMismatch):
        b.finish(STAMP1)
    assert not a.is_complete()
    assert b.finish(STAMP0) is True  # reran against the current staging
    assert a.completion(0).stamp == STAMP0


def test_tier_shard_markers_are_named_per_tier(tmp_path):
    a = SideOutput(tmp_path, "dch", "work", shard=Shard(0, 2), tier=1)
    a.begin()
    a.finish(STAMP0)
    assert (a.dir / "_SUCCESS.t1.0-of-2").exists()
    with pytest.raises(ValueError):
        SideOutput(tmp_path, "dch", "work", tier=2)
