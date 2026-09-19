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
