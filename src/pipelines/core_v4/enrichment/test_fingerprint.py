import duckdb
import pytest

from pipelines.core_v4.enrichment.fingerprint import StagingTierError, combine, describe, same, staging_stamp
from pipelines.core_v4.enrichment.fixtures import make_staging_fixture


@pytest.fixture
def con(tmp_path):
    return duckdb.connect(str(make_staging_fixture(tmp_path / "staging.duckdb")))


def test_stamp_changes_with_the_id_set(con):
    before = staging_stamp(con, "work")
    assert before["n"] == 4
    assert staging_stamp(con, "work") == before  # deterministic
    con.execute("DELETE FROM work WHERE openaireId = 'w4'")
    after = staging_stamp(con, "work")
    assert after["n"] == 3 and not same(before, after)
    con.execute("UPDATE work SET title = 'changed'")  # texts are not covered, only ids
    assert staging_stamp(con, "work") == after


def test_same_count_different_ids_differ(con):
    before = staging_stamp(con, "project")
    con.execute("UPDATE project SET id = id + 1 WHERE openaireId = 'p1'")
    after = staging_stamp(con, "project")
    assert after["n"] == before["n"] and not same(before, after)


def test_tier_stamps_partition_and_combine_to_the_whole(con):
    t0, t1, whole = staging_stamp(con, "work", 0), staging_stamp(con, "work", 1), staging_stamp(con, "work")
    assert (t0["n"], t1["n"]) == (2, 2)
    assert combine(t0, t1) == whole
    assert combine(t0, None) is None


def test_attached_catalog_and_errors(con, tmp_path):
    path = tmp_path / "staging.duckdb"
    con.close()
    other = duckdb.connect()
    other.execute(f"ATTACH '{path}' AS stg (READ_ONLY)")
    assert staging_stamp(other, "work", 0, "stg")["n"] == 2
    with pytest.raises(StagingTierError, match="only applies to works"):
        staging_stamp(other, "project", 0, "stg")
    other.close()
    plain = duckdb.connect(str(path))
    plain.execute("ALTER TABLE work DROP COLUMN link_tier")
    with pytest.raises(StagingTierError, match="link_tier"):
        staging_stamp(plain, "work", 0)


def test_same_and_describe():
    assert not same(None, None) and not same({"n": 1, "sum": "1", "xor": "1"}, None)
    assert same({"n": 1, "sum": "5", "xor": "1"}, {"n": 1, "sum": 5, "xor": "1"})
    assert describe(None) == "no fingerprint recorded" and describe({"n": 1234, "sum": "0", "xor": "0"}) == "1,234 ids"
