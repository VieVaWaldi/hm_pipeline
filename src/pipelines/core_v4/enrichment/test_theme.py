import duckdb
import pyarrow as pa
import pytest

from pipelines.core_v4.enrichment import theme
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

# topic_id, field_id, subfield_id
TAXONOMY = [
    (1, 20, 2000),     # field 20 -> Economy
    (12033, 14, 1400),  # explicit Economy topic
    (2, 14, 1409),     # subfield 1409 -> Tourism
    (12399, 14, 1401),  # explicit Tourism topic
    (3, 33, 3300),     # no theme
]


@pytest.fixture
def env(tmp_path):
    csv = tmp_path / "tax.csv"
    csv.write_text("topic_id,subfield_id,field_id,topic_name\n" + "\n".join(f"{t},{s},{f},x" for t, f, s in TAXONOMY))
    topics = SideOutput(tmp_path / "e", "topics", "project")
    topics.begin()
    rows = [(1, 1, 0.5), (2, 2, 0.5), (3, 12033, 0.3), (4, 12399, 0.3), (5, 3, 0.9), (6, 1, 0.05), (7, 99, 0.9),
            (8, 1, 0.2), (8, 2, 0.4)]  # id 8 has two topic rows: the best (2, Tourism) wins
    topics.write(pa.table({"id": pa.array([r[0] for r in rows], pa.uint64()), "topic_id": pa.array([r[1] for r in rows], pa.int32()),
                           "score": pa.array([r[2] for r in rows], pa.float32())}))
    topics.finish()
    return duckdb.connect(), tmp_path / "e", str(csv), theme.load_themes()


def themes_of(env):
    return dict(duckdb.sql(f"SELECT id, theme FROM ({SideOutput(env[1], 'theme', 'project').read_all_sql()})").fetchall())


def test_theme_rules_and_sparsity(env):
    n = theme.run(env[0], "project", env[1], env[2], env[3], 0.1)
    assert themes_of(env) == {1: "Economy", 2: "Tourism", 3: "Economy", 4: "Tourism", 8: "Tourism"}
    assert n == 5  # 5 (no theme), 6 (below min score), 7 (unknown topic) are absent
    assert SideOutput(env[1], "theme", "project").is_complete()


def test_min_score_threshold(env):
    theme.run(env[0], "project", env[1], env[2], env[3], 0.4)
    assert themes_of(env) == {1: "Economy", 2: "Tourism", 8: "Tourism"}


def test_rerun_replaces_output(env):
    theme.run(env[0], "project", env[1], env[2], env[3], 0.1)
    theme.run(env[0], "project", env[1], env[2], env[3], 0.4)
    assert len(themes_of(env)) == 3


def test_dry_run_and_incomplete_gate(env, tmp_path):
    theme.run(env[0], "project", env[1], env[2], env[3], 0.1, dry_run=True)
    assert not (env[1] / "theme").exists()
    (env[1] / "topics" / "project" / "_SUCCESS").unlink()
    with pytest.raises(SystemExit):
        theme.run(env[0], "project", env[1], env[2], env[3], 0.1)
    theme.run(env[0], "project", env[1], env[2], env[3], 0.1, allow_incomplete=True)


def test_shards_partition(env):
    for k in range(2):
        theme.run(env[0], "project", env[1], env[2], env[3], 0.1, shard=Shard(k, 2))
    assert len(themes_of(env)) == 5


def test_themes_yaml_matches_brief():
    t = theme.load_themes()["themes"]
    assert t["Economy"]["fields"] == [20] and t["Economy"]["topics"] == [12033, 14219, 12312, 14352, 14433]
    assert t["Tourism"]["subfields"] == [1409]
    assert t["Tourism"]["topics"] == [10055, 11793, 11925, 12399, 12456, 12584, 12402, 11474, 11410]
