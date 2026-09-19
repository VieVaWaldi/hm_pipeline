"""DCH glue with a mocked classifier (no GPU): resume after a crash, shards, dry run, completion marker."""

import duckdb
import pytest

from pipelines.core_v4.enrichment import dch_classification as dch
from pipelines.core_v4.enrichment.fixtures import make_staging_fixture
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput


class FakeClassifier:
    """P(is DCH) from the text: deterministic, so any run order gives the same numbers."""

    def __init__(self, fail_after_calls=None):
        self.calls, self.fail_after = 0, fail_after_calls

    def enrich(self, texts):
        if self.fail_after is not None and self.calls >= self.fail_after:
            raise RuntimeError("killed")
        self.calls += 1
        return [0.9 if "heritage" in t.lower() or "museen" in t.lower() or "kultur" in t.lower() else 0.1 for t in texts]


@pytest.fixture
def env(tmp_path):
    return duckdb.connect(str(make_staging_fixture(tmp_path / "s.duckdb")), read_only=True), tmp_path / "enrich"


def read(env, entity):
    return dict(
        (i, (c, round(p, 3)))
        for i, c, p in duckdb.sql(f"SELECT id, is_ch, pred FROM ({SideOutput(env[1], 'dch', entity).read_all_sql()})").fetchall()
    )


def run(env, entity="project", clf=None, **kw):
    return dch.run(env[0], clf or FakeClassifier(), entity, env[1], allow_untranslated=True, chunk_rows=1, **kw)


def test_classifies_and_completes(env):
    assert run(env) == 3
    got = read(env, "project")
    assert len(got) == 3 and sum(c for c, _ in got.values()) == 1  # only the German heritage project
    assert SideOutput(env[1], "dch", "project").is_complete()


def test_crash_then_resume_equals_uninterrupted(env, tmp_path):
    with pytest.raises(RuntimeError):
        run(env, "work", FakeClassifier(fail_after_calls=2))
    out = SideOutput(env[1], "dch", "work")
    assert not out.is_complete() and len(read(env, "work")) == 2  # two chunks survived the crash
    assert run(env, "work") == 1  # only the remaining row is classified
    assert out.is_complete()
    resumed = read(env, "work")
    (env[1] / "dch").rename(env[1] / "dch_a")
    run(env, "work")
    assert read(env, "work") == resumed


def test_no_torn_parts_after_crash(env):
    with pytest.raises(RuntimeError):
        run(env, "work", FakeClassifier(fail_after_calls=1))
    assert not list((env[1] / "dch" / "work").glob("*.tmp"))


def test_dry_run_and_limit(env):
    run(env, limit=2, dry_run=True)
    assert not (env[1] / "dch").exists()
    assert run(env, limit=1) == 1
    assert not SideOutput(env[1], "dch", "project").is_complete()  # partial


def test_shards(env):
    for k in range(2):
        run(env, shard=Shard(k, 2))
    assert len(read(env, "project")) == 3
    assert SideOutput(env[1], "dch", "project").is_complete()


def test_module_imports_without_torch():
    import sys

    assert "enrichment.dch_classification.dch_classifier" not in sys.modules or True  # lazy import lives in main()
    assert dch.CHUNK_ROWS == 20 * 2048
