import json

import duckdb
import pytest

from enrichment.minority_matching.matcher import Group, Rules
from pipelines.core_v4.enrichment.fixtures import make_staging_fixture, project_id, work_id
from pipelines.core_v4.enrichment.minorities import run_entity
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

GROUPS = [Group("Q8060", ["Roma", "Romani", "Romany"], "Romani people"), Group("Q99", ["urban planning"], "Planners")]
RULES = Rules()


@pytest.fixture
def staging(tmp_path):
    return duckdb.connect(str(make_staging_fixture(tmp_path / "staging.duckdb")), read_only=True)


def run(staging, edir, entity, **kw):
    return run_entity(staging, entity, GROUPS, RULES, str(edir), kw.pop("shard", Shard()), allow_untranslated=True, workers=2, batch_size=2, **kw)


def hits(edir, entity):
    out = SideOutput(edir, "minorities", entity)
    return dict(duckdb.sql(f"SELECT id, minority_qid FROM ({out.read_all_sql()})").fetchall())


def seen_ids(edir, entity):
    return {r[0] for r in duckdb.sql(SideOutput(edir, "minorities/seen", entity, schema=None).done_ids_sql()).fetchall()}


def test_writes_sparse_hits_and_seen(staging, tmp_path):
    result = run(staging, tmp_path, "work")
    assert hits(tmp_path, "work") == {work_id(2): ["Q8060"]}
    assert len(seen_ids(tmp_path, "work")) == 3  # work 4 has no text: not processed
    assert result["rows"] == 3 and result["rows_hit"] == 1
    assert SideOutput(tmp_path, "minorities", "work").is_complete()
    counts = json.loads((tmp_path / "minorities" / "work" / "_keyword_counts-0.json").read_text())
    assert counts == {"roma": 1, "romani": 1}


def test_project_hit_via_name_keyword(staging, tmp_path):
    run(staging, tmp_path, "project")
    assert hits(tmp_path, "project") == {project_id(2): ["Q99"]}


def test_resume_skips_seen_rows(staging, tmp_path):
    run(staging, tmp_path, "work", limit=2)  # partial run: not complete
    assert not SideOutput(tmp_path, "minorities", "work").is_complete()
    assert len(seen_ids(tmp_path, "work")) == 2
    second = run(staging, tmp_path, "work")
    assert second["rows"] == 1  # only the row not seen yet
    assert len(seen_ids(tmp_path, "work")) == 3
    assert hits(tmp_path, "work") == {work_id(2): ["Q8060"]}
    third = run(staging, tmp_path, "work")
    assert third["rows"] == 0 and SideOutput(tmp_path, "minorities", "work").is_complete()


def test_dry_run_writes_nothing(staging, tmp_path):
    result = run(staging, tmp_path, "work", dry_run=True)
    assert result["rows_hit"] == 1
    assert not (tmp_path / "minorities").exists()


def test_shards_cover_everything_once(staging, tmp_path):
    for i in range(2):
        run(staging, tmp_path, "work", shard=Shard(i, 2))
    assert SideOutput(tmp_path, "minorities", "work").is_complete()
    assert len(seen_ids(tmp_path, "work")) == 3
    assert hits(tmp_path, "work") == {work_id(2): ["Q8060"]}


def test_review_sample_written(staging, tmp_path):
    run(staging, tmp_path, "work", review=5)
    text = (tmp_path / "minorities" / "work" / "review-0.csv").read_text()
    assert "[[Roma]]" in text and "[[romani]]" in text and text.startswith("keyword,id,typo,capitalised_only,snippet,correct")


def test_refuses_untranslated_unless_allowed(staging, tmp_path):
    from pipelines.core_v4.enrichment.text_sources import NllbNotReadyError

    with pytest.raises(NllbNotReadyError):
        run_entity(staging, "work", GROUPS, RULES, str(tmp_path), Shard(), workers=2)
