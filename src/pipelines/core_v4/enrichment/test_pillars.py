import json

import duckdb
import pytest

from pipelines.core_v4.enrichment.fixtures import make_staging_fixture, project_id
from pipelines.core_v4.enrichment.pillars import load_config, pillars_sql, run_entity, stem_regex
from pipelines.core_v4.enrichment.side_outputs import PILLARS, Shard, SideOutput


@pytest.fixture
def staging(tmp_path):
    return duckdb.connect(str(make_staging_fixture(tmp_path / "staging.duckdb")), read_only=True)


def mask_of(text: str) -> int:
    cfg = load_config()
    return duckdb.sql(pillars_sql("project", cfg, f"SELECT 1::UBIGINT AS id, '{text}' AS full_text")).fetchone()[1]


def test_bit_order_and_stems():
    assert PILLARS == ["inclusive", "sustainable", "resilient", "innovative", "global"]
    assert mask_of("An inclusive approach") == 1
    assert mask_of("Inclusion of minorities") == 1
    assert mask_of("sustainable and resilient") == 2 | 4
    assert mask_of("Innovation and globalisation") == 8 | 16
    assert mask_of("all five: inclusive, sustainable, resilient, innovative, global") == 31
    assert mask_of("nothing relevant here") == 0


def test_whole_word_start_only():
    assert mask_of("an exclusive club") == 0  # 'inclusi' must start the word
    assert mask_of("sustained growth") == 0  # sustainab*, not sustain*
    assert mask_of("preinnovation") == 0
    assert stem_regex(["global*"]) == r"\b(?:global\w*)"
    assert stem_regex(["exact"]) == r"\b(?:exact\b)"
    with pytest.raises(ValueError):
        stem_regex(["bad stem!*"])


def test_run_writes_sparse_parts_and_rates(staging, tmp_path):
    result = run_entity(staging, "project", str(tmp_path), Shard(), allow_untranslated=True, batch_size=2)
    out = SideOutput(tmp_path, "pillars", "project")
    assert dict(duckdb.sql(f"SELECT id, pillars FROM ({out.read_all_sql()})").fetchall()) == {project_id(2): 1 | 2 | 4}
    assert result["rows"] == 3 and result["rows_any"] == 1
    assert result["counts"] == {"inclusive": 1, "sustainable": 1, "resilient": 1, "innovative": 0, "global": 0}
    assert out.is_complete()
    assert json.loads((out.dir / "_match_rates-0.json").read_text())["rows"] == 3


def test_flags_pillar_above_threshold(staging, tmp_path, caplog):
    cfg = load_config()
    cfg["flag_above"] = 0.2  # 1 of 3 projects = 33%
    with caplog.at_level("WARNING"):
        result = run_entity(staging, "project", str(tmp_path), Shard(), cfg=cfg, allow_untranslated=True)
    assert result["flagged"] == ["inclusive", "sustainable", "resilient"]
    assert "FLAG" in caplog.text


def test_rerun_replaces_shard_output_and_dry_run_writes_nothing(staging, tmp_path):
    for _ in range(2):
        run_entity(staging, "project", str(tmp_path), Shard(), allow_untranslated=True)
    assert len(SideOutput(tmp_path, "pillars", "project").parts()) == 1  # not duplicated
    run_entity(staging, "work", str(tmp_path), Shard(), allow_untranslated=True, dry_run=True)
    assert not (tmp_path / "pillars" / "work").exists()


def test_shards(staging, tmp_path):
    for i in range(2):
        run_entity(staging, "project", str(tmp_path), Shard(i, 2), allow_untranslated=True)
    out = SideOutput(tmp_path, "pillars", "project")
    assert out.is_complete()
    assert duckdb.sql(f"SELECT count(*) FROM ({out.read_all_sql()})").fetchone()[0] == 1


def test_config_must_follow_bit_order(tmp_path):
    bad = tmp_path / "p.yaml"
    bad.write_text("flag_above: 0.3\npillars:\n  sustainable: ['a*']\n  inclusive: ['b*']\n  resilient: ['c*']\n  innovative: ['d*']\n  global: ['e*']\n")
    with pytest.raises(ValueError):
        load_config(bad)
