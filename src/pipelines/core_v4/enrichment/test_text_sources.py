import logging

import duckdb
import pyarrow as pa
import pytest

from pipelines.core_v4.enrichment.fixtures import make_staging_fixture, project_id, work_id
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput
from pipelines.core_v4.enrichment.text_sources import NllbNotReadyError, field_sql, text_batches, text_sql


@pytest.fixture
def staging(tmp_path):
    return duckdb.connect(str(make_staging_fixture(tmp_path / "staging.duckdb")), read_only=True)


def write_nllb(edir, entity, rows, complete=True):
    """rows: (id, field, text_en, lang). Also writes nllb/seen."""
    nllb, seen = SideOutput(edir, "nllb", entity), SideOutput(edir, "nllb/seen", entity)
    for o in (nllb, seen):
        o.begin()
    nllb.write(pa.table({"id": pa.array([r[0] for r in rows], pa.uint64()), "field": [r[1] for r in rows],
                         "text_en": [r[2] for r in rows], "src_lang": [r[3] for r in rows]}))
    seen.write(pa.table({"id": pa.array([r[0] for r in rows], pa.uint64()), "field": [r[1] for r in rows],
                         "src_lang": [r[3] for r in rows], "translated": [True] * len(rows)}))
    if complete:
        nllb.finish()
        seen.finish()


def texts(con, sql):
    return dict(con.execute(sql).fetchall())


def test_project_text_without_nllb(staging, tmp_path):
    sql = text_sql("project", ["title", "summary", "acronym", "keywords", "subjects"], with_nllb=False, enrichment_dir=tmp_path)
    got = texts(staging, sql)
    assert len(got) == 3  # row 4 has neither title nor summary
    assert got[project_id(2)] == "Sustainable inclusive cities Resilient urban planning for everyone. SIC cities urban"
    assert got[project_id(3)] == "Titre seulement"  # NULL fields are skipped, not 'None'


def test_work_text_fields(staging, tmp_path):
    sql = text_sql("work", ["title", "description", "subjects", "container"], with_nllb=False, enrichment_dir=tmp_path)
    got = texts(staging, sql)
    assert len(got) == 3
    assert got[work_id(2)] == "Roma communities in Europe A study of Romani minorities. Roma Minority Studies"


def test_unknown_field_rejected(tmp_path):
    with pytest.raises(ValueError):
        text_sql("project", ["descriptions"], with_nllb=False, enrichment_dir=tmp_path)
    assert field_sql("work", "description", "w") == "w.descriptions[1]"


def test_nllb_coalesces_per_field(staging, tmp_path):
    write_nllb(tmp_path, "project", [(project_id(1), "title", "Digital Cultural Heritage Platform", "deu_Latn")])
    got = texts(staging, text_sql("project", ["title", "summary"], enrichment_dir=tmp_path))
    # title translated, summary (no translation row) keeps the original
    assert got[project_id(1)] == "Digital Cultural Heritage Platform Ein Projekt über digitale Archive."
    assert got[project_id(2)] == "Sustainable inclusive cities Resilient urban planning for everyone."


def test_refuses_incomplete_nllb(staging, tmp_path):
    with pytest.raises(NllbNotReadyError, match="allow-untranslated"):
        text_sql("project", ["title"], enrichment_dir=tmp_path)  # nothing at all
    write_nllb(tmp_path, "project", [(project_id(1), "title", "X", "deu_Latn")], complete=False)
    with pytest.raises(NllbNotReadyError):
        text_sql("project", ["title"], enrichment_dir=tmp_path)


def test_requires_seen_too(tmp_path):
    write_nllb(tmp_path, "project", [(1, "title", "X", "deu_Latn")])
    (tmp_path / "nllb" / "seen" / "project" / "_SUCCESS").unlink()
    with pytest.raises(NllbNotReadyError):
        text_sql("project", ["title"], enrichment_dir=tmp_path)


def test_allow_untranslated_falls_back_with_warning(staging, tmp_path, caplog):
    with caplog.at_level(logging.WARNING):
        sql = text_sql("project", ["title"], enrichment_dir=tmp_path, allow_untranslated=True)
    assert "untranslated" in caplog.text
    assert texts(staging, sql)[project_id(1)] == "Digitale Kulturerbe Plattform"


def test_nllb_is_per_entity(staging, tmp_path):
    write_nllb(tmp_path, "project", [(project_id(1), "title", "X", "deu_Latn")])
    text_sql("project", ["title"], enrichment_dir=tmp_path)
    with pytest.raises(NllbNotReadyError):  # work has no NLLB output
        text_sql("work", ["title"], enrichment_dir=tmp_path)


def test_streaming_batches_resume_and_shard(staging, tmp_path):
    kwargs = dict(with_nllb=False, enrichment_dir=tmp_path)
    all_ids = {i for b in text_batches(staging, "project", ["title"], batch_size=1, **kwargs) for i in b.column("id").to_pylist()}
    assert all_ids == {project_id(i) for i in (1, 2, 3)}
    batches = list(text_batches(staging, "project", ["title"], batch_size=2, **kwargs))
    assert all(isinstance(b, pa.RecordBatch) for b in batches)

    done = SideOutput(tmp_path, "dch", "project")
    done.begin()
    done.write(pa.table({"id": pa.array([project_id(1)], pa.uint64()), "is_ch": [True], "pred": [0.9]}))
    rest = {i for b in text_batches(staging, "project", ["title"], exclude_ids_sql=done.done_ids_sql(), **kwargs) for i in b.column("id").to_pylist()}
    assert rest == {project_id(2), project_id(3)}

    shards = [
        {i for b in text_batches(staging, "project", ["title"], shard=Shard(k, 2), **kwargs) for i in b.column("id").to_pylist()}
        for k in range(2)
    ]
    assert shards[0] | shards[1] == all_ids and not shards[0] & shards[1]


def test_limit_and_read_only(staging, tmp_path):
    n = sum(b.num_rows for b in text_batches(staging, "work", ["title"], with_nllb=False, enrichment_dir=tmp_path, limit=2))
    assert n == 2
    with pytest.raises(duckdb.Error):
        staging.execute("DELETE FROM work")
