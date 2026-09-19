import json

import duckdb
import pyarrow as pa
import pytest

from pipelines.core_v4.enrichment.fixtures import make_staging_fixture, project_id, work_id
from pipelines.core_v4.enrichment.nllb_translation import field_batches, process_rows, run_entity
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput
from pipelines.core_v4.enrichment.text_sources import NllbNotReadyError, text_sql


class FakeLid:
    """German if the text has a German marker, else English; probability 0.9."""

    def predict(self, text):
        if text is None or len(text.strip()) < 20:
            return None, 0.0
        german = any(w in text for w in ("Kultur", "Zukunft", "Museen", "Projekt", "digitale"))
        return ("deu_Latn", 0.9) if german else ("eng_Latn", 0.99)


class FakeTranslator:
    supported_languages = {"deu_Latn", "fra_Latn"}

    def __init__(self):
        self.calls = 0

    def translate(self, texts, langs):
        self.calls += len(texts)
        return [f"EN[{t}]" for t in texts]


@pytest.fixture
def staging(tmp_path):
    return duckdb.connect(str(make_staging_fixture(tmp_path / "staging.duckdb")), read_only=True)


def test_process_rows_routing():
    rows = [
        (1, "title", "Die Zukunft der Museen"),  # german -> translated
        (1, "summary", "Plain English summary text here"),  # english -> seen, not translated
        (2, "title", "Kultur"),  # < 20 chars -> not identifiable, not translated
        (2, "summary", None),  # empty -> no row at all
        (3, "title", "Ein Projekt über digitale Archive."),
    ]
    nllb, seen = process_rows(rows, FakeLid(), FakeTranslator())
    assert {(r["id"], r["field"]) for r in nllb} == {(1, "title"), (3, "title")}
    assert nllb[0]["text_en"].startswith("EN[")
    by_key = {(r["id"], r["field"]): r for r in seen}
    assert set(by_key) == {(1, "title"), (1, "summary"), (2, "title"), (3, "title")}
    assert by_key[(1, "title")]["translated"] and not by_key[(1, "summary")]["translated"]
    assert by_key[(2, "title")]["src_lang"] is None


def test_unsupported_language_is_seen_not_translated():
    class Lid:
        def predict(self, text):
            return "xxx_Latn", 0.99

    nllb, seen = process_rows([(1, "title", "some long enough title")], Lid(), FakeTranslator())
    assert nllb == [] and seen[0]["translated"] is False and seen[0]["src_lang"] == "xxx_Latn"


def test_min_prob_threshold():
    class Lid:
        def predict(self, text):
            return "deu_Latn", 0.4

    assert process_rows([(1, "title", "irgendein langer Titel hier")], Lid(), FakeTranslator(), min_prob=0.5)[0] == []
    assert len(process_rows([(1, "title", "irgendein langer Titel hier")], Lid(), FakeTranslator(), min_prob=0.3)[0]) == 1


def test_field_batches_read_only_and_streaming(staging):
    batches = list(field_batches(staging, "project", ["title", "summary"], batch_size=2))
    assert all(isinstance(b, pa.RecordBatch) for b in batches)
    assert sum(b.num_rows for b in batches) == 3  # the fixture's project 4 has no title and no summary


def test_run_entity_writes_and_success_gates_text_sources(staging, tmp_path):
    edir = tmp_path / "enrich"
    tr = FakeTranslator()
    counters = run_entity(staging, "project", ["title", "summary"], FakeLid(), tr, str(edir))
    assert counters["rows"] == 3
    con = duckdb.connect()
    nllb = {(r[0], r[1]): r[2] for r in SideOutput(edir, "nllb", "project").read_all(con).fetchall()}
    assert (project_id(1), "title") in nllb and (project_id(1), "summary") in nllb
    assert (project_id(2), "title") not in nllb  # english
    seen = SideOutput(edir, "nllb/seen", "project").read_all(con).fetchall()
    assert len(seen) == 5  # p1: title+summary, p2: title+summary, p3: title
    assert SideOutput(edir, "nllb", "project").is_complete() and SideOutput(edir, "nllb/seen", "project").is_complete()

    # downstream: text_sources now uses the translations (and would have refused before _SUCCESS)
    got = dict(staging.execute(text_sql("project", ["title", "summary"], enrichment_dir=edir)).fetchall())
    assert got[project_id(1)].startswith("EN[Digitale Kulturerbe Plattform] EN[")
    assert got[project_id(2)] == "Sustainable inclusive cities Resilient urban planning for everyone."

    # resume: a second run finds nothing left
    tr.calls = 0
    assert run_entity(staging, "project", ["title", "summary"], FakeLid(), tr, str(edir))["rows"] == 0 and tr.calls == 0


def test_partial_run_writes_no_success_and_resumes(staging, tmp_path):
    edir = tmp_path / "enrich"
    run_entity(staging, "work", ["title", "description"], FakeLid(), FakeTranslator(), str(edir), limit=1)
    assert not SideOutput(edir, "nllb/seen", "work").is_complete()
    with pytest.raises(NllbNotReadyError):
        text_sql("work", ["title"], enrichment_dir=edir)
    counters = run_entity(staging, "work", ["title", "description"], FakeLid(), FakeTranslator(), str(edir))
    assert counters["rows"] == 2  # 3 rows have text, one was already done
    assert SideOutput(edir, "nllb/seen", "work").is_complete()
    ids = {r[0] for r in SideOutput(edir, "nllb/seen", "work").read_all(duckdb.connect()).fetchall()}
    assert ids == {work_id(1), work_id(2), work_id(3)}


def test_dry_run_writes_nothing(staging, tmp_path):
    edir = tmp_path / "enrich"
    run_entity(staging, "project", ["title", "summary"], FakeLid(), FakeTranslator(), str(edir), dry_run=True, limit=2)
    assert not edir.exists()


def test_shards_partition_the_work(staging, tmp_path):
    edir = tmp_path / "enrich"
    total = 0
    for i in range(2):
        total += run_entity(staging, "project", ["title", "summary"], FakeLid(), FakeTranslator(), str(edir), shard=Shard(i, 2))["rows"]
    assert total == 3
    assert SideOutput(edir, "nllb/seen", "project").is_complete()  # both shards finished -> _SUCCESS


def test_runaway_translation_is_dropped():
    class Runaway(FakeTranslator):
        def translate(self, texts, langs):
            return [t + " " + "This is the first time I have seen this. " * 5 for t in texts]

    nllb, seen = process_rows([(1, "title", "Die Zukunft der Museen")], FakeLid(), Runaway())
    assert nllb == [] and seen[0]["translated"] is False and seen[0]["src_lang"] == "deu_Latn"


# ---- tiers and the staging fingerprint -----------------------------------------------------
def test_tier_run_translates_only_that_tier_and_completes_it_alone(staging, tmp_path):
    edir = tmp_path / "enrich"
    counters = run_entity(staging, "work", ["title", "description"], FakeLid(), FakeTranslator(), str(edir), tier=0)
    assert counters["rows"] == 2  # works 1 and 2; work 3 (tier 1) is not touched
    seen = SideOutput(edir, "nllb/seen", "work")
    assert seen.is_complete(0) and not seen.is_complete() and not seen.is_complete(1)
    assert (seen.dir / "_SUCCESS.tier0").exists()
    ids = {r[0] for r in seen.read_all(duckdb.connect()).fetchall()}
    assert ids == {work_id(1), work_id(2)}
    # text enrichments may read tier 0 now, and only tier 0
    text_sql("work", ["title"], enrichment_dir=edir, tier=0)
    with pytest.raises(NllbNotReadyError):
        text_sql("work", ["title"], enrichment_dir=edir)

    # tier 1 later, with a different shard count: both tiers complete, `_SUCCESS` appears
    for i in range(2):
        run_entity(staging, "work", ["title", "description"], FakeLid(), FakeTranslator(), str(edir), tier=1, shard=Shard(i, 2))
    assert seen.is_complete() and (seen.dir / "_SUCCESS").exists()
    assert {r[0] for r in seen.read_all(duckdb.connect()).fetchall()} == {work_id(1), work_id(2), work_id(3)}
    assert run_entity(staging, "work", ["title", "description"], FakeLid(), FakeTranslator(), str(edir))["rows"] == 0  # nothing left


def test_success_records_the_staging_fingerprint(staging, tmp_path):
    from pipelines.core_v4.enrichment.fingerprint import staging_stamp

    edir = tmp_path / "enrich"
    run_entity(staging, "work", ["title", "description"], FakeLid(), FakeTranslator(), str(edir), tier=0)
    run_entity(staging, "work", ["title", "description"], FakeLid(), FakeTranslator(), str(edir), tier=1)
    for name in ("nllb", "nllb/seen"):
        o = SideOutput(edir, name, "work")
        assert json.loads(o.tier_success_path(0).read_text()) == staging_stamp(staging, "work", 0)
        assert json.loads(o.tier_success_path(1).read_text()) == staging_stamp(staging, "work", 1)
        assert o.completion().stamp == staging_stamp(staging, "work")  # `_SUCCESS`: both tiers combined


def test_tier_needs_the_link_tier_column(tmp_path):
    con = duckdb.connect(str(make_staging_fixture(tmp_path / "s.duckdb")))
    con.execute("ALTER TABLE work DROP COLUMN link_tier")
    with pytest.raises(RuntimeError, match="link_tier"):
        run_entity(con, "work", ["title"], FakeLid(), FakeTranslator(), str(tmp_path / "e"), tier=0)
