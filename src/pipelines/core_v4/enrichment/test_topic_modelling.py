import random

import duckdb
import numpy as np
import pandas as pd
import pytest

from enrichment.topic_modelling.classifier import TfidfTopicClassifier, normalise_text, normalise_texts
from pipelines.core_v4.enrichment import topic_modelling as tm
from pipelines.core_v4.enrichment.fixtures import make_staging_fixture, project_id
from pipelines.core_v4.enrichment.side_outputs import Shard, SideOutput

TOPICS = pd.DataFrame(
    {
        "topic_id": [100, 200, 300],
        "keywords": ["museum; heritage; archive; digital culture", "urban planning; city; sustainable; resilient",
                     "economy; finance; market; trade; growth"],
        "summary": ["Digital archives and cultural heritage in museums and culture.",
                    "Sustainable and resilient urban city planning for inclusive communities.",
                    "Economic growth, finance and trade in the market economy."],
    }
)
WORDS = " ".join(TOPICS.keywords) + " " + " ".join(TOPICS.summary) + " project study european research"


def random_texts(n, seed=0):
    rng = random.Random(seed)
    vocab = WORDS.replace(";", " ").replace(".", " ").replace(",", " ").split()
    return [" ".join(rng.choices(vocab, k=rng.randint(5, 40))) for _ in range(n)] + ["", "   ", "the and of"]


@pytest.fixture(scope="module")
def classifier():
    return TfidfTopicClassifier.build(TOPICS, random_texts(60, seed=1))


def test_normalise_texts_matches_normalise_text():
    texts = random_texts(20, seed=3) + ["Über Museen, Archive & 3D-Modelle!"]
    assert normalise_texts(texts) == [normalise_text(t) for t in texts]


def test_batched_path_matches_per_row_path(classifier):
    texts = random_texts(80, seed=2)
    slow, fast = classifier.enrich(texts), classifier.enrich_batch(texts)
    assert [p.topic_id for p in slow] == [p.topic_id for p in fast]
    np.testing.assert_allclose([p.score for p in slow], [p.score for p in fast], atol=1e-6)
    assert fast[-1].topic_id == -1  # no usable text


@pytest.fixture
def env(tmp_path, classifier):
    path = tm.model_path(tmp_path / "enrich")
    classifier.save(path)
    con = duckdb.connect(str(make_staging_fixture(tmp_path / "staging.duckdb")), read_only=True)
    return con, tmp_path / "enrich", path


def score_fn(env):
    return tm.make_scorer(env[2], workers=1)[0]


def read(env, entity="project", shard=Shard()):
    return dict(
        (i, (t, round(s, 4)))
        for i, t, s in duckdb.sql(f"SELECT id, topic_id, score FROM ({SideOutput(env[1], 'topics', entity, shard).read_all_sql()})").fetchall()
    )


def test_run_writes_parts_and_marks_complete(env):
    n = tm.run(env[0], "project", env[1], score_fn(env), allow_untranslated=True)
    assert n == 3  # row 4 has no title/summary
    out = SideOutput(env[1], "topics", "project")
    assert out.is_complete() and len(read(env)) == 3
    assert read(env)[project_id(2)][0] == 200  # "sustainable inclusive cities ... urban planning"


def test_resume_skips_done_rows(env):
    tm.run(env[0], "project", env[1], score_fn(env), allow_untranslated=True, limit=2)
    out = SideOutput(env[1], "topics", "project")
    assert not out.is_complete()  # partial (--limit) run
    assert len(read(env)) == 2
    assert tm.run(env[0], "project", env[1], score_fn(env), allow_untranslated=True) == 1  # only the remaining row
    assert out.is_complete() and len(read(env)) == 3
    assert tm.run(env[0], "project", env[1], score_fn(env), allow_untranslated=True) == 0  # nothing left


def test_dry_run_writes_nothing(env):
    tm.run(env[0], "project", env[1], score_fn(env), allow_untranslated=True, limit=2, dry_run=True)
    assert not (env[1] / "topics" / "project").exists()


def test_shards_partition_the_rows(env):
    for k in range(2):
        tm.run(env[0], "project", env[1], score_fn(env), shard=Shard(k, 2), allow_untranslated=True)
    assert len(read(env)) == 3
    assert SideOutput(env[1], "topics", "project").is_complete()


def test_process_pool_gives_same_results(env):
    score, executor = tm.make_scorer(env[2], workers=2)
    try:
        tm.run(env[0], "work", env[1], score, allow_untranslated=True)
    finally:
        executor.shutdown()
    pooled = read(env, "work")
    (env[1] / "topics" / "work").rename(env[1] / "pooled_work")
    tm.run(env[0], "work", env[1], score_fn(env), allow_untranslated=True)
    assert read(env, "work") == pooled


def test_refuses_untranslated_by_default(env):
    from pipelines.core_v4.enrichment.text_sources import NllbNotReadyError

    with pytest.raises(NllbNotReadyError):
        tm.run(env[0], "project", env[1], score_fn(env))


def test_build_model(tmp_path):
    csv = tmp_path / "topics.csv"
    TOPICS.to_csv(csv, index=False)
    con = duckdb.connect(str(make_staging_fixture(tmp_path / "staging.duckdb")), read_only=True)
    path = tm.build_model(con, tmp_path / "enrich", allow_untranslated=True, topics_csv=csv, sample_size=10)
    assert path.exists() and not path.with_name(path.name + ".tmp").exists()
    assert TfidfTopicClassifier.load(path).enrich_batch(["museum archive heritage"])[0].topic_id == 100
