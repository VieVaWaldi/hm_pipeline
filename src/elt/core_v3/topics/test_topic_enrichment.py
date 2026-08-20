"""
Lightweight verification script for the topic enrichment pipeline.
Safe for login node: single process, tiny batches, temp DB, no ProcessPoolExecutor.
"""

import logging
import os
import shutil
import sys
import tempfile

import duckdb
import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../src"))

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

CORE_V3_DB = "/work/lu72hip/data/duckdb/core/core_v3.duckdb"
TOPICS_CSV = "data/topics/openalex_topic_mapping.csv"
N_TOPICS = 10
N_ROWS = 20

# ---------------------------------------------------------------------------
# 1. Create temp DB with a small slice of core_v3 data
# ---------------------------------------------------------------------------
tmp_dir = tempfile.mkdtemp()
tmp_db = os.path.join(tmp_dir, "test_core_v3.duckdb")
logging.info(f"Temp DB: {tmp_db}")

con = duckdb.connect(tmp_db)
con.execute(f"ATTACH '{CORE_V3_DB}' AS src (READ_ONLY)")
con.execute(
    f"CREATE TABLE project AS SELECT * FROM src.project WHERE summary IS NOT NULL LIMIT {N_ROWS}"
)
con.execute(f"CREATE TABLE work AS SELECT * FROM src.work LIMIT {N_ROWS}")
con.execute("DETACH src")
logging.info(
    f"project rows: {con.execute('SELECT count(*) FROM project').fetchone()[0]}"
)
logging.info(f"work rows:    {con.execute('SELECT count(*) FROM work').fetchone()[0]}")
con.close()

# ---------------------------------------------------------------------------
# 2. Create topic + relation_topic via SQLAlchemy (same path as real scripts)
# ---------------------------------------------------------------------------
from elt.core_v3.model.core_orm_model import Base, Topic, CREATE_RELATION_TOPIC_SQL

engine = create_engine(
    f"duckdb:///{tmp_db}", poolclass=NullPool, implicit_returning=False
)
Base.metadata.create_all(engine, tables=[Topic.__table__], checkfirst=True)
engine.dispose()

con = duckdb.connect(tmp_db)
con.execute(CREATE_RELATION_TOPIC_SQL)
con.close()
logging.info("Tables topic + relation_topic created via SQLAlchemy.")

# ---------------------------------------------------------------------------
# 3. Seed topics via native DuckDB (avoids SQLAlchemy session complexity in test)
# ---------------------------------------------------------------------------
df = pd.read_csv(TOPICS_CSV).head(N_TOPICS).replace({np.nan: None})
con = duckdb.connect(tmp_db)

records = [
    (
        int(row["topic_id"]),
        row["subfield_id"],
        row["field_id"],
        row["domain_id"],
        row["topic_name"],
        row["subfield_name"],
        row["field_name"],
        row["domain_name"],
        row["keywords"],
        row["summary"],
        row["wikipedia_url"],
        None,
        None,
    )
    for _, row in df.iterrows()
]
con.executemany(
    "INSERT INTO topic VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records
)
logging.info(
    f"Seeded {con.execute('SELECT count(*) FROM topic').fetchone()[0]} topics."
)

# ---------------------------------------------------------------------------
# 4. Build minimal TF-IDF model (topics only, no spacy for login-node speed)
# ---------------------------------------------------------------------------
topic_texts = [f"{r['keywords']} {r['summary']}" for _, r in df.iterrows()]
topic_ids = [int(r["topic_id"]) for _, r in df.iterrows()]

vec = TfidfVectorizer(max_features=5000, stop_words="english")
topic_vectors = vec.fit_transform(topic_texts)
topic_id_mapping = {i: tid for i, tid in enumerate(topic_ids)}
logging.info(f"TF-IDF model built — vocab: {len(vec.vocabulary_)}")


def classify(text: str):
    if not text or not text.strip():
        return -1, 0.0
    v = vec.transform([text])
    sims = cosine_similarity(v, topic_vectors).flatten()
    best = int(np.argmax(sims))
    return topic_id_mapping[best], float(sims[best])


# ---------------------------------------------------------------------------
# 5. Classify projects
# ---------------------------------------------------------------------------
project_sql = """
    SELECT id, CONCAT_WS(' ', title, acronym, summary, keywords,
        list_aggregate(subjects, 'string_agg', ' ')) AS full_text
    FROM project WHERE title IS NOT NULL OR summary IS NOT NULL
"""
project_results = [
    ("project", int(doc_id), *classify(full_text or ""))
    for doc_id, full_text in con.execute(project_sql).fetchall()
]
logging.info(f"Classified {len(project_results)} projects.")

# ---------------------------------------------------------------------------
# 6. Classify works
# ---------------------------------------------------------------------------
work_sql = """
    SELECT id, CONCAT_WS(' ',
        title,
        descriptions[1],
        list_aggregate(
            list_filter(list_transform(subjects, s -> s.subject.value), x -> x IS NOT NULL),
            'string_agg', ' '
        ),
        container.name
    ) AS full_text
    FROM work WHERE title IS NOT NULL OR len(descriptions) > 0
"""
work_results = [
    ("work", int(doc_id), *classify(full_text or ""))
    for doc_id, full_text in con.execute(work_sql).fetchall()
]
logging.info(f"Classified {len(work_results)} works.")

# ---------------------------------------------------------------------------
# 7. Write relation_topic rows
# ---------------------------------------------------------------------------
all_records = [
    (etype, src_id, tid, score)
    for etype, src_id, tid, score in (project_results + work_results)
    if tid != -1
]
con.executemany(
    "INSERT INTO relation_topic (type, source_id, topic_id, score) VALUES (?, ?, ?, ?)",
    all_records,
)
n = con.execute("SELECT count(*) FROM relation_topic").fetchone()[0]
logging.info(f"Inserted {n} relation_topic rows.")

# ---------------------------------------------------------------------------
# 8. Spot-check results
# ---------------------------------------------------------------------------
rows = con.execute("""
    SELECT rt.type, rt.source_id, t.topic_name, rt.score
    FROM relation_topic rt
    JOIN topic t ON rt.topic_id = t.id
    LIMIT 5
""").fetchall()
print("\n--- Sample results ---")
for r in rows:
    print(f"  [{r[0]}] id={r[1]}  topic={r[2]!r}  score={r[3]:.4f}")

assert n > 0, "No relation_topic rows written!"
con.close()
shutil.rmtree(tmp_dir)
logging.info("Cleanup done. Test PASSED.")
