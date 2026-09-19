"""
Lightweight verification script for topic modelling. Safe for a login node:
single process, tiny batches, temp DB, no ProcessPoolExecutor.

Copies a small slice of core_v3's real staging db into a scratch duckdb, then
runs the same classifier used in production against it. Never touches the
source database.

Usage:
    uv run python -m pipelines.core_v3.enrichment.test_topic_modelling
"""

import logging
import shutil
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

from common.config.pipelines import get_pipeline_paths
from common.file_handling.path_utils import get_project_root_path
from common.log.logger import setup_logging
from enrichment.topic_modelling.classifier import TfidfTopicClassifier
from enrichment.topic_modelling.schema import CREATE_RELATION_TOPIC_SQL, CREATE_TOPIC_SQL
from pipelines.core_v3.enrichment.seed_topics import seed_topics

N_TOPICS = 10
N_ROWS = 20


def main() -> None:
    setup_logging("enrichment-topic_modelling", "test")

    src_db_path = get_pipeline_paths()["core_v3"]["path_staging_duck"]
    logging.info(f"Source DB: {src_db_path}")

    tmp_dir = tempfile.mkdtemp()
    tmp_db = str(Path(tmp_dir) / "test_topic_modelling.duckdb")
    logging.info(f"Temp DB: {tmp_db}")

    con = duckdb.connect(tmp_db)
    try:
        con.execute(f"ATTACH '{src_db_path}' AS src (READ_ONLY)")
        con.execute(
            f"CREATE TABLE project AS SELECT * FROM src.project WHERE summary IS NOT NULL LIMIT {N_ROWS}"
        )
        con.execute(f"CREATE TABLE work AS SELECT * FROM src.work LIMIT {N_ROWS}")
        con.execute("DETACH src")
        logging.info(f"project rows: {con.execute('SELECT count(*) FROM project').fetchone()[0]}")
        logging.info(f"work rows:    {con.execute('SELECT count(*) FROM work').fetchone()[0]}")

        con.execute(CREATE_TOPIC_SQL)
        con.execute(CREATE_RELATION_TOPIC_SQL)
        topics_df = pd.read_csv(
            get_project_root_path() / "data/topics/openalex_topic_mapping.csv"
        ).head(N_TOPICS)
        seed_topics(con, topics_df)

        classifier = TfidfTopicClassifier.build(topics_df)

        project_rows = con.execute("""
            SELECT id, CONCAT_WS(' ', title, acronym, summary, keywords,
                list_aggregate(subjects, 'string_agg', ' ')) AS full_text
            FROM project WHERE title IS NOT NULL OR summary IS NOT NULL
        """).fetchall()
        work_rows = con.execute("""
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
        """).fetchall()

        project_predictions = classifier.enrich([text or "" for _, text in project_rows])
        work_predictions = classifier.enrich([text or "" for _, text in work_rows])
        logging.info(f"Classified {len(project_predictions)} projects, {len(work_predictions)} works.")

        records = [
            ("project", int(doc_id), p.topic_id, p.score)
            for (doc_id, _), p in zip(project_rows, project_predictions)
            if p.topic_id != -1
        ] + [
            ("work", int(doc_id), p.topic_id, p.score)
            for (doc_id, _), p in zip(work_rows, work_predictions)
            if p.topic_id != -1
        ]
        con.executemany(
            "INSERT INTO relation_topic (type, source_id, topic_id, score) VALUES (?, ?, ?, ?)",
            records,
        )
        n = con.execute("SELECT count(*) FROM relation_topic").fetchone()[0]
        logging.info(f"Inserted {n} relation_topic rows.")

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
    finally:
        con.close()
        shutil.rmtree(tmp_dir)

    logging.info("Cleanup done. Test PASSED.")


if __name__ == "__main__":
    main()
