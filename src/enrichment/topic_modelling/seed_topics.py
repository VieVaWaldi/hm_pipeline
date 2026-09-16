import numpy as np
import pandas as pd
from pandas import DataFrame

# TODO: core_orm_model.py was removed from enrichment_lib (no model lives outside the
# actual pipeline). Point this at the core_v4 model once it exists.
from enrichment_lib.core_orm_model import TopicOA
from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create
from common.file_handling.path_utils import get_project_root_path


def seed_topics(df: DataFrame):
    df = df.replace({np.nan: None})
    with create_db_session()() as session:
        for idx, row in df.iterrows():
            fields = {
                "id": row["topic_id"],
                "subfield_id": row["subfield_id"],
                "field_id": row["field_id"],
                "domain_id": row["domain_id"],
                "topic_name": row["topic_name"],
                "subfield_name": row["subfield_name"],
                "field_name": row["field_name"],
                "domain_name": row["domain_name"],
                "keywords": row["keywords"],
                "summary": row["summary"],
                "wikipedia_url": row["wikipedia_url"],
                "is_ch": row["prediction"] == "yes",
                "confidence": row["confidence"]
            }
            get_or_create(
                session,
                TopicOA,
                unique_key={"id": row["topic_id"]},
                **fields,
            )
            if idx % 100 == 0:
                session.commit()
                session.expunge_all()
                print(f"Processed {idx} rows.")
        session.commit()  # Never forget the final commit ...


if __name__ == "__main__":
    print("Seeding Topics")
    df = pd.read_csv(get_project_root_path() / "data/topics/openalex_topics_classified.csv")
    seed_topics(df)
