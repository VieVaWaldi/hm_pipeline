"""
DuckDB schema for topic modelling's own output tables.

Topic modelling owns these tables (they aren't part of any core pipeline's
schema) — whichever pipeline runs this enrichment creates them in its own
duckdb file via these statements, native SQL rather than an ORM: `source_id`
must be UBIGINT to match project/work ids, which SQLAlchemy has no type for.
"""

CREATE_TOPIC_SQL = """
    CREATE TABLE IF NOT EXISTS topic (
        id            INTEGER PRIMARY KEY,
        subfield_id   TEXT,
        field_id      TEXT,
        domain_id     TEXT,
        topic_name    TEXT,
        subfield_name TEXT,
        field_name    TEXT,
        domain_name   TEXT,
        keywords      TEXT,
        summary       TEXT,
        wikipedia_url TEXT,
        created_at    TIMESTAMP,
        updated_at    TIMESTAMP
    )
"""

CREATE_RELATION_TOPIC_SQL = """
    CREATE TABLE IF NOT EXISTS relation_topic (
        type       TEXT    NOT NULL,   -- "project" or "work"
        source_id  UBIGINT NOT NULL,
        topic_id   INTEGER NOT NULL,
        score      FLOAT,
        created_at TIMESTAMP,
        PRIMARY KEY (type, source_id, topic_id)
    )
"""
