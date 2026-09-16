from datetime import datetime

from sqlalchemy import BigInteger, Column, DateTime, Float, ForeignKey, Integer, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Topic(Base):
    """ORM reference only — create via CREATE_TOPIC_SQL for DuckDB."""
    __tablename__ = "topic"
    __table_args__ = {"keep_existing": True}

    id = Column(Integer, primary_key=True, autoincrement=False)  # OpenAlex topic_id
    subfield_id = Column(Text)
    field_id = Column(Text)
    domain_id = Column(Text)
    topic_name = Column(Text)
    subfield_name = Column(Text)
    field_name = Column(Text)
    domain_name = Column(Text)
    keywords = Column(Text)
    summary = Column(Text)
    wikipedia_url = Column(Text)  # nullable — some OpenAlex topics have no Wikipedia page
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


CREATE_TOPIC_SQL = """
    CREATE TABLE IF NOT EXISTS topic (
        id           INTEGER PRIMARY KEY,
        subfield_id  TEXT,
        field_id     TEXT,
        domain_id    TEXT,
        topic_name   TEXT,
        subfield_name TEXT,
        field_name   TEXT,
        domain_name  TEXT,
        keywords     TEXT,
        summary      TEXT,
        wikipedia_url TEXT,
        created_at   TIMESTAMP,
        updated_at   TIMESTAMP
    )
"""


class Project(Base):
    """Minimal mapping of the existing core_v3 project table — read-only reference."""
    __tablename__ = "project"
    __table_args__ = {"keep_existing": True}

    id = Column(BigInteger, primary_key=True)
    title = Column(Text)
    acronym = Column(Text)
    summary = Column(Text)
    keywords = Column(Text)


class Work(Base):
    """Minimal mapping of the existing core_v3 work table — read-only reference."""
    __tablename__ = "work"
    __table_args__ = {"keep_existing": True}

    id = Column(BigInteger, primary_key=True)
    title = Column(Text)


class RelationTopic(Base):
    """
    ORM reference only — create this table via native DuckDB SQL so that
    source_id can be declared UBIGINT (SQLAlchemy has no UBIGINT type).
    See _CREATE_RELATION_TOPIC_SQL below.
    """
    __tablename__ = "relation_topic"
    __table_args__ = {"keep_existing": True}

    type = Column(Text, nullable=False, primary_key=True)       # "project" or "work"
    source_id = Column(BigInteger, nullable=False, primary_key=True)  # UBIGINT in DB
    topic_id = Column(Integer, ForeignKey("topic.id"), nullable=False, primary_key=True)
    score = Column(Float)
    created_at = Column(DateTime, default=datetime.now)


# Use this to create the table — source_id must be UBIGINT to match project/work ids
CREATE_RELATION_TOPIC_SQL = """
    CREATE TABLE IF NOT EXISTS relation_topic (
        type      TEXT    NOT NULL,
        source_id UBIGINT NOT NULL,
        topic_id  INTEGER NOT NULL,
        score     FLOAT,
        created_at TIMESTAMP,
        PRIMARY KEY (type, source_id, topic_id)
    )
"""
