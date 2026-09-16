from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, ARRAY, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Entry(Base):
    __tablename__ = "entry"
    # __table_args__ = {"schema": "arxiv"}

    id = Column(Integer, Sequence("entry_id_seq"), primary_key=True)
    id_original = Column(String, unique=True, nullable=False)
    title = Column(String, unique=True, nullable=False)

    doi = Column(String, unique=True)
    summary = Column(String)
    full_text = Column(String)
    journal_ref = Column(String)
    comment = Column(String)
    primary_category = Column(String)
    category_term = Column(String)
    categories = Column(ARRAY(String))
    published_date = Column(DateTime)
    updated_date = Column(DateTime)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    authors = relationship(
        "Author", secondary="j_entry_author", back_populates="entries"
    )
    links = relationship(
        "Link", secondary="j_entry_link", back_populates="entries"
    )


class Author(Base):
    __tablename__ = "author"
    # __table_args__ = {"schema": "arxiv"}

    id = Column(Integer, Sequence("author_id_seq"), primary_key=True)
    name = Column(String, unique=True, nullable=False)
    affiliation = Column(String)
    affiliations = Column(ARRAY(String))

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    entries = relationship(
        "Entry", secondary="j_entry_author", back_populates="authors"
    )


class Link(Base):
    __tablename__ = "link"
    # __table_args__ = {"schema": "arxiv"}

    id = Column(Integer, Sequence("link_id_seq"), primary_key=True)
    href = Column(String, nullable=False)
    title = Column(String)
    rel = Column(String)
    type = Column(String)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    entries = relationship(
        "Entry", secondary="j_entry_link", back_populates="links"
    )


""" Junction tables """


class JunctionEntryAuthor(Base):
    __tablename__ = "j_entry_author"
    # __table_args__ = {"schema": "arxiv"}

    entry_id = Column(Integer, ForeignKey("entry.id"), primary_key=True)
    author_id = Column(Integer, ForeignKey("author.id"), primary_key=True)
    author_position = Column(Integer, nullable=False)


class JunctionEntryLink(Base):
    __tablename__ = "j_entry_link"
    # __table_args__ = {"schema": "arxiv"}

    entry_id = Column(Integer, ForeignKey("entry.id"), primary_key=True)
    link_id = Column(Integer, ForeignKey("link.id"), primary_key=True)
