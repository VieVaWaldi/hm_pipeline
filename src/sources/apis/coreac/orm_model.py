from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Text, ARRAY, Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Work(Base):
    __tablename__ = "work"
    # __table_args__ = {"schema": "coreac"}

    id = Column(Integer, Sequence("work_id_seq"), primary_key=True)
    id_original = Column(String, unique=True)

    title = Column(String, unique=True, nullable=False)
    doi = Column(String, unique=True)
    arxiv_id = Column(String, unique=True)
    mag_id = Column(String, unique=True)
    pubmed_id = Column(String, unique=True)
    oai_ids = Column(ARRAY(String))

    language_code = Column(String)
    language_name = Column(String)
    document_type = Column(String)
    field_of_study = Column(String)
    abstract = Column(Text)
    fulltext = Column(Text)

    publisher = Column(String)
    authors = Column(ARRAY(String))
    contributors = Column(ARRAY(String))
    journals_title = Column(ARRAY(String))
    download_url = Column(String)
    outputs = Column(ARRAY(String))
    source_fulltext_urls = Column(ARRAY(String))

    year_published = Column(String)
    created_date = Column(DateTime)
    updated_date = Column(DateTime)
    published_date = Column(DateTime)
    deposited_date = Column(DateTime)
    accepted_date = Column(DateTime)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    links = relationship("Link", secondary="j_work_link", back_populates="works")
    references = relationship(
        "Reference", secondary="j_work_reference", back_populates="works"
    )
    data_providers = relationship(
        "DataProvider", secondary="j_work_data_provider", back_populates="works"
    )


class Link(Base):
    __tablename__ = "link"
    # __table_args__ = {"schema": "coreac"}

    id = Column(Integer, Sequence("link_id_seq"), primary_key=True)
    url = Column(String, nullable=False)
    type = Column(String)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    works = relationship("Work", secondary="j_work_link", back_populates="links")


class Reference(Base):
    __tablename__ = "reference"
    # __table_args__ = {"schema": "coreac"}

    id = Column(Integer, Sequence("reference_id_seq"), primary_key=True)
    id_original = Column(String)
    title = Column(String, unique=True, nullable=False)
    authors = Column(ARRAY(String))
    date = Column(DateTime)
    doi = Column(String)
    raw = Column(Text)
    cites = Column(String)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    works = relationship(
        "Work", secondary="j_work_reference", back_populates="references"
    )


class DataProvider(Base):
    __tablename__ = "data_provider"
    # __table_args__ = {"schema": "coreac"}

    id = Column(Integer, Sequence("data_provider_id_seq"), primary_key=True)
    id_original = Column(String)
    name = Column(String, nullable=False)
    url = Column(String)
    logo = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    # Relationships
    works = relationship(
        "Work", secondary="j_work_data_provider", back_populates="data_providers"
    )


""" Junction tables """


class JunctionWorkLink(Base):
    __tablename__ = "j_work_link"
    # __table_args__ = {"schema": "coreac"}

    work_id = Column(Integer, ForeignKey("work.id"), primary_key=True)
    link_id = Column(Integer, ForeignKey("link.id"), primary_key=True)


class JunctionWorkReference(Base):
    __tablename__ = "j_work_reference"
    # __table_args__ = {"schema": "coreac"}

    work_id = Column(Integer, ForeignKey("work.id"), primary_key=True)
    reference_id = Column(Integer, ForeignKey("reference.id"), primary_key=True)


class JunctionWorkDataProvider(Base):
    __tablename__ = "j_work_data_provider"
    # __table_args__ = {"schema": "coreac"}

    work_id = Column(Integer, ForeignKey("work.id"), primary_key=True)
    data_provider_id = Column(
        Integer, ForeignKey("data_provider.id"), primary_key=True
    )
