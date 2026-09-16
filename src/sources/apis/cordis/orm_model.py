from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    DateTime,
    ForeignKey,
    Text,
    Float,
    Boolean,
    Date,
    JSON,
    Sequence,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


""" Base """


class Person(Base):
    __tablename__ = "person"
    # __table_args__ = {"schema": "cordis"}

    id = Column(Integer, Sequence("person_id_seq"), primary_key=True)
    title = Column(Text)
    name = Column(Text)
    first_name = Column(Text)
    last_name = Column(Text)
    telephone_number = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    institutions = relationship(
        "Institution", secondary="j_institution_person", back_populates="persons"
    )
    research_outputs = relationship(
        "ResearchOutput",
        secondary="j_researchoutput_person",
        back_populates="persons",
    )


class Topic(Base):
    __tablename__ = "topic"
    # __table_args__ = {"schema": "cordis"}

    id = Column(Integer, Sequence("topic_id_seq"), primary_key=True)
    name = Column(Text, unique=True, nullable=False)
    level = Column(Integer, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    projects = relationship(
        "Project", secondary="j_project_topic", back_populates="topics"
    )
    research_outputs = relationship(
        "ResearchOutput",
        secondary="j_researchoutput_topic",
        back_populates="topics",
    )


class Weblink(Base):
    __tablename__ = "weblink"
    # __table_args__ = {"schema": "cordis"}

    id = Column(Integer, Sequence("weblink_id_seq"), primary_key=True)
    url = Column(Text, unique=True, nullable=False)
    title = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    projects = relationship(
        "Project", secondary="j_project_weblink", back_populates="weblinks"
    )
    research_outputs = relationship(
        "ResearchOutput",
        secondary="j_researchoutput_weblink",
        back_populates="weblinks",
    )


class FundingProgramme(Base):
    __tablename__ = "fundingprogramme"
    # __table_args__ = {"schema": "cordis"}

    id = Column(Integer, Sequence("fundingprogramme_id_seq"), primary_key=True)
    code = Column(Text, unique=True, nullable=False)
    title = Column(Text)
    short_title = Column(Text)
    framework_programme = Column(Text)
    pga = Column(Text)
    rcn = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    projects = relationship(
        "Project",
        secondary="j_project_fundingprogramme",
        back_populates="funding_programmes",
    )


""" Main Tables """


class Institution(Base):
    __tablename__ = "institution"
    # __table_args__ = {"schema": "cordis"}

    id = Column(Integer, Sequence("institution_id_seq"), primary_key=True)
    legal_name = Column(Text, unique=True, nullable=False)
    sme = Column(Boolean)
    url = Column(Text)
    short_name = Column(Text)
    vat_number = Column(Text)
    street = Column(Text)
    postbox = Column(Text)
    postalcode = Column(Text)
    city = Column(Text)
    country = Column(Text)
    geolocation = Column(JSON)

    type_title = Column(Text)
    nuts_level_0 = Column(Text)
    nuts_level_1 = Column(Text)
    nuts_level_2 = Column(Text)
    nuts_level_3 = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    persons = relationship(
        "Person", secondary="j_institution_person", back_populates="institutions"
    )
    projects = relationship(
        "Project",
        secondary="j_project_institution",
        back_populates="institutions",
    )
    research_outputs = relationship(
        "ResearchOutput",
        secondary="j_researchoutput_institution",
        back_populates="institutions",
    )


class ResearchOutput(Base):
    __tablename__ = "researchoutput"
    # __table_args__ = {"schema": "cordis"}

    id = Column(Integer, Sequence("researchoutput_id_seq"), primary_key=True)
    id_original = Column(Text, unique=True, nullable=False)
    from_pdf = Column(Boolean)
    type = Column(Text, nullable=False)
    doi = Column(Text)
    title = Column(Text, nullable=False)
    publication_date = Column(Date)
    journal = Column(Text)
    summary = Column(Text)
    comment = Column(Text)
    fulltext = Column(Text)
    funding_number = Column(Text)

    journal_number = Column(Text)
    journal_title = Column(Text)
    published_pages = Column(Text)
    published_year = Column(Text)
    publisher = Column(Text)
    issn = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    projects = relationship(
        "Project",
        secondary="j_project_researchoutput",
        back_populates="research_outputs",
    )
    persons = relationship(
        "Person",
        secondary="j_researchoutput_person",
        back_populates="research_outputs",
    )
    topics = relationship(
        "Topic",
        secondary="j_researchoutput_topic",
        back_populates="research_outputs",
    )
    weblinks = relationship(
        "Weblink",
        secondary="j_researchoutput_weblink",
        back_populates="research_outputs",
    )
    institutions = relationship(
        "Institution",
        secondary="j_researchoutput_institution",
        back_populates="research_outputs",
    )


class Project(Base):
    __tablename__ = "project"
    # __table_args__ = {"schema": "cordis"}

    id = Column(Integer, Sequence("project_id_seq"), primary_key=True)
    id_original = Column(Text, unique=True, nullable=False)
    doi = Column(Text)
    title = Column(Text, nullable=False)
    acronym = Column(Text)
    status = Column(Text)
    start_date = Column(Date)
    end_date = Column(Date)
    ec_signature_date = Column(Date)
    total_cost = Column(Float)
    ec_max_contribution = Column(Float)
    objective = Column(Text)
    call_identifier = Column(Text)
    call_title = Column(Text)
    call_rcn = Column(Text)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    research_outputs = relationship(
        "ResearchOutput",
        secondary="j_project_researchoutput",
        back_populates="projects",
    )
    topics = relationship(
        "Topic", secondary="j_project_topic", back_populates="projects"
    )
    weblinks = relationship(
        "Weblink", secondary="j_project_weblink", back_populates="projects"
    )
    funding_programmes = relationship(
        "FundingProgramme",
        secondary="j_project_fundingprogramme",
        back_populates="projects",
    )
    institutions = relationship(
        "Institution",
        secondary="j_project_institution",
        back_populates="projects",
    )


""" Junction Tables """

""" Junction Institution """


class JunctionInstitutionPerson(Base):
    __tablename__ = "j_institution_person"
    # __table_args__ = {"schema": "cordis"}

    institution_id = Column(
        Integer,
        ForeignKey("institution.id"),
        primary_key=True,
    )
    person_id = Column(Integer, ForeignKey("person.id"), primary_key=True, index=True)


""" Junction ResearchOutput """


class JunctionResearchOutputPerson(Base):
    __tablename__ = "j_researchoutput_person"
    # __table_args__ = {"schema": "cordis"}

    researchoutput_id = Column(
        Integer,
        ForeignKey("researchoutput.id"),
        primary_key=True,
    )
    person_id = Column(Integer, ForeignKey("person.id"), primary_key=True, index=True)
    person_position = Column(Integer, nullable=False)


class JunctionResearchOutputTopic(Base):
    __tablename__ = "j_researchoutput_topic"
    # __table_args__ = {"schema": "cordis"}

    researchoutput_id = Column(
        Integer,
        ForeignKey("researchoutput.id"),
        primary_key=True,
    )
    topic_id = Column(Integer, ForeignKey("topic.id"), primary_key=True, index=True)


class JunctionResearchOutputWeblink(Base):
    __tablename__ = "j_researchoutput_weblink"
    # __table_args__ = {"schema": "cordis"}

    researchoutput_id = Column(
        Integer,
        ForeignKey("researchoutput.id"),
        primary_key=True,
    )
    weblink_id = Column(Integer, ForeignKey("weblink.id"), primary_key=True, index=True)


class JunctionResearchOutputInstitution(Base):
    __tablename__ = "j_researchoutput_institution"
    # __table_args__ = {"schema": "cordis"}

    researchoutput_id = Column(
        Integer,
        ForeignKey("researchoutput.id"),
        primary_key=True,
    )
    institution_id = Column(
        Integer, ForeignKey("institution.id"), primary_key=True, index=True
    )


""" Junction Project """


class JunctionProjectTopic(Base):
    __tablename__ = "j_project_topic"
    # __table_args__ = {"schema": "cordis"}

    project_id = Column(Integer, ForeignKey("project.id"), primary_key=True)
    topic_id = Column(Integer, ForeignKey("topic.id"), primary_key=True, index=True)


class JunctionProjectWeblink(Base):
    __tablename__ = "j_project_weblink"
    # __table_args__ = {"schema": "cordis"}

    project_id = Column(Integer, ForeignKey("project.id"), primary_key=True)
    weblink_id = Column(Integer, ForeignKey("weblink.id"), primary_key=True, index=True)


class JunctionProjectFundingProgramme(Base):
    __tablename__ = "j_project_fundingprogramme"
    # __table_args__ = {"schema": "cordis"}

    project_id = Column(Integer, ForeignKey("project.id"), primary_key=True)
    fundingprogramme_id = Column(
        Integer, ForeignKey("fundingprogramme.id"), primary_key=True, index=True
    )


class JunctionProjectInstitution(Base):
    __tablename__ = "j_project_institution"
    # __table_args__ = {"schema": "cordis"}

    project_id = Column(Integer, ForeignKey("project.id"), primary_key=True)
    institution_id = Column(
        Integer, ForeignKey("institution.id"), primary_key=True, index=True
    )

    institution_position = Column(Integer, nullable=False)
    ec_contribution = Column(Float)
    net_ec_contribution = Column(Float)
    total_cost = Column(Float)
    type = Column(Text)
    organization_id = Column(Text)
    rcn = Column(Integer)


class JunctionProjectResearchOutput(Base):
    __tablename__ = "j_project_researchoutput"
    # __table_args__ = {"schema": "cordis"}

    project_id = Column(Integer, ForeignKey("project.id"), primary_key=True)
    researchoutput_id = Column(
        Integer, ForeignKey("researchoutput.id"), primary_key=True, index=True
    )
