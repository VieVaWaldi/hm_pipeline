from datetime import datetime

from sqlalchemy import (
    Column,
    Integer,
    DateTime,
    ForeignKey,
    Text,
    Numeric,
    Boolean,
    CheckConstraint,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


class Stakeholder(Base):
    __tablename__ = "stakeholder"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    abbreviation = Column(Text)
    webpage_url = Column(Text)
    social_media_url = Column(Text)
    legal_status = Column(Text)
    description = Column(Text)
    ownership = Column(
        Text, CheckConstraint("ownership IN ('public', 'private', 'mixed')")
    )

    street_name = Column(Text)
    house_number = Column(Text)
    postal_code = Column(Text)
    city = Column(Text)
    country = Column(Text)

    nuts_code_id = Column(Integer, ForeignKey("meta_heritage.nuts_code.id"))
    nace_code_id = Column(Integer, ForeignKey("meta_heritage.nace_code.id"))

    contact_firstname = Column(Text)
    contact_surname = Column(Text)
    contact_email = Column(Text)
    contact_phone = Column(Text)

    latitude = Column(Numeric)
    longitude = Column(Numeric)

    data_source_type = Column(Text)
    data_source_name = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    nuts_code = relationship("NutsCode", back_populates="stakeholders")
    nace_code = relationship("NaceCode", back_populates="stakeholders")

    eu_project_participations = relationship(
        "EuProjectParticipation",
        secondary="meta_heritage.j_stakeholder_eu_project_participation",
        back_populates="stakeholders",
    )
    organization_types = relationship(
        "OrganizationType",
        secondary="meta_heritage.j_stakeholder_organization_type",
        back_populates="stakeholders",
    )
    network_memberships = relationship(
        "Network",
        secondary="meta_heritage.j_stakeholder_network_membership",
        back_populates="stakeholders",
    )
    cultural_routes = relationship(
        "CulturalRoute",
        secondary="meta_heritage.j_stakeholder_cultural_route",
        back_populates="stakeholders",
    )
    heritage_topics = relationship(
        "CHTopic",
        secondary="meta_heritage.j_stakeholder_heritage_topic",
        back_populates="stakeholders",
    )


class NutsCode(Base):
    __tablename__ = "nuts_code"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    country_code = Column(Text, nullable=False)
    level_1 = Column(Text)
    level_2 = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stakeholders = relationship("Stakeholder", back_populates="nuts_code")


class NaceCode(Base):
    __tablename__ = "nace_code"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    level_1 = Column(Text, nullable=False)
    level_2 = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stakeholders = relationship("Stakeholder", back_populates="nace_code")


class EuProjectParticipation(Base):
    __tablename__ = "eu_project_participation"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    project_id = Column(Text)
    project_name = Column(Text)
    pic_code = Column(Text)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stakeholders = relationship(
        "Stakeholder",
        secondary="meta_heritage.j_stakeholder_eu_project_participation",
        back_populates="eu_project_participations",
    )


class OrganizationType(Base):
    __tablename__ = "organization_type"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    type_number = Column(Integer, unique=True)
    name = Column(Text, unique=True, nullable=False)
    is_predefined = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stakeholders = relationship(
        "Stakeholder",
        secondary="meta_heritage.j_stakeholder_organization_type",
        back_populates="organization_types",
    )


class Network(Base):
    __tablename__ = "network"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    network_number = Column(Integer, unique=True)
    name = Column(Text, unique=True, nullable=False)
    is_predefined = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stakeholders = relationship(
        "Stakeholder",
        secondary="meta_heritage.j_stakeholder_network_membership",
        back_populates="network_memberships",
    )


class CulturalRoute(Base):
    __tablename__ = "cultural_route"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    route_number = Column(Integer, unique=True)
    name = Column(Text, unique=True, nullable=False)
    is_predefined = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stakeholders = relationship(
        "Stakeholder",
        secondary="meta_heritage.j_stakeholder_cultural_route",
        back_populates="cultural_routes",
    )


class CHTopic(Base):
    __tablename__ = "ch_topic"
    __table_args__ = {"schema": "meta_heritage"}

    id = Column(Integer, primary_key=True)
    topic_number = Column(Integer, unique=True)
    name = Column(Text, unique=True, nullable=False)
    is_predefined = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    stakeholders = relationship(
        "Stakeholder",
        secondary="meta_heritage.j_stakeholder_heritage_topic",
        back_populates="heritage_topics",
    )


""" Junction tables """


class JunctionStakeholderEuProjectParticipation(Base):
    __tablename__ = "j_stakeholder_eu_project_participation"
    __table_args__ = {"schema": "meta_heritage"}

    stakeholder_id = Column(
        Integer,
        ForeignKey("meta_heritage.stakeholder.id", ondelete="CASCADE"),
        primary_key=True,
    )
    eu_project_participation_id = Column(
        Integer,
        ForeignKey("meta_heritage.eu_project_participation.id", ondelete="CASCADE"),
        primary_key=True,
    )


class JunctionStakeholderOrganizationType(Base):
    __tablename__ = "j_stakeholder_organization_type"
    __table_args__ = {"schema": "meta_heritage"}

    stakeholder_id = Column(
        Integer,
        ForeignKey("meta_heritage.stakeholder.id", ondelete="CASCADE"),
        primary_key=True,
    )
    organization_type_id = Column(
        Integer,
        ForeignKey("meta_heritage.organization_type.id", ondelete="CASCADE"),
        primary_key=True,
    )


class JunctionStakeholderNetworkMembership(Base):
    __tablename__ = "j_stakeholder_network_membership"
    __table_args__ = {"schema": "meta_heritage"}

    membership_interest = Column(Text)

    stakeholder_id = Column(
        Integer,
        ForeignKey("meta_heritage.stakeholder.id", ondelete="CASCADE"),
        primary_key=True,
    )
    network_id = Column(
        Integer,
        ForeignKey("meta_heritage.network.id", ondelete="CASCADE"),
        primary_key=True,
    )


class JunctionStakeholderCulturalRoute(Base):
    __tablename__ = "j_stakeholder_cultural_route"
    __table_args__ = {"schema": "meta_heritage"}

    stakeholder_id = Column(
        Integer,
        ForeignKey("meta_heritage.stakeholder.id", ondelete="CASCADE"),
        primary_key=True,
    )
    cultural_route_id = Column(
        Integer,
        ForeignKey("meta_heritage.cultural_route.id", ondelete="CASCADE"),
        primary_key=True,
    )


class JunctionStakeholderHeritageTopic(Base):
    __tablename__ = "j_stakeholder_heritage_topic"
    __table_args__ = {"schema": "meta_heritage"}

    stakeholder_id = Column(
        Integer,
        ForeignKey("meta_heritage.stakeholder.id", ondelete="CASCADE"),
        primary_key=True,
    )
    heritage_topic_id = Column(
        Integer,
        ForeignKey("meta_heritage.ch_topic.id", ondelete="CASCADE"),
        primary_key=True,
    )
