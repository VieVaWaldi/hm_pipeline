"""
Tiny staging fixture shared by the core_v4 enrichment tests. There is no local copy of the real
core_v3 staging sample, so this builds a duckdb file with the same table shapes
(src/sources/dumps/openaire/staging.py for project/work; the Phase 3 contract for organization).

Ids are hash('p<i>') etc. like the real ones (UBIGINT).
"""

from pathlib import Path

import duckdb

PROJECTS = [
    # (i, title, summary, acronym, keywords, subjects)
    (1, "Digitale Kulturerbe Plattform", "Ein Projekt über digitale Archive.", "DKP", "kultur", ["heritage", "archive"]),
    (2, "Sustainable inclusive cities", "Resilient urban planning for everyone.", "SIC", "cities", ["urban"]),
    (3, "Titre seulement", None, None, None, []),
    (4, None, None, "NOTEXT", None, ["x"]),  # no title and no summary: excluded from text
]

WORKS = [
    # (i, title, description, subject, container)
    (1, "Die Zukunft der Museen", "Museen im digitalen Zeitalter.", "Museum", "Kulturjournal"),
    (2, "Roma communities in Europe", "A study of Romani minorities.", "Roma", "Minority Studies"),
    (3, "Only a title", None, None, None),
    (4, None, None, "orphan", "Nowhere"),  # no title, no description: excluded
]

ORGS = [
    # (i, country, street, postcode, city, address_country)
    (1, "DE", "Hauptstr. 1", "10115", "Berlin", "DE"),
    (2, "UK", "High Street 2", "OX1 1AA", "Oxford", "UK"),
    (3, None, None, None, None, None),
]


def make_staging_fixture(path: Path) -> Path:
    con = duckdb.connect(str(path))
    con.execute(
        "CREATE TABLE project (id UBIGINT, openaireId VARCHAR, title VARCHAR, summary VARCHAR, acronym VARCHAR,"
        " keywords VARCHAR, subjects VARCHAR[], doi VARCHAR)"
    )
    for i, title, summary, acronym, keywords, subjects in PROJECTS:
        con.execute(
            "INSERT INTO project VALUES (hash(?), ?, ?, ?, ?, ?, ?, NULL)",
            [f"p{i}", f"p{i}", title, summary, acronym, keywords, subjects],
        )
    con.execute(
        "CREATE TABLE work (id UBIGINT, openaireId VARCHAR, title VARCHAR, descriptions VARCHAR[],"
        " subjects STRUCT(subject STRUCT(scheme VARCHAR, value VARCHAR))[], container STRUCT(name VARCHAR),"
        " countries VARCHAR[])"
    )
    for i, title, description, subject, container in WORKS:
        subjects = [] if subject is None else [{"subject": {"scheme": "keyword", "value": subject}}]
        con.execute(
            "INSERT INTO work VALUES (hash(?), ?, ?, ?, ?, ?, ?)",
            [f"w{i}", f"w{i}", title, [description] if description else [], subjects,
             {"name": container} if container else None, ["DE"]],
        )
    con.execute(
        "CREATE TABLE organization (id UBIGINT, openaireId VARCHAR, legalName VARCHAR, countryCode VARCHAR,"
        " address_street VARCHAR, address_postalcode VARCHAR, address_city VARCHAR, address_country VARCHAR,"
        " nuts3 VARCHAR, geolocation STRUCT(lat DOUBLE, lon DOUBLE), geolocation_source VARCHAR, region VARCHAR)"
    )
    for i, cc, street, postcode, city, addr_cc in ORGS:
        con.execute(
            "INSERT INTO organization VALUES (hash(?), ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL)",
            [f"o{i}", f"o{i}", f"Org {i}", cc, street, postcode, city, addr_cc],
        )
    con.close()
    return path


def project_id(i: int) -> int:
    return duckdb.sql(f"SELECT hash('p{i}')").fetchone()[0]


def work_id(i: int) -> int:
    return duckdb.sql(f"SELECT hash('w{i}')").fetchone()[0]
