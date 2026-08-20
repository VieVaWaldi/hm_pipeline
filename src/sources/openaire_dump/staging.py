"""
OpenAIRE Staging — Transform openaire_raw.duckdb → openaire_staging.duckdb

Applies per table:
  - ID hashing (all 4 tables): id → hash(id) as new PK; old id kept as openaireId
  - Column sanitization via Python UDFs
  - Struct unpacking: country → countryCode; pids → rorId, wikiId
  - Column drops, renames, row filters
  - frameworkProgrammes extraction from fundings[].fundingStream
  - relation: hash source and target in place
"""

import logging
from datetime import datetime
from pathlib import Path

from lib.database.duck.create_connection import create_duck_connection
from lib.file_handling.file_utils import ensure_path_exists
from lib.sanitizers.parse_text import (
    parse_content,
    parse_names_and_identifiers,
    parse_string,
    parse_titles_and_labels,
    parse_web_resources,
)
from utils.config.config_loader import get_query_config
from utils.logger.logger import setup_logging
from utils.logger.timer import log_run_time

setup_logging("staging", "openaire")

config = get_query_config()["openaire_dump"]
RAW_DB = Path(config["path_duck"])
STAGING_DB = Path(
    config["path_duck_staging_2"]
)  # IMPORTANT i changed this for the pids temp

ensure_path_exists(STAGING_DB)

logging.info("OPENAIRE STAGING")
logging.info(f"Source: {RAW_DB}")
logging.info(f"Target: {STAGING_DB}")

con = create_duck_connection(str(STAGING_DB))
con.execute("SET memory_limit='160GB'")
con.execute("SET threads=32")

con.execute(f"ATTACH '{RAW_DB}' AS raw (READ_ONLY)")

total_start = datetime.now()


# -------------------------------------------------------------------------
# Register Python UDFs
# -------------------------------------------------------------------------
con.create_function(
    "sanitize_name", parse_names_and_identifiers, null_handling="special"
)
con.create_function("sanitize_title", parse_titles_and_labels, null_handling="special")
con.create_function("sanitize_content", parse_content, null_handling="special")
con.create_function("sanitize_url", parse_web_resources, null_handling="special")
con.create_function("sanitize_string", parse_string, null_handling="special")


# -------------------------------------------------------------------------
# 1. ORGANIZATION
# -------------------------------------------------------------------------
logging.info("--- Staging organization ---")
t = datetime.now()

con.execute(
    """
    CREATE TABLE organization AS
    SELECT
        hash(id)                                                            AS id,
        id                                                                  AS openaireId,
        sanitize_name(legalName)                                            AS legalName,
        sanitize_name(legalShortName)                                       AS legalShortName,
        sanitize_url(websiteUrl)                                            AS websiteUrl,
        list_filter(
            list_transform(alternativeNames, x -> sanitize_name(x)),
            x -> x IS NOT NULL
        )                                                                   AS alternativeNames,
        country.code                                                        AS countryCode,
        (list_filter(pids, p -> p.scheme = 'ROR'))[1].value                 AS rorId,
        (list_filter(pids, p -> p.scheme = 'Wikidata'))[1].value            AS wikiId
        pids                                                                AS pids
    FROM raw.organization
"""
)

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM organization").fetchone()[0]
logging.info(f"organization rows: {count:,}")


# -------------------------------------------------------------------------
# 2. PROJECT
# -------------------------------------------------------------------------
logging.info("--- Staging project ---")
t = datetime.now()

con.execute(
    """
    CREATE TABLE project AS
    SELECT
        hash(id)                                                            AS id,
        id                                                                  AS openaireId,
        code                                                                AS grantId,
        sanitize_title(title)                                               AS title,
        sanitize_string(acronym)                                            AS acronym,
        sanitize_url(websiteUrl)                                            AS websiteUrl,
        CASE WHEN year(startDate) < 1000 OR year(startDate) > 3000 THEN NULL ELSE startDate END  AS startDate,
        CASE WHEN year(endDate)   < 1000 OR year(endDate)   > 3000 THEN NULL ELSE endDate   END  AS endDate,
        sanitize_string(callIdentifier)                                     AS callIdentifier,
        sanitize_string(keywords)                                           AS keywords,
        openAccessMandateForPublications,
        openAccessMandateForDataset,
        list_filter(
            list_transform(subjects, x -> sanitize_string(x)),
            x -> x IS NOT NULL
        )                                                                   AS subjects,
        fundings,
        list_distinct(list_filter(
            list_transform(
                fundings,
                f -> CASE
                    WHEN f.fundingStream IS NOT NULL
                     AND f.fundingStream.id IS NOT NULL
                     AND contains(f.fundingStream.id, '::')
                    THEN string_split(f.fundingStream.id, '::')[2]
                    ELSE NULL
                END
            ),
            x -> x IS NOT NULL AND x != ''
        ))                                                                  AS frameworkProgrammes,
        sanitize_content(summary)                                           AS summary,
        granted
    FROM raw.project
    WHERE title IS NOT NULL
      AND lower(trim(title)) != 'unidentified'
"""
)

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM project").fetchone()[0]
logging.info(f"project rows: {count:,}")


# -------------------------------------------------------------------------
# 3. WORK  (raw table is named 'work', loaded from publication/ only)
# -------------------------------------------------------------------------
logging.info("--- Staging work ---")
t = datetime.now()

con.execute(
    """
    CREATE TABLE work AS
    SELECT
        hash(id)                                                            AS id,
        id                                                                  AS openaireId,
        sanitize_title(mainTitle)                                           AS title,
        CASE WHEN year(publicationDate) < 1000 OR year(publicationDate) > 3000
             THEN NULL ELSE publicationDate END                             AS publicationDate,
        sanitize_name(publisher)                                            AS publisher,
        openAccessColor,
        isGreen,
        isInDiamondJournal,
        publiclyFunded,
        language,
        bestAccessRight,
        authors,
        subjects,
        list_filter(
            list_transform(descriptions, x -> sanitize_content(x)),
            x -> x IS NOT NULL
        )                                                                   AS descriptions,
        pids,
        sources,
        formats,
        instances,
        indicators.citationImpact.citationCount                             AS citationCount,
        indicators.citationImpact.influence                                 AS influence,
        indicators.usageCounts.views                                        AS views,
        container
    FROM raw.work
"""
)

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM work").fetchone()[0]
logging.info(f"work rows: {count:,}")


# -------------------------------------------------------------------------
# 4. RELATION  (hash source and target)
# -------------------------------------------------------------------------
logging.info("--- Staging relation ---")
t = datetime.now()

con.execute(
    """
    CREATE TABLE relation AS
    SELECT
        hash(source)    AS source,
        sourceType,
        hash(target)    AS target,
        targetType,
        relType,
        provenance,
        validated
    FROM raw.relation
"""
)

log_run_time(t)
count = con.execute("SELECT COUNT(*) FROM relation").fetchone()[0]
logging.info(f"relation rows: {count:,}")


# -------------------------------------------------------------------------
# VERIFY
# -------------------------------------------------------------------------
logging.info("--- Final verification ---")

for table in ["organization", "project", "work", "relation"]:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logging.info(f"  {table}: {count:,} rows")

staging_size_mb = STAGING_DB.stat().st_size / (1024**2)
logging.info(f"Staging DuckDB size: {staging_size_mb:,.1f} MB")

log_run_time(total_start)
logging.info(f"Database location: {STAGING_DB}")

con.close()
