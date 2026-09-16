import logging
import re
from pathlib import Path
from typing import Optional, Tuple, List

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
from common.sanitizers.parse_primitives import parse_float
from common.sanitizers.parse_text import (
    parse_names_and_identifiers,
    parse_content,
    parse_web_resources,
    parse_string,
)
from sources.external.meta_heritage.orm_model import (
    Stakeholder,
    NutsCode,
    NaceCode,
    OrganizationType,
    CHTopic,
    JunctionStakeholderOrganizationType,
    JunctionStakeholderHeritageTopic,
)
from common.log.logger import setup_logging


def parse_address(address: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Parse German address into street_name, house_number, postal_code, city."""
    if not address or pd.isna(address):
        return None, None, None, None
    
    address = address.strip()
    if not address:
        return None, None, None, None
    
    # Pattern for German addresses: street_name house_number, postal_code city
    # House number should be numeric optionally followed by a single letter
    pattern = r'^(.+?)\s+(\d+[a-zA-Z]?),\s*(\d{5})\s+(.+)$'
    match = re.match(pattern, address)
    
    if match:
        street_name = match.group(1).strip()
        house_number = match.group(2).strip()
        postal_code = match.group(3).strip()
        city = match.group(4).strip()
        
        # Validate house number is numeric with optional single letter
        if re.match(r'^\d+[a-zA-Z]?$', house_number):
            return street_name, house_number, postal_code, city
    
    # Fallback: try to extract what we can without house number
    # Pattern: street_name, postal_code city
    fallback_pattern = r'^(.+?),\s*(\d{5})\s+(.+)$'
    fallback_match = re.match(fallback_pattern, address)
    
    if fallback_match:
        street_name = fallback_match.group(1).strip()
        postal_code = fallback_match.group(2).strip()
        city = fallback_match.group(3).strip()
        return street_name, None, postal_code, city
    
    return None, None, None, None


def get_nuts_level1_from_region(region: Optional[str]) -> Optional[str]:
    """Map region names to NUTS level 1 codes."""
    if not region or pd.isna(region):
        return None

    region_mapping = {
        "Baden-Württemberg": "DE1",
        "Bayern": "DE2",
        "Bavaria": "DE2",
        "Berlin": "DE3",
        "Brandenburg": "DE4",
        "Bremen": "DE5",
        "Hamburg": "DE6",
        "Hessen": "DE7",
        "Mecklenburg-Vorpommern": "DE8",
        "Niedersachsen": "DE9",
        "Nordrhein-Westfalen": "DEA",
        "Rheinland-Pfalz": "DEB",
        "Saarland": "DEC",
        "Sachsen": "DED",
        "Saxony": "DED",
        "Sachsen-Anhalt": "DEE",
        "Schleswig-Holstein": "DEF",
        "Thüringen": "DEG",
        "Thuringia": "DEG",
    }

    return region_mapping.get(region.strip())


def get_organization_type_mapping(main_entity: Optional[str]) -> Optional[str]:
    """Map MainEntity to organization type."""
    if not main_entity or pd.isna(main_entity):
        return None

    main_entity_lower = main_entity.lower()

    if any(
        keyword in main_entity_lower
        for keyword in ["lodging business", "food establishment", "local business"]
    ):
        return "Company or Corporation"
    elif any(
        keyword in main_entity_lower
        for keyword in [
            "historical building",
            "landmark",
            "museum",
            "nature attraction",
            "tourist attraction",
            "cave",
            "mine",
        ]
    ):
        return "Gallery, Library, Archive, Museum (GLAM)"

    return None


def get_heritage_topics(main_entity: Optional[str]) -> List[str]:
    """Map MainEntity to heritage topics."""
    if not main_entity or pd.isna(main_entity):
        return []

    main_entity_lower = main_entity.lower()
    topics = []

    if "cave" in main_entity_lower or "mine" in main_entity_lower:
        topics.extend(["Industrial heritage", "Local history"])
    elif "food establishment" in main_entity_lower:
        topics.append("Culinary heritage")
    elif "historical building" in main_entity_lower or "landmark" in main_entity_lower:
        topics.extend(["Architectural heritage", "Local history"])
    elif "local business" in main_entity_lower:
        topics.extend(
            ["Arts and crafts heritage (e.g. watch-maker)", "Corporate heritage"]
        )
    elif "nature attraction" in main_entity_lower:
        topics.append("Natural heritage (incl. natural reserve, garden, park)")
    elif "museum" in main_entity_lower or "tourist attraction" in main_entity_lower:
        topics.append("Local history")

    return topics


def get_nace_code_mapping(
    main_entity: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    """Map MainEntity to NACE codes."""
    if not main_entity or pd.isna(main_entity):
        return None, None

    main_entity_lower = main_entity.lower()

    if "lodging business" in main_entity_lower:
        return "I. ACCOMMODATION AND FOOD SERVICE ACTIVITIES", "I55. Accommodation"
    elif "food establishment" in main_entity_lower:
        return (
            "I. ACCOMMODATION AND FOOD SERVICE ACTIVITIES",
            "I56. Food and beverage service activities",
        )
    elif any(
        keyword in main_entity_lower
        for keyword in [
            "museum",
            "landmark",
            "historical building",
            "tourist attraction",
        ]
    ):
        return (
            "S. ARTS, SPORTS AND RECREATION",
            "S91. Libraries, archives, museums and other cultural activities",
        )
    elif any(keyword in main_entity_lower for keyword in ["cave or mine", "nature attraction"]):
        return (
            "S. ARTS, SPORTS AND RECREATION",
            "S93. Sports activities and amusement and recreation activities",
        )
    elif "local business" in main_entity_lower:
        return "C. MANUFACTURING", None

    return None, None


def create_stakeholder_entities(
    session: Session, row: pd.Series
) -> Optional[Stakeholder]:
    """Create stakeholder and related entities."""
    name = parse_names_and_identifiers(row.get("Name"))
    if not name:
        return None

    # Parse address
    street_name, house_number, postal_code, city = parse_address(row.get("Address"))

    # Get NUTS code
    nuts_level1 = get_nuts_level1_from_region(row.get("Region"))
    nuts_code = None
    if nuts_level1:
        nuts_code, _ = get_or_create(
            session, NutsCode, {"country_code": "DE", "level_1": nuts_level1}
        )

    # Get NACE code
    nace_level1, nace_level2 = get_nace_code_mapping(row.get("DS"))
    nace_code = None
    if nace_level1:
        nace_code, _ = get_or_create(
            session, NaceCode, {"level_1": nace_level1, "level_2":nace_level2}, 
        )

    # Create stakeholder
    stakeholder_data = {
        "name": name,
        "webpage_url": parse_web_resources(row.get("URL")),
        "description": parse_content(row.get("Description")),
        "street_name": parse_string(street_name),
        "house_number": parse_string(house_number),
        "postal_code": parse_string(postal_code),
        "city": parse_names_and_identifiers(city),
        "country": "DE",
        "contact_phone": parse_string(row.get("Phone")),
        "latitude": parse_float(row.get("latitude")),
        "longitude": parse_float(row.get("longitude")),
        "data_source_type": "Open Data",
        "data_source_name": "Knowledge Graph Tourism Germany",
    }

    if nuts_code:
        stakeholder_data["nuts_code_id"] = nuts_code.id
    if nace_code:
        stakeholder_data["nace_code_id"] = nace_code.id

    stakeholder, _ = get_or_create(
        session, Stakeholder, {"name": name}, **stakeholder_data
    )

    return stakeholder

def create_junctions(session: Session, stakeholder: Stakeholder, row: pd.Series):
    """Create junction tables for stakeholder."""
    main_entity = row.get('DS')  # Changed from 'MainEntity' to 'DS'
    
    # Organization type junction
    org_type_name = get_organization_type_mapping(main_entity)
    if org_type_name:
        org_type, _ = get_or_create(
            session,
            OrganizationType,
            {'name': org_type_name}
        )
        
        get_or_create(
            session,
            JunctionStakeholderOrganizationType,
            {
                'stakeholder_id': stakeholder.id,
                'organization_type_id': org_type.id
            }
        )
    
    # Heritage topics junctions
    heritage_topic_names = get_heritage_topics(main_entity)
    for topic_name in heritage_topic_names:
        heritage_topic, _ = get_or_create(
            session,
            CHTopic,
            {'name': topic_name}
        )
        
        get_or_create(
            session,
            JunctionStakeholderHeritageTopic,
            {
                'stakeholder_id': stakeholder.id,
                'heritage_topic_id': heritage_topic.id
            }
        )


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of tourism records."""
    for _, row in batch_df.iterrows():
        try:
            stakeholder = create_stakeholder_entities(session, row)
            if not stakeholder:
                logging.warning(
                    f"Skipping row with missing name: {row.get('ID', 'Unknown ID')}"
                )
                continue

            session.flush()
            create_junctions(session, stakeholder, row)

        except Exception as e:
            logging.error(f"Error processing row {row.get('Name', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load tourism data from CSV file."""
    logging.info(f"Starting to load data from {file_path}")
    df = pd.read_csv(file_path, sep=";")

    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]

        try:
            with create_db_session()() as session:
                logging.info(
                    f"Processing batch {i//batch_size + 1}: rows {i+1} to {min(i+batch_size, len(df))}"
                )

                process_batch(session, batch_df)
                session.commit()

        except Exception as e:
            logging.error(f"Error in batch {i//batch_size + 1}: {e}")
            session.rollback()


if __name__ == "__main__":
    files = ["Public/DeutschlandTourismus/deutschland_tourismus.csv"]
    source_name = "meta_heritage"
    log_name = "Tourism_Germany"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")