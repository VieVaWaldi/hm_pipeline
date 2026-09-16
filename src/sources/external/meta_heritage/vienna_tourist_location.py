import logging
import re
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
from common.sanitizers.parse_primitives import parse_float
from common.sanitizers.parse_text import (
    parse_names_and_identifiers,
    parse_string,
    parse_web_resources,
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


def parse_street_address(street_address: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse street address into street name and house number components."""
    if not street_address:
        return None, None
    
    # Simplified pattern to match street name followed by house number
    pattern = r'^(.+?)\s+(\d+(?:[-/]\d+)*[a-zA-Z]?)(?:\s*/.*)?$'
    match = re.match(pattern, street_address.strip())
    
    if match:
        street_name = parse_string(match.group(1))
        house_number = parse_string(match.group(2))
        return street_name, house_number
    else:
        return parse_string(street_address), None


def get_organization_types(name: str, category: str, subcategory: str, url: str = "") -> List[str]:
    """Determine organization types based on keywords in name, category, subcategory, and URL."""
    text_to_check = f"{name} {category} {subcategory} {url}".lower()
    
    glam_keywords = ["museum", "music", "art", "theatre"]
    company_keywords = ["cafe", "shopping", "restaurant", "hotel", "accommodation", "pension", "apartments", "gaestezimmer"]
    
    if any(keyword in text_to_check for keyword in company_keywords):
        return ["Company or Corporation"]
    elif any(keyword in text_to_check for keyword in glam_keywords):
        return ["Gallery, Library, Archive, Museum (GLAM)"]
    else:
        return ["Gallery, Library, Archive, Museum (GLAM)"]


def get_heritage_topics(name: str, category: str, subcategory: str) -> List[str]:
    """Determine heritage topics based on keywords in name, category, and subcategory."""
    text_to_check = f"{name} {category} {subcategory}".lower()
    topics = ["Local history"]
    
    if any(keyword in text_to_check for keyword in ["cafe"]):
        topics.append("Tea & Coffee culture")
    
    if any(keyword in text_to_check for keyword in ["music", "theatre"]):
        topics.append("Immaterial heritage (e.g. oral history, folk traditions)")
    
    if any(keyword in text_to_check for keyword in ["restaurant", "gaststaette"]):
        topics.append("Culinary heritage")
    
    return topics


def get_nace_code_levels(name: str, category: str, subcategory: str, url: str = "") -> Tuple[str, str]:
    """Determine NACE code levels based on keywords in name, category, subcategory, and URL."""
    text_to_check = f"{name} {category} {subcategory} {url}".lower()
    
    accommodation_keywords = ["hotel", "pension", "appartement", "accommodation", "apartments", "gaestezimmer"]
    restaurant_keywords = ["restaurant", "cafe", "gaststaette"]
    
    if any(keyword in text_to_check for keyword in accommodation_keywords):
        return ("I. ACCOMMODATION AND FOOD SERVICE ACTIVITIES", "I55. Accommodation")
    elif any(keyword in text_to_check for keyword in restaurant_keywords):
        return ("I. ACCOMMODATION AND FOOD SERVICE ACTIVITIES", "I56. Food and beverage service activities")
    else:
        return ("S. Arts, Sports a Recreation", "S91. Libraries, archives, museums and other cultural activities")


def create_stakeholder_entities(session: Session, row: pd.Series) -> Stakeholder:
    """Create stakeholder and related entities from row data."""
    latitude = parse_float(row.get("GEOLAT"))
    longitude = parse_float(row.get("GEOLONG"))
    
    street_name, house_number = parse_street_address(row.get("STREET", ""))
    
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={"country_code": "AT - Austria", "level_1": "AT1 - Oesterreich", "level_2": "AT13 - Wien"}
    )
    
    nace_level_1, nace_level_2 = get_nace_code_levels(
        row.get("NAME", ""), 
        row.get("CATEGORY_NAME", ""), 
        row.get("SUBCATEGORY_NAME", ""),
        row.get("WEBSITE", "")
    )
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={"level_1": nace_level_1, "level_2": nace_level_2}
    )
    
    stakeholder, _ = get_or_create(
        session,
        Stakeholder,
        unique_key={"name": parse_names_and_identifiers(row.get("NAME"))},
        street_name=street_name,
        house_number=house_number,
        postal_code=parse_string(row.get("POSTALCODE")),
        city="Vienna",
        country="AT - Austria",
        contact_email=parse_string(row.get("EMAIL")),
        contact_phone=parse_string(row.get("PHONE")),
        webpage_url=parse_web_resources(row.get("WEBSITE")),
        latitude=latitude,
        longitude=longitude,
        ownership="private",
        nuts_code_id=nuts_code.id,
        nace_code_id=nace_code.id,
        data_source_type="Open Data",
        data_source_name="data.gv.at Touristic Locations"
    )
    
    return stakeholder


def create_organization_type_junctions(session: Session, stakeholder: Stakeholder, row: pd.Series):
    """Create organization type junctions for stakeholder."""
    org_type_names = get_organization_types(
        row.get("NAME", ""), 
        row.get("CATEGORY_NAME", ""), 
        row.get("SUBCATEGORY_NAME", ""),
        row.get("WEBSITE", "")
    )
    
    for org_type_name in org_type_names:
        org_type, _ = get_or_create(
            session,
            OrganizationType,
            unique_key={"name": org_type_name}
        )
        
        get_or_create(
            session,
            JunctionStakeholderOrganizationType,
            unique_key={
                "stakeholder_id": stakeholder.id,
                "organization_type_id": org_type.id
            }
        )


def create_heritage_topic_junctions(session: Session, stakeholder: Stakeholder, row: pd.Series):
    """Create heritage topic junctions for stakeholder."""
    topic_names = get_heritage_topics(
        row.get("NAME", ""), 
        row.get("CATEGORY_NAME", ""), 
        row.get("SUBCATEGORY_NAME", "")
    )
    
    for topic_name in topic_names:
        topic, _ = get_or_create(
            session,
            CHTopic,
            unique_key={"name": topic_name}
        )
        
        get_or_create(
            session,
            JunctionStakeholderHeritageTopic,
            unique_key={
                "stakeholder_id": stakeholder.id,
                "heritage_topic_id": topic.id
            }
        )


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of touristic location records."""
    for _, row in batch_df.iterrows():
        try:
            stakeholder = create_stakeholder_entities(session, row)
            session.flush()
            
            create_organization_type_junctions(session, stakeholder, row)
            create_heritage_topic_junctions(session, stakeholder, row)
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('NAME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load touristic location data from CSV file."""
    logging.info(f"Starting to load data from {file_path}")
    df = pd.read_csv(file_path, sep=",")

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
    files = ["Public/Austria/Austria data.gv.at/Touristic Locations Wien/Touristic locations.csv"]
    source_name = "meta_heritage"
    log_name = "Vienna_Touristic_Locations"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")