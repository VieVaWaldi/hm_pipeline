import logging
import re
from pathlib import Path
from typing import Optional, Tuple

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


def parse_coordinates(shape_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Extract longitude and latitude from POINT string."""
    if not shape_str:
        return None, None
    
    match = re.search(r'POINT \(([0-9.-]+) ([0-9.-]+)\)', shape_str)
    if match:
        longitude = parse_float(match.group(1))
        latitude = parse_float(match.group(2))
        return longitude, latitude
    
    return None, None


def parse_address(address_str: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse address string into street name and house number."""
    if not address_str:
        return None, None
    
    address_str = address_str.strip()
    
    # Match pattern: "Street Name" followed by numbers with optional separators
    # Handles: "Johannesgasse 8", "Willendorfergasse 2-8", "Mariannengasse 1/13"
    match = re.match(r'^(.+?)\s+(\d+(?:[-/]\d+)*[a-zA-Z]?)(?:\s*/.*)?$', address_str)
    if match:
        street_name = parse_string(match.group(1))
        house_number = parse_string(match.group(2))
        return street_name, house_number
    
    # If no number found, treat entire string as street name
    return parse_string(address_str), None


def create_heritage_topics(session: Session) -> list:
    """Create and return heritage topic entities."""
    heritage_topics = []
    
    topic_names = [
        "Architectural heritage",
        "Local history", 
        "Aristocratic heritage",
        "Industrial heritage"
    ]
    
    for topic_name in topic_names:
        topic, _ = get_or_create(
            session,
            CHTopic,
            unique_key={"name": topic_name},
            is_predefined=True
        )
        heritage_topics.append(topic)
    
    return heritage_topics


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of tourist attraction records."""
    
    # Create shared entities once per batch
    organization_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={"name": "Gallery, Library, Archive, Museum (GLAM)"},
        type_number=1,
        is_predefined=True
    )
    
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={
            "country_code": "AT - Austria",
            "level_1": "AT1 - Ostösterreich",
            "level_2": "AT13 - Wien"
        }
    )
    
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={
            "level_1": "S. Arts, Sports an Recreation",
            "level_2": "S91. Libraries, archives, museums and other cultural activities"
        }
    )
    
    heritage_topics = create_heritage_topics(session)
    
    session.flush()
    
    for _, row in batch_df.iterrows():
        try:
            name = parse_names_and_identifiers(row.get('NAME'))
            if not name:
                logging.warning(f"Skipping row with empty name: {row}")
                continue
            
            # Parse coordinates
            longitude, latitude = parse_coordinates(row.get('SHAPE'))
            
            # Parse address
            street_name, house_number = parse_address(row.get('ADRESSE'))
            
            # Parse additional info URL
            webpage_url = parse_web_resources(row.get('WEITERE_INF'))
            
            # Create stakeholder
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={"name": name},
                ownership="public",
                street_name=street_name,
                house_number=house_number,
                city="Vienna",
                country="AT - Austria",
                longitude=longitude,
                latitude=latitude,
                webpage_url=webpage_url,
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type="Open Data",
                data_source_name="data.gv.at POIs"
            )
            
            session.flush()
            
            # Create junction with organization type
            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "organization_type_id": organization_type.id
                }
            )
            
            # Create junctions with heritage topics
            for heritage_topic in heritage_topics:
                get_or_create(
                    session,
                    JunctionStakeholderHeritageTopic,
                    unique_key={
                        "stakeholder_id": stakeholder.id,
                        "heritage_topic_id": heritage_topic.id
                    }
                )
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('NAME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load tourist attractions data from CSV file."""
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
    files = ["Public/Austria/Austria data.gv.at/Points of Interest Wien/Tourist Attractions.csv"]
    source_name = "meta_heritage"
    log_name = "Vienna_Tourist_Attractions"
    batch_size = 100
    
    setup_logging("meta_heritage", log_name)
    
    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")