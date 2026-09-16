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


def extract_coordinates_from_shape(shape_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Extract longitude and latitude from POINT string."""
    if not shape_str or not isinstance(shape_str, str):
        return None, None
    
    # Extract coordinates from POINT (longitude latitude) format
    match = re.search(r'POINT \(([0-9.-]+)\s+([0-9.-]+)\)', shape_str)
    if match:
        longitude = parse_float(match.group(1))
        latitude = parse_float(match.group(2))
        return longitude, latitude
    
    return None, None


def parse_vienna_address(address_str: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse Vienna address format."""
    if not address_str or not isinstance(address_str, str):
        return None, None
    
    # Clean up encoding issues
    address_clean = address_str.replace('√§', 'ä').replace('√∂', 'ö').replace('√º', 'ü')
    
    # Remove district prefix
    address_clean = re.sub(r'^\d+\.,?\s*', '', address_clean)
    
    # Match street name and house number
    match = re.search(r'^(.+?)\s+(\d+(?:[-/]\d+)*[a-zA-Z]?)(?:\s*/.*)?$', address_clean)
    if match:
        street_name = match.group(1).strip()
        house_number = match.group(2).strip()
        return street_name, house_number
    
    return address_clean.strip(), None


def create_or_get_nuts_code(session: Session) -> NutsCode:
    """Create or get the NUTS code for Vienna."""
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={"country_code": "AT", "level_1": "AT1 - Ostösterreich", "level_2": "AT13 - Wien"}
    )
    return nuts_code


def create_or_get_nace_code(session: Session) -> NaceCode:
    """Create or get the NACE code for museums."""
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={"level_1": "S. Arts, Sports an Recreation", "level_2": "S91. Libraries, archives, museums and other cultural activities"}
    )
    return nace_code


def create_or_get_organization_type(session: Session) -> OrganizationType:
    """Get the GLAM organization type."""
    org_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={"type_number": 1}
    )
    return org_type


def create_or_get_heritage_topics(session: Session) -> list[CHTopic]:
    """Get all relevant heritage topics for museums."""
    topic_numbers = [14, 4, 1, 5, 8]  # Local history, Architectural, Aristocratic, Arts and crafts, Industrial
    topics = []
    
    for topic_number in topic_numbers:
        topic, _ = get_or_create(
            session,
            CHTopic,
            unique_key={"topic_number": topic_number}
        )
        topics.append(topic)
    
    return topics


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of museum records."""
    # Pre-create common entities
    nuts_code = create_or_get_nuts_code(session)
    nace_code = create_or_get_nace_code(session)
    organization_type = create_or_get_organization_type(session)
    heritage_topics = create_or_get_heritage_topics(session)
    
    session.flush()
    
    for _, row in batch_df.iterrows():
        try:
            # Extract coordinates
            longitude, latitude = extract_coordinates_from_shape(row.get('SHAPE'))
            
            # Parse address
            street_name, house_number = parse_vienna_address(row.get('ADRESSE'))
            
            # Create stakeholder
            stakeholder_data = {
                'name': parse_names_and_identifiers(row.get('NAME')),
                'ownership': 'public',
                'street_name': parse_string(street_name),
                'house_number': parse_string(house_number),
                'city': 'Vienna',
                'country': 'Austria',
                'latitude': latitude,
                'longitude': longitude,
                'webpage_url': parse_web_resources(row.get('WEITERE_INF')),
                'nuts_code_id': nuts_code.id,
                'nace_code_id': nace_code.id,
                'data_source_type': 'government_open_data',
                'data_source_name': 'data.gv.at Museums Vienna'
            }
            
            # Remove None values
            stakeholder_data = {k: v for k, v in stakeholder_data.items() if v is not None}
            
            stakeholder, created = get_or_create(
                session,
                Stakeholder,
                unique_key={'name': stakeholder_data['name']},
                **{k: v for k, v in stakeholder_data.items() if k != 'name'}
            )
            
            session.flush()
            
            # Create organization type junction
            org_junction, _ = get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'organization_type_id': organization_type.id
                }
            )
            
            # Create heritage topic junctions
            for topic in heritage_topics:
                topic_junction, _ = get_or_create(
                    session,
                    JunctionStakeholderHeritageTopic,
                    unique_key={
                        'stakeholder_id': stakeholder.id,
                        'heritage_topic_id': topic.id
                    }
                )

        except Exception as e:
            logging.error(f"Error processing row {row.get('NAME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load museum data from CSV file."""
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
    files = ["Public/Austria/Austria data.gv.at/Museums Wien/Museums.csv"]
    source_name = "meta_heritage"
    log_name = "Austria_Museums_Wien"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")