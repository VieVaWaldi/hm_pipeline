import logging
import re
from pathlib import Path
from typing import List, Optional

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
    parse_string
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


def extract_address_parts(address: str) -> tuple[Optional[str], Optional[str]]:
    """Extract street name and house number from address string."""
    if not address:
        return None, None
    
    address = address.strip()
    
    # Pattern to match street name and house number
    match = re.match(r'^(.+?)\s+(\d+.*?)$', address)
    if match:
        street_name = match.group(1).strip()
        house_number = match.group(2).strip()
        return street_name, house_number
    
    # If no number found, treat entire string as street name
    return address, None


def get_organization_type_mapping(title: str, category: str, description: str) -> int:
    """Map entry to organization type based on keywords."""
    combined_text = f"{title} {category} {description}".lower()
    
    # Check for cafes and shopping first (Company or Corporation)
    if any(keyword in combined_text for keyword in ['cafe', 'shopping']):
        return 4  # Company or Corporation
    
    # Check for museum, music, art (GLAM)
    if any(keyword in combined_text for keyword in ['museum', 'music', 'art']):
        return 1  # GLAM
    
    # Default to GLAM for all other entries
    return 1  # GLAM


def get_heritage_topics(title: str, category: str, description: str) -> List[int]:
    """Get heritage topic numbers based on keywords."""
    combined_text = f"{title} {category} {description}".lower()
    topics = [14]  # Local history for all entries
    
    # Additional mappings
    if any(keyword in combined_text for keyword in ['cafe']):
        topics.append(12)  # Tea & Coffee culture
    
    if any(keyword in combined_text for keyword in ['music']):
        topics.append(15)  # Immaterial heritage
    
    if any(keyword in combined_text for keyword in ['restaurant', 'gaststätte']):
        topics.append(10)  # Culinary heritage
    
    return list(set(topics))  # Remove duplicates


def create_or_get_nuts_code(session: Session) -> NutsCode:
    """Create or get NUTS code for Vienna."""
    nuts_code, _ = get_or_create(
        session, 
        NutsCode,
        unique_key={'country_code': 'AT - Austria', 'level_1': 'AT1 - Ostösterreich', 'level_2': 'AT13 - Wien'}
    )
    return nuts_code


def create_or_get_nace_code(session: Session) -> NaceCode:
    """Create or get NACE code for cultural activities."""
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={'level_1': 'S. Arts, Sports an Recreation', 'level_2': 'S91. Libraries, archives, museums and other cultural activities'}
    )
    return nace_code


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of Vienna location records."""
    # Create shared NUTS and NACE codes
    nuts_code = create_or_get_nuts_code(session)
    nace_code = create_or_get_nace_code(session)
    session.flush()
    
    for _, row in batch_df.iterrows():
        try:
            # Parse address
            street_name, house_number = extract_address_parts(row.get('address'))
            
            # Create stakeholder
            stakeholder_data = {
                'name': parse_names_and_identifiers(row.get('title')),
                'description': parse_content(row.get('Beschreibung')),
                'ownership': 'public',
                'street_name': parse_string(street_name),
                'house_number': parse_string(house_number),
                'postal_code': parse_string(row.get('zip')),
                'city': 'Vienna',
                'country': 'AT - Austria',
                'contact_phone': parse_string(row.get('tel_1')),
                'contact_email': parse_string(row.get('email')),
                'webpage_url': parse_web_resources(row.get('web_url')),
                'latitude': parse_float(row.get('geo_latitude')),
                'longitude': parse_float(row.get('geo_longitude')),
                'nuts_code_id': nuts_code.id,
                'nace_code_id': nace_code.id,
                'data_source_type': 'Open Data',
                'data_source_name': 'data.gv.at Top Locations Vienna'
            }
            
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={'name': stakeholder_data['name'], 'city': stakeholder_data['city']},
                **stakeholder_data
            )
            
            # Get organization type
            org_type_number = get_organization_type_mapping(
                row.get('title', ''), 
                row.get('category', ''), 
                row.get('Beschreibung', '')
            )
            
            org_type, _ = get_or_create(
                session,
                OrganizationType,
                unique_key={'type_number': org_type_number}
            )
            
            # Get heritage topics
            heritage_topic_numbers = get_heritage_topics(
                row.get('title', ''), 
                row.get('category', ''), 
                row.get('Beschreibung', '')
            )
            
            heritage_topics = []
            for topic_number in heritage_topic_numbers:
                topic, _ = get_or_create(
                    session,
                    CHTopic,
                    unique_key={'topic_number': topic_number}
                )
                heritage_topics.append(topic)
            
            session.flush()
            
            # Create junctions
            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'organization_type_id': org_type.id
                }
            )
            
            for topic in heritage_topics:
                get_or_create(
                    session,
                    JunctionStakeholderHeritageTopic,
                    unique_key={
                        'stakeholder_id': stakeholder.id,
                        'heritage_topic_id': topic.id
                    }
                )
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('title', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load Vienna location data from CSV file."""
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
    files = ["Public/Austria/Austria data.gv.at/Top Locations Wien/top-locations-wien-csv.csv"]
    source_name = "meta_heritage"
    log_name = "Vienna_Top_Locations"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")