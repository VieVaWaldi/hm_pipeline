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
    JunctionStakeholderOrganizationType,
)
from common.log.logger import setup_logging


def extract_coordinates(shape_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Extract latitude and longitude from POINT geometry string."""
    if not shape_str:
        return None, None
    
    # Extract coordinates from POINT (longitude latitude) format
    match = re.search(r'POINT \(([0-9.-]+)\s+([0-9.-]+)\)', shape_str)
    if match:
        longitude = parse_float(match.group(1))
        latitude = parse_float(match.group(2))
        return latitude, longitude
    
    return None, None


def parse_address(address_str: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse address string to extract street name and house number."""
    if not address_str:
        return None, None
    
    # Remove district prefix (e.g., "08., ")
    # Pattern: digits, dot, comma, space
    cleaned_address = re.sub(r'^\d+\.,\s*', '', address_str.strip())
    
    if not cleaned_address:
        return None, None
    
    # Split by space and take last part as house number, rest as street name
    parts = cleaned_address.strip().split()
    if len(parts) == 1:
        # Only street name, no house number
        return parse_string(parts[0]), None
    else:
        # Last part is house number, rest is street name
        house_number = parts[-1]
        street_name = ' '.join(parts[:-1])
        return parse_string(street_name), parse_string(house_number)


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of hotel accommodation records."""
    for _, row in batch_df.iterrows():
        try:
            # Extract coordinates
            latitude, longitude = extract_coordinates(row.get('SHAPE', ''))
            
            # Parse address
            street_name, house_number = parse_address(row.get('ADRESSE', ''))
            
            # Create or get NUTS code
            nuts_code, _ = get_or_create(
                session,
                NutsCode,
                unique_key={'country_code': 'AT', 'level_1': 'Ostösterreich', 'level_2': 'Wien'}
            )
            
            # Create or get NACE code
            nace_code, _ = get_or_create(
                session,
                NaceCode,
                unique_key={'level_1': 'ACCOMMODATION AND FOOD SERVICE ACTIVITIES', 'level_2': 'Accommodation'}
            )
            
            # Create or get organization type
            org_type, _ = get_or_create(
                session,
                OrganizationType,
                unique_key={'name': 'Company or Corporation'},
                type_number=4,
                is_predefined=True
            )
            
            # Flush to get IDs
            session.flush()
            
            # Create stakeholder
            stakeholder_data = {
                'name': parse_names_and_identifiers(row.get('BETRIEB')),
                'street_name': street_name,
                'house_number': house_number,
                'city': 'Vienna',
                'country': 'Austria',
                'contact_email': parse_string(row.get('KONTAKT_EMAIL')),
                'contact_phone': parse_string(row.get('KONTAKT_TEL')),
                'webpage_url': parse_web_resources(row.get('WEBLINK1')),
                'latitude': latitude,
                'longitude': longitude,
                'ownership': 'private',
                'nuts_code_id': nuts_code.id,
                'nace_code_id': nace_code.id,
                'data_source_type': 'Open Data',
                'data_source_name': 'data.gv.at Accomodation Vienna'
            }
            
            # Filter out None values
            stakeholder_data = {k: v for k, v in stakeholder_data.items() if v is not None}
            
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={'name': stakeholder_data['name']},
                **stakeholder_data
            )
            
            # Flush again to get stakeholder ID
            session.flush()
            
            # Create junction for organization type
            junction_org_type = JunctionStakeholderOrganizationType(
                stakeholder_id=stakeholder.id,
                organization_type_id=org_type.id
            )
            session.add(junction_org_type)
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('BETRIEB', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load hotel accommodation data from CSV file."""
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
    files = ["Public/Austria/Austria data.gv.at/Hotels & Accommodation Wien/Accomodation.csv"]
    source_name = "meta_heritage"
    log_name = "Austria_Hotels_Accommodation"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")