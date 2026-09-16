import logging
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
from common.sanitizers.parse_primitives import parse_float
from common.sanitizers.parse_text import parse_names_and_identifiers, parse_string
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


def parse_address(ubicazione: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parse UBICAZIONE field into street_name and house_number."""
    if not ubicazione:
        return None, None
    
    ubicazione_str = parse_string(ubicazione)
    if not ubicazione_str:
        return None, None
    
    # Split by comma to separate street from number
    if ',' in ubicazione_str:
        parts = ubicazione_str.split(',', 1)  # Split only on first comma
        street_name = parse_string(parts[0].strip())
        house_number = parse_string(parts[1].strip())
        return street_name, house_number
    else:
        # No comma found, return as street_name only
        return parse_string(ubicazione_str.strip()), None


def create_predefined_entities(session: Session):
    """Create all predefined entities needed for the data."""
    # Create NUTS code
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        {"country_code": "IT - Italy", "level_1": "ITH - Nord-East", "level_2": "ITH5 - Emilia-Romagna"}
    )
    
    # Create NACE code
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        {"level_1": "S. Arts, Sports an Recreation", "level_2": "S91. Libraries, archives, museums and other cultural activities"}
    )
    
    # Create organization type (GLAM)
    org_type, _ = get_or_create(
        session,
        OrganizationType,
        {"name": "Gallery, Library, Archive, Museum (GLAM)"},
        type_number=1,
        is_predefined=True
    )
    
    # Create heritage topics
    religious_topic, _ = get_or_create(
        session,
        CHTopic,
        {"name": "Religious heritage"},
        topic_number=16,
        is_predefined=True
    )
    
    architectural_topic, _ = get_or_create(
        session,
        CHTopic,
        {"name": "Architectural heritage"},
        topic_number=4,
        is_predefined=True
    )
    
    aristocratic_topic, _ = get_or_create(
        session,
        CHTopic,
        {"name": "Aristocratic heritage"},
        topic_number=1,
        is_predefined=True
    )
    
    return nuts_code, nace_code, org_type, religious_topic, architectural_topic, aristocratic_topic


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of cultural interest place records."""
    # Create all predefined entities
    nuts_code, nace_code, org_type, religious_topic, architectural_topic, aristocratic_topic = create_predefined_entities(session)
    
    for _, row in batch_df.iterrows():
        try:
            # Parse address
            street_name, house_number = parse_address(row.get('UBICAZIONE'))
            
            # Create stakeholder
            stakeholder_data = {
                'name': parse_names_and_identifiers(row.get('NOME')),
                'legal_status': parse_string(row.get('TIPOLOGIA')),
                'ownership': 'mixed',
                'street_name': street_name,
                'house_number': house_number,
                'country': 'Italy',
                'latitude': parse_float(row.get('Latitudine')),
                'longitude': parse_float(row.get('Longitudine')),
                'nuts_code_id': nuts_code.id,
                'nace_code_id': nace_code.id,
                'data_source_type': 'Government Open Data',
                'data_source_name': 'Italy dati.gov.it Cultural  Sites'
            }
            
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                {'name': stakeholder_data['name']},
                **{k: v for k, v in stakeholder_data.items() if k != 'name'}
            )
            
            # Flush to ensure IDs are available
            session.flush()
            
            # Create junction with organization type
            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                {'stakeholder_id': stakeholder.id, 'organization_type_id': org_type.id}
            )
            
            # Create junctions with heritage topics
            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                {'stakeholder_id': stakeholder.id, 'heritage_topic_id': religious_topic.id}
            )
            
            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                {'stakeholder_id': stakeholder.id, 'heritage_topic_id': architectural_topic.id}
            )
            
            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                {'stakeholder_id': stakeholder.id, 'heritage_topic_id': aristocratic_topic.id}
            )

        except Exception as e:
            logging.error(f"Error processing row {row.get('NOME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load cultural interest places data from XLS file."""
    logging.info(f"Starting to load data from {file_path}")
    df = pd.read_excel(file_path, engine='xlrd')

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
    files = ["Public/Italy dati.gov.it/Places of Cultural Interest Emilia-Romagna/Places of Cultural Interest.xls"]
    source_name = "meta_heritage"
    log_name = "Italy_Cultural_Places"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")