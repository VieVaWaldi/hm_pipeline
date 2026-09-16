import logging
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
from common.sanitizers.parse_primitives import parse_float
from common.sanitizers.parse_text import parse_names_and_identifiers, parse_string, parse_content
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


def parse_address(address: str) -> tuple[str, str]:
    """Parse address into street_name and house_number."""
    if not address:
        return None, None
    
    # Split by comma
    parts = address.split(',', 1)
    if len(parts) == 2:
        street_name = parts[0].strip()
        house_number = parts[1].strip()
        return street_name, house_number
    else:
        # No comma found, return entire address as street_name
        return address.strip(), None


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of theater records."""
    # Create or get shared entities once per batch
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={
            'country_code': 'IT',
            'level_1': 'ITH - Nord-East',
            'level_2': 'ITH5 - Emilia-Romagna'
        }
    )
    
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={
            'level_1': 'S. Arts, Sports an Recreation',
            'level_2': 'S91. Libraries, archives, museums and other cultural activities'
        }
    )
    
    organization_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={'type_number': 1}
    )
    
    heritage_topic, _ = get_or_create(
        session,
        CHTopic,
        unique_key={'topic_number': 15}
    )
    
    for _, row in batch_df.iterrows():
        try:
            # Parse address
            street_name, house_number = parse_address(row.get('INDIRIZZO'))
            
            # Create stakeholder
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={
                    'name': parse_names_and_identifiers(row.get('NOME')),
                    'data_source_name': 'Italy dati.gov.it Theater'
                },
                description=parse_content(row.get('CATEGORIA')),
                ownership='private',
                street_name=parse_string(street_name),
                house_number=parse_string(house_number),
                country='Italy',
                contact_phone=parse_string(row.get('TELEFONO')),
                latitude=parse_float(row.get('Latitudine')),
                longitude=parse_float(row.get('Longitudine')),
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type='government_open_data',
                data_source_name='Italy dati.gov.it Theater'
            )
            
            # Flush to get stakeholder ID
            session.flush()
            
            # Create junction for organization type
            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'organization_type_id': organization_type.id
                }
            )
            
            # Create junction for heritage topic
            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'heritage_topic_id': heritage_topic.id
                }
            )

        except Exception as e:
            logging.error(f"Error processing row {row.get('NOME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load theater data from XLS file."""
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
    files = ["Public/Italy dati.gov.it/Theatre Emilia-Romagna/Theatre.xls"]
    source_name = "meta_heritage"
    log_name = "Italy_Theatre_Emilia_Romagna"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")