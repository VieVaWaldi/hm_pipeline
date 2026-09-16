import logging
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
from common.sanitizers.parse_primitives import parse_float
from common.sanitizers.parse_text import parse_names_and_identifiers, parse_web_resources
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


def extract_house_number(address: str) -> tuple[str, str]:
    """Extract house number from address string after comma."""
    if not address or ',' not in address:
        return address.strip() if address else "", ""
    
    parts = address.split(',', 1)
    street_name = parts[0].strip()
    house_number = parts[1].strip()
    
    return street_name, house_number


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of POI records."""
    for _, row in batch_df.iterrows():
        try:
            # Parse address
            address = parse_names_and_identifiers(row.get('Indirizzo'))
            street_name, house_number = extract_house_number(address) if address else ("", "")
            
            # Create NUTS code
            nuts_code, _ = get_or_create(
                session,
                NutsCode,
                unique_key={
                    'country_code': 'IT - Italy',
                    'level_1': 'ITH - Nord-East',
                    'level_2': 'ITH5 - Emilia-Romagna'
                }
            )
            
            # Create NACE code
            nace_code, _ = get_or_create(
                session,
                NaceCode,
                unique_key={
                    'level_1': 'S. Arts, Sports an Recreation',
                    'level_2': 'S91. Libraries, archives, museums and other cultural activities'
                }
            )
            
            # Create stakeholder
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={'name': parse_names_and_identifiers(row.get('Nome punto interesse'))},
                street_name=street_name if street_name else None,
                house_number=house_number if house_number else None,
                city=parse_names_and_identifiers(row.get('Comune')),
                country='Italy',
                webpage_url=parse_web_resources(row.get('Url')),
                latitude=parse_float(row.get('Latitudine')),
                longitude=parse_float(row.get('Longitudine')),
                ownership='public',
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type='Italian Government Open Data',
                data_source_name='dati.gov.it POIs'
            )
            
            session.flush()
            
            # Create organization type junction
            org_type, _ = get_or_create(
                session,
                OrganizationType,
                unique_key={'type_number': 1}
            )
            
            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'organization_type_id': org_type.id
                }
            )
            
            # Create heritage topic junctions
            architectural_topic, _ = get_or_create(
                session,
                CHTopic,
                unique_key={'topic_number': 4}
            )
            
            religious_topic, _ = get_or_create(
                session,
                CHTopic,
                unique_key={'topic_number': 16}
            )
            
            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'heritage_topic_id': architectural_topic.id
                }
            )
            
            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'heritage_topic_id': religious_topic.id
                }
            )
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('Nome punto interesse', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load POI data from XLS file."""
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
    files = ["Public/Italy dati.gov.it/Points of Interest Emilia-Romagna/POIs.xls"]
    source_name = "meta_heritage"
    log_name = "Italy_POIs_Emilia_Romagna"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")