import logging
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
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


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of wine tourism records."""
    for _, row in batch_df.iterrows():
        try:
            # Parse data from CSV
            name = parse_names_and_identifiers(row.get('Nome'))
            website = parse_web_resources(row.get('Website'))
            
            if not name:
                logging.warning(f"Skipping row with missing name: {row}")
                continue

            # Create or get NUTS code
            nuts_code, _ = get_or_create(
                session,
                NutsCode,
                unique_key={'country_code': 'PT - Portugal', 'level_1': 'PT1 - Continente', 'level_2': 'PT11 - Norte'}
            )

            # Create or get NACE code
            nace_code, _ = get_or_create(
                session,
                NaceCode,
                unique_key={'level_1': 'I. ACCOMMODATION AND FOOD SERVICE ACTIVITIES', 'level_2': 'I56. Food and beverage service activities'}
            )

            # Create stakeholder
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={'name': name},
                webpage_url=website,
                ownership='private',
                city='Porto',
                country='Portugal',
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type='Open Data',
                data_source_name='dados.gov.pt Porto Wine Tourism'
            )

            session.flush()

            # Get organization type: Company or Corporation (4)
            organization_type, _ = get_or_create(
                session,
                OrganizationType,
                unique_key={'type_number': 4}
            )

            # Get heritage topic: Viticulture (13)
            heritage_topic, _ = get_or_create(
                session,
                CHTopic,
                unique_key={'topic_number': 13}
            )

            # Create junctions
            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={'stakeholder_id': stakeholder.id, 'organization_type_id': organization_type.id}
            )

            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={'stakeholder_id': stakeholder.id, 'heritage_topic_id': heritage_topic.id}
            )

        except Exception as e:
            logging.error(f"Error processing row {row.get('Nome', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load wine tourism data from CSV file."""
    logging.info(f"Starting to load data from {file_path}")
    
    # Try different encodings to handle the file
    encodings = ['utf-8', 'latin-1', 'iso-8859-1', 'cp1252']
    df = None
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, sep=";", encoding=encoding)
            logging.info(f"Successfully read file with {encoding} encoding")
            break
        except UnicodeDecodeError:
            continue
    
    if df is None:
        raise ValueError(f"Could not read file {file_path} with any of the tried encodings: {encodings}")

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
    files = ["Public/Portugal dados.gov.pt/Porto/Entities honoured by the Best of Wine Tourism award, belonging to the Best of Club - Porto group.csv"]
    source_name = "meta_heritage"
    log_name = "Porto_Wine_Tourism"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")
        