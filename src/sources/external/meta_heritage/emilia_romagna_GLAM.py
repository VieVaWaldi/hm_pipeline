import logging
from pathlib import Path
from typing import List

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


def create_stakeholder(session: Session, row: pd.Series) -> Stakeholder:
    """Create a stakeholder from a row of data."""
    # Determine city - use localita if different from comune, otherwise use comune
    city = parse_names_and_identifiers(row.get('localita'))
    if not city or city == parse_names_and_identifiers(row.get('comune')):
        city = parse_names_and_identifiers(row.get('comune'))
    
    stakeholder_data = {
        'name': parse_names_and_identifiers(row.get('titolo')),
        'description': parse_content(row.get('descrizione')),
        'webpage_url': parse_web_resources(row.get('url scheda')),
        'ownership': 'public',
        'street_name': parse_string(row.get('indirizzo')),
        'postal_code': parse_string(row.get('ISTATcomune')),
        'city': city,
        'country': 'Italy',
        'latitude': parse_float(row.get('latitudine')),
        'longitude': parse_float(row.get('longitudine')),
        'data_source_type': 'Open Data',
        'data_source_name': 'Italy dati.gov.it GLAM'
    }
    
    # Filter out None values for unique key
    unique_key = {}
    if stakeholder_data['name']:
        unique_key['name'] = stakeholder_data['name']
    if stakeholder_data['city']:
        unique_key['city'] = stakeholder_data['city']
    
    stakeholder, created = get_or_create(
        session, 
        Stakeholder, 
        unique_key, 
        **{k: v for k, v in stakeholder_data.items() if v is not None}
    )
    
    return stakeholder


def create_nuts_code(session: Session) -> NutsCode:
    """Create or get the NUTS code for Emilia-Romagna."""
    nuts_data = {
        'country_code': 'IT - Italy',
        'level_1': 'ITH - Nord-East',
        'level_2': 'ITH5 - Emilia-Romagna'
    }
    
    nuts_code, created = get_or_create(
        session,
        NutsCode,
        nuts_data
    )
    
    return nuts_code


def create_nace_code(session: Session) -> NaceCode:
    """Create or get the NACE code for cultural activities."""
    nace_data = {
        'level_1': 'S. Arts, Sports an Recreation',
        'level_2': 'S91. Libraries, archives, museums and other cultural activities'
    }
    
    nace_code, created = get_or_create(
        session,
        NaceCode,
        nace_data
    )
    
    return nace_code


def get_organization_type(session: Session) -> OrganizationType:
    """Get the GLAM organization type."""
    org_type, created = get_or_create(
        session,
        OrganizationType,
        {'name': 'Gallery, Library, Archive, Museum (GLAM)'}
    )
    
    return org_type


def get_heritage_topics(session: Session) -> List[CHTopic]:
    """Get all heritage topics for GLAM entries."""
    topic_names = [
        'Architectural heritage',
        'Local history', 
        'Religious heritage',
        'Aristocratic heritage',
        'Industrial heritage',
        'Archeological heritage',
        'Automotive heritage'
    ]
    
    topics = []
    for topic_name in topic_names:
        topic, created = get_or_create(
            session,
            CHTopic,
            {'name': topic_name}
        )
        topics.append(topic)
    
    return topics


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of GLAM records."""
    # Create shared entities once per batch
    nuts_code = create_nuts_code(session)
    nace_code = create_nace_code(session)
    organization_type = get_organization_type(session)
    heritage_topics = get_heritage_topics(session)
    
    for _, row in batch_df.iterrows():
        try:
            # Create stakeholder
            stakeholder = create_stakeholder(session, row)
            
            # Set NUTS and NACE codes
            stakeholder.nuts_code = nuts_code
            stakeholder.nace_code = nace_code
            
            # Flush to get stakeholder ID
            session.flush()
            
            # Create organization type junction
            org_junction = JunctionStakeholderOrganizationType(
                stakeholder_id=stakeholder.id,
                organization_type_id=organization_type.id
            )
            session.add(org_junction)
            
            # Create heritage topic junctions
            for topic in heritage_topics:
                topic_junction = JunctionStakeholderHeritageTopic(
                    stakeholder_id=stakeholder.id,
                    heritage_topic_id=topic.id
                )
                session.add(topic_junction)
                
        except Exception as e:
            logging.error(f"Error processing row {row.get('titolo', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load GLAM data from XLS file."""
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
    files = ["Public/Italy dati.gov.it/GLAM of historical-artistic interest Emilia Romagna/GLAM.xls"]
    source_name = "meta_heritage"
    log_name = "Italy_GLAM_Emilia_Romagna"
    batch_size = 100
    
    setup_logging("meta_heritage", log_name)
    
    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")
