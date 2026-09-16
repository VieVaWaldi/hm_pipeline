import logging
from pathlib import Path
from typing import Optional

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
from common.sanitizers.parse_text import (
    parse_names_and_identifiers,
    parse_content,
    parse_web_resources,
    parse_string
)
from sources.external.meta_heritage.orm_model import (
    Stakeholder,
    OrganizationType,
    JunctionStakeholderOrganizationType,
)
from common.log.logger import setup_logging


def get_organization_type_mapping(org_type_str: Optional[str]) -> Optional[str]:
    """Map organization type from CSV to predefined values."""
    if not org_type_str:
        return None
    
    org_type_str = org_type_str.strip()
    
    mapping = {
        'Company (SME)': 'Company or Corporation',
        'Academia / R&D Institution': 'Research institution (e.g., university)',
        'Start-up': 'Company or Corporation',
        'NGO': 'Non-Profit Organisation (e.g., Charity, NGO, Foundation)',
        'Cultural institution (galleries, libraries, archives, museums)': 'Gallery, Library, Archive, Museum (GLAM)',
        'Authority/Government': 'Public Sector Organisation (e.g. Government agency, Municipal authority)',
        'Association/Agency': 'Association',
        'Other': 'Company or Corporation'
    }
    
    return mapping.get(org_type_str, 'Company or Corporation')


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of B2B event participant records."""
    for _, row in batch_df.iterrows():
        try:
            # Create stakeholder
            stakeholder_data = {
                'name': parse_names_and_identifiers(row.get('Organization Name')),
                'description': parse_content(row.get('Organization Description')),
                'webpage_url': parse_web_resources(row.get('Organisation Website')),
                'contact_firstname': parse_names_and_identifiers(row.get('First Name')),
                'contact_surname': parse_names_and_identifiers(row.get('Last Name')),
                'contact_email': parse_string(row.get('Email')),
                'contact_phone': parse_string(row.get('Phone')),
                'city': parse_names_and_identifiers(row.get('City')),
                'country': parse_names_and_identifiers(row.get('Country')),
                'data_source_type': 'private data',
                'data_source_name': 'B2B Event participants'
            }
            
            # Filter out None values
            stakeholder_data = {k: v for k, v in stakeholder_data.items() if v is not None}
            
            # Skip if no name
            if not stakeholder_data.get('name'):
                logging.warning(f"Skipping row with no organization name: {row.get('Email', 'Unknown')}")
                continue
            
            # Create or get stakeholder
            stakeholder, created = get_or_create(
                session,
                Stakeholder,
                {'name': stakeholder_data['name']},
                **stakeholder_data
            )
            
            session.flush()
            
            # Create organization type junction
            org_type_name = get_organization_type_mapping(row.get('Organization Type'))
            if org_type_name:
                org_type, _ = get_or_create(
                    session,
                    OrganizationType,
                    {'name': org_type_name}
                )
                
                session.flush()
                
                # Create junction
                get_or_create(
                    session,
                    JunctionStakeholderOrganizationType,
                    {
                        'stakeholder_id': stakeholder.id,
                        'organization_type_id': org_type.id
                    }
                )
                
        except Exception as e:
            logging.error(f"Error processing row {row.get('Organization Name', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load B2B event participant data from Excel file."""
    logging.info(f"Starting to load data from {file_path}")
    
    # Read Excel file
    df = pd.read_excel(file_path, engine='openpyxl')
    
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
    files = ["Private/B2B Event 2025_Participants.xlsx"]
    source_name = "meta_heritage"
    log_name = "B2B_Event_Participants"
    batch_size = 100
    
    setup_logging("meta_heritage", log_name)
    
    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")