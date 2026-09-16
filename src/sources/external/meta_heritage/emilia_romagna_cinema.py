import logging
from pathlib import Path
from typing import Tuple, Optional

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
    Network,
    CHTopic,
    JunctionStakeholderOrganizationType,
    JunctionStakeholderNetworkMembership,
    JunctionStakeholderHeritageTopic,
)
from common.log.logger import setup_logging


def parse_address(address_str: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse address string to extract street name and house number."""
    if not address_str:
        return None, None
    
    address_str = address_str.strip()
    
    # Try splitting by comma first
    if ',' in address_str:
        parts = address_str.split(',', 1)
        street_name = parse_string(parts[0])
        house_number = parse_string(parts[1])
        return street_name, house_number
    
    # Try splitting by single quote
    if "'" in address_str:
        parts = address_str.split("'", 1)
        street_name = parse_string(parts[0])
        house_number = parse_string(parts[1])
        return street_name, house_number
    
    # If no separator found, extract any numbers for house_number
    import re
    numbers = re.findall(r'\d+', address_str)
    if numbers:
        # Extract the last number as house number
        last_number = numbers[-1]
        # Remove the number from the string for street name
        street_name = re.sub(rf'\b{re.escape(last_number)}\b', '', address_str).strip()
        street_name = parse_string(street_name)
        house_number = parse_string(last_number)
        return street_name, house_number
    
    # If no numbers found, treat entire string as street name
    return parse_string(address_str), None


def determine_ownership(name: str) -> str:
    """Determine ownership based on cinema name."""
    if not name:
        return "private"
    
    name_upper = name.upper()
    
    if any(keyword in name_upper for keyword in ["ALADDIN", "ASTRA", "ELISEO"]):
        return "private"
    elif any(keyword in name_upper for keyword in ["ARENA SAN BIAGIO", "SAN BIAGIO"]):
        return "public"
    elif any(keyword in name_upper for keyword in ["BOGART", "VICTOR"]):
        return "mixed"
    else:
        return "private"


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of cinema records."""
    
    # Get or create required reference entities
    nuts_code, _ = get_or_create(
        session, 
        NutsCode,
        unique_key={"country_code": "IT", "level_1": "ITH - Nord-East", "level_2": "ITH5 - Emilia-Romagna"}
    )
    
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={"level_1": "S. Arts, Sports an Recreation", "level_2": "S91. Libraries, archives, museums and other cultural activities"}
    )
    
    organization_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={"name": "Gallery, Library, Archive, Museum (GLAM)"},
        type_number=1
    )
    
    heritage_topic, _ = get_or_create(
        session,
        CHTopic,
        unique_key={"name": "Film and cinematic Heritage"},
        is_predefined=False
    )
    
    session.flush()
    
    for _, row in batch_df.iterrows():
        try:
            # Parse address
            street_name, house_number = parse_address(row.get('INDIRIZZO'))
            
            # Determine ownership
            ownership = determine_ownership(row.get('NOME'))
            
            # Parse coordinates
            latitude = parse_float(row.get('Latitudine'))
            longitude = parse_float(row.get('Longitudine'))
            
            # Create description from manager/owner info
            description_parts = []
            if pd.notna(row.get('GESTOREPROPRIETA')):
                description_parts.append(f"Manager/Owner: {row.get('GESTOREPROPRIETA')}")
            if pd.notna(row.get('TIPON_SCHERMI')):
                description_parts.append(f"Type: {row.get('TIPON_SCHERMI')}")
            if pd.notna(row.get('MI_SCHERMI')):
                description_parts.append(f"Screens: {row.get('MI_SCHERMI')}")
            if pd.notna(row.get('TOT_POSTI')):
                description_parts.append(f"Total seats: {row.get('TOT_POSTI')}")
            
            description = parse_content(" | ".join(description_parts)) if description_parts else None
            
            # Create stakeholder
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={"name": parse_names_and_identifiers(row.get('NOME'))},
                street_name=street_name,
                house_number=house_number,
                city="Cesena",
                country="Italy",
                contact_phone=parse_string(row.get('TELEFONO')),
                contact_email=parse_string(row.get('EMAIL')),
                webpage_url=parse_web_resources(row.get('SITO_WEB')),
                description=description,
                ownership=ownership,
                latitude=latitude,
                longitude=longitude,
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type="government_open_data",
                data_source_name="dati.gov.it Cinema"
            )
            
            session.flush()
            
            # Create organization type junction
            org_type_junction, _ = get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "organization_type_id": organization_type.id
                }
            )
            
            # Create heritage topic junction
            heritage_topic_junction, _ = get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "heritage_topic_id": heritage_topic.id
                }
            )
            
            # Handle network membership if association exists
            association = row.get('ASSOCIAZIO')
            if pd.notna(association) and association.strip():
                network, _ = get_or_create(
                    session,
                    Network,
                    unique_key={"name": parse_names_and_identifiers(association)},
                    is_predefined=False
                )
                
                session.flush()
                
                network_junction, _ = get_or_create(
                    session,
                    JunctionStakeholderNetworkMembership,
                    unique_key={
                        "stakeholder_id": stakeholder.id,
                        "network_id": network.id
                    }
                )

        except Exception as e:
            logging.error(f"Error processing row {row.get('NOME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load cinema data from Excel file."""
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
    files = ["Public/Italy dati.gov.it/Cinema Emilia-Romagna/List and location of cinemas in the Municipality of Cesena.xls"]
    source_name = "meta_heritage"
    log_name = "Italy_Cinemas_Cesena"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")