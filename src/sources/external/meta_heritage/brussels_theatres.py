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
    parse_string,
    parse_web_resources
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


def parse_address(address_str: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse address string into street name and house number."""
    if not address_str:
        return None, None
    
    # Split by comma to separate street name from house number
    parts = address_str.split(',')
    if len(parts) == 2:
        street_name = parse_string(parts[0].strip())
        house_number = parse_string(parts[1].strip())
        return street_name, house_number
    
    return parse_string(address_str), None


def parse_coordinates(coord_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Parse coordinates string into latitude and longitude."""
    if not coord_str:
        return None, None
    
    try:
        parts = coord_str.split(',')
        if len(parts) == 2:
            lat = parse_float(parts[0].strip())
            lon = parse_float(parts[1].strip())
            return lat, lon
    except Exception:
        pass
    
    return None, None


def create_nuts_code(session: Session) -> NutsCode:
    """Create or get NUTS code for Brussels."""
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={"country_code": "BE - Belgium", "level_1": "B1 - Région de Bruxelles-Capitale/Brussels Hoofdstedelijk Gewest"},
        level_2=None
    )
    return nuts_code


def create_nace_code(session: Session) -> NaceCode:
    """Create or get NACE code for cultural activities."""
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={"level_1": "S. Arts, Sports an Recreation"},
        level_2="S91. Libraries, archives, museums and other cultural activities"
    )
    return nace_code


def create_heritage_topic(session: Session) -> CHTopic:
    """Create or get Immaterial Heritage topic."""
    heritage_topic, _ = get_or_create(
        session,
        CHTopic,
        unique_key={"topic_number": 15},
        name="Immaterial heritage (e.g. oral history, folk traditions)",
        is_predefined=True
    )
    return heritage_topic


def create_organization_type(session: Session) -> OrganizationType:
    """Create or get GLAM organization type."""
    org_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={"type_number": 1},
        name="Gallery, Library, Archive, Museum (GLAM)",
        is_predefined=True
    )
    return org_type


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of theater records."""
    # Create common entities once per batch
    nuts_code = create_nuts_code(session)
    nace_code = create_nace_code(session)
    org_type = create_organization_type(session)
    heritage_topic = create_heritage_topic(session)
    
    session.flush()
    
    for _, row in batch_df.iterrows():
        try:
            # Parse address
            street_name, house_number = parse_address(row.get('adresse'))
            
            # Parse coordinates
            latitude, longitude = parse_coordinates(row.get('coordonnees_geographiques'))
            
            # Create stakeholder
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={"name": parse_names_and_identifiers(row.get('nom'))},
                webpage_url=parse_web_resources(row.get('lien_web')),
                ownership="private",
                street_name=street_name,
                house_number=house_number,
                postal_code=parse_string(row.get('code_postal')),
                city=parse_names_and_identifiers(row.get('gemeente')),
                country="Belgium",
                latitude=latitude,
                longitude=longitude,
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type="Open data",
                data_source_name="data.gov.be Belgium Theater"
            )
            
            session.flush()
            
            # Create junction with organization type
            junction_org_type, _ = get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "organization_type_id": org_type.id
                }
            )
            
            # Create junction with heritage topic
            junction_heritage_topic, _ = get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "heritage_topic_id": heritage_topic.id
                }
            )
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('nom', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load theater data from CSV file."""
    logging.info(f"Starting to load data from {file_path}")
    df = pd.read_csv(file_path, sep=";")
    
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
    files = ["Public/Belgium data.gov.be/Theatre Brussels/bruxelles_theatres.csv"]
    source_name = "meta_heritage"
    log_name = "Belgium_Theaters"
    batch_size = 100
    
    setup_logging("meta_heritage", log_name)
    
    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")