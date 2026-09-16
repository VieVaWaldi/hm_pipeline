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
from common.sanitizers.parse_specialized import parse_geolocation
from common.sanitizers.parse_text import (
    parse_names_and_identifiers,
    parse_string,
    parse_web_resources,
    parse_content
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


def parse_address(address: str) -> Tuple[Optional[str], Optional[str]]:
    """Parse address into street name and house number."""
    if not address:
        return None, None
    
    address = parse_string(address)
    if not address:
        return None, None
    
    # Check for comma format: "Street Name, House Number"
    if ',' in address:
        parts = address.split(',', 1)
        street_name = parse_string(parts[0])
        house_number = parse_string(parts[1])
        return street_name, house_number
    
    # Check for space format: "Street Name House Number"
    parts = address.strip().split()
    if len(parts) > 1 and parts[-1][0].isdigit():
        house_number = parse_string(parts[-1])
        street_name = parse_string(' '.join(parts[:-1]))
        return street_name, house_number
    
    # No house number found
    return address, None


def create_nuts_code(session: Session) -> NutsCode:
    """Create or get the NUTS code for Brussels."""
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={
            "country_code": "BE - Belgium",
            "level_1": "B1 - Région de Bruxelles-Capitale/Brussels Hoofdstedelijk Gewest"
        }
    )
    return nuts_code


def create_nace_code(session: Session) -> NaceCode:
    """Create or get the NACE code for cultural activities."""
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={
            "level_1": "S. Arts, Sports an Recreation",
            "level_2": "S91. Libraries, archives, museums and other cultural activities"
        }
    )
    return nace_code


def create_organization_type(session: Session) -> OrganizationType:
    """Create or get the GLAM organization type."""
    org_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={"type_number": 1},
        name="Gallery, Library, Archive, Museum (GLAM)"
    )
    return org_type


def create_heritage_topic(session: Session) -> CHTopic:
    """Create or get the Film and cinematic Heritage topic."""
    heritage_topic, _ = get_or_create(
        session,
        CHTopic,
        unique_key={"name": "Film and cinematic Heritage"},
        is_predefined=False
    )
    return heritage_topic


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of cinema records."""
    # Create shared entities once per batch
    nuts_code = create_nuts_code(session)
    nace_code = create_nace_code(session)
    organization_type = create_organization_type(session)
    heritage_topic = create_heritage_topic(session)
    
    for _, row in batch_df.iterrows():
        try:
            # Parse geolocation
            latitude = None
            longitude = None
            if pd.notna(row.get('geolocalisation')):
                coords = parse_geolocation(str(row['geolocalisation']), swap_lat_lon=True)
                if coords:
                    longitude, latitude = coords
            
            # Parse address
            street_name, house_number = parse_address(row.get('adresse'))
            
            # Create stakeholder
            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={
                    "name": parse_names_and_identifiers(row.get('denomination_du_lieu'))
                },
                description=parse_content(row.get('organisateur_exploitant')),
                webpage_url=parse_web_resources(row.get('site_web')),
                contact_email=parse_string(row.get('e_mail_de_contact')),
                ownership="private",
                street_name=street_name,
                house_number=house_number,
                postal_code=parse_string(row.get('code_postal')),
                city=parse_names_and_identifiers(row.get('ville')),
                country="Belgium",
                latitude=parse_float(latitude) if latitude else None,
                longitude=parse_float(longitude) if longitude else None,
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type="csv",
                data_source_name="data.gov.be Belgium Cinema"
            )
            
            session.flush()
            
            # Create junction with organization type
            get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "organization_type_id": organization_type.id
                }
            )
            
            # Create junction with heritage topic
            get_or_create(
                session,
                JunctionStakeholderHeritageTopic,
                unique_key={
                    "stakeholder_id": stakeholder.id,
                    "heritage_topic_id": heritage_topic.id
                }
            )
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('denomination_du_lieu', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load cinema data from CSV file."""
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
    files = ["Public/Belgium data.gov.be/Cinema Brussels/salles-de-projection-cinema-et-cine-clubs.csv"]
    source_name = "meta_heritage"
    log_name = "Belgium_Cinema_Brussels"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")