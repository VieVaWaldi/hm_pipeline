import logging
from pathlib import Path
from typing import List, Tuple

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


def parse_address(address_str: str) -> Tuple[str, str]:
    """Parse address into street name and house number."""
    if not address_str:
        return None, None
    
    address_str = address_str.strip()
    
    # Handle format "Street Name, Number" (e.g., "Place Sainte-Catherine, 45")
    if ',' in address_str:
        parts = address_str.rsplit(',', 1)
        if len(parts) == 2:
            street_part = parts[0].strip()
            number_part = parts[1].strip()
            # Verify the number part contains digits
            if any(char.isdigit() for char in number_part):
                return street_part, number_part
    
    # Handle format "Street Name Number" (e.g., "Rue de la Paix 123")
    parts = address_str.rsplit(' ', 1)
    if len(parts) == 2:
        street_part = parts[0].strip()
        number_part = parts[1].strip()
        # Check if last part contains digits
        if any(char.isdigit() for char in number_part):
            return street_part, number_part
    
    # If no clear number found, return entire string as street name
    return address_str, None


def get_heritage_topic_mapping(name: str) -> List[str]:
    """Map museum name to heritage topics."""
    name_lower = name.lower() if name else ""
    topics = ["Local history"]  # Default for all entries
    
    if "coudenberg palace" in name_lower:
        topics.append("Archeological heritage")
    elif "fashion" in name_lower and "lace" in name_lower:
        topics.append("Arts and crafts heritage (e.g. watch-maker)")
    elif "belgian beer world" in name_lower:
        topics.append("Beer brewery culture")
    
    return topics


def create_stakeholder_entities(session: Session, row: pd.Series) -> Stakeholder:
    """Create or get stakeholder and related entities."""
    # Parse address
    street_name, house_number = parse_address(row.get('adresse'))
    
    # Parse coordinates
    coords_str = row.get('coordonnees_geographiques', '')
    latitude = None
    longitude = None
    if coords_str and ',' in coords_str:
        try:
            parts = coords_str.split(',')
            if len(parts) == 2:
                latitude = parse_float(parts[0].strip())
                longitude = parse_float(parts[1].strip())
        except:
            pass
    
    # Create or get NUTS code
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={"country_code": "BE - Belgium", "level_1": "B1 - Région de Bruxelles-Capitale/Brussels Hoofdstedelijk Gewest"},
        level_2=None
    )
    
    # Create or get NACE code
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={"level_1": "S. Arts, Sports an Recreation"},
        level_2="S91. Libraries, archives, museums and other cultural activities"
    )
    
    # Create or get stakeholder
    stakeholder_name = parse_names_and_identifiers(row.get('name'))
    webpage_url = parse_web_resources(row.get('lien_web'))
    city = parse_names_and_identifiers(row.get('gemeente'))
    postal_code = row.get('code_postal')
    
    stakeholder, _ = get_or_create(
        session,
        Stakeholder,
        unique_key={"name": stakeholder_name},
        webpage_url=webpage_url,
        street_name=street_name,
        house_number=house_number,
        postal_code=postal_code,
        city=city,
        country="Belgium",
        latitude=latitude,
        longitude=longitude,
        nuts_code_id=nuts_code.id,
        nace_code_id=nace_code.id,
        data_source_type="Open Data Portal",
        data_source_name="data.gov.be Belgium Museum"
    )
    
    return stakeholder


def create_junctions(session: Session, stakeholder: Stakeholder, row: pd.Series):
    """Create junction relationships for stakeholder."""
    # Organization type junction (GLAM)
    org_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={"name": "Gallery, Library, Archive, Museum (GLAM)"}
    )
    
    junction_org_type, _ = get_or_create(
        session,
        JunctionStakeholderOrganizationType,
        unique_key={
            "stakeholder_id": stakeholder.id,
            "organization_type_id": org_type.id
        }
    )
    
    # Heritage topic junctions
    heritage_topics = get_heritage_topic_mapping(row.get('name'))
    for topic_name in heritage_topics:
        heritage_topic, _ = get_or_create(
            session,
            CHTopic,
            unique_key={"name": topic_name}
        )
        
        junction_heritage, _ = get_or_create(
            session,
            JunctionStakeholderHeritageTopic,
            unique_key={
                "stakeholder_id": stakeholder.id,
                "heritage_topic_id": heritage_topic.id
            }
        )


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of museum records."""
    for _, row in batch_df.iterrows():
        try:
            # Create stakeholder and related entities
            stakeholder = create_stakeholder_entities(session, row)
            
            # Flush to get IDs
            session.flush()
            
            # Create junctions
            create_junctions(session, stakeholder, row)
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('name', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load museum data from CSV file."""
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
    files = ["Public/Belgium data.gov.be/Museum Brussels/bruxelles_musees.csv"]
    source_name = "meta_heritage"
    log_name = "Belgium_Brussels_Museums"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")