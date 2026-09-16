import logging
import re
from pathlib import Path
from typing import Tuple, Optional

import pandas as pd
from pandas import DataFrame
from sqlalchemy.orm import Session

from common.database.postgres.create_db_session import create_db_session
from common.database.postgres.get_or_create import get_or_create, ModelCreationMonitor
from common.file_handling.path_utils import get_source_data_path
from common.sanitizers.parse_primitives import parse_float
from common.sanitizers.parse_text import parse_names_and_identifiers, parse_content
from sources.external.meta_heritage.orm_model import (
    Stakeholder,
    NutsCode,
    OrganizationType,
    CHTopic,
    JunctionStakeholderOrganizationType,
    JunctionStakeholderHeritageTopic,
)
from common.log.logger import setup_logging


def extract_coordinates(shape_str: str) -> Tuple[Optional[float], Optional[float]]:
    """Extract latitude and longitude from POINT string."""
    if not shape_str or not isinstance(shape_str, str):
        return None, None
    
    # Extract coordinates from "POINT (longitude latitude)" format
    match = re.search(r'POINT \(([0-9.-]+)\s+([0-9.-]+)\)', shape_str)
    if match:
        longitude = parse_float(match.group(1))
        latitude = parse_float(match.group(2))
        return latitude, longitude
    
    return None, None


def create_stakeholder_entities(session: Session, row: pd.Series) -> Stakeholder:
    """Create stakeholder and related entities from row data."""
    # Parse coordinates
    latitude, longitude = extract_coordinates(row.get('SHAPE'))
    
    # Create description from KATEGORIE and BESCHREIBUNG
    kategorie = parse_content(row.get('KATEGORIE'))
    beschreibung = parse_content(row.get('BESCHREIBUNG'))
    description_parts = []
    if kategorie:
        description_parts.append(f"Kategorie: {kategorie}")
    if beschreibung:
        description_parts.append(beschreibung)
    description = "\n".join(description_parts) if description_parts else None
    
    # Create NUTS code - ensure every stakeholder gets the same Austrian NUTS code
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={
            'country_code': 'AT',
            'level_1': 'AT1 - Ostösterreich',
            'level_2': 'AT13 - Wien'
        }
    )
    
    # Create stakeholder
    stakeholder_name = parse_names_and_identifiers(row.get('NAME'))
    stakeholder, _ = get_or_create(
        session,
        Stakeholder,
        unique_key={'name': stakeholder_name},
        city=stakeholder_name,
        description=description,
        ownership='private',
        country='AT - Austria',
        latitude=latitude,
        longitude=longitude,
        nuts_code_id=nuts_code.id,
        data_source_type='government_open_data',
        data_source_name='data.gv.at Castles Vienna'
    )
    
    return stakeholder


def create_junction_relationships(session: Session, stakeholder: Stakeholder):
    """Create junction relationships for stakeholder."""
    # Get GLAM organization type
    glam_org_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={'type_number': 1}
    )
    
    # Create organization type junction
    get_or_create(
        session,
        JunctionStakeholderOrganizationType,
        unique_key={
            'stakeholder_id': stakeholder.id,
            'organization_type_id': glam_org_type.id
        }
    )
    
    # Create heritage topic junctions for all three topics
    heritage_topics = [
        {'topic_number': 1},  # Aristocratic heritage
        {'topic_number': 4},  # Architectural heritage
        {'topic_number': 14}  # Local history
    ]
    
    for topic_key in heritage_topics:
        heritage_topic, _ = get_or_create(
            session,
            CHTopic,
            unique_key=topic_key
        )
        
        get_or_create(
            session,
            JunctionStakeholderHeritageTopic,
            unique_key={
                'stakeholder_id': stakeholder.id,
                'heritage_topic_id': heritage_topic.id
            }
        )


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of castle records."""
    for _, row in batch_df.iterrows():
        try:
            # Create stakeholder and related entities
            stakeholder = create_stakeholder_entities(session, row)
            
            # Flush to get the stakeholder ID
            session.flush()
            
            # Create junction relationships
            create_junction_relationships(session, stakeholder)
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('NAME', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load castle data from XLS file."""
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
    files = ["Public/Austria/Austria data.gv.at/Castles Wien/BURGSCHLOSSOGD-csv.xls"]
    source_name = "meta_heritage"
    log_name = "Austrian_Castles_Wien"
    batch_size = 100
    
    setup_logging("meta_heritage", log_name)
    
    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")