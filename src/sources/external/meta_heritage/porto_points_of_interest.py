import logging
from pathlib import Path
from typing import Optional

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


def parse_multilingual_field(field_str: str, lang_code: str = 'pt-PT') -> Optional[str]:
    """Parse multilingual field to extract text for specific language."""
    if not field_str or field_str == 'nan':
        return None
    
    try:
        # Look for the language code in the string
        lang_pattern = f"'{lang_code}'"
        if lang_pattern in field_str:
            # Find the value after the language code
            start_idx = field_str.find(lang_pattern)
            value_start = field_str.find("'value':", start_idx)
            if value_start != -1:
                # Find the opening quote for the value
                quote_start = field_str.find("'", value_start + 8)
                if quote_start != -1:
                    # Find the closing quote, accounting for escaped quotes
                    quote_end = quote_start + 1
                    while quote_end < len(field_str):
                        if field_str[quote_end] == "'" and field_str[quote_end-1] != "\\":
                            break
                        quote_end += 1
                    
                    if quote_end < len(field_str):
                        return field_str[quote_start + 1:quote_end]
    except Exception as e:
        logging.warning(f"Error parsing multilingual field: {e}")
    
    return None


def parse_vcard_address(vcard_str: Optional[str]) -> dict:
    """Parse VCARD address string to extract structured address components."""
    address_info = {
        'street_name': None,
        'house_number': None,
        'postal_code': None,
        'city': None,
        'country': None
    }
    
    if not vcard_str or vcard_str == 'nan':
        return address_info
    
    try:
        lines = vcard_str.split('\n')
        for line in lines:
            if line.startswith('ADR'):
                # ADR format: ADR WORK:;;Street;City;Region;PostalCode;Country
                adr_parts = line.split(':')
                if len(adr_parts) > 1:
                    # Use tab or semicolon as separator
                    address_parts = adr_parts[1].replace('\t', ';').split(';')
                    if len(address_parts) >= 4:
                        street = parse_string(address_parts[2]) if len(address_parts) > 2 else None
                        city = parse_string(address_parts[3]) if len(address_parts) > 3 else None
                        postal_code = parse_string(address_parts[5]) if len(address_parts) > 5 else None
                        country = parse_string(address_parts[6]) if len(address_parts) > 6 else None
                        
                        address_info['street_name'] = street
                        address_info['city'] = city or 'Porto'
                        address_info['postal_code'] = postal_code
                        address_info['country'] = country or 'Portugal'
                        break
    except Exception as e:
        logging.warning(f"Error parsing VCARD address: {e}")
    
    return address_info


def create_stakeholder_entities(session: Session, row: pd.Series):
    """Create NUTS code, NACE code entities for a stakeholder."""
    
    # Create NUTS code
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={'country_code': 'PT - Portugal'},
        level_1='PT1 - Continente',
        level_2='PT11 - Norte'
    )
    
    # Create NACE code
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={'level_1': 'S. ARTS, SPORTS AND RECREATION'},
        level_2='S91. Libraries, archives, museums and other cultural activities'
    )
    
    return nuts_code, nace_code


def create_classification_entities(session: Session):
    """Create organization type and heritage topic entities."""
    
    # Create organization type (GLAM)
    org_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={'type_number': 1},
        name='Gallery, Library, Archive, Museum (GLAM)',
        is_predefined=True
    )
    
    # Create heritage topics
    heritage_topics = []
    
    # Local history
    topic_local, _ = get_or_create(
        session,
        CHTopic,
        unique_key={'topic_number': 14},
        name='Local history',
        is_predefined=True
    )
    heritage_topics.append(topic_local)
    
    # Arts and crafts heritage
    topic_arts, _ = get_or_create(
        session,
        CHTopic,
        unique_key={'topic_number': 5},
        name='Arts and crafts heritage (e.g. watch-maker)',
        is_predefined=True
    )
    heritage_topics.append(topic_arts)
    
    # Architectural heritage
    topic_arch, _ = get_or_create(
        session,
        CHTopic,
        unique_key={'topic_number': 4},
        name='Architectural heritage',
        is_predefined=True
    )
    heritage_topics.append(topic_arch)
    
    return org_type, heritage_topics


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of Porto POI records."""
    for _, row in batch_df.iterrows():
        try:
            # Parse name from label field (multilingual JSON-like structure)
            label_str = str(row.get('label', ''))
            stakeholder_name = parse_multilingual_field(label_str, 'pt-PT')
            
            # If no Portuguese name found, try English
            if not stakeholder_name:
                stakeholder_name = parse_multilingual_field(label_str, 'en-GB')
            
            # Fallback to ID if no name found
            if not stakeholder_name:
                stakeholder_name = parse_names_and_identifiers(str(row.get('id', 'Unknown POI')))
            
            if not stakeholder_name:
                logging.warning(f"Skipping row with no valid name: {row.get('id', 'Unknown')}")
                continue
            
            # Parse description from multilingual description field
            description_str = str(row.get('description', ''))
            description = parse_multilingual_field(description_str, 'pt-PT')
            if not description:
                description = parse_multilingual_field(description_str, 'en-GB')
            description = parse_content(description) if description else None
            
            # Parse address from VCARD format
            address_info = parse_vcard_address(str(row.get('address', '')))
            
            # Parse coordinates
            latitude = parse_float(row.get('latitude'))
            longitude = parse_float(row.get('longitude'))
            
            # Parse webpage URL from base field
            webpage_url = parse_web_resources(str(row.get('base', ''))) if pd.notna(row.get('base')) else None
            
            # Create supporting entities
            nuts_code, nace_code = create_stakeholder_entities(session, row)
            org_type, heritage_topics = create_classification_entities(session)
            
            # Create stakeholder
            stakeholder, created = get_or_create(
                session,
                Stakeholder,
                unique_key={'name': stakeholder_name},
                description=description,
                ownership='private',
                street_name=address_info['street_name'],
                house_number=address_info['house_number'],
                postal_code=address_info['postal_code'],
                city='Porto',
                country='Portugal',
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                latitude=latitude,
                longitude=longitude,
                webpage_url=webpage_url,
                data_source_type='Open Data',
                data_source_name='dados.gov.pt Porto POIs'
            )
            
            # Flush to get stakeholder ID
            session.flush()
            
            # Create organization type junction
            org_junction, _ = get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={
                    'stakeholder_id': stakeholder.id,
                    'organization_type_id': org_type.id
                }
            )
            
            # Create heritage topic junctions
            for heritage_topic in heritage_topics:
                heritage_junction, _ = get_or_create(
                    session,
                    JunctionStakeholderHeritageTopic,
                    unique_key={
                        'stakeholder_id': stakeholder.id,
                        'heritage_topic_id': heritage_topic.id
                    }
                )
            
        except Exception as e:
            logging.error(f"Error processing row {row.get('id', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load Porto POI data from CSV file."""
    logging.info(f"Starting to load data from {file_path}")
    
    # Use more robust CSV parsing options
    try:
        df = pd.read_csv(
            file_path, 
            sep=",",
            quotechar="'",
            escapechar="\\",
            on_bad_lines='skip',
            encoding='utf-8'
        )
    except UnicodeDecodeError:
        # Try with different encoding if UTF-8 fails
        df = pd.read_csv(
            file_path, 
            sep=",",
            quotechar="'",
            escapechar="\\",
            on_bad_lines='skip',
            encoding='latin1'
        )

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
    files = [
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in Porto relating to cinemas.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in Porto relating to exhibition centres and art galleries.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in Porto relating to fado houses.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in Porto relating to Statues, Sculptures and Fountains.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in Porto relating to streets and squares.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in Porto relating to theatres.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in the City of Porto relating to Libraries, Archives and Documentation Centres.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in the City of Porto relating to Museums and Theme Centres.csv",
        "Public/Portugal dados.gov.pt/Porto/Points of Interest in the City of Porto relating to the cultural agenda.csv"
    ]
    source_name = "meta_heritage"
    log_name = "Porto_POIs"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed for {file}")