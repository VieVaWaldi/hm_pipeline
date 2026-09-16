import logging
from pathlib import Path

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
    parse_titles_and_labels,
)
from sources.external.meta_heritage.orm_model import (
    Stakeholder,
    NutsCode,
    NaceCode,
    OrganizationType,
    JunctionStakeholderOrganizationType,
)
from common.log.logger import setup_logging


def create_nuts_code(session: Session) -> NutsCode:
    """Create or get the NUTS code for Vienna, Austria."""
    nuts_code, _ = get_or_create(
        session,
        NutsCode,
        unique_key={"country_code": "AT - Austria", "level_1": "AT1 - Ostösterreich", "level_2": "AT13 - Wien"}
    )
    return nuts_code


def create_nace_code(session: Session) -> NaceCode:
    """Create or get the NACE code for telecommunications/computer programming."""
    nace_code, _ = get_or_create(
        session,
        NaceCode,
        unique_key={"level_1": "K. TELECOMMUNICATION ACTIVITIES", "level_2": "K62. Computer programming, consultancy and related activities"}
    )
    return nace_code


def create_organization_type(session: Session) -> OrganizationType:
    """Create or get the organization type for Company or Corporation."""
    org_type, _ = get_or_create(
        session,
        OrganizationType,
        unique_key={"type_number": 4},
        name="Company or Corporation"
    )
    return org_type


def process_batch(session: Session, batch_df: DataFrame, nuts_code: NutsCode, nace_code: NaceCode, org_type: OrganizationType):
    """Process a batch of innovative company records."""
    for _, row in batch_df.iterrows():
        try:
            company_name = parse_names_and_identifiers(row.get('Firmenname'))
            if not company_name:
                logging.warning(f"Skipping row with empty company name")
                continue

            website = parse_web_resources(row.get('Website'))
            innovation_title = parse_titles_and_labels(row.get('Titel Innovation'))
            teaser = parse_content(row.get('Teaser für HP'))
            project_status = parse_titles_and_labels(row.get('Projektstatus'))
            funding_institution = parse_names_and_identifiers(row.get('Förderinstitution'))
            long_description = parse_content(row.get('Beschreibung_Lang'))

            description_parts = []
            if innovation_title:
                description_parts.append(f"Innovation: {innovation_title}")
            if teaser:
                description_parts.append(f"Teaser: {teaser}")
            if project_status:
                description_parts.append(f"Project Status: {project_status}")
            if funding_institution:
                description_parts.append(f"Funding Institution: {funding_institution}")
            if long_description:
                description_parts.append(f"Description: {long_description}")
            
            combined_description = "\n\n".join(description_parts) if description_parts else None

            stakeholder, _ = get_or_create(
                session,
                Stakeholder,
                unique_key={"name": company_name},
                webpage_url=website,
                description=combined_description,
                ownership="private",
                city="Vienna",
                country="Austria",
                nuts_code_id=nuts_code.id,
                nace_code_id=nace_code.id,
                data_source_type="CSV",
                data_source_name="data.gv.at Innovative Companies"
            )

            session.flush()

            junction, _ = get_or_create(
                session,
                JunctionStakeholderOrganizationType,
                unique_key={"stakeholder_id": stakeholder.id, "organization_type_id": org_type.id}
            )

        except Exception as e:
            logging.error(f"Error processing row {row.get('Firmenname', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load innovative companies data from CSV file."""
    logging.info(f"Starting to load data from {file_path}")
    
    df = pd.read_csv(file_path, sep=",", encoding='utf-8')
    
    with create_db_session()() as session:
        nuts_code = create_nuts_code(session)
        nace_code = create_nace_code(session)
        org_type = create_organization_type(session)
        session.flush()

    for i in range(0, len(df), batch_size):
        batch_df = df.iloc[i : i + batch_size]

        try:
            with create_db_session()() as session:
                nuts_code = create_nuts_code(session)
                nace_code = create_nace_code(session)
                org_type = create_organization_type(session)
                
                logging.info(
                    f"Processing batch {i//batch_size + 1}: rows {i+1} to {min(i+batch_size, len(df))}"
                )

                process_batch(session, batch_df, nuts_code, nace_code, org_type)
                session.commit()

        except Exception as e:
            logging.error(f"Error in batch {i//batch_size + 1}: {e}")
            session.rollback()


if __name__ == "__main__":
    files = ["Public/Austria/Austria data.gv.at/Innovative companies Wien/Innovative companies.csv"]
    source_name = "meta_heritage"
    log_name = "Austria_Innovative_Companies"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")