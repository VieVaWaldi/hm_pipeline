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
    parse_web_resources,
    parse_string,
)
from sources.external.meta_heritage.orm_model import (
    Stakeholder,
    OrganizationType,
    JunctionStakeholderOrganizationType,
)
from common.log.logger import setup_logging


def get_organization_type_mapping(industry: str) -> Optional[str]:
    """Map industry values to organization type names."""
    mapping = {
        "Academic and Research Institution": "Research institution (e.g., university)",
        "Associated Programme or Network": "Business Network incl. Chamber of Commerce",
        "Civil Society Organisation": "Non-Profit Organisation (e.g., Charity, NGO, Foundation)",
        "GLAM": "Gallery, Library, Archive, Museum (GLAM)",
        "Private or Industrial Sector": "Company or Corporation",
        "Public Sector Organisation": "Public Sector Organisation (e.g. Government agency, Municipal authority)",
        "Other": "Company or Corporation",
        "Research and advocacy centre (e.g., ThinkTank)": "Research and advocacy centre (e.g., ThinkTank)",
    }
    return mapping.get(industry)


def process_batch(session: Session, batch_df: DataFrame):
    """Process a batch of TMO member records."""
    for _, row in batch_df.iterrows():
        try:
            # Parse and sanitize data
            name = parse_names_and_identifiers(row.get("Institution Name"))
            city = parse_string(row.get("Billing City"))
            country = parse_string(row.get("Billing Country (text only)"))
            webpage_url = parse_web_resources(row.get("Website"))
            industry = parse_string(row.get("Industry"))

            if not name:
                logging.warning(f"Skipping row with empty name: {row.to_dict()}")
                continue

            # Create or get stakeholder
            stakeholder, created = get_or_create(
                session=session,
                model=Stakeholder,
                unique_key={"name": name},
                city=city,
                country=country,
                webpage_url=webpage_url,
                data_source_type="private data",
                data_source_name="TMO members",
            )

            # Create organization type if industry is provided
            if industry:
                org_type_name = get_organization_type_mapping(industry)
                if org_type_name:
                    org_type, _ = get_or_create(
                        session=session,
                        model=OrganizationType,
                        unique_key={"name": org_type_name},
                    )
                    
                    session.flush()
                    
                    # Create junction
                    get_or_create(
                        session=session,
                        model=JunctionStakeholderOrganizationType,
                        unique_key={
                            "stakeholder_id": stakeholder.id,
                            "organization_type_id": org_type.id,
                        },
                    )
                else:
                    logging.warning(f"Unknown industry type: {industry}")

        except Exception as e:
            logging.error(f"Error processing row {row.get('Institution Name', 'Unknown')}: {e}")
            continue


def run_loader(file_path: Path, batch_size: int):
    """Load TMO members data from CSV file."""
    logging.info(f"Starting to load data from {file_path}")
    df = pd.read_csv(file_path, sep=";", encoding="latin-1")

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
    files = ["Private/TMO-members 2025-01-23 export_ISO.csv"]
    source_name = "meta_heritage"
    log_name = "TMO_Members"
    batch_size = 100

    setup_logging("meta_heritage", log_name)

    for file in files:
        file_path = get_source_data_path(source_name, None) / file
        run_loader(file_path, batch_size)
        ModelCreationMonitor.log_stats()
        logging.info(f"Loading completed.")