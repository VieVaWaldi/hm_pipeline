from typing import Dict, List, Tuple, Optional

from sqlalchemy.orm import Session

from enrichment.ocr.pdf_ocr_reader import pdf_to_text
from common.api_runner.run_loader import ILoader
from common.database.postgres.get_or_create import get_or_create
from common.file_handling.dict_utils import ensure_list, get_nested
from common.sanitizers.parse_primitives import (
    parse_date,
)
from common.sanitizers.parse_text import (
    parse_string,
    parse_titles_and_labels,
    parse_content,
    parse_names_and_identifiers,
    parse_web_resources,
)
from sources.apis.arxiv.orm_model import (
    Entry,
    Author,
    Link,
    JunctionEntryAuthor,
    JunctionEntryLink,
)


class ArxivLoader(ILoader):
    """
    Data loading for Arxiv documents.
    Transforms JSON documents into SQLAlchemy ORM models and adds them to the session.
    """

    def __init__(self, path_to_file=None):
        super().__init__(path_to_file)

    def load(self, session: Session, document: Dict):
        assert (
            isinstance(document, dict) and document
        ), "document must be a non-empty dictionary"

        entry_data = document.get("ns0:entry", {})
        if not entry_data:
            return

        entry, created_entry = self._create_entry(session, entry_data)
        if not created_entry:
            return

        authors = self._create_authors(session, entry_data)
        links = self._create_links(session, entry_data)

        # Flush the session to get all entity IDs
        session.flush()

        self._create_entry_authors(session, entry, authors)
        self._create_entry_links(session, entry, links)

    def _create_entry(
        self, session: Session, entry_data: Dict
    ) -> Tuple[Optional[Entry], bool]:
        """Create or retrieve an Entry entity from the document."""

        id_original = parse_string(get_nested(entry_data, "ns0:id"))
        if not id_original:
            return None, False

        title = parse_titles_and_labels(get_nested(entry_data, "ns0:title"))
        if not title:
            return None, False

        doi = parse_string(get_nested(entry_data, "ns1:doi"))
        summary = parse_content(get_nested(entry_data, "ns0:summary"))
        published_date = parse_date(get_nested(entry_data, "ns0:published"))
        updated_date = parse_date(get_nested(entry_data, "ns0:updated"))
        journal_ref = parse_string(get_nested(entry_data, "ns1:journal_ref"))
        comment = parse_string(get_nested(entry_data, "ns1:comment"))
        primary_category = parse_string(
            get_nested(entry_data, "ns1:primary_category.@term")
        )
        category_term = parse_string(get_nested(entry_data, "category_term"))

        categories = self._extract_categories(entry_data)
        full_text = self._extract_full_text()

        entry, created = get_or_create(
            session,
            Entry,
            {"title": title},
            id_original=id_original,
            doi=doi,
            summary=summary,
            full_text=full_text,
            journal_ref=journal_ref,
            comment=comment,
            primary_category=primary_category,
            category_term=category_term,
            categories=categories,
            published_date=published_date,
            updated_date=updated_date,
        )

        return entry, created

    def _extract_categories(self, entry_data: Dict) -> List[str]:
        """Extract all categories from the document as a list."""
        categories_data = ensure_list(get_nested(entry_data, "ns0:category"))

        categories = []
        for category in categories_data:
            if isinstance(category, dict):
                term = parse_string(category.get("@term"))
                if term:
                    categories.append(term)

        return categories

    def _extract_full_text(self):
        directory = self.path_to_file.parent
        pdf_files = list(directory.glob("*.pdf"))
        pdf_count = len(pdf_files)

        if pdf_count > 1:
            raise ValueError(
                f"Found {pdf_count} PDF files in {directory}, expected at most 1. CUZ I SAY SO right now, lets see"
            )
        elif pdf_count == 1:
            pdf_path = pdf_files[0].absolute()
            full_text = pdf_to_text(pdf_path)
            return parse_content(full_text)

    def _create_authors(
        self, session: Session, entry_data: Dict
    ) -> List[Tuple[Author, int]]:
        """Create or retrieve Author entities and return with their positions."""
        authors_with_positions = []
        seen_authors = set()

        author_list = ensure_list(get_nested(entry_data, "ns0:author"))
        for position, author_data in enumerate(author_list):
            if not isinstance(author_data, dict):
                continue

            author_name = parse_names_and_identifiers(
                get_nested(author_data, "ns0:name")
            )
            if not author_name or author_name in seen_authors:
                continue
            seen_authors.add(author_name)

            author_affiliation = parse_names_and_identifiers(
                get_nested(author_data, "ns1:affiliation")
            )
            author_affiliations = self._extract_affiliations(author_data)

            author, _ = get_or_create(
                session,
                Author,
                {"name": author_name},
                affiliation=author_affiliation,
                affiliations=author_affiliations if author_affiliations else None,
            )
            authors_with_positions.append((author, position))

        return authors_with_positions

    def _extract_affiliations(self, author_data: Dict) -> List[str]:
        """Extract all affiliations for an author if they exist."""
        affiliations = []

        affiliation_array = ensure_list(get_nested(author_data, "ns1:affiliation"))
        for affiliation in affiliation_array:
            if affiliation:
                clean_affiliation = parse_names_and_identifiers(affiliation)
                if clean_affiliation:
                    affiliations.append(clean_affiliation)

        return affiliations

    def _create_links(self, session: Session, entry_data: Dict) -> List[Link]:
        """Create or retrieve Link entities from the document."""
        links = []
        seen_hrefs = set()

        link_list = ensure_list(get_nested(entry_data, "ns0:link"))
        for link_data in link_list:
            if isinstance(link_data, dict):
                href = parse_web_resources(get_nested(link_data, "@href"))
                if not href or href in seen_hrefs:
                    continue
                seen_hrefs.add(href)

                title = parse_string(get_nested(link_data, "@title"))
                rel = parse_string(get_nested(link_data, "@rel"))
                link_type = parse_string(get_nested(link_data, "@type"))

                link, _ = get_or_create(
                    session,
                    Link,
                    {"href": href},
                    title=title,
                    rel=rel,
                    type=link_type,
                )
                links.append(link)

        return links

    def _create_entry_authors(
        self,
        session: Session,
        entry: Entry,
        authors_with_positions: List[Tuple[Author, int]],
    ):
        """Create junction table entries between Entry and Author entities."""
        for author, position in authors_with_positions:
            if entry.id and author.id:
                entry_author, _ = get_or_create(
                    session,
                    JunctionEntryAuthor,
                    {"entry_id": entry.id, "author_id": author.id},
                    author_position=position,
                )

    def _create_entry_links(self, session: Session, entry: Entry, links: List[Link]):
        """Create junction table entries between Entry and Link entities."""
        for link in links:
            if entry.id and link.id:
                entry_link, _ = get_or_create(
                    session,
                    JunctionEntryLink,
                    {"entry_id": entry.id, "link_id": link.id},
                )
