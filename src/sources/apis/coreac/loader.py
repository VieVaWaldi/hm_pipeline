from typing import Dict, List, Tuple, Optional

from sqlalchemy.orm import Session

from common.file_handling.dict_utils import ensure_list, get_nested
from common.database.postgres.get_or_create import get_or_create
from common.sanitizers.parse_primitives import (
    parse_date,
)
from common.api_runner.run_loader import ILoader
from common.sanitizers.parse_text import (
    parse_string,
    parse_titles_and_labels,
    parse_names_and_identifiers,
    parse_content,
    parse_web_resources,
)
from sources.apis.coreac.orm_model import (
    Work,
    Link,
    Reference,
    DataProvider,
    JunctionWorkLink,
    JunctionWorkReference,
    JunctionWorkDataProvider,
)


class CoreacLoader(ILoader):
    """
    Data loading for CoreAc documents.
    Transforms JSON documents into SQLAlchemy ORM models and adds them to the session.
    """

    def __init__(self, path_to_file=None):
        super().__init__(path_to_file)

    def load(self, session: Session, document: Dict):
        assert (
            isinstance(document, dict) and document
        ), "document must be a non-empty dictionary"

        work, created_work = self._create_work(session, document)
        if not created_work:
            return

        links = self._create_links(session, document)
        references = self._create_references(session, document)
        data_providers = self._create_data_providers(session, document)

        # Flush the session to get all entity IDs
        session.flush()

        self._create_work_links(session, work, links)
        self._create_work_references(session, work, references)
        self._create_work_data_providers(session, work, data_providers)

    def _create_work(
        self, session: Session, document: Dict
    ) -> Tuple[Optional[Work], bool]:
        """Create or retrieve a Work entity from the document."""

        id_original = parse_string(get_nested(document, "id"))
        title = parse_titles_and_labels(get_nested(document, "title"))
        if not title:
            return None, False

        doi = get_nested(document, "doi")
        arxiv_id = get_nested(document, "arxivId")
        mag_id = get_nested(document, "magId")
        pubmed_id = get_nested(document, "pubmedId")

        language_code = get_nested(document, "language.code")
        language_name = get_nested(document, "language.name")

        document_type = parse_names_and_identifiers(
            get_nested(document, "documentType")
        )
        field_of_study = parse_names_and_identifiers(
            get_nested(document, "fieldOfStudy")
        )
        abstract = parse_content(get_nested(document, "abstract"))
        fulltext = parse_content(get_nested(document, "fullText"))

        publisher = parse_names_and_identifiers(get_nested(document, "publisher"))

        oai_ids = self._extract_oai_ids(document)
        authors = self._extract_authors(document)
        contributors = self._extract_contributors(document)
        journals_title = self._extract_journal_titles(document)
        outputs = self._extract_outputs(document)
        source_fulltext_urls = self._extract_source_fulltext_urls(document)

        download_url = parse_web_resources(get_nested(document, "downloadUrl"))

        year_published = parse_string(get_nested(document, "yearPublished"))
        created_date = parse_date(get_nested(document, "createdDate"))
        updated_date = parse_date(get_nested(document, "updatedDate"))
        published_date = parse_date(get_nested(document, "publishedDate"))
        deposited_date = parse_date(get_nested(document, "depositedDate"))
        accepted_date = parse_date(get_nested(document, "acceptedDate"))

        work, created = get_or_create(
            session,
            Work,
            {"id_original": id_original},
            title=title,
            doi=doi,
            arxiv_id=arxiv_id,
            mag_id=mag_id,
            pubmed_id=pubmed_id,
            oai_ids=oai_ids,
            language_code=language_code,
            language_name=language_name,
            document_type=document_type,
            field_of_study=field_of_study,
            abstract=abstract,
            fulltext=fulltext,
            publisher=publisher,
            authors=authors,
            contributors=contributors,
            journals_title=journals_title,
            download_url=download_url,
            outputs=outputs,
            source_fulltext_urls=source_fulltext_urls,
            year_published=year_published,
            created_date=created_date,
            updated_date=updated_date,
            published_date=published_date,
            deposited_date=deposited_date,
            accepted_date=accepted_date,
        )

        return work, True

    def _extract_oai_ids(self, document: Dict) -> List[str]:
        oai_ids = ensure_list(get_nested(document, "oaiIds"))
        return [parse_web_resources(oai_id) for oai_id in oai_ids if oai_id]

    def _extract_authors(self, document: Dict) -> List[str]:
        authors_data = ensure_list(get_nested(document, "authors"))
        return [
            parse_names_and_identifiers(author.get("name"))
            for author in authors_data
            if author and author.get("name")
        ]

    def _extract_contributors(self, document: Dict) -> List[str]:
        contributors = ensure_list(get_nested(document, "contributors"))
        return [
            parse_names_and_identifiers(contributor)
            for contributor in contributors
            if contributor
        ]

    def _extract_journal_titles(self, document: Dict) -> List[str]:
        journals = ensure_list(get_nested(document, "journals"))
        return [
            parse_titles_and_labels(journal.get("title"))
            for journal in journals
            if journal and journal.get("title")
        ]

    def _extract_outputs(self, document: Dict) -> List[str]:
        outputs = ensure_list(get_nested(document, "outputs"))
        return [parse_web_resources(output) for output in outputs if output]

    def _extract_source_fulltext_urls(self, document: Dict) -> List[str]:
        urls = ensure_list(get_nested(document, "sourceFulltextUrls"))
        return [parse_web_resources(url) for url in urls if url]

    def _create_links(self, session: Session, document: Dict) -> List[Link]:
        """
        Tracks already processed URLs in seen_urls to avoid duplicates links in the same entry
        which can lead to UniqueViolation if we dont flush after each url.
        """
        links_data = ensure_list(get_nested(document, "links"))
        links = []
        seen_urls = set()

        for link_data in links_data:
            if not link_data:
                continue

            url = parse_web_resources(get_nested(link_data, "url"))
            if not url or url in seen_urls:
                continue

            seen_urls.add(url)
            link_type = parse_names_and_identifiers(get_nested(link_data, "type"))

            link, _ = get_or_create(session, Link, {"url": url}, type=link_type)
            links.append(link)

        return links

    def _create_references(self, session: Session, document: Dict) -> List[Reference]:
        references_data = ensure_list(get_nested(document, "references"))
        references = []

        for ref_data in references_data:
            if not ref_data:
                continue

            title = parse_titles_and_labels(get_nested(ref_data, "title"))
            if not title:
                continue

            id_original = parse_names_and_identifiers(get_nested(ref_data, "id"))
            doi = get_nested(ref_data, "doi")
            raw = parse_content(get_nested(ref_data, "raw"))
            cites = parse_names_and_identifiers(get_nested(ref_data, "cites"))

            # ToDo: Not sure if authors in references also use authors.name or not
            authors_data = ensure_list(get_nested(ref_data, "authors"))
            authors = [
                parse_names_and_identifiers(author) for author in authors_data if author
            ]

            date = parse_date(get_nested(ref_data, "date"))

            reference, _ = get_or_create(
                session,
                Reference,
                {"title": title},
                id_original=id_original,
                authors=authors,
                date=date,
                doi=doi,
                raw=raw,
                cites=cites,
            )
            references.append(reference)

        return references

    def _create_data_providers(
        self, session: Session, document: Dict
    ) -> List[DataProvider]:
        providers_data = ensure_list(get_nested(document, "dataProviders"))
        providers = []

        for provider_data in providers_data:
            if not provider_data:
                continue

            name = parse_names_and_identifiers(get_nested(provider_data, "name"))
            if not name:
                continue

            id_original = parse_string(get_nested(provider_data, "id"))
            url = parse_web_resources(get_nested(provider_data, "url"))
            logo = parse_web_resources(get_nested(provider_data, "logo"))

            provider, _ = get_or_create(
                session,
                DataProvider,
                {"name": name},
                id_original=id_original,
                url=url,
                logo=logo,
            )
            providers.append(provider)

        return providers

    def _create_work_links(self, session: Session, work: Work, links: List[Link]):
        for link in links:
            if work.id and link.id:
                work_link, _ = get_or_create(
                    session, JunctionWorkLink, {"work_id": work.id, "link_id": link.id}
                )

    def _create_work_references(
        self, session: Session, work: Work, references: List[Reference]
    ):
        for reference in references:
            if work.id and reference.id:
                work_reference, _ = get_or_create(
                    session,
                    JunctionWorkReference,
                    {"work_id": work.id, "reference_id": reference.id},
                )

    def _create_work_data_providers(
        self, session: Session, work: Work, providers: List[DataProvider]
    ):
        for provider in providers:
            if work.id and provider.id:
                work_provider, _ = get_or_create(
                    session,
                    JunctionWorkDataProvider,
                    {"work_id": work.id, "data_provider_id": provider.id},
                )
