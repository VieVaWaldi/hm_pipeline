import logging
import time
import xml.etree.ElementTree as xml
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Dict

from dateutil.relativedelta import relativedelta

from common.api_runner.run_extractor import IExtractor, ExtractorConfig
from common.file_handling.file_utils import ensure_path_exists
from common.requests.requests import make_get_request
from common.sanitizers.parse_file_names import parse_file_names
from common.errors.error_handling import log_and_exit


class ArxivExtractor(IExtractor):
    def __init__(self, extractor_config: ExtractorConfig):
        super().__init__(extractor_config, sleep_between_extractions=5)
        self.base_url = "https://export.arxiv.org/api/query?"
        self.ID = "{http://www.w3.org/2005/Atom}"

    def should_continue(self):
        """Continue while next checkpoint < today"""
        return self.get_checkpoint_end() < datetime.now()

    def build_query(self) -> str:
        start = self.checkpoint_to_arxiv(self.checkpoint_to_dt(self.checkpoint))
        end = self.checkpoint_to_arxiv(self.get_checkpoint_end(minus_1_day=True))

        query = f"search_query={self.query}"
        query += f"+AND+submittedDate:[{start}+TO+{end}]"
        query += f"&sortBy=submittedDate&sortOrder=ascending"
        return query

    def extract_until_next_checkpoint(self) -> bool:
        logging.info(
            f"Starting extraction from checkpoint {self.checkpoint} to {self.checkpoint_range}.",
        )

        start_idx = 0
        max_results = 50

        has_results = True
        while has_results:
            params = {"start": start_idx, "max_results": max_results}
            xml_content = self.request_arxiv_api(params)

            entries = self.extract_entries(xml_content)
            self.check_and_save_new_entries(entries)

            meta_data = self.extract_meta_data(xml_content)
            total_results = meta_data["total_results"]

            start_idx += max_results
            if start_idx > total_results:
                has_results = False
            time.sleep(5)

        should_continue = self.should_continue()
        if should_continue:
            new_checkpoint = self.checkpoint_to_human(self.get_checkpoint_end())
            self.save_checkpoint(new_checkpoint)
            logging.info(
                f"Updated to new checkpoint: {self.checkpoint_to_human(self.get_checkpoint_end())}"
            )
        else:
            logging.info(
                f"Checkpoint is in the future, not updating. Checkpoint remains {self.checkpoint}"
            )

        return should_continue

    def get_checkpoint_end(self, minus_1_day: bool = False) -> datetime:
        """."""
        checkpoint_end_dt = datetime.strptime(
            self.checkpoint, "%Y-%m-%d-%H-%M"
        ) + relativedelta(months=int(self.checkpoint_range))
        if minus_1_day:
            checkpoint_end_dt -= timedelta(days=1)
        return checkpoint_end_dt

    def checkpoint_to_dt(self, checkpoint: str):
        return datetime.strptime(checkpoint, "%Y-%m-%d-%H-%M")

    def checkpoint_to_arxiv(self, checkpoint: datetime) -> str:
        return checkpoint.strftime("%Y%m%d%H%M")

    def checkpoint_to_human(self, checkpoint: datetime) -> str:
        return checkpoint.strftime("%Y-%m-%d-%H-%M")

    def request_arxiv_api(self, params: dict) -> str:
        query = self.build_query()
        url = f"{self.base_url}{query}"
        response = make_get_request(url, params=params, expect_json=False)
        self.response_has_entries(response.text)
        return response.text

    def response_has_entries(self, response_xml: str) -> bool:
        if "<entry" not in response_xml:
            logging.warning(
                "No entries found after all retries. This could be normal if there are no results in this range."
            )
            return False
        return True

    def extract_meta_data(self, xml_content: str) -> Dict[str, Any]:
        try:
            root = xml.fromstring(xml_content)
            ns = {
                "atom": "http://www.w3.org/2005/Atom",
                "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
            }
            total_results = int(root.find("opensearch:totalResults", ns).text)
            start_index = int(root.find("opensearch:startIndex", ns).text)
            items_per_page = int(root.find("opensearch:itemsPerPage", ns).text)
            return {
                "total_results": total_results,
                "start_index": start_index,
                "items_per_page": items_per_page,
            }
        except Exception as e:
            log_and_exit(f"Error parsing metadata", e)

    def extract_entries(self, xml_content: str) -> List[str]:
        try:
            root = xml.fromstring(xml_content)
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns)
            return [xml.tostring(entry, encoding="unicode") for entry in entries]
        except Exception as e:
            log_and_exit(f"Error extracting entries", e)

    def check_and_save_new_entries(self, entries: List[str]):
        for entry in entries:
            try:
                element = xml.fromstring(entry)
                title = parse_file_names(element.find(f"{self.ID}title").text)
                published_date = element.find(f"{self.ID}published").text
                dt = datetime.fromisoformat(
                    published_date.replace("Z", "+00:00")
                ).strftime("%Y-%m-%d_%H-%M")

                entry_path = self.data_path / f"{dt}-{title}"
                file_path = entry_path / f"{title}.xml"
                ensure_path_exists(file_path)

                tree = xml.ElementTree(element)
                tree.write(file_path, encoding="utf-8", xml_declaration=True)

                attachment_path = entry_path / "attachments"
                ensure_path_exists(attachment_path)
                self.try_save_pdf(element, attachment_path, title)

            except Exception as e:
                log_and_exit(f"Error saving entries to file", e)

    def try_save_pdf(self, element: Any, path: Path, title: str):
        pdf_links = [
            link
            for link in element.findall(f"{self.ID}link")
            if link.attrib.get("title") == "pdf"
        ]
        for link in pdf_links:
            file_path = path / f"{title}.pdf"
            response = make_get_request(
                link.attrib["href"], can_fail=True, expect_json=False
            )
            if response is None:
                logging.warning(f"Failed saving PDF")
                return
            try:
                with file_path.open("wb") as f:
                    f.write(response.content)
            except Exception as e:
                logging.warning(f"Failed saving PDF", e)
            logging.info(f"Saved {file_path}")
