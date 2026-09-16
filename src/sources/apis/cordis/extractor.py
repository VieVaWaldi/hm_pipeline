import json
import logging
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

from dateutil.relativedelta import relativedelta

from common.api_runner.run_extractor import IExtractor, ExtractorConfig
from common.extractor.utils import trim_excessive_whitespace
from common.file_handling.archive_utils import unpack_and_remove_zip
from common.file_handling.file_parsing.xml_parser import (
    extract_element_texts,
    extract_element_as_dict,
)
from common.file_handling.file_utils import (
    load_file,
    write_file,
    ensure_path_exists,
    delete_if_empty,
)
from common.requests.file_downloader import SeleniumFileDownloader
from common.requests.requests import (
    make_delete_request,
    make_get_request,
    download_file,
    get_base_url,
)
from common.sanitizers.parse_file_names import parse_file_names
from common.errors.error_handling import log_and_exit


class CordisExtractor(IExtractor):
    """
    Cordis extractor that follows the async cordis API pattern:
    1. Call getExtraction to create a task
    2. Monitor getExtractionStatus until data is ready
    3. Download the data as ZIP
    4. Process and save records
    5. Call deleteExtraction to clean up

    Handles projects that may have start dates in the future.
    When no more results are found, resets checkpoint to ~5 years ago
    to collect updates on existing projects.
    """

    def __init__(self, extractor_config: ExtractorConfig):
        super().__init__(extractor_config, sleep_between_extractions=5)
        self.base_url = "https://cordis.europa.eu/"
        self.api_key = os.getenv("API_KEY_CORDIS")
        if not self.api_key:
            log_and_exit("API_KEY_CORDIS not found in environment variables")

    def should_continue(self) -> bool:
        """Continue while next checkpoint < today + 1 year.
        Cordis doesnt seem to allow more than 1 year"""
        one_year_future = datetime.now() + relativedelta(years=1)
        return self.get_checkpoint_end() < one_year_future

    def build_query(self) -> str:
        start_date = self.checkpoint
        end_date = self.checkpoint_to_string(self.get_checkpoint_end(minus_1_day=True))

        date_query = f"{self.checkpoint_name}={start_date}-{end_date}"
        full_query = f"{date_query} AND ({self.query})"
        return full_query

    def extract_until_next_checkpoint(self) -> bool:
        logging.info(
            f"Starting extraction from {self.checkpoint} to {self.get_checkpoint_end()}"
        )

        query = self.build_query()

        task_id = self._create_extraction_task(query)
        download_uri, number_of_records = self._wait_for_extraction(task_id)

        if number_of_records > 0:
            logging.info(f"Found {number_of_records} records to process")
            self._download_and_process_data(download_uri)
        else:
            logging.info("No records found in this checkpoint range")

        self._delete_extraction(task_id)
        should_continue = self.should_continue()

        if should_continue:
            new_checkpoint = self.checkpoint_to_string(self.get_checkpoint_end())
            self.save_checkpoint(new_checkpoint)
            logging.info(f"Advanced checkpoint to: {new_checkpoint}")
        else:
            reset_checkpoint = self._get_reset_checkpoint()
            self.save_checkpoint(reset_checkpoint)
            logging.info(
                f"No more future data. Reset checkpoint to: {reset_checkpoint} for update collection"
            )
        return should_continue

    def get_checkpoint_end(self, minus_1_day: bool = False) -> datetime:
        checkpoint_dt = self.string_to_checkpoint(self.checkpoint)
        checkpoint_end = checkpoint_dt + relativedelta(years=int(self.checkpoint_range))

        if minus_1_day:
            checkpoint_end -= timedelta(days=1)

        return checkpoint_end

    def string_to_checkpoint(self, checkpoint_str: str) -> datetime:
        return datetime.strptime(checkpoint_str, "%Y-%m-%d")

    def checkpoint_to_string(self, checkpoint_dt: datetime) -> str:
        return checkpoint_dt.strftime("%Y-%m-%d")

    def _get_reset_checkpoint(self) -> str:
        """
        Find a checkpoint from our progression that's approximately 5 years ago.
        This ensures we reuse existing checkpoint directories for overwriting.
        """
        target_date = datetime.now() - relativedelta(years=5)

        current = self.string_to_checkpoint(self.checkpoint_start)
        best_checkpoint = self.checkpoint_start
        min_diff = abs((current - target_date).days)

        while current < datetime.now():
            diff = abs((current - target_date).days)
            if diff < min_diff:
                min_diff = diff
                best_checkpoint = self.checkpoint_to_string(current)
            current += relativedelta(years=int(self.checkpoint_range))

        logging.info(
            f"Found reset checkpoint {best_checkpoint} (~5 years ago from today)"
        )
        return best_checkpoint

    def _create_extraction_task(self, query: str) -> str:
        url = f"{self.base_url}api/dataextractions/getExtraction"
        params = {
            "query": query,
            "outputFormat": "xml",
            "key": self.api_key,
            "archived": True,
        }

        response = make_get_request(url, params, timeout=300)
        if response.get("payload", {}).get("error"):
            log_and_exit(f"Cordis API error: {response['payload']['error']}")

        task_id = response["payload"]["taskID"]
        logging.info(f"Created extraction task: {task_id}")
        return task_id

    def _wait_for_extraction(self, task_id: str) -> Tuple[str, int]:
        url = f"{self.base_url}api/dataextractions/getExtractionStatus"
        params = {"key": self.api_key, "taskId": task_id}

        start_time = datetime.now()
        max_wait = timedelta(hours=16)

        while True:
            response = make_get_request(url, params)
            payload = response.get("payload", {})

            if payload.get("failed"):
                dump = json.dumps(response, indent=2)
                logging.info(f"Full API response: {dump}")
                log_and_exit(f"Extraction status error: {dump}")

            if payload.get("error"):
                log_and_exit(f"Extraction status error: {payload['error']}")

            progress = payload.get("progress", "")

            if progress == "Finished":
                download_uri = payload["destinationFileUri"]
                num_records = int(payload.get("numberOfRecords", 0))
                logging.info(
                    f"Extraction completed: {num_records} records ready for download"
                )
                return download_uri, num_records

            if datetime.now() - start_time > max_wait:
                log_and_exit(
                    f"Extraction timeout: Task {task_id} took longer than {max_wait} hours"
                )

            logging.info(f"Extraction in progress: {progress}. Waiting...")
            time.sleep(60)

    def _download_and_process_data(self, download_uri: str):
        zip_path = download_file(download_uri, self.data_path)
        logging.info(f"Downloaded ZIP to: {zip_path}")

        unpack_and_remove_zip(zip_path)

        # Cordis nests a ZIP in a ZIP
        xml_zip_path = zip_path.parent / "xml.zip"
        if xml_zip_path.exists():
            unpack_and_remove_zip(xml_zip_path)
            logging.info("Unpacked nested XML ZIP")

        self._process_xml_files()

    def _process_xml_files(self):
        """
        Process all XML files in the data path.
        Each XML becomes a record_dataset with its own folder.
        """
        xml_files = list(self.data_path.glob("*.xml"))
        logging.info(f"Processing {len(xml_files)} XML files")

        for file_path in xml_files:
            if not file_path.is_file() or file_path.suffix != ".xml":
                logging.warning(f"Skipping non-XML file: {file_path}")
                continue

            try:
                project_info = self._extract_project_info(file_path)

                record_folder = self._create_record_folder(project_info)
                xml_content = load_file(file_path)

                if xml_content:
                    xml_transformed = trim_excessive_whitespace(xml_content)
                    xml_destination = record_folder / file_path.name
                    write_file(xml_destination, xml_transformed)
                    logging.info(f"Saved XML: {xml_destination}")

                if self.download_attachments:
                    self._download_attachments(file_path, record_folder)

                os.remove(file_path)
            except Exception as e:
                logging.error(f"Error processing {file_path}: {e}")

    def _extract_project_info(self, xml_path: Path) -> dict:
        try:
            title = xml_path.stem
            titles = extract_element_texts(xml_path, "title")
            if len(titles) > 0 and titles[0]:
                title = titles[0]

            start_date = "1900-01-01"
            start_dates = extract_element_texts(xml_path, "startDate")
            if len(start_dates) > 0 and start_dates[0]:
                try:
                    date_str = (
                        start_dates[0][:10]
                        if len(start_dates[0]) >= 10
                        else start_dates[0]
                    )
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    start_date = date_obj.strftime("%Y-%m-%d")
                except (ValueError, AttributeError) as e:
                    logging.warning(f"Could not parse date '{start_dates[0]}': {e}")
            return {"start_date": start_date, "title": title}

        except Exception as e:
            logging.warning(f"Could not extract project info from {xml_path}: {e}")
            return {"start_date": "1900-01-01", "title": xml_path.stem}

    def _create_record_folder(self, project_info: dict) -> Path:
        date_str = project_info["start_date"]
        title = parse_file_names(project_info["title"])

        folder_name = f"{date_str}-{title[:80]}"  # max title length for os path length
        record_path = self.data_path / folder_name
        ensure_path_exists(record_path)

        return record_path

    def _download_attachments(self, xml_path: Path, record_folder: Path):
        try:
            attachment_dir = record_folder / "attachments"

            if attachment_dir.exists():
                shutil.rmtree(attachment_dir)
                logging.info(f"Cleared existing attachments in {attachment_dir}")

            ensure_path_exists(attachment_dir)

            link_dicts = extract_element_as_dict(xml_path, "webLink")
            eu_links = [
                dic["webLink"]["physUrl"]
                for dic in link_dicts
                if dic.get("webLink", {}).get("physUrl")
                and get_base_url(dic["webLink"]["physUrl"]) == "europa.eu"
            ]

            if not eu_links:
                return

            ensure_path_exists(attachment_dir)

            downloader = SeleniumFileDownloader(attachment_dir)
            successful = 0
            failed = 0

            for url in eu_links:
                try:
                    if downloader.download_file(url, only_from_url="europa.eu"):
                        successful += 1
                    else:
                        failed += 1
                except Exception as e:
                    logging.warning(f"Failed to download {url}: {e}")
                    failed += 1

            downloader.close()
            delete_if_empty(attachment_dir)

            logging.info(
                f"Downloaded attachments for {record_folder.name}: "
                f"{successful} successful, {failed} failed"
            )

        except Exception as e:
            logging.error(f"Error downloading attachments: {e}")

    def _delete_extraction(self, task_id: str):
        try:
            url = f"{self.base_url}api/dataextractions/deleteExtraction"
            params = {"key": self.api_key, "taskId": task_id}

            make_delete_request(url, params)
            logging.info(f"Deleted extraction task: {task_id}")

        except Exception as e:
            logging.warning(f"Could not delete extraction task {task_id}: {e}")
