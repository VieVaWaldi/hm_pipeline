import argparse
import json
import logging
import os
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from common.errors.error_handling import log_and_exit
from common.file_handling.file_parsing.json_parser import get_all_keys_value_recursively
from common.file_handling.file_utils import load_file
from common.requests.requests import make_get_request
from common.api_runner.run_extractor import IExtractor, ExtractorConfig

"""


Gotta check the coreac api manually again for verified chekpoint, start cp, and new query


"""


class CoreExtractor(IExtractor):

    def should_continue(self) -> bool:
        pass

    def __init__(self, extractor_config: ExtractorConfig):
        super().__init__(extractor_config, sleep_between_extractions=10)
        self.base_url = "https://api.core.ac.uk/v3"
        self.api_key = os.getenv("API_KEY_CORE")
        self.headers = {"Authorization": f"Bearer {self.api_key}"}
        if not self.api_key:
            log_and_exit("API Key missing")

    def extract_until_next_checkpoint(self) -> bool:
        logging.info(
            f"Starting new extraction from checkpoint {self.checkpoint} to {self.checkpoint_range}.",
        )

        core_data, total_hits = self._paginated_search_core()

        if core_data:
            data_path = self.save_extracted_data(core_data)

            checkpoint = self.get_new_checkpoint_from_data()
            self.save_checkpoint(checkpoint)

            logging.info(">>> Successfully finished extraction")
            return total_hits != 0
        return False

    def build_query(self):
        checkpoint_to = self.checkpoint + self.checkpoint_range # ToDo: calc this correctly
        base_query = f"({self.checkpoint_name}>{self.checkpoint} AND {self.checkpoint_name}<{checkpoint_to}) AND "
        return base_query + self.query

    def get_checkpoint_end(self, minus_1_day=False) -> Any:
        pass

    def create_checkpoint_end_for_this_run(self, next_checkpoint: str) -> str:
        days_to_add = int(next_checkpoint)
        last_checkpoint_date = datetime.strptime(self.last_checkpoint, "%Y-%m-%d")
        new_checkpoint_date = last_checkpoint_date + relativedelta(days=days_to_add)
        return new_checkpoint_date.strftime("%Y-%m-%d")

    def save_extracted_data(self, data: List[Dict[str, Any]]) -> Path:
        for index, entry in enumerate(data):
            if entry is None:
                continue
            title = self.clean_title(entry["title"], entry, index)
            date = (
                self.parse_date_to_obj(entry["publishedDate"]).strftime("%Y-%m-%d")
                if entry.get("publishedDate")
                else f"NO-DATE"
            )
            filename = f"{date}_{title}.json"
            file_path = os.path.join(self.data_path, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                json_string = json.dumps(entry, indent=4, ensure_ascii=False)
                f.write(json_string)

        logging.info(
            f"Saved {len(data)} entries as individual files in {self.data_path}",
        )
        return Path("")

    def non_contextual_transformation(self, data_path: str):
        pass

    def get_new_checkpoint_from_data(self) -> str:
        date_elements = get_all_keys_value_recursively(
            self.data_path, self.checkpoint_name
        )
        date_objects = []
        for date_str in date_elements:
            date_objects.append(self.parse_date_to_obj(date_str[0]))

        if not date_objects:
            log_and_exit("Lost json elements when converting to datatime.")

        return max(date_objects).strftime("%Y-%m-%d")

    def _paginated_search_core(self) -> (List[Dict[str, Any]], int):
        all_data = []
        total_hits = 0
        offset = 0
        query = self.build_query()

        while True:
            core_data, hits = self._search_core(query, offset)
            if (
                not core_data and hits != 1
            ):  # and hits != 1 for documentType coreac bug fml
                break

            all_data.extend(core_data)
            total_hits = hits
            offset += len(core_data)

            if offset >= total_hits or offset >= 2000:  # Respect the 2000 item limit
                break

        return all_data, total_hits

    def _search_core(self, query: str, offset: int = 0, chunk_size: int = 3)  -> (List[Dict[str, Any]], int):

        params = {
            "q": query,
            "sort": "publishedDate",
            "limit": chunk_size,
            "offset": offset,
        }
        response = make_get_request(
            f"{self.base_url}/search/works", params, self.headers
        )


    # def _search_core(
    #     self,
    #     query: str,
    #     offset: int = 0,
    #     chunk_size: int = 3,
    #     max_retries=3,
    #     initial_delay=10,
    # ) -> (List[Dict[str, Any]], int):
    #     """Search CORE API with retry mechanism and exponential backoff."""
    #     params = {
    #         "q": query,
    #         "sort": "publishedDate",
    #         "limit": chunk_size,
    #         "offset": offset,
    #     }
    #
    #     for attempt in range(max_retries):
    #         logging.info(
    #             f"Attempt {attempt+1}/{max_retries}: Fetching results from CORE API..."
    #         )
    #
    #         try:
    #             time.sleep(2)
    #             response = make_get_request(
    #                 f"{self.base_url}/search/works", params, self.headers
    #             )
    #         except Exception as e:
    #             # Stupid freaking coreac document type bug, need to go though all entries one by one fml
    #             if "Cannot assign array to property App" in e.args[0]:
    #                 logging.info("Skipping this chunk of 10 elements")
    #                 return [
    #                     None
    #                 ] * chunk_size, chunk_size + 1  # just make it keep going and skip the chunk ...
    #             else:
    #                 # Trying again cuz fuck them and their "200k" requests per day,
    #                 # i probably shouldnt comment like this
    #                 response = None
    #
    #         # Check if we got a valid response with results
    #         if response and "results" in response and "totalHits" in response:
    #             return response["results"], response["totalHits"]
    #         else:
    #             # Calculate delay with exponential backoff
    #             delay = initial_delay * (4**attempt)
    #             logging.warning(
    #                 f"Received empty or error response. Retrying in {delay} seconds..."
    #             )
    #             time.sleep(delay)
    #
    #     # If we've exhausted all retries and still don't have a good response
    #     logging.error("No valid response after all retries.")
    #     raise Exception("Stopping the extraction after 3 retries, try again soon.")

    def clean_title(self, param, entry, index):
        return (
            param.replace(" ", "").replace("/", "").replace(".", "").lower()[:30]
            if entry.get("title")
            else f"NO-TITLE_{index}"
        )

    def parse_date_to_obj(self, date_string: str) -> datetime:
        try:
            return datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            try:
                return datetime.strptime(date_string, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Unable to parse date string: {date_string}")
