import logging
import os
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from common.errors.error_handling import log_and_exit
from common.file_handling.file_utils import ensure_path_exists
from common.requests.requests import get_base_url


class SeleniumFileDownloader:
    """
    Use this for urls that redirect you to the actual downloadable file.
    This uses selenium and creates a headless browser to download the file.
    """

    def __init__(self, download_path: Path):
        self.download_path = download_path
        self.driver = self._setup_driver()
        logging.info(f"Setup PDFDownloader for path {self.download_path}.")

    def _setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": str(self.download_path),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "plugins.always_open_pdf_externally": True,
            },
        )
        return webdriver.Chrome(options=chrome_options)

    def download_file(self, url: str, only_from_url: str) -> bool:
        if get_base_url(url) != only_from_url:
            log_and_exit(
                f"Trying to download a non {only_from_url} cordis pdf file."
            )
        try:
            self.driver.get(url)
            return self._wait_for_download()
        except Exception as e:
            print(f"Error downloading file from {url}: {str(e)}")
            return False

    def _wait_for_download(self, timeout: int = 90) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            downloading_files = [
                f
                for f in os.listdir(self.download_path)
                if f.endswith(".crdownload")
                or f.endswith(".tmp")
                or f.startswith(".com.google.Chrome")
            ]
            if not downloading_files:
                return True
            time.sleep(1)
        self._cleanup_incomplete_files()
        return False

    def _cleanup_incomplete_files(self) -> None:
        for filename in os.listdir(self.download_path):
            if (
                filename.endswith(".crdownload")
                or filename.endswith(".tmp")
                or filename.startswith(".com.google.Chrome")
            ):
                file_path = os.path.join(self.download_path, filename)
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting {filename}: {str(e)}")

    def close(self):
        if self.driver:
            self.driver.quit()


if __name__ == "__main__":
    save_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/rmme"
    )
    ensure_path_exists(save_path)
    downloader = SeleniumFileDownloader(save_path)

    result = downloader.download_file(
        "https://ec.europa.eu/research/participants/documents/downloadPublic?documentIds=080166e5e9e8c93d&appId=PPGMS",
        "europa.eu",
    )
    print(f"Download successful: {result}")
