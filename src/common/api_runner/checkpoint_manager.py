import datetime
import logging
from pathlib import Path

from common.file_handling.path_utils import get_project_root_path
from common.config.settings import get_settings
from common.file_handling.file_utils import (
    ensure_path_exists,
    load_file,
    write_file,
)

CP_FILE_NAME = "mtime.cp"


class CheckpointManager:
    def __init__(self, source_name: str, query_id: str):
        self.source_name: str = f"{source_name}_{query_id}"
        self.cp_time: datetime = self._load_cp()
        self.next_cp_time: datetime = self.cp_time
        self.err: datetime = datetime.timedelta(seconds=5)

    def should_skip_or_store(self, file_mtime: datetime):
        """
        Returns True if file modification time is smaller than checkpoint,
        stores file modification time if it's greater.
        """
        should_skip = file_mtime + self.err < self.cp_time
        if not should_skip:
            self.next_cp_time = max(file_mtime, self.next_cp_time)
        return should_skip

    def update_cp(self):
        """
        Takes datetime in UTC timezone and saves as unix timestamp.
        """
        cp_path = self._get_cp_path(self.source_name) / CP_FILE_NAME
        ts = str(int(self.next_cp_time.timestamp()))
        write_file(cp_path, ts)
        logging.info(
            f"Updated data loading checkpoint for {self.source_name} with {self.next_cp_time}"
        )

    def _load_cp(self) -> datetime:
        """
        Loads unix timestamp and parses it into datetime in UTC timezone.
        """
        cp_path = self._get_cp_path(self.source_name)
        cp_txt = load_file(cp_path / CP_FILE_NAME)

        if cp_txt is None or cp_txt == "":
            cp_time = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
        else:
            cp_time = datetime.datetime.fromtimestamp(
                int(cp_txt), tz=datetime.timezone.utc
            )

        logging.info(
            f"Loaded data loading checkpoint for {self.source_name} with {cp_time}"
        )
        return cp_time

    def _get_cp_path(self, source_name: str) -> Path:
        cp_path = (
            get_project_root_path()
            / get_settings().checkpoint_path
            / "loading"
            / source_name
        )
        ensure_path_exists(cp_path)
        return cp_path

    def _rm_cp(self, source_name: str):
        logging.warning(f"Removing checkpoint for {source_name}")
        self.source_name = source_name
        self.next_cp_time = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
        self.update_cp()


def test_cp():
    cp = CheckpointManager("test", "0")
    cp._rm_cp("test_0")

    # 1. Test: New run starts from ts 0, doesnt skip larger values
    test_time = datetime.datetime.fromtimestamp(100, tz=datetime.timezone.utc)
    assert not cp.should_skip_or_store(test_time)

    # 2. Test: Update time, updates to highest last mod time
    cp.update_cp()
    assert cp.cp_time == test_time

    # 3. Test: error margin works
    test_time = cp.cp_time + datetime.timedelta(seconds=9)
    cp.update_cp()
    assert not cp.should_skip_or_store(test_time)

    # 4: Test: Skips times before this one
    test_time = datetime.datetime.fromtimestamp(0, tz=datetime.timezone.utc)
    assert cp.should_skip_or_store(test_time)

    print(">>> All tests passed <<<")


if __name__ == "__main__":
    test_cp()
