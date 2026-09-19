"""
Stage-to-stage duckdb copies for core_v3 (staging -> staging_2 -> final; see
orchestration/rules/pipeline/core_v3/enrichment.smk). Each enrichment stage
starts from a byte copy of the previous stage's file, so every stage stays a
snapshot. Copied to a temp name and renamed, so an interrupted copy never
leaves a truncated file at the real path.
"""

import logging
import os
import shutil
from datetime import datetime
from pathlib import Path

from common.log.timer import log_run_time


def fresh_copy(src: str, dst: str) -> None:
    """Replaces `dst` with a copy of `src`. Neither may be open in another process."""
    src_path, dst_path = Path(src), Path(dst)
    if not src_path.exists():
        raise FileNotFoundError(f"{src_path} does not exist — run the previous core_v3 stage first.")

    logging.info(f"Copying {src_path} ({src_path.stat().st_size / 1024**3:.1f} GB) -> {dst_path}")
    t = datetime.now()
    dst_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dst_path.with_name(dst_path.name + ".copying")
    shutil.copyfile(src_path, tmp_path)
    os.replace(tmp_path, dst_path)
    wal = dst_path.with_name(dst_path.name + ".wal")  # a stale WAL would replay onto the new file
    if wal.exists():
        wal.unlink()
    log_run_time(t)
