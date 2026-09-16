import re
from typing import Optional

from common.sanitizers.parse_text import parse_titles_and_labels

REPLACEMENTS = {
    ":": "-",
    "<": "(",
    ">": ")",
    '"': "'",
    "|": "-",
    "?": "",
    "*": "",
    "\\": "-",
    "/": "-",
    "\0": "",
}

RESERVED_NAMES = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    "COM1",
    "COM2",
    "COM3",
    "COM4",
    "COM5",
    "COM6",
    "COM7",
    "COM8",
    "COM9",
    "LPT1",
    "LPT2",
    "LPT3",
    "LPT4",
    "LPT5",
    "LPT6",
    "LPT7",
    "LPT8",
    "LPT9",
}


def parse_file_names(value: Optional[str]) -> Optional[str]:
    """
    Sanitize filename by removing or replacing characters that are invalid
    for filesystem paths while preserving readability.

    Based on parse_titles_and_labels but adds filesystem-specific sanitization:
    - Removes/replaces filesystem-reserved characters
    - Handles Windows/Unix path restrictions
    - Preserves hyphens and underscores for readability
    - Limits length to reasonable filesystem limits

    Returns None if string ends up being empty.
    """

    clean_str = parse_titles_and_labels(value)
    if not clean_str:
        return None

    for old_char, new_char in REPLACEMENTS.items():
        clean_str = clean_str.replace(old_char, new_char)

    clean_str = re.sub(r"[\x00-\x1f\x7f]", "", clean_str)

    name_part = clean_str.split(".")[0].upper()
    if name_part in RESERVED_NAMES:
        clean_str = f"_{clean_str}"

    clean_str = clean_str.rstrip(". ")

    clean_str = re.sub(
        r"[-_\s]+",
        lambda m: "-" if "-" in m.group() else "_" if "_" in m.group() else " ",
        clean_str,
    )

    max_length = 195
    if len(clean_str.encode("utf-8")) > max_length:

        truncated = clean_str[:max_length]
        last_break = max(
            truncated.rfind(" "), truncated.rfind("-"), truncated.rfind("_")
        )
        if last_break > max_length * 0.8:
            clean_str = truncated[:last_break].rstrip()
        else:
            clean_str = truncated.rstrip()

    clean_str = clean_str.strip("-_ ")

    return clean_str or None
