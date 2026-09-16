import re
from datetime import date, datetime
from typing import Optional


def trim_excessive_whitespace(file_content: str) -> str:
    """
    1. Replaces multiple newlines with a single newline
    2. Replace multiple spaces with a single space, but not newlines
    3. Ensure there is a newline after each tag for readability
    """
    trimmed_content = re.sub(r"\n\s*\n", "\n", file_content)
    trimmed_content = re.sub(r"[ \t]+", " ", trimmed_content)
    trimmed_content = re.sub(r"(>)(<)", r"\1\n\2", trimmed_content)
    return trimmed_content


# ToDo: I thought this might belong to the extractors, but after the refactor i am not really certain anymore lul


def parse_date(date_str: Optional[str]) -> Optional[date]:
    """Parses date strings in YYYY-MM-DD or YYYY-MM-DD HH:MM:SS format."""
    if date_str:
        try:
            # First try YYYY-MM-DD format
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            try:
                # Then try YYYY-MM-DD HH:MM:SS format
                return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").date()
            except (ValueError, TypeError):
                return None
    return None
