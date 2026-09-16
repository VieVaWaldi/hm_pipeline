from typing import Optional, Any


""" 
DOIs and acronyms should generally NOT be sanitized.
"""


_BOM_CHARS = "\ufeff\u200b\u200c\u200d\ufffe"


def _ensure_string(value: Any) -> Optional[str]:
    """Convert any value to string, strip BOM/zero-width chars, or return None."""
    if value is None:
        return None
    try:
        s = str(value)
    except (ValueError, TypeError):
        return None
    return s.strip(_BOM_CHARS) or None


def parse_string(value: Optional[str]) -> Optional[str]:
    """
    Use for all other string than dont fit one of the next methods.
    Trims whitespace and normalizes internal spaces.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None
    clean_str = " ".join(filter(None, value.strip().split()))
    return clean_str or None


def parse_names_and_identifiers(value: Optional[str]) -> Optional[str]:
    """
    Use e.g. for author names, institution names, department names.

    Preserves hyphens, apostrophes, and periods in names.
    Trims whitespace and normalizes internal spaces.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip()
    clean_str = clean_str.replace("\t", " ").replace("\r", "")
    clean_str = " ".join(filter(None, clean_str.split()))

    return clean_str or None


def parse_titles_and_labels(value: Optional[str]) -> Optional[str]:
    """
    Use e.g. for topic names, scientific domains, funding programme names,
    project titles, publication titles.

    Preserves hyphens, colons, and other punctuation relevant to titles.
    Replaces newlines/tabs with spaces and normalizes internal spacing.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip()
    clean_str = (
        clean_str.replace("\n", " ")
        .replace("\t", " ")
        .replace("\r", "")
        .replace("–", "-")
        .replace("—", "-")
    )
    clean_str = " ".join(filter(None, clean_str.split()))

    return clean_str or None


def parse_content(value: Optional[str]) -> Optional[str]:
    """
    Use e.g. for abstracts, full text, and other multi-paragraph content.

    Preserves paragraph structure (newlines) while normalizing spacing within paragraphs.
    Removes excessive newlines and normalizes to single newlines between paragraphs.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip()
    clean_str = clean_str.replace("\r", "").replace("\x00", "")

    paragraphs = clean_str.split("\n")
    cleaned_paragraphs = []

    for paragraph in paragraphs:
        cleaned = paragraph.strip().replace("\t", " ")
        cleaned = " ".join(filter(None, cleaned.split()))
        if cleaned:
            cleaned_paragraphs.append(cleaned)

    result = "\n".join(cleaned_paragraphs)
    return result or None


def parse_web_resources(value: Optional[str]) -> Optional[str]:
    """
    Use for URLs and other web resources.

    Minimal processing - only trims whitespace and removes carriage returns.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip().replace("\r", "")
    clean_str = clean_str.replace("\n", "").replace("\t", "")

    return clean_str or None


def flatten_string(value: Optional[str]) -> Optional[str]:
    """
    Used for cleaner log files.
    Flattens a string by removing all newlines, tabs, and normalizing whitespace.
    Converts multi-line content into a single line with normalized spacing.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip()
    clean_str = clean_str.replace("\n", " ").replace("\r", "").replace("\t", " ")

    clean_str = " ".join(filter(None, clean_str.split()))

    return clean_str or None
