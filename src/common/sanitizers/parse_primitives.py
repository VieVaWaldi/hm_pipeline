from datetime import datetime
from typing import Optional, Any


def parse_bool(value: Any) -> Optional[bool]:
    """Use for all booleans"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return None


def parse_number(value: Optional[str]) -> Optional[int]:
    """Use for all decimals"""
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: Any) -> Optional[float]:
    """Use for all floats"""
    if value is not None:
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return None


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse date string into datetime object."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
