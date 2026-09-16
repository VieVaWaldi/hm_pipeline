from typing import Any


def get_nested(data: dict, path: str) -> Any:
    """
    Safely gets nested dictionary values using dot notation.
    Example: _get_nested(data, "a.b.c") is equivalent to data.get("a", {}).get("b", {}).get("c")
    """
    current = data
    for part in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part, {})
    return current if current != {} else None


def ensure_list(value: Any) -> list:
    """
    Ensures the input value is a list. If it's not a list, wraps it in one.
    If the value is None, returns an empty list.
    """
    if value is None:
        return []
    return value if isinstance(value, list) else [value]
