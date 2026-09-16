from typing import Optional


def parse_geolocation(geolocation: str, swap_lat_lon: bool) -> Optional[list]:
    """
    Parse geolocation string and return as [lon, lat] array.
    Returns None if coordinates are invalid.
    Cordis geolocations with (brackets) are [lat, lon] in the raw data.
    """
    if not geolocation:
        return None

    cleaned = geolocation.replace("(", "").replace(")", "")
    try:
        lat, lon = map(lambda x: float(x.strip()), cleaned.split(","))
        if swap_lat_lon and not geolocation.startswith("("):
            lat, lon = lon, lat
        if lat < -90 or lat > 90 or lon < -180 or lon > 180:
            return None

        return [lat, lon]
    except (ValueError, TypeError):
        return None
