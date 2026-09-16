import logging
import os
import time
from typing import Dict

from common.requests.requests import make_get_request
from _datamodels.digicher.entities import Institutions
from enrichment_lib.geolocation.noise_words import normalize_institution_name


def search_geolocation(institution: Institutions) -> Dict:
    try:
        return _search_geolocation(institution)
    except Exception as e:
        logging.warning(f"Probably ran into a request limit, \n{e}")
        raise e


def _search_geolocation(institution: Institutions) -> Dict:
    """
    First tries the normalized name with OpenAlex,
    then uses original name with Open Alex,
    if that returns no results tries to use the address using Mapbox.
    """
    # Original name
    original_name = institution.name.replace("!", "").replace("|", "")

    # Normalized name for better matching
    normalized_name = normalize_institution_name(institution.name)

    # First attempt: Try with the normalized name in OpenAlex
    time.sleep(0.1)  # Need this for request limit
    openalex_result = make_get_request(
        "https://api.openalex.org/institutions",
        params={
            "search": normalized_name,
            "select": "id,display_name,geo",
            "mailto": "walter.ehrenberger@uni-jena.de",
        },
    )

    # Check if OpenAlex returned a valid result with geo information
    if (
        openalex_result.get("results")
        and len(openalex_result["results"]) > 0
        and openalex_result["results"][0].get("geo")
        and openalex_result["results"][0]["geo"].get("latitude")
        and openalex_result["results"][0]["geo"].get("longitude")
    ):

        result = openalex_result["results"][0]
        return {
            "source": "OpenAlexNormalized",
            "name": result["display_name"],
            "latitude": result["geo"]["latitude"],
            "longitude": result["geo"]["longitude"],
            "confidence": "high",
        }

    # Second attempt: If no results, try with the original name in OpenAlex
    if original_name != normalized_name:
        time.sleep(0.1)  # Need this for request limit
        openalex_result_original = make_get_request(
            "https://api.openalex.org/institutions",
            params={
                "search": original_name,
                "select": "id,display_name,geo",
                "mailto": "walter.ehrenberger@uni-jena.de",
            },
        )

        if (
            openalex_result_original.get("results")
            and len(openalex_result_original["results"]) > 0
            and openalex_result_original["results"][0].get("geo")
            and openalex_result_original["results"][0]["geo"].get("latitude")
            and openalex_result_original["results"][0]["geo"].get("longitude")
        ):

            result = openalex_result_original["results"][0]
            return {
                "source": "OpenAlex",
                "name": result["display_name"],
                "latitude": result["geo"]["latitude"],
                "longitude": result["geo"]["longitude"],
                "confidence": "high",
            }

    # Third attempt: Try Mapbox only if we have both city and country
    if institution.address_city and institution.address_country:
        return search_geolocation_mapbox(institution)

    # If everything fails, return an empty result
    return {
        "source": None,
        "name": institution.name,
        "latitude": None,
        "longitude": None,
        "confidence": None,
    }


def search_geolocation_mapbox(institution: Institutions) -> Dict:
    """
    Try to find institution location using Mapbox with structured input.
    Only used when we have both city and country for accurate results.
    """
    mapbox_token = os.getenv("API_KEY_MAPBOX")

    if not mapbox_token:
        print("Mapbox API key not found")
        return {
            "source": None,
            "name": institution.name,
            "latitude": None,
            "longitude": None,
            "confidence": None,
        }

    # Build query parameters with only the data we have
    query_params = {}

    if institution.address_city:
        query_params["place"] = institution.address_city

    if institution.address_country:
        query_params["country"] = institution.address_country

    if institution.address_postalcode:
        query_params["postcode"] = institution.address_postalcode

    if institution.address_street:
        query_params["street"] = institution.address_street

    # Only proceed if we have at least city and country
    if not (query_params.get("place") and query_params.get("country")):
        return {
            "source": None,
            "name": institution.name,
            "latitude": None,
            "longitude": None,
            "confidence": None,
        }

    # Add token to params
    query_params["access_token"] = mapbox_token

    # Make request to Mapbox
    mapbox_result = make_get_request(
        "https://api.mapbox.com/search/geocode/v6/forward", params=query_params
    )

    # Check if Mapbox returned valid results
    if (
        mapbox_result.get("features")
        and len(mapbox_result["features"]) > 0
        and mapbox_result["features"][0].get("geometry")
        and mapbox_result["features"][0]["geometry"].get("coordinates")
    ):

        feature = mapbox_result["features"][0]
        coordinates = feature["geometry"]["coordinates"]

        return {
            "source": "Mapbox",
            "name": feature["properties"].get("name", institution.name),
            "latitude": coordinates[1],  # Mapbox returns [lon, lat]
            "longitude": coordinates[0],
            "confidence": "medium",
        }

    return {
        "source": None,
        "name": institution.name,
        "latitude": None,
        "longitude": None,
        "confidence": None,
    }
