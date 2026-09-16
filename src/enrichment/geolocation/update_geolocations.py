import logging
import math
import os
import time
from datetime import datetime
from typing import Dict, List, Optional

from datamodels.digicher.entities import Institutions

from enrichment_lib.geolocation.noise_words import normalize_institution_name
from enrichment_lib.utils.batch_requester import BatchRequester
from common.database.postgres.create_db_session import create_db_session
from common.requests.requests import make_get_request
from common.sanitizers.parse_specialized import parse_geolocation
from common.log.logger import setup_logging

# This should probably define its own interface so it doesnt have to import the models.
# Makes it work across pipelines. Double check needed.


class GeolocationBatchProcessor:
    """
    Batch processor for updating institution geolocations using the BatchRequester pattern.
    """

    def __init__(self, batch_size: int = 64, commit_frequency: int = 10):
        self.batch_size = batch_size
        self.commit_frequency = commit_frequency
        self.mapbox_token = os.getenv("API_KEY_MAPBOX")

        self.stats = {
            "processed": 0,
            "updated": 0,
            "openalex_hits": 0,
            "mapbox_hits": 0,
            "no_results": 0,
            "errors": 0,
        }

    def get_distance_in_meters(self, point1: List[float], point2: List[float]) -> float:
        """Calculate great-circle distance between two points using Haversine formula."""
        R = 6371000

        lat1, lng1 = point1
        lat2, lng2 = point2

        d_lat = (lat2 - lat1) * math.pi / 180
        d_lng = (lng2 - lng1) * math.pi / 180

        a = math.sin(d_lat / 2) * math.sin(d_lat / 2) + math.cos(
            lat1 * math.pi / 180
        ) * math.cos(lat2 * math.pi / 180) * math.sin(d_lng / 2) * math.sin(d_lng / 2)

        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance = R * c

        return distance

    def search_openalex_geolocation(
        self, institution_name: str, use_normalized: bool = True
    ) -> Optional[Dict]:
        try:
            search_name = (
                normalize_institution_name(institution_name)
                if use_normalized
                else institution_name
            )
            search_name = search_name.replace("!", "").replace("|", "")

            time.sleep(0.1)

            response = make_get_request(
                "https://api.openalex.org/institutions",
                params={
                    "search": search_name,
                    "select": "id,display_name,geo",
                    "mailto": "walter.ehrenberger@uni-jena.de",
                },
            )

            if (
                response.get("results")
                and len(response["results"]) > 0
                and response["results"][0].get("geo")
                and response["results"][0]["geo"].get("latitude")
                and response["results"][0]["geo"].get("longitude")
            ):

                result = response["results"][0]
                source_type = "OpenAlexNormalized" if use_normalized else "OpenAlex"

                return {
                    "source": source_type,
                    "name": result["display_name"],
                    "latitude": result["geo"]["latitude"],
                    "longitude": result["geo"]["longitude"],
                    "confidence": "high",
                }
        except Exception as e:
            logging.warning(f"OpenAlex API error for {institution_name}: {e}")

        return None

    def search_mapbox_geolocation(self, institution: Institutions) -> Optional[Dict]:
        if not self.mapbox_token:
            return None

        if not (institution.address_city and institution.address_country):
            return None

        try:
            query_params = {
                "place": institution.address_city,
                "country": institution.address_country,
                "access_token": self.mapbox_token,
            }

            if institution.address_postalcode:
                query_params["postcode"] = institution.address_postalcode
            if institution.address_street:
                query_params["street"] = institution.address_street

            response = make_get_request(
                "https://api.mapbox.com/search/geocode/v6/forward", params=query_params
            )

            if (
                response.get("features")
                and len(response["features"]) > 0
                and response["features"][0].get("geometry")
                and response["features"][0]["geometry"].get("coordinates")
            ):

                feature = response["features"][0]
                coordinates = feature["geometry"]["coordinates"]

                return {
                    "source": "Mapbox",
                    "name": feature["properties"].get("name", institution.name),
                    "latitude": coordinates[1],  # Mapbox returns [lon, lat]
                    "longitude": coordinates[0],
                    "confidence": "medium",
                }
        except Exception as e:
            logging.warning(f"Mapbox API error for {institution.name}: {e}")

        return None

    def search_geolocation_for_institution(
        self, institution: Institutions
    ) -> Optional[Dict]:
        """
        Search for geolocation using multiple sources with fallback strategy.
        Returns the best result found or None if no valid geolocation is found.
        """
        result = self.search_openalex_geolocation(institution.name, use_normalized=True)
        if result:
            self.stats["openalex_hits"] += 1
            return result

        result = self.search_openalex_geolocation(
            institution.name, use_normalized=False
        )
        if result:
            self.stats["openalex_hits"] += 1
            return result

        result = self.search_mapbox_geolocation(institution)
        if result:
            self.stats["mapbox_hits"] += 1
            return result

        self.stats["no_results"] += 1
        return None

    def should_update_geolocation(
        self, institution: Institutions, new_result: Dict, min_distance: float = 150.0
    ) -> bool:
        """
        Args:
            institution: The institution object
            new_result: The new geolocation result
            min_distance: Minimum distance in meters to consider updating
        """
        if not institution.address_geolocation:
            return True

        if not (new_result.get("latitude") and new_result.get("longitude")):
            return False

        try:
            old_coords = institution.address_geolocation
            new_coords = [new_result["latitude"], new_result["longitude"]]
            distance = self.get_distance_in_meters(old_coords, new_coords)

            return distance > min_distance

        except Exception as e:
            logging.warning(f"Error calculating distance for {institution.name}: {e}")
            return False

    def process_institution_batch(
        self, institutions: List[Institutions]
    ) -> List[Institutions]:
        """Process a batch of institutions and return those that were updated."""
        updated_institutions = []

        for institution in institutions:
            self.stats["processed"] += 1

            try:
                geo_result = self.search_geolocation_for_institution(institution)

                if not geo_result:
                    continue

                if not self.should_update_geolocation(institution, geo_result):
                    continue

                new_coords = parse_geolocation(
                    f"{geo_result['latitude']}, {geo_result['longitude']}",
                    swap_lat_lon=False,
                )

                institution.address_geolocation = new_coords
                institution.updated_at = datetime.now()

                updated_institutions.append(institution)
                self.stats["updated"] += 1

                logging.info(
                    f"Updated {institution.name} with {geo_result['source']} coordinates"
                )

            except Exception as e:
                self.stats["errors"] += 1
                logging.error(f"Error processing {institution.name}: {e}")

        return updated_institutions

    def upload_geolocations_to_db(self, updated_institutions: List[Institutions]):
        """Upload the updated institutions to the database."""
        if not updated_institutions:
            return

        with create_db_session()() as session:
            for institution in updated_institutions:
                session.merge(institution)
            session.commit()

    def run_geolocation_enrichment(
        self, batch_requester: BatchRequester, offset: int = 0, dry_run: bool = False
    ):
        """
        Args:
            batch_requester: BatchRequester instance configured for Institutions
            offset: Starting offset for processing
            dry_run: If True, don't commit changes to database
        """
        start_time = datetime.now()

        logging.info("STARTING GEOLOCATION BATCH ENRICHMENT PROCESS")
        logging.info(f"Batch size: {batch_requester.batch_size}")
        logging.info(f"Commit frequency: {self.commit_frequency}")

        if dry_run:
            logging.info(
                "RUNNING IN DRY RUN MODE - NO DATABASE CHANGES WILL BE MADE"
            )

        if offset > 0:
            logging.info(f"Skipping {offset} institutions.")

        total_count = batch_requester.get_total_count()
        logging.info(f"📊 Total institutions to process: {total_count}")

        batch_updates = []

        for batch_idx, institution_batch in enumerate(
            batch_requester.next_institution_batch(offset_start=offset)
        ):
            batch_start = datetime.now()

            updated_institutions = self.process_institution_batch(institution_batch)

            if updated_institutions:
                batch_updates.extend(updated_institutions)

            processing_time = datetime.now() - batch_start
            current_position = offset + (batch_idx + 1) * batch_requester.batch_size
            progress = min((current_position / total_count) * 100, 100.0)

            logging.info(
                f"Batch #{batch_idx + 1}: Processed {len(institution_batch)} institutions "
                f"in {processing_time.total_seconds():.1f}s. "
                f"Updated: {len(updated_institutions)}. "
                f"Progress: {progress:.1f}% ({current_position}/{total_count})"
            )

            if (
                not dry_run
                and (batch_idx + 1) % self.commit_frequency == 0
                and batch_updates
            ):
                self.upload_geolocations_to_db(batch_updates)
                logging.info(f"Committed {len(batch_updates)} updates to database")
                batch_updates = []

        if not dry_run and batch_updates:
            self.upload_geolocations_to_db(batch_updates)
            logging.info(f"Final commit: {len(batch_updates)} updates")

        self.log_final_statistics(start_time, dry_run)

    def log_final_statistics(self, start_time: datetime, dry_run: bool):
        """Log comprehensive statistics about the enrichment process."""
        end_time = datetime.now()
        duration = end_time - start_time

        logging.info("GEOLOCATION ENRICHMENT COMPLETED")
        logging.info(f"Duration: {duration}")
        logging.info(f"Institutions processed: {self.stats['processed']}")
        logging.info(f"Institutions updated: {self.stats['updated']}")
        logging.info(f"OpenAlex hits: {self.stats['openalex_hits']}")
        logging.info(f"Mapbox hits: {self.stats['mapbox_hits']}")
        logging.info(f"No results found: {self.stats['no_results']}")
        logging.info(f"Errors encountered: {self.stats['errors']}")

        if self.stats["processed"] > 0:
            success_rate = (self.stats["updated"] / self.stats["processed"]) * 100
            logging.info(f"📈 Success rate: {success_rate:.1f}%")

        if dry_run:
            logging.info("🔍 DRY RUN COMPLETED - No changes were made to the database")
        else:
            logging.info("💾 All changes have been committed to the database")


if __name__ == "__main__":

    setup_logging("enrichment", "geolocation_batch_update")
    logging.info("Starting geolocation enrichment.")

    batch_size = 32

    filter_condition = Institutions.address_geolocation.is_(None)
    batch_requester = BatchRequester(
        Institutions, batch_size=batch_size, filter_condition=filter_condition
    )

    processor = GeolocationBatchProcessor(batch_size=batch_size, commit_frequency=5)
    processor.run_geolocation_enrichment(batch_requester, offset=0, dry_run=False)
