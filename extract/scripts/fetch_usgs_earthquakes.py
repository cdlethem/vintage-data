#!/usr/bin/env python3
"""Fetch the USGS all-day earthquake GeoJSON feed as NDJSON.

The official feed is a single, current FeatureCollection. Polling it captures both
new earthquake records and revisions to records already seen. Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

SOURCE = "usgs_earthquakes"
URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
DEFAULT_TIMEOUT_SECONDS = 30
MAX_TIMEOUT_SECONDS = 60
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)


def normalize_features(document, fetched_at):
    """Validate a USGS FeatureCollection and add extraction metadata to features."""
    if not isinstance(document, dict) or document.get("type") != "FeatureCollection":
        raise ValueError("USGS response is not a GeoJSON FeatureCollection")

    features = document.get("features")
    if not isinstance(features, list):
        raise ValueError("USGS GeoJSON FeatureCollection is missing a features list")

    for index, feature in enumerate(features):
        if not isinstance(feature, dict) or feature.get("type") != "Feature":
            raise ValueError(f"USGS feature {index} is not a GeoJSON Feature")

        event_id = feature.get("id")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError(f"USGS feature {index} is missing its event id")

        if not isinstance(feature.get("properties"), dict):
            raise ValueError(f"USGS feature {event_id} is missing properties")

        geometry = feature.get("geometry")
        if not isinstance(geometry, dict) or geometry.get("type") != "Point":
            raise ValueError(f"USGS feature {event_id} is missing point geometry")
        coordinates = geometry.get("coordinates")
        if not isinstance(coordinates, list) or len(coordinates) < 2:
            raise ValueError(f"USGS feature {event_id} has invalid point coordinates")

        record = dict(feature)
        record.update({"id": event_id, "source": SOURCE, "fetched_at": fetched_at})
        yield record


def fetch_features(timeout=DEFAULT_TIMEOUT_SECONDS):
    """Request the one USGS all-day feed and yield normalized feature records."""
    if not 1 <= timeout <= MAX_TIMEOUT_SECONDS:
        raise ValueError(
            f"timeout must be between 1 and {MAX_TIMEOUT_SECONDS} seconds"
        )

    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    yield from normalize_features(document, fetched_at)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS,
        help=f"request timeout in seconds (1-{MAX_TIMEOUT_SECONDS})",
    )
    args = parser.parse_args()
    for record in fetch_features(args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
