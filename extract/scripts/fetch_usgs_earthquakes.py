#!/usr/bin/env python3
"""Fetch the official USGS all-earthquakes daily GeoJSON feed.

The feed is a complete rolling 24-hour snapshot delivered in one request. Repeated
polls intentionally emit the same stable event IDs with updated properties so later
pipeline stages can retain revisions. Empty FeatureCollections are valid.

Stdlib only.
"""

import argparse
import json
import math
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator

SOURCE = "usgs_earthquakes"
URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120


def _validate_timeout(timeout: float) -> None:
    if (
        not isinstance(timeout, (int, float))
        or isinstance(timeout, bool)
        or not math.isfinite(timeout)
    ):
        raise ValueError("timeout must be a finite number")
    if timeout <= 0 or timeout > MAX_TIMEOUT:
        raise ValueError(
            f"timeout must be greater than 0 and at most {MAX_TIMEOUT} seconds"
        )


def _record(feature: Any, index: int, fetched_at: str) -> dict[str, Any]:
    label = f"feature at index {index}"
    if not isinstance(feature, dict):
        raise ValueError(f"{label} is not an object")
    if feature.get("type") != "Feature":
        raise ValueError(f"{label} is not a GeoJSON Feature")

    event_id = feature.get("id")
    if event_id is None or event_id == "":
        raise ValueError(f"{label} is missing the USGS event id")
    if not isinstance(event_id, (str, int)) or isinstance(event_id, bool):
        raise ValueError(f"{label} has an invalid USGS event id")

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError(f"{label} has invalid properties")

    geometry = feature.get("geometry")
    if not isinstance(geometry, dict) or geometry.get("type") != "Point":
        raise ValueError(f"{label} does not have Point geometry")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise ValueError(f"{label} has invalid Point coordinates")

    record = dict(feature)
    record.update(
        {
            "id": str(event_id),
            "source": SOURCE,
            "fetched_at": fetched_at,
        }
    )
    return record


def fetch_earthquakes(timeout: float = DEFAULT_TIMEOUT) -> Iterator[dict[str, Any]]:
    """Yield normalized earthquake records from one complete daily-feed request."""
    _validate_timeout(timeout)
    request = urllib.request.Request(
        URL,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/geo+json, application/json",
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    if not isinstance(document, dict):
        raise ValueError("USGS response is not a JSON object")
    features = document.get("features")
    if document.get("type") != "FeatureCollection" or not isinstance(features, list):
        raise ValueError(
            "USGS response is not a GeoJSON FeatureCollection with a features list"
        )

    for index, feature in enumerate(features):
        yield _record(feature, index, fetched_at)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    args = parser.parse_args()
    for record in fetch_earthquakes(args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
