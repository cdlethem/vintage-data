#!/usr/bin/env python3
"""Snapshot the official USGS all-day earthquake GeoJSON feed as NDJSON.

The all-day feed is a complete, non-paginated FeatureCollection. Each run makes one
bounded request and preserves each valid Point feature's properties and geometry.

Stdlib only.
"""

import argparse
import json
import math
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "usgs_earthquakes"
URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)


def normalize_feature(feature: Any, fetched_at: str) -> dict[str, Any]:
    """Validate one USGS GeoJSON feature and add extraction metadata."""
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ValueError("USGS collection contains an invalid GeoJSON feature")

    event_id = feature.get("id")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("USGS earthquake feature is missing a valid id")

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError(f"USGS earthquake {event_id!r} has invalid properties")

    geometry = feature.get("geometry")
    if not isinstance(geometry, dict) or geometry.get("type") != "Point":
        raise ValueError(f"USGS earthquake {event_id!r} has invalid Point geometry")

    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise ValueError(
            f"USGS earthquake {event_id!r} coordinates must be a list with at least two ordinates"
        )
    for index, ordinate in enumerate(coordinates):
        if (
            not isinstance(ordinate, (int, float))
            or isinstance(ordinate, bool)
            or not math.isfinite(ordinate)
        ):
            raise ValueError(
                f"USGS earthquake {event_id!r} coordinate ordinate {index} "
                "must be a finite, non-boolean number"
            )

    record = dict(feature)
    record.update(
        {
            "id": event_id,
            "source": SOURCE,
            "fetched_at": fetched_at,
        }
    )
    return record


def fetch_earthquakes(timeout: int = 60) -> Iterator[dict[str, Any]]:
    """Fetch and validate one complete all-day earthquake collection."""
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("timeout must be a positive integer")

    request = urllib.request.Request(
        URL,
        headers={
            "Accept": "application/geo+json",
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    if not isinstance(document, dict):
        raise ValueError("USGS response is not an object")
    if document.get("type") != "FeatureCollection":
        raise ValueError("USGS response is not a GeoJSON FeatureCollection")
    features = document.get("features")
    if not isinstance(features, list):
        raise ValueError("USGS FeatureCollection is missing a features list")

    for feature in features:
        yield normalize_feature(feature, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)

    for record in fetch_earthquakes(timeout=args.timeout):
        print(json.dumps(record, ensure_ascii=False, allow_nan=False))


if __name__ == "__main__":
    main()
