#!/usr/bin/env python3
"""Snapshot the official USGS all-day earthquake feed as NDJSON.

The feed is a single GeoJSON FeatureCollection. Each run makes one bounded request,
preserves each event's properties and Point geometry, and uses the stable USGS feature
ID as the record ID.

Stdlib only.
"""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from typing import Any, Iterator, Sequence
import urllib.request


SOURCE = "usgs_earthquakes"
URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)


def _validate_coordinates(coordinates: Any, earthquake_id: str) -> None:
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise ValueError(
            f"USGS earthquake {earthquake_id!r} coordinates must be a list "
            "with at least two ordinates"
        )

    for ordinate in coordinates:
        # bool is an int subclass. Check it first, and do not coerce integers to
        # float: valid JSON integers can be larger than the float range.
        if isinstance(ordinate, bool):
            break
        if isinstance(ordinate, int):
            continue
        if isinstance(ordinate, float) and math.isfinite(ordinate):
            continue
        break
    else:
        return

    raise ValueError(
        f"USGS earthquake {earthquake_id!r} coordinates must contain only "
        "finite, non-boolean numeric ordinates"
    )


def normalize_feature(feature: Any, fetched_at: str) -> dict[str, Any]:
    """Validate one USGS GeoJSON feature without changing source values."""
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ValueError("USGS collection contains an invalid GeoJSON feature")

    earthquake_id = feature.get("id")
    if not isinstance(earthquake_id, str) or not earthquake_id:
        raise ValueError("USGS earthquake feature is missing its stable source id")

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError(f"USGS earthquake {earthquake_id!r} has invalid properties")

    geometry = feature.get("geometry")
    if not isinstance(geometry, dict) or geometry.get("type") != "Point":
        raise ValueError(
            f"USGS earthquake {earthquake_id!r} geometry must be a GeoJSON Point"
        )
    _validate_coordinates(geometry.get("coordinates"), earthquake_id)

    return {
        "source": SOURCE,
        "id": earthquake_id,
        "fetched_at": fetched_at,
        "properties": properties,
        "geometry": geometry,
    }


def fetch_earthquakes(timeout: int = DEFAULT_TIMEOUT) -> Iterator[dict[str, Any]]:
    """Fetch the official all-day feed in one bounded request."""
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")

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
        raise ValueError("USGS FeatureCollection is missing features")

    for feature in features:
        yield normalize_feature(feature, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    for record in fetch_earthquakes(timeout=args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
