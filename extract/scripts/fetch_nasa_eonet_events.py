#!/usr/bin/env python3
"""Snapshot open NASA EONET events from the v3 GeoJSON endpoint as NDJSON.

The endpoint emits one GeoJSON feature per event observation. Event metadata and
observation fields are in ``feature.properties`` while the observation geometry is the
feature's top-level ``geometry``. A run makes one bounded request; this endpoint does
not document a pagination contract.

Locations are suitable for general reference, not authoritative emergency response or
precise event boundaries.

Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "nasa_eonet_events"
URL = "https://eonet.gsfc.nasa.gov/api/v3/events/geojson"
DEFAULT_LIMIT = 100
MAX_LIMIT = 200
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
EVENT_ID_PATTERN = re.compile(r"EONET_[0-9]+")
ATTRIBUTION = "NASA Earth Observatory Natural Event Tracker (EONET)"
PRECISION_CAVEAT = (
    "General-reference event locations from source reports; not authoritative for "
    "emergency response or precise event boundaries."
)


def normalize_feature(feature: Any, fetched_at: str) -> dict[str, Any]:
    """Validate and normalize one EONET GeoJSON observation feature."""
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ValueError("EONET collection contains an invalid GeoJSON feature")

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError("EONET feature has invalid properties")

    event_id = properties.get("id")
    if not isinstance(event_id, str) or EVENT_ID_PATTERN.fullmatch(event_id) is None:
        raise ValueError("EONET feature properties.id must match EONET_<digits>")

    title = properties.get("title")
    if not isinstance(title, str) or not title:
        raise ValueError(f"EONET event {event_id!r} has an invalid title")

    categories = properties.get("categories")
    if not isinstance(categories, list):
        raise ValueError(f"EONET event {event_id!r} has invalid categories")

    sources = properties.get("sources")
    if not isinstance(sources, list):
        raise ValueError(f"EONET event {event_id!r} has invalid sources")
    source_links: list[str] = []
    for source in sources:
        if not isinstance(source, dict):
            raise ValueError(f"EONET event {event_id!r} has an invalid source")
        source_url = source.get("url")
        if not isinstance(source_url, str) or not source_url:
            raise ValueError(f"EONET event {event_id!r} has an invalid source URL")
        source_links.append(source_url)

    geometry = feature.get("geometry")
    if not isinstance(geometry, dict):
        raise ValueError(f"EONET event {event_id!r} has invalid geometry")
    if not isinstance(geometry.get("type"), str):
        raise ValueError(f"EONET event {event_id!r} has invalid geometry type")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list):
        raise ValueError(f"EONET event {event_id!r} has invalid coordinates")

    observation_date = geometry.get("date")
    if not isinstance(observation_date, str) or not observation_date:
        raise ValueError(f"EONET event {event_id!r} has an invalid observation date")

    magnitude_value = geometry.get("magnitudeValue")
    if magnitude_value is not None and (
        not isinstance(magnitude_value, (int, float))
        or isinstance(magnitude_value, bool)
    ):
        raise ValueError(f"EONET event {event_id!r} has an invalid magnitude value")
    magnitude_unit = geometry.get("magnitudeUnit")
    if magnitude_unit is not None and not isinstance(magnitude_unit, str):
        raise ValueError(f"EONET event {event_id!r} has an invalid magnitude unit")

    return {
        "source": SOURCE,
        "id": event_id,
        "fetched_at": fetched_at,
        "title": title,
        "categories": categories,
        "source_links": source_links,
        "event_link": properties.get("link"),
        "date": observation_date,
        "magnitude_value": magnitude_value,
        "magnitude_unit": magnitude_unit,
        "geometry": geometry,
        "coordinates": coordinates,
        "attribution": ATTRIBUTION,
        "precision_caveat": PRECISION_CAVEAT,
        "raw": feature,
    }


def fetch_events(
    limit: int = DEFAULT_LIMIT, timeout: int = 60
) -> Iterator[dict[str, Any]]:
    """Fetch one bounded collection of currently open EONET observations."""
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ValueError("timeout must be a positive integer")

    query = urllib.parse.urlencode({"status": "open", "limit": limit})
    request = urllib.request.Request(
        f"{URL}?{query}",
        headers={
            "Accept": "application/geo+json",
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    if not isinstance(document, dict):
        raise ValueError("EONET response is not an object")
    if document.get("type") != "FeatureCollection":
        raise ValueError("EONET response is not a GeoJSON FeatureCollection")
    features = document.get("features")
    if not isinstance(features, list):
        raise ValueError("EONET FeatureCollection is missing features")

    for feature in features:
        yield normalize_feature(feature, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)

    for record in fetch_events(limit=args.limit, timeout=args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
