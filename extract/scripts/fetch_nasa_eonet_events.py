#!/usr/bin/env python3
"""Fetch open NASA EONET events as NDJSON.

EONET is useful for event discovery and general reference. Its event geometries and
observation times are not authoritative spatial or temporal measurements. Each output
record retains the complete EONET GeoJSON feature and NASA/source attribution.

Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.parse
import urllib.request
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from typing import Any

EVENTS_URL = "https://eonet.gsfc.nasa.gov/api/v3/events/geojson"
DEFAULT_LIMIT = 50
MAX_LIMIT = 100
DEFAULT_MAX_PAGES = 1
MAX_PAGES = 3
EVENT_ID = re.compile(r"EONET_[A-Za-z0-9_-]+\Z")
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
NASA_ATTRIBUTION = "NASA Earth Observatory Natural Event Tracker (EONET)"


def _request_url(limit: int, page: int) -> str:
    query = urllib.parse.urlencode({"status": "open", "limit": limit, "page": page})
    return f"{EVENTS_URL}?{query}"


def _load_page(limit: int, page: int, timeout: int) -> Mapping[str, Any]:
    request = urllib.request.Request(
        _request_url(limit, page),
        headers={"Accept": "application/geo+json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    if not isinstance(document, Mapping):
        raise ValueError("EONET response is not a JSON object")
    features = document.get("features")
    if document.get("type") != "FeatureCollection" or not isinstance(features, list):
        raise ValueError("EONET response is not a GeoJSON FeatureCollection")
    return document


def _event_observation(properties: Mapping[str, Any]) -> Mapping[str, Any]:
    observations = properties.get("geometry")
    if observations is None:
        return {}
    if not isinstance(observations, list) or any(
        not isinstance(observation, Mapping) for observation in observations
    ):
        raise ValueError("EONET feature has malformed geometry observations")
    return observations[-1] if observations else {}


def _coordinates(feature: Mapping[str, Any], observation: Mapping[str, Any]) -> Any:
    if "coordinates" in observation:
        return observation["coordinates"]
    geometry = feature.get("geometry")
    if geometry is None:
        return None
    if not isinstance(geometry, Mapping):
        raise ValueError("EONET feature has malformed GeoJSON geometry")
    return geometry.get("coordinates")


def normalize_feature(feature: Any, fetched_at: str) -> dict[str, Any]:
    """Return one stable event record, rejecting malformed EONET features."""
    if not isinstance(feature, Mapping) or feature.get("type") != "Feature":
        raise ValueError("EONET response contains a non-Feature entry")
    event_id = feature.get("id")
    if not isinstance(event_id, str) or not EVENT_ID.fullmatch(event_id):
        raise ValueError("EONET feature is missing a valid EONET event ID")
    properties = feature.get("properties")
    if not isinstance(properties, Mapping):
        raise ValueError(f"EONET event {event_id} has malformed properties")
    title = properties.get("title")
    if not isinstance(title, str) or not title:
        raise ValueError(f"EONET event {event_id} is missing a title")
    categories = properties.get("categories", [])
    sources = properties.get("sources", [])
    if not isinstance(categories, list) or not isinstance(sources, list):
        raise ValueError(f"EONET event {event_id} has malformed categories or sources")

    observation = _event_observation(properties)
    magnitude = {
        "value": observation.get("magnitudeValue"),
        "unit": observation.get("magnitudeUnit"),
    }
    source_links = [
        source["url"]
        for source in sources
        if isinstance(source, Mapping) and isinstance(source.get("url"), str)
    ]
    return {
        "source": "nasa_eonet_events",
        "attribution": NASA_ATTRIBUTION,
        "fetched_at": fetched_at,
        "id": event_id,
        "title": title,
        "date": observation.get("date"),
        "magnitude": magnitude,
        "categories": categories,
        "source_links": source_links,
        "event_url": properties.get("link"),
        "coordinates": _coordinates(feature, observation),
        "raw": feature,
    }


def fetch_events(
    limit: int = DEFAULT_LIMIT,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: int = 60,
) -> Iterator[dict[str, Any]]:
    """Yield open EONET events, bounded by both request size and page count."""
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    if not 1 <= max_pages <= MAX_PAGES:
        raise ValueError(f"max_pages must be between 1 and {MAX_PAGES}")
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    fetched_at = datetime.now(timezone.utc).isoformat()
    seen_ids: set[str] = set()
    for page in range(1, max_pages + 1):
        features = _load_page(limit, page, timeout)["features"]
        for feature in features:
            record = normalize_feature(feature, fetched_at)
            if record["id"] not in seen_ids:
                seen_ids.add(record["id"])
                yield record
        if len(features) < limit:
            return


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch open NASA EONET events as NDJSON")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_events(args.limit, args.max_pages, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
