#!/usr/bin/env python3
"""Snapshot the GDACS recent disaster events as NDJSON.

GDACS (a UN/European Commission cooperation framework) tracks sudden-onset
disaster events — earthquakes, tropical cyclones, floods, droughts,
volcanoes and wildfires — with an alert level and the affected
country/region. The documented EVENTS4APP endpoint returns the cached
collection of the latest events (at most 100), regenerated whenever the
catalog changes, so new events appear and alert levels change between
snapshots.

The API features carry no top-level feature id; each record's id is
composed from the stable source fields eventtype, eventid, episodeid and
polygonlabel, e.g. "EQ15744891743419Centroid".

Each run makes one bounded request and preserves each event's properties
and centroid geometry. Locations are suitable for general reference, not
authoritative emergency response or precise event boundaries.

Stdlib only.
"""

import argparse
from datetime import datetime, timezone
import json
import math
import os
from typing import Any, Iterator, Sequence
import urllib.request


SOURCE = "gdacs_active_events"
URL = "https://www.gdacs.org/gdacsapi/api/events/geteventlist/EVENTS4APP"
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 300
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
GEOMETRY_TYPES = {
    "Point",
    "MultiPoint",
    "LineString",
    "MultiLineString",
    "Polygon",
    "MultiPolygon",
    "GeometryCollection",
}


def _validate_ordinate(ordinate: Any, feature_id: str) -> None:
    if isinstance(ordinate, bool):
        raise ValueError(
            f"GDACS event {feature_id!r} geometry must contain only "
            "finite, non-boolean numeric ordinates"
        )
    if isinstance(ordinate, int):
        return
    if isinstance(ordinate, float) and math.isfinite(ordinate):
        return
    raise ValueError(
        f"GDACS event {feature_id!r} geometry must contain only "
        "finite, non-boolean numeric ordinates"
    )


def _validate_coordinates(coordinates: Any, feature_id: str) -> None:
    """Validate a (possibly nested) coordinate list of finite numbers."""
    if not isinstance(coordinates, list) or not coordinates:
        raise ValueError(
            f"GDACS event {feature_id!r} geometry must carry a non-empty "
            "list of coordinates"
        )
    for element in coordinates:
        if isinstance(element, list):
            _validate_coordinates(element, feature_id)
        else:
            _validate_ordinate(element, feature_id)


def _validate_geometry(geometry: Any, feature_id: str) -> None:
    if not isinstance(geometry, dict):
        raise ValueError(
            f"GDACS event {feature_id!r} must carry a GeoJSON geometry"
        )
    geometry_type = geometry.get("type")
    if geometry_type not in GEOMETRY_TYPES:
        raise ValueError(
            f"GDACS event {feature_id!r} geometry type {geometry_type!r} "
            "is not a GeoJSON geometry"
        )
    if geometry_type == "GeometryCollection":
        geometries = geometry.get("geometries")
        if not isinstance(geometries, list):
            raise ValueError(
                f"GDACS event {feature_id!r} GeometryCollection is missing "
                "geometries"
            )
        for member in geometries:
            _validate_geometry(member, feature_id)
        return
    _validate_coordinates(geometry.get("coordinates"), feature_id)


def _feature_id(properties: dict[str, Any]) -> str:
    """Compose the stable source id from the event's identifying fields."""
    eventtype = properties.get("eventtype")
    eventid = properties.get("eventid")
    episodeid = properties.get("episodeid")
    polygonlabel = properties.get("polygonlabel")
    if not isinstance(eventtype, str) or not eventtype:
        raise ValueError(
            "GDACS event feature is missing its stable source id: "
            "eventtype must be a non-empty string"
        )
    for field, value in (("eventid", eventid), ("episodeid", episodeid)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(
                f"GDACS event feature is missing its stable source id: "
                f"{field} must be an integer"
            )
    if not isinstance(polygonlabel, str) or not polygonlabel:
        raise ValueError(
            "GDACS event feature is missing its stable source id: "
            "polygonlabel must be a non-empty string"
        )
    return f"{eventtype}{eventid}{episodeid}{polygonlabel}"


def normalize_feature(feature: Any, fetched_at: str) -> dict[str, Any]:
    """Validate one GDACS GeoJSON feature without changing source values."""
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise ValueError("GDACS collection contains an invalid GeoJSON feature")

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise ValueError("GDACS event feature has invalid properties")

    feature_id = _feature_id(properties)
    _validate_geometry(feature.get("geometry"), feature_id)

    return {
        "source": SOURCE,
        "id": feature_id,
        "fetched_at": fetched_at,
        "properties": properties,
        "geometry": feature["geometry"],
    }


def fetch_events(timeout: int = DEFAULT_TIMEOUT) -> Iterator[dict[str, Any]]:
    """Fetch the recent-event collection in one bounded request."""
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")

    request = urllib.request.Request(
        URL,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    if not isinstance(document, dict):
        raise ValueError("GDACS response is not an object")
    if document.get("type") != "FeatureCollection":
        raise ValueError("GDACS response is not a GeoJSON FeatureCollection")
    features = document.get("features")
    if not isinstance(features, list):
        raise ValueError("GDACS FeatureCollection is missing features")

    for feature in features:
        yield normalize_feature(feature, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    for record in fetch_events(timeout=args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
