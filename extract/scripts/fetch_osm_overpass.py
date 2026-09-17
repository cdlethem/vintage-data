#!/usr/bin/env python3
"""Fetch a small, explicitly filtered OpenStreetMap area through Overpass.

Each invocation makes one bounded HTTP POST and keeps no persistent state. Bounds
and tag filters are required; broad requests fail before network access. Overpass
and HTTP timeouts, response bytes, retry count, and retry delay are all finite.

OpenStreetMap data is available under ODbL 1.0. Attribute OpenStreetMap and its
contributors when using or redistributing this extract:
https://www.openstreetmap.org/copyright

After review, an authorized network-enabled operator can smoke-test the disabled
source with an overall 180-second limit using:

    python3 extract/scripts/fetch_osm_overpass.py --bbox 51.50,-0.13,51.51,-0.12 --tag amenity=drinking_water

The operator must record the tested commit, endpoint, timestamp, exit status, and
parsed nonempty NDJSON evidence. Empty output requires investigation, not a wider
query. Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
import socket
import time
from typing import Any, Sequence
import urllib.error
import urllib.parse
import urllib.request


SOURCE = "osm_overpass"
ENDPOINT = "https://overpass-api.de/api/interpreter"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_HTTP_TIMEOUT = 45.0
MAX_HTTP_TIMEOUT = 120.0
DEFAULT_QUERY_TIMEOUT = 25
MAX_QUERY_TIMEOUT = 60
DEFAULT_RETRIES = 2
MAX_RETRIES = 2
DEFAULT_RETRY_DELAY = 5.0
MAX_RETRY_DELAY = 30.0
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_TAGS = 5
MAX_TAG_KEY_LENGTH = 255
MAX_TAG_VALUE_LENGTH = 1024
MAX_LATITUDE_SPAN = 0.1
MAX_LONGITUDE_SPAN = 0.1
MAX_BBOX_AREA = 0.01
_RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})
_ELEMENT_TYPES = frozenset({"node", "way", "relation"})


class OverpassError(RuntimeError):
    """The Overpass request or response cannot satisfy the extractor contract."""


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise OverpassError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise OverpassError(f"{field} must be a finite number")
    return number


def parse_bbox(value: str | Sequence[float]) -> tuple[float, float, float, float]:
    """Parse and validate ``south,west,north,east`` as a small bounding box."""
    if isinstance(value, str):
        parts: Sequence[Any] = value.split(",")
    elif isinstance(value, Sequence):
        parts = value
    else:
        raise ValueError("bbox must be south,west,north,east")
    if len(parts) != 4:
        raise ValueError("bbox must contain south,west,north,east")
    try:
        coordinates = tuple(float(part) for part in parts)
    except (TypeError, ValueError) as error:
        raise ValueError("bbox coordinates must be finite numbers") from error
    if not all(math.isfinite(number) for number in coordinates):
        raise ValueError("bbox coordinates must be finite numbers")
    south, west, north, east = coordinates
    if not -90 <= south <= 90 or not -90 <= north <= 90:
        raise ValueError("bbox latitudes must be between -90 and 90")
    if not -180 <= west <= 180 or not -180 <= east <= 180:
        raise ValueError("bbox longitudes must be between -180 and 180")
    if south >= north or west >= east:
        raise ValueError("bbox must have south < north and west < east")
    latitude_span = north - south
    longitude_span = east - west
    if (
        latitude_span > MAX_LATITUDE_SPAN
        or longitude_span > MAX_LONGITUDE_SPAN
        or latitude_span * longitude_span > MAX_BBOX_AREA
    ):
        raise ValueError(
            "bbox is too large; latitude and longitude spans must each be at most "
            f"{MAX_LATITUDE_SPAN:g} degrees and area at most {MAX_BBOX_AREA:g} square degrees"
        )
    return south, west, north, east


def _validate_tag_part(name: str, text: Any, maximum: int) -> str:
    if not isinstance(text, str) or not text:
        raise ValueError(f"tag {name} must be a non-empty string")
    if len(text) > maximum:
        raise ValueError(f"tag {name} exceeds {maximum} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValueError(f"tag {name} contains a control character")
    if any(0xD800 <= ord(character) <= 0xDFFF for character in text):
        raise ValueError(f"tag {name} contains an invalid Unicode code point")
    return text


def parse_tag(value: str) -> tuple[str, str]:
    """Parse one explicit key=value filter without interpreting its contents as QL."""
    if not isinstance(value, str) or "=" not in value:
        raise ValueError("tag must use key=value syntax")
    key, tag_value = value.split("=", 1)
    return (
        _validate_tag_part("key", key, MAX_TAG_KEY_LENGTH),
        _validate_tag_part("value", tag_value, MAX_TAG_VALUE_LENGTH),
    )


def validate_tags(values: Sequence[str | tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError("tags must be a sequence of key=value filters")
    if not values:
        raise ValueError("at least one --tag key=value filter is required")
    if len(values) > MAX_TAGS:
        raise ValueError(f"at most {MAX_TAGS} tag filters may be requested")
    tags: list[tuple[str, str]] = []
    for value in values:
        if isinstance(value, str):
            tag = parse_tag(value)
        elif isinstance(value, tuple) and len(value) == 2:
            tag = (
                _validate_tag_part("key", value[0], MAX_TAG_KEY_LENGTH),
                _validate_tag_part("value", value[1], MAX_TAG_VALUE_LENGTH),
            )
        else:
            raise ValueError("tags must contain key=value strings or key/value pairs")
        if tag in tags:
            raise ValueError("tag filters must not contain duplicates")
        tags.append(tag)
    return tuple(tags)


def _ql_string(value: str) -> str:
    # JSON and Overpass QL use the same quoted-string escapes needed here. The
    # returned token always includes quotes, so user text cannot add QL syntax.
    return json.dumps(value, ensure_ascii=False)


def build_query(
    bbox: tuple[float, float, float, float],
    tags: Sequence[tuple[str, str]],
    query_timeout: int,
) -> str:
    if isinstance(query_timeout, bool) or not isinstance(query_timeout, int):
        raise ValueError("query_timeout must be an integer")
    if not 1 <= query_timeout <= MAX_QUERY_TIMEOUT:
        raise ValueError(f"query_timeout must be between 1 and {MAX_QUERY_TIMEOUT} seconds")
    south, west, north, east = bbox
    bounds = ",".join(format(number, ".15g") for number in (south, west, north, east))
    filters = "".join(f"[{_ql_string(key)}={_ql_string(value)}]" for key, value in tags)
    return (
        f"[out:json][timeout:{query_timeout}][maxsize:{MAX_RESPONSE_BYTES}];\n"
        f"nwr{filters}({bounds});\n"
        "out body geom;"
    )


def _validated_request_options(
    http_timeout: float, retries: int, retry_delay: float
) -> tuple[float, int, float]:
    if isinstance(http_timeout, bool) or not isinstance(http_timeout, (int, float)):
        raise ValueError("http_timeout must be a finite number")
    timeout = float(http_timeout)
    if not math.isfinite(timeout) or not 1 <= timeout <= MAX_HTTP_TIMEOUT:
        raise ValueError(f"http_timeout must be between 1 and {MAX_HTTP_TIMEOUT:g} seconds")
    if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= MAX_RETRIES:
        raise ValueError(f"retries must be an integer between 0 and {MAX_RETRIES}")
    if isinstance(retry_delay, bool) or not isinstance(retry_delay, (int, float)):
        raise ValueError("retry_delay must be a finite number")
    delay = float(retry_delay)
    if not math.isfinite(delay) or not 0 <= delay <= MAX_RETRY_DELAY:
        raise ValueError(f"retry_delay must be between 0 and {MAX_RETRY_DELAY:g} seconds")
    return timeout, retries, delay


def _post_query(
    query: str,
    *,
    http_timeout: float,
    retries: int,
    retry_delay: float,
) -> Any:
    body = urllib.parse.urlencode({"data": query}).encode("ascii")
    request = urllib.request.Request(
        ENDPOINT,
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
            "User-Agent": USER_AGENT,
        },
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=http_timeout) as response:
                payload = response.read(MAX_RESPONSE_BYTES + 1)
            if len(payload) > MAX_RESPONSE_BYTES:
                raise OverpassError(
                    f"Overpass response exceeded the {MAX_RESPONSE_BYTES}-byte limit"
                )
            try:
                return json.loads(payload)
            except (json.JSONDecodeError, UnicodeDecodeError) as error:
                raise OverpassError("Overpass returned malformed JSON") from error
        except urllib.error.HTTPError as error:
            status = error.code
            error.close()
            if status not in _RETRYABLE_HTTP_CODES or attempt == retries:
                raise OverpassError(f"Overpass returned HTTP {status}") from error
        except (urllib.error.URLError, TimeoutError, socket.timeout) as error:
            if attempt == retries:
                reason = getattr(error, "reason", error)
                raise OverpassError(f"Overpass request failed: {reason}") from error
        if retry_delay:
            time.sleep(retry_delay * (attempt + 1))
    raise AssertionError("retry loop exhausted without returning or raising")


def _coordinate_pair(value: Any, context: str) -> dict[str, float]:
    if not isinstance(value, dict):
        raise OverpassError(f"{context} must be an object")
    latitude = _finite_number(value.get("lat"), f"{context}.lat")
    longitude = _finite_number(value.get("lon"), f"{context}.lon")
    if not -90 <= latitude <= 90 or not -180 <= longitude <= 180:
        raise OverpassError(f"{context} coordinates are outside valid ranges")
    return {"lat": latitude, "lon": longitude}


def _geometry(value: Any, context: str) -> list[dict[str, float]]:
    if not isinstance(value, list):
        raise OverpassError(f"{context} must be a list")
    return [_coordinate_pair(point, f"{context}[{index}]") for index, point in enumerate(value)]


def _bounds(value: Any, context: str) -> dict[str, float]:
    if not isinstance(value, dict):
        raise OverpassError(f"{context} must be an object")
    try:
        south = _finite_number(value["minlat"], f"{context}.minlat")
        west = _finite_number(value["minlon"], f"{context}.minlon")
        north = _finite_number(value["maxlat"], f"{context}.maxlat")
        east = _finite_number(value["maxlon"], f"{context}.maxlon")
    except KeyError as error:
        raise OverpassError(f"{context} is missing {error.args[0]}") from error
    if not (-90 <= south <= north <= 90 and -180 <= west <= east <= 180):
        raise OverpassError(f"{context} is invalid")
    return {"minlat": south, "minlon": west, "maxlat": north, "maxlon": east}


def _element_id(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise OverpassError(f"{context} must be a non-negative integer")
    return value


def normalize_element(element: Any, fetched_at: str) -> dict[str, Any]:
    if not isinstance(element, dict):
        raise OverpassError("Overpass element must be an object")
    element_type = element.get("type")
    if element_type not in _ELEMENT_TYPES:
        raise OverpassError(f"Overpass element has invalid type {element_type!r}")
    osm_id = _element_id(element.get("id"), f"Overpass {element_type} id")
    tags = element.get("tags")
    if not isinstance(tags, dict) or any(
        not isinstance(key, str) or not isinstance(value, str) for key, value in tags.items()
    ):
        raise OverpassError(f"Overpass {element_type}/{osm_id} has invalid tags")

    record: dict[str, Any] = {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{element_type}/{osm_id}",
        "element_type": element_type,
        "osm_id": osm_id,
        "tags": dict(tags),
    }
    context = f"Overpass {element_type}/{osm_id}"
    if element_type == "node":
        point = _coordinate_pair(element, context)
        record.update(point)
    elif element_type == "way":
        nodes = element.get("nodes")
        if not isinstance(nodes, list):
            raise OverpassError(f"{context}.nodes must be a list")
        record["nodes"] = [
            _element_id(node_id, f"{context}.nodes[{index}]")
            for index, node_id in enumerate(nodes)
        ]
        record["geometry"] = _geometry(element.get("geometry"), f"{context}.geometry")
    else:
        members = element.get("members")
        if not isinstance(members, list):
            raise OverpassError(f"{context}.members must be a list")
        normalized_members: list[dict[str, Any]] = []
        for index, member in enumerate(members):
            member_context = f"{context}.members[{index}]"
            if not isinstance(member, dict):
                raise OverpassError(f"{member_context} must be an object")
            member_type = member.get("type")
            if member_type not in _ELEMENT_TYPES:
                raise OverpassError(f"{member_context} has invalid type")
            ref = _element_id(member.get("ref"), f"{member_context}.ref")
            role = member.get("role")
            if not isinstance(role, str):
                raise OverpassError(f"{member_context}.role must be a string")
            normalized = dict(member)
            normalized.update({"type": member_type, "ref": ref, "role": role})
            if "geometry" in member:
                normalized["geometry"] = _geometry(
                    member["geometry"], f"{member_context}.geometry"
                )
            if "lat" in member or "lon" in member:
                normalized.update(_coordinate_pair(member, member_context))
            normalized_members.append(normalized)
        record["members"] = normalized_members

    if "bounds" in element:
        record["bounds"] = _bounds(element["bounds"], f"{context}.bounds")
    if "center" in element:
        record["center"] = _coordinate_pair(element["center"], f"{context}.center")
    return record


def fetch_overpass(
    bbox: str | Sequence[float],
    tags: Sequence[str | tuple[str, str]],
    *,
    http_timeout: float = DEFAULT_HTTP_TIMEOUT,
    query_timeout: int = DEFAULT_QUERY_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    fetched_at: str | None = None,
) -> list[dict[str, Any]]:
    """Return one fully validated snapshot; no records escape on partial failure."""
    parsed_bbox = parse_bbox(bbox)
    parsed_tags = validate_tags(tags)
    timeout, retry_count, delay = _validated_request_options(
        http_timeout, retries, retry_delay
    )
    query = build_query(parsed_bbox, parsed_tags, query_timeout)
    document = _post_query(
        query,
        http_timeout=timeout,
        retries=retry_count,
        retry_delay=delay,
    )
    if not isinstance(document, dict):
        raise OverpassError("Overpass response must be an object")
    remark = document.get("remark")
    if remark not in (None, ""):
        if not isinstance(remark, str):
            raise OverpassError("Overpass response has an invalid remark")
        raise OverpassError(f"Overpass reported an error: {remark}")
    elements = document.get("elements")
    if not isinstance(elements, list):
        raise OverpassError("Overpass response.elements must be a list")
    timestamp = fetched_at or datetime.now(timezone.utc).isoformat()
    records = [normalize_element(element, timestamp) for element in elements]
    ids = [record["id"] for record in records]
    if len(ids) != len(set(ids)):
        raise OverpassError("Overpass response contains duplicate qualified element ids")
    return records


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch one small, tag-filtered OpenStreetMap snapshot through Overpass."
    )
    parser.add_argument(
        "--bbox",
        required=True,
        help="required south,west,north,east bounds; each span is at most 0.1 degrees",
    )
    parser.add_argument(
        "--tag",
        action="append",
        required=True,
        help="required key=value filter; repeat to combine filters",
    )
    parser.add_argument("--http-timeout", type=float, default=DEFAULT_HTTP_TIMEOUT)
    parser.add_argument("--query-timeout", type=int, default=DEFAULT_QUERY_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY)
    args = parser.parse_args(argv)
    try:
        records = fetch_overpass(
            args.bbox,
            args.tag,
            http_timeout=args.http_timeout,
            query_timeout=args.query_timeout,
            retries=args.retries,
            retry_delay=args.retry_delay,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except ValueError as error:
        parser.error(str(error))
    except OverpassError as error:
        parser.exit(1, f"Overpass extraction failed: {error}\n")


if __name__ == "__main__":
    main()
