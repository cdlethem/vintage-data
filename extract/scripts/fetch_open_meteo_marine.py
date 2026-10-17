#!/usr/bin/env python3
"""Fetch bounded hourly marine conditions from Open-Meteo's keyless Marine API.

Each request explicitly selects points, hourly variables, UTC, a past horizon,
and a forecast horizon. Points are sent in bounded batches. Every emitted row
represents one point and one valid hour (analysis or forecast), while
``fetched_at`` identifies the collection and therefore the model revision.
Open-Meteo does not supply a model-run issuance timestamp, so collection time
must not be presented as provider issuance time.

The default ten-point basket covers the open ocean basins (mid-Atlantic,
south Atlantic, north and south Pacific, southern Pacific, Indian Ocean,
Bay of Bengal) plus three semi-enclosed seas (Mediterranean, North Sea,
Barents Sea). All ten points and all six variables return complete 48-hour
series with the default window (verified 2026-10-17); nulls are preserved
as null if a point ever loses model coverage.

Marine data are licensed CC BY 4.0. Attribute Open-Meteo and its upstream
model providers. The public endpoint is intended for non-commercial use and
publishes access limits; this extractor's conservative hourly schedule and
batching stay far below those limits. Stdlib only.
"""

import argparse
import hashlib
import json
import math
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "open_meteo_marine"
API_URL = "https://marine-api.open-meteo.com/v1/marine"
PROVIDER_URL = "https://open-meteo.com/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

MAX_POINTS = 20
MAX_BATCH_SIZE = 10
MAX_PAST_DAYS = 2
MAX_FORECAST_DAYS = 2
DEFAULT_PAST_DAYS = 1
DEFAULT_FORECAST_DAYS = 1
DEFAULT_BATCH_SIZE = 10

HOURLY_VARIABLES = (
    "wave_height",
    "wave_period",
    "wave_direction",
    "wind_wave_height",
    "swell_wave_height",
    "sea_surface_temperature",
)
HOURLY_VARIABLE_ALLOWLIST = frozenset(HOURLY_VARIABLES)
POINT_ID_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


@dataclass(frozen=True)
class Point:
    """One explicitly configured marine observation point."""

    id: str
    name: str
    latitude: float
    longitude: float


DEFAULT_POINTS = (
    Point("mid_atlantic", "Mid-Atlantic", 40.0, -40.0),
    Point("south_atlantic", "South Atlantic", -30.0, -15.0),
    Point("north_pacific", "North Pacific", 30.0, -150.0),
    Point("south_pacific", "South Pacific", -30.0, -120.0),
    Point("southern_pacific", "Southern Pacific", -55.0, 140.0),
    Point("indian_ocean", "Indian Ocean", -20.0, 80.0),
    Point("bay_of_bengal", "Bay of Bengal", 10.0, 90.0),
    Point("mediterranean", "Mediterranean", 35.0, 15.0),
    Point("north_sea", "North Sea", 58.0, 3.0),
    Point("barents_sea", "Barents Sea", 75.0, 20.0),
)


class OpenMeteoAPIError(RuntimeError):
    """An error document returned by Open-Meteo."""


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{field} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be a finite number")
    return number


def validate_points(points: Sequence[Point]) -> tuple[Point, ...]:
    """Validate the request's explicit, bounded point list."""
    if isinstance(points, (str, bytes)):
        raise TypeError("points must be a sequence of Point objects")
    requested = tuple(points)
    if not requested:
        raise ValueError("at least one point is required")
    if len(requested) > MAX_POINTS:
        raise ValueError(f"at most {MAX_POINTS} points may be requested per run")

    seen = set()
    normalized = []
    for index, point in enumerate(requested):
        if not isinstance(point, Point):
            raise TypeError(f"point {index} must be a Point")
        point_id = point.id.strip() if isinstance(point.id, str) else ""
        name = point.name.strip() if isinstance(point.name, str) else ""
        if POINT_ID_RE.fullmatch(point_id) is None:
            raise ValueError(f"point {index} has invalid id {point.id!r}")
        if point_id in seen:
            raise ValueError(f"duplicate point id {point_id!r}")
        if not name:
            raise ValueError(f"point {point_id!r} must have a non-empty name")
        latitude = _finite_number(point.latitude, f"point {point_id!r} latitude")
        longitude = _finite_number(point.longitude, f"point {point_id!r} longitude")
        if not -90 <= latitude <= 90:
            raise ValueError(f"point {point_id!r} latitude must be between -90 and 90")
        if not -180 <= longitude <= 180:
            raise ValueError(f"point {point_id!r} longitude must be between -180 and 180")
        seen.add(point_id)
        normalized.append(Point(point_id, name, latitude, longitude))
    return tuple(normalized)


def validate_variables(variables: Sequence[str]) -> tuple[str, ...]:
    """Validate an explicit, duplicate-free subset of maintained hourly variables."""
    if isinstance(variables, (str, bytes)):
        raise TypeError("variables must be a sequence of hourly variable names")
    requested = tuple(variables)
    if not requested:
        raise ValueError("at least one hourly variable is required")
    if not all(isinstance(variable, str) for variable in requested):
        raise TypeError("hourly variable names must be strings")
    if len(set(requested)) != len(requested):
        raise ValueError("hourly variables must not contain duplicates")
    unsupported = sorted(set(requested) - HOURLY_VARIABLE_ALLOWLIST)
    if unsupported:
        raise ValueError(f"hourly variables are not allowlisted: {', '.join(unsupported)}")
    return requested


def _positive_number(value: Any, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) <= 0
    ):
        raise ValueError(f"{field} must be a positive finite number")
    return float(value)


def _utc_timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty timestamp string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} is not an ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must carry a UTC offset: {value!r}")
    return parsed.astimezone(timezone.utc).isoformat()


def _utc_valid_time(value: Any, field: str) -> str:
    """Parse one hourly valid time from a response already proven to be UTC.

    Open-Meteo returns offset-less ISO times for timezone=UTC requests
    (e.g. ``2026-10-16T00:00``); the response's ``utc_offset_seconds == 0``
    and ``timezone == "GMT"`` checks make that reading safe.
    """
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty timestamp string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} is not an ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _collection_timestamp(value: str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    return _utc_timestamp(value, "fetched_at")


def _stable_revision_id(point_id: str, valid_at: str, fetched_at: str) -> str:
    identity = f"{SOURCE}|{point_id}|{valid_at}|{fetched_at}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def _request_document(
    points: Sequence[Point],
    variables: Sequence[str],
    past_days: int,
    forecast_days: int,
    timeout: float,
) -> Any:
    parameters = {
        "latitude": ",".join(str(point.latitude) for point in points),
        "longitude": ",".join(str(point.longitude) for point in points),
        "hourly": ",".join(variables),
        "past_days": str(past_days),
        "forecast_days": str(forecast_days),
        "models": "best_match",
        "timezone": "UTC",
    }
    request = urllib.request.Request(
        API_URL + "?" + urllib.parse.urlencode(parameters),
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _response_documents(document: Any, expected: int) -> list[dict[str, Any]]:
    if isinstance(document, dict) and document.get("error") is True:
        reason = document.get("reason")
        raise OpenMeteoAPIError(f"Open-Meteo request failed: {reason or 'unknown error'}")
    if expected == 1 and isinstance(document, dict):
        documents = [document]
    elif isinstance(document, list):
        documents = document
    else:
        raise TypeError("Open-Meteo batch response must be a JSON list")
    if len(documents) != expected:
        raise ValueError(
            f"Open-Meteo returned {len(documents)} point responses; expected {expected}"
        )
    for index, item in enumerate(documents):
        if not isinstance(item, dict):
            raise TypeError(f"Open-Meteo point response {index} must be an object")
        if item.get("error") is True:
            reason = item.get("reason")
            raise OpenMeteoAPIError(
                f"Open-Meteo point response {index} failed: {reason or 'unknown error'}"
            )
    return documents


def _response_coordinate(document: dict[str, Any], field: str, low: float, high: float) -> float:
    if field not in document:
        raise ValueError(f"Open-Meteo response is missing {field}")
    value = _finite_number(document[field], f"Open-Meteo response {field}")
    if not low <= value <= high:
        raise ValueError(f"Open-Meteo response {field} is outside valid bounds")
    return value


def _validate_unit(value: Any, variable: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Open-Meteo hourly_units[{variable!r}] must be a non-empty string"
        )
    return value.strip()


def _normalize_point_response(
    document: dict[str, Any],
    point: Point,
    variables: Sequence[str],
    fetched_at: str,
) -> Iterator[dict[str, Any]]:
    hourly = document.get("hourly")
    if not isinstance(hourly, dict):
        raise TypeError("Open-Meteo response is missing hourly object")
    hourly_units = document.get("hourly_units")
    if not isinstance(hourly_units, dict):
        raise TypeError("Open-Meteo response is missing hourly_units object")
    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise TypeError("Open-Meteo hourly.time must be a non-empty list")

    # Validate every configured unit and complete series before yielding anything.
    units = {variable: _validate_unit(hourly_units.get(variable), variable) for variable in variables}
    series = {}
    for variable in variables:
        values = hourly.get(variable)
        if not isinstance(values, list):
            raise TypeError(f"Open-Meteo hourly.{variable} must be a list")
        if len(values) != len(times):
            raise ValueError(
                f"Open-Meteo hourly.{variable} has {len(values)} values; expected {len(times)}"
            )
        for index, value in enumerate(values):
            if value is not None:
                _finite_number(value, f"Open-Meteo hourly.{variable}[{index}]")
        series[variable] = values

    # Prove the response is a UTC document before reading its offset-less times.
    utc_offset_seconds = document.get("utc_offset_seconds")
    if isinstance(utc_offset_seconds, bool) or not isinstance(utc_offset_seconds, int):
        raise TypeError("Open-Meteo response utc_offset_seconds must be an integer")
    if utc_offset_seconds != 0:
        raise ValueError("Open-Meteo response is not UTC despite timezone=UTC request")
    response_timezone = document.get("timezone")
    if response_timezone != "GMT":
        raise ValueError("Open-Meteo response timezone must be GMT for a UTC request")
    timezone_abbreviation = document.get("timezone_abbreviation")
    if not isinstance(timezone_abbreviation, str) or not timezone_abbreviation.strip():
        raise ValueError("Open-Meteo response timezone_abbreviation must be a non-empty string")

    valid_times = [
        _utc_valid_time(value, f"Open-Meteo hourly.time[{index}]")
        for index, value in enumerate(times)
    ]
    if len(set(valid_times)) != len(valid_times):
        raise ValueError("Open-Meteo hourly.time contains duplicate valid times")

    latitude = _response_coordinate(document, "latitude", -90, 90)
    longitude = _response_coordinate(document, "longitude", -180, 180)
    elevation = document.get("elevation")
    if elevation is not None:
        elevation = _finite_number(elevation, "Open-Meteo response elevation")

    provider = {
        "name": "Open-Meteo",
        "url": PROVIDER_URL,
        "model": "best_match",
    }
    for index, valid_at in enumerate(valid_times):
        marine = {variable: series[variable][index] for variable in variables}
        yield {
            "id": _stable_revision_id(point.id, valid_at, fetched_at),
            "source": SOURCE,
            "fetched_at": fetched_at,
            "valid_at": valid_at,
            "point_id": point.id,
            "point_name": point.name,
            "requested_latitude": point.latitude,
            "requested_longitude": point.longitude,
            "latitude": latitude,
            "longitude": longitude,
            "elevation": elevation,
            "timezone": response_timezone,
            "timezone_abbreviation": timezone_abbreviation.strip(),
            "utc_offset_seconds": utc_offset_seconds,
            "provider": dict(provider),
            "units": dict(units),
            "marine": marine,
        }


def fetch_marine(
    points: Sequence[Point] = DEFAULT_POINTS,
    variables: Sequence[str] = HOURLY_VARIABLES,
    *,
    past_days: int = DEFAULT_PAST_DAYS,
    forecast_days: int = DEFAULT_FORECAST_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout: float = 30,
    request_delay: float = 1.0,
    fetched_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch explicit points in bounded batches and yield hourly marine revisions."""
    requested_points = validate_points(points)
    requested_variables = validate_variables(variables)
    if isinstance(past_days, bool) or not isinstance(past_days, int):
        raise ValueError("past_days must be an integer")
    if not 0 <= past_days <= MAX_PAST_DAYS:
        raise ValueError(f"past_days must be between 0 and {MAX_PAST_DAYS}")
    if isinstance(forecast_days, bool) or not isinstance(forecast_days, int):
        raise ValueError("forecast_days must be an integer")
    if not 0 <= forecast_days <= MAX_FORECAST_DAYS:
        raise ValueError(f"forecast_days must be between 0 and {MAX_FORECAST_DAYS}")
    if past_days + forecast_days < 1:
        raise ValueError("at least one day of history or forecast is required")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise ValueError("batch_size must be an integer")
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError(f"batch_size must be between 1 and {MAX_BATCH_SIZE}")
    timeout = _positive_number(timeout, "timeout")
    if isinstance(request_delay, bool) or not isinstance(request_delay, (int, float)):
        raise ValueError("request_delay must be a non-negative number")
    if not math.isfinite(request_delay) or request_delay < 0:
        raise ValueError("request_delay must be a non-negative number")

    collection_time = _collection_timestamp(fetched_at)
    for start in range(0, len(requested_points), batch_size):
        if start and request_delay:
            time.sleep(request_delay)
        batch = requested_points[start : start + batch_size]
        document = _request_document(batch, requested_variables, past_days, forecast_days, timeout)
        responses = _response_documents(document, len(batch))
        for point, response in zip(batch, responses):
            yield from _normalize_point_response(
                response, point, requested_variables, collection_time
            )


def _run_summary(
    records: int,
    points: Sequence[Point],
    variables: Sequence[str],
    past_days: int,
    forecast_days: int,
    fetched_at: str,
) -> None:
    payload = {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "records": records,
        "points": len(points),
        "variables": len(variables),
        "past_days": past_days,
        "forecast_days": forecast_days,
    }
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def _point_argument(value: str) -> Point:
    parts = value.split(",")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("point must be ID,NAME,LATITUDE,LONGITUDE")
    point_id, name, latitude, longitude = parts
    try:
        point = Point(point_id, name, float(latitude), float(longitude))
        return validate_points((point,))[0]
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--point",
        action="append",
        type=_point_argument,
        metavar="ID,NAME,LATITUDE,LONGITUDE",
        help="marine point; repeat for multiple points (defaults to the maintained point list)",
    )
    parser.add_argument(
        "--variable",
        action="append",
        choices=HOURLY_VARIABLES,
        help="hourly variable; repeat for multiple variables (defaults to the maintained list)",
    )
    parser.add_argument("--past-days", type=int, default=DEFAULT_PAST_DAYS)
    parser.add_argument("--forecast-days", type=int, default=DEFAULT_FORECAST_DAYS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--request-delay", type=float, default=1.0)
    args = parser.parse_args(argv)

    try:
        records = 0
        collection_time = datetime.now(timezone.utc).isoformat()
        for record in fetch_marine(
            points=args.point or DEFAULT_POINTS,
            variables=args.variable or HOURLY_VARIABLES,
            past_days=args.past_days,
            forecast_days=args.forecast_days,
            batch_size=args.batch_size,
            timeout=args.timeout,
            request_delay=args.request_delay,
            fetched_at=collection_time,
        ):
            records += 1
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        _run_summary(
            records,
            args.point or DEFAULT_POINTS,
            args.variable or HOURLY_VARIABLES,
            args.past_days,
            args.forecast_days,
            collection_time,
        )
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
