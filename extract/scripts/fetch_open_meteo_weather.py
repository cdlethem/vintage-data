#!/usr/bin/env python3
"""Fetch bounded hourly forecasts from Open-Meteo's keyless Forecast API.

Each request explicitly selects locations, hourly variables, UTC, and a forecast
horizon. Locations are sent in bounded batches. Every emitted row represents one
location and forecast-valid hour, while ``fetched_at`` identifies the collection
and therefore the forecast revision. Open-Meteo does not supply a genuine
forecast issuance timestamp, so ``forecast_issued_at`` is always null; collection
time must not be presented as provider issuance time.

Weather data are licensed CC BY 4.0. Attribute Open-Meteo and its upstream data
providers. The public endpoint is intended for non-commercial use and publishes
access limits; this extractor's conservative six-hour schedule and batching stay
far below those limits. Stdlib only.
"""

import argparse
import hashlib
import json
import math
import os
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "open_meteo_weather"
API_URL = "https://api.open-meteo.com/v1/forecast"
PROVIDER_URL = "https://open-meteo.com/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

MAX_LOCATIONS = 20
MAX_BATCH_SIZE = 10
MAX_FORECAST_DAYS = 16
DEFAULT_FORECAST_DAYS = 7
DEFAULT_BATCH_SIZE = 10

HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation_probability",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
)
HOURLY_VARIABLE_ALLOWLIST = frozenset(HOURLY_VARIABLES)
LOCATION_ID_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


@dataclass(frozen=True)
class Location:
    """One explicitly configured forecast point."""

    id: str
    name: str
    latitude: float
    longitude: float


DEFAULT_LOCATIONS = (
    Location("new_york", "New York", 40.7128, -74.0060),
    Location("london", "London", 51.5072, -0.1276),
    Location("tokyo", "Tokyo", 35.6762, 139.6503),
    Location("sydney", "Sydney", -33.8688, 151.2093),
    Location("sao_paulo", "Sao Paulo", -23.5505, -46.6333),
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


def validate_locations(locations: Sequence[Location]) -> tuple[Location, ...]:
    """Validate the request's explicit, bounded location list."""
    if isinstance(locations, (str, bytes)):
        raise TypeError("locations must be a sequence of Location objects")
    requested = tuple(locations)
    if not requested:
        raise ValueError("at least one location is required")
    if len(requested) > MAX_LOCATIONS:
        raise ValueError(f"at most {MAX_LOCATIONS} locations may be requested per run")

    seen = set()
    normalized = []
    for index, location in enumerate(requested):
        if not isinstance(location, Location):
            raise TypeError(f"location {index} must be a Location")
        location_id = location.id.strip() if isinstance(location.id, str) else ""
        name = location.name.strip() if isinstance(location.name, str) else ""
        if LOCATION_ID_RE.fullmatch(location_id) is None:
            raise ValueError(f"location {index} has invalid id {location.id!r}")
        if location_id in seen:
            raise ValueError(f"duplicate location id {location_id!r}")
        if not name:
            raise ValueError(f"location {location_id!r} must have a non-empty name")
        latitude = _finite_number(location.latitude, f"location {location_id!r} latitude")
        longitude = _finite_number(location.longitude, f"location {location_id!r} longitude")
        if not -90 <= latitude <= 90:
            raise ValueError(f"location {location_id!r} latitude must be between -90 and 90")
        if not -180 <= longitude <= 180:
            raise ValueError(f"location {location_id!r} longitude must be between -180 and 180")
        seen.add(location_id)
        normalized.append(Location(location_id, name, latitude, longitude))
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
        or not math.isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{field} must be positive")
    return float(value)


def _utc_timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty ISO date/time string")
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field} is not a valid ISO date/time: {value!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


def _collection_timestamp(value: str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    return _utc_timestamp(value, "fetched_at")


def _stable_revision_id(location_id: str, valid_at: str, fetched_at: str) -> str:
    identity = f"{SOURCE}|{location_id}|{valid_at}|{fetched_at}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def _request_document(
    locations: Sequence[Location], variables: Sequence[str], forecast_days: int, timeout: float
) -> Any:
    parameters = {
        "latitude": ",".join(str(location.latitude) for location in locations),
        "longitude": ",".join(str(location.longitude) for location in locations),
        "hourly": ",".join(variables),
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
            f"Open-Meteo returned {len(documents)} location responses; expected {expected}"
        )
    for index, item in enumerate(documents):
        if not isinstance(item, dict):
            raise TypeError(f"Open-Meteo location response {index} must be an object")
        if item.get("error") is True:
            reason = item.get("reason")
            raise OpenMeteoAPIError(
                f"Open-Meteo location response {index} failed: {reason or 'unknown error'}"
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


def _normalize_location_response(
    document: dict[str, Any],
    location: Location,
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
    if not isinstance(times, list):
        raise TypeError("Open-Meteo hourly.time must be a list")

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

    valid_times = [
        _utc_timestamp(value, f"Open-Meteo hourly.time[{index}]")
        for index, value in enumerate(times)
    ]
    if len(set(valid_times)) != len(valid_times):
        raise ValueError("Open-Meteo hourly.time contains duplicate forecast-valid times")

    latitude = _response_coordinate(document, "latitude", -90, 90)
    longitude = _response_coordinate(document, "longitude", -180, 180)
    elevation = document.get("elevation")
    if elevation is not None:
        elevation = _finite_number(elevation, "Open-Meteo response elevation")
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

    provider = {
        "name": "Open-Meteo",
        "url": PROVIDER_URL,
        "model": "best_match",
    }
    for index, valid_at in enumerate(valid_times):
        weather = {variable: series[variable][index] for variable in variables}
        yield {
            "id": _stable_revision_id(location.id, valid_at, fetched_at),
            "source": SOURCE,
            "fetched_at": fetched_at,
            "forecast_issued_at": None,
            "forecast_valid_at": valid_at,
            "location_id": location.id,
            "location_name": location.name,
            "requested_latitude": location.latitude,
            "requested_longitude": location.longitude,
            "latitude": latitude,
            "longitude": longitude,
            "elevation": elevation,
            "timezone": response_timezone,
            "timezone_abbreviation": timezone_abbreviation.strip(),
            "utc_offset_seconds": utc_offset_seconds,
            "provider": dict(provider),
            "units": dict(units),
            "weather": weather,
        }


def fetch_forecasts(
    locations: Sequence[Location] = DEFAULT_LOCATIONS,
    variables: Sequence[str] = HOURLY_VARIABLES,
    *,
    forecast_days: int = DEFAULT_FORECAST_DAYS,
    batch_size: int = DEFAULT_BATCH_SIZE,
    timeout: float = 30,
    request_delay: float = 1.0,
    fetched_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch explicit locations in bounded batches and yield hourly forecast revisions."""
    requested_locations = validate_locations(locations)
    requested_variables = validate_variables(variables)
    if isinstance(forecast_days, bool) or not isinstance(forecast_days, int):
        raise ValueError("forecast_days must be an integer")
    if not 1 <= forecast_days <= MAX_FORECAST_DAYS:
        raise ValueError(f"forecast_days must be between 1 and {MAX_FORECAST_DAYS}")
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
    for start in range(0, len(requested_locations), batch_size):
        if start and request_delay:
            time.sleep(request_delay)
        batch = requested_locations[start : start + batch_size]
        document = _request_document(batch, requested_variables, forecast_days, timeout)
        responses = _response_documents(document, len(batch))
        for location, response in zip(batch, responses):
            yield from _normalize_location_response(
                response, location, requested_variables, collection_time
            )


def _location_argument(value: str) -> Location:
    parts = value.split(",")
    if len(parts) != 4:
        raise argparse.ArgumentTypeError("location must be ID,NAME,LATITUDE,LONGITUDE")
    location_id, name, latitude, longitude = parts
    try:
        location = Location(location_id, name, float(latitude), float(longitude))
        return validate_locations((location,))[0]
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--location",
        action="append",
        type=_location_argument,
        metavar="ID,NAME,LATITUDE,LONGITUDE",
        help="forecast point; repeat for multiple points (defaults to the maintained location list)",
    )
    parser.add_argument(
        "--variable",
        action="append",
        choices=HOURLY_VARIABLES,
        help="hourly variable; repeat for multiple variables (defaults to the maintained list)",
    )
    parser.add_argument("--forecast-days", type=int, default=DEFAULT_FORECAST_DAYS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--request-delay", type=float, default=1.0)
    args = parser.parse_args(argv)

    try:
        records = fetch_forecasts(
            locations=args.location or DEFAULT_LOCATIONS,
            variables=args.variable or HOURLY_VARIABLES,
            forecast_days=args.forecast_days,
            batch_size=args.batch_size,
            timeout=args.timeout,
            request_delay=args.request_delay,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
