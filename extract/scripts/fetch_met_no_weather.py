#!/usr/bin/env python3
"""Fetch bounded hourly forecasts from MET Norway's keyless Locationforecast API.

Each request explicitly selects one location (latitude, longitude, ground
altitude) and returns the nine-day, hourly "compact" forecast for that point.
Every emitted row represents one location and forecast-valid hour, while
``fetched_at`` identifies the collection and therefore the forecast revision.
MET Norway serves current forecasts only and does not publish a forecast
issuance timestamp in the JSON format, so ``forecast_issued_at`` is always
null; collection time must not be presented as provider issuance time.

Weather data are licensed under the Norwegian Licence for Open Government
Data (NLOD) 2.0 and Creative Commons BY 4.0. Attribute MET Norway as the data
source. The service terms cap traffic at 20 requests/second and forbid
repeating identical requests more often than every 30 minutes (Nordic
locations) or every 3 hours (elsewhere); data update hourly for the Nordic
region and every 6 hours for the rest of the world. This extractor's
six-hour schedule with one request per location stays far below those
limits. Stdlib only.
"""

import argparse
import hashlib
import json
import math
import os
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "met_no_weather"
API_URL = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
PROVIDER_URL = "https://api.met.no/"
PROVIDER_NAME = "MET Norway"
LICENSE = "NLOD 2.0 / CC BY 4.0"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

MAX_LOCATIONS = 20
DEFAULT_REQUEST_DELAY = 1.0
# MET Norway publishes a nine-day hourly forecast; keep the bounded horizon.
MAX_FORECAST_HOURS = 9 * 24

# Compact forecast variables and their fixed MET Norway units.
INSTANT_VARIABLES = (
    "air_pressure_at_sea_level",  # Pa
    "air_temperature",  # Celsius
    "cloud_area_fraction",  # percent
    "relative_humidity",  # percent
    "wind_from_direction",  # degrees
    "wind_speed",  # m/s
)
INSTANT_VARIABLE_UNITS = {
    "air_pressure_at_sea_level": "Pa",
    "air_temperature": "C",
    "cloud_area_fraction": "%",
    "relative_humidity": "%",
    "wind_from_direction": "degrees",
    "wind_speed": "m/s",
}
PRECIPITATION_PERIODS = ("next_1_hours", "next_6_hours")

LOCATION_ID_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


@dataclass(frozen=True)
class Location:
    """One explicitly configured forecast point."""

    id: str
    name: str
    latitude: float
    longitude: float
    altitude: int


DEFAULT_LOCATIONS = (
    Location("oslo", "Oslo", 59.9139, 10.7522, 25),
    Location("london", "London", 51.5072, -0.1276, 50),
    Location("new_york", "New York", 40.7128, -74.0060, 10),
    Location("tokyo", "Tokyo", 35.6762, 139.6503, 40),
    Location("sydney", "Sydney", -33.8688, 151.2093, 30),
    Location("sao_paulo", "Sao Paulo", -23.5505, -46.6333, 760),
)


class MetNoAPIError(RuntimeError):
    """An HTTP or structural error returned by MET Norway."""


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"MET Norway {field} must be a number")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"MET Norway {field} must be finite")
    return number


def validate_locations(locations: Sequence[Location]) -> tuple[Location, ...]:
    """Validate the request's explicit, bounded location list."""
    if not isinstance(locations, Sequence) or not locations:
        raise ValueError("at least one location is required")
    if len(locations) > MAX_LOCATIONS:
        raise ValueError(f"at most {MAX_LOCATIONS} locations may be requested per run")

    seen = set()
    normalized = []
    for location in locations:
        if not isinstance(location, Location):
            raise TypeError("each location must be a Location")
        if not isinstance(location.id, str) or LOCATION_ID_RE.fullmatch(location.id) is None:
            raise ValueError(
                f"location id {location.id!r} must match {LOCATION_ID_RE.pattern}"
            )
        if location.id in seen:
            raise ValueError(f"location id {location.id!r} is duplicated")
        seen.add(location.id)
        if not isinstance(location.name, str) or not location.name.strip():
            raise ValueError(f"location {location.id} name must be a non-empty string")
        latitude = _finite_number(location.latitude, f"location {location.id} latitude")
        if not -90.0 <= latitude <= 90.0:
            raise ValueError(f"location {location.id} latitude is outside valid bounds")
        longitude = _finite_number(location.longitude, f"location {location.id} longitude")
        if not -180.0 <= longitude <= 180.0:
            raise ValueError(f"location {location.id} longitude is outside valid bounds")
        if isinstance(location.altitude, bool) or not isinstance(location.altitude, int):
            raise ValueError(f"location {location.id} altitude must be an integer in metres")
        if not -500 <= location.altitude <= 10000:
            raise ValueError(f"location {location.id} altitude is outside valid bounds")
        normalized.append(
            Location(location.id, location.name.strip(), latitude, longitude, location.altitude)
        )
    return tuple(normalized)


def _utc_timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"MET Norway {field} must be a non-empty string")
    raw = value.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"MET Norway {field} is not ISO 8601: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"MET Norway {field} must include a UTC offset")
    return parsed.astimezone(timezone.utc).isoformat()


def _collection_timestamp(value: str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat()
    return _utc_timestamp(value, "fetched_at")


def _stable_revision_id(location_id: str, valid_at: str, fetched_at: str) -> str:
    identity = f"{SOURCE}|{location_id}|{valid_at}|{fetched_at}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def _request_document(location: Location, timeout: float) -> Any:
    parameters = {
        "lat": str(location.latitude),
        "lon": str(location.longitude),
        "altitude": str(location.altitude),
    }
    request = urllib.request.Request(
        API_URL + "?" + urllib.parse.urlencode(parameters),
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = ""
        try:
            detail = exc.read(400).decode("utf-8", errors="replace")
        except Exception:
            pass
        raise MetNoAPIError(
            f"MET Norway returned HTTP {exc.code} for {location.id}: {detail or exc.reason}"
        ) from exc


def _response_coordinates(
    document: dict[str, Any], location: Location
) -> tuple[float, float, float]:
    geometry = document.get("geometry")
    if not isinstance(geometry, dict) or geometry.get("type") != "Point":
        raise TypeError("MET Norway response geometry must be a Point feature")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, (list, tuple)) or len(coordinates) != 3:
        raise TypeError("MET Norway response coordinates must have longitude, latitude, altitude")
    longitude = _finite_number(coordinates[0], "MET Norway response longitude")
    latitude = _finite_number(coordinates[1], "MET Norway response latitude")
    altitude = _finite_number(coordinates[2], "MET Norway response altitude")
    if not -180.0 <= longitude <= 180.0 or not -90.0 <= latitude <= 90.0:
        raise ValueError("MET Norway response coordinates are outside valid bounds")
    return latitude, longitude, altitude


def _instant_details(data: dict[str, Any], index: int) -> dict[str, float]:
    block = data.get("instant")
    if not isinstance(block, dict):
        raise TypeError(f"MET Norway timeseries[{index}] is missing the instant block")
    details = block.get("details")
    if not isinstance(details, dict):
        raise TypeError(f"MET Norway timeseries[{index}].instant is missing details")
    values = {}
    for variable in INSTANT_VARIABLES:
        if variable not in details:
            raise ValueError(
                f"MET Norway timeseries[{index}] is missing {variable}"
            )
        values[variable] = _finite_number(details[variable], f"MET Norway {variable}")
    return values


def _precipitation_amounts(data: dict[str, Any], index: int) -> dict[str, float | None]:
    amounts = {}
    for period in PRECIPITATION_PERIODS:
        amounts[period] = None
        block = data.get(period)
        if block is None:
            # Period summaries taper irregularly toward the end of the horizon.
            continue
        if not isinstance(block, dict):
            raise TypeError(f"MET Norway timeseries[{index}] {period} block must be an object")
        details = block.get("details")
        if not isinstance(details, dict) or "precipitation_amount" not in details:
            raise ValueError(f"MET Norway timeseries[{index}].{period} is missing precipitation_amount")
        amounts[period] = _finite_number(
            details["precipitation_amount"], f"MET Norway {period} precipitation_amount"
        )
    return amounts


def _symbol_code(data: dict[str, Any], index: int) -> str | None:
    block = data.get("next_1_hours")
    if block is None:
        return None
    if not isinstance(block, dict):
        raise TypeError(f"MET Norway timeseries[{index}] next_1_hours block must be an object")
    summary = block.get("summary")
    if summary is None:
        return None
    if not isinstance(summary, dict):
        raise TypeError(f"MET Norway timeseries[{index}].next_1_hours summary must be an object")
    code = summary.get("symbol_code")
    if code is None:
        return None
    if not isinstance(code, str) or not code.strip():
        raise ValueError(
            f"MET Norway timeseries[{index}].next_1_hours symbol_code must be a non-empty string"
        )
    return code.strip()


def _normalize_location_response(
    document: Any,
    location: Location,
    fetched_at: str,
) -> Iterator[dict[str, Any]]:
    if not isinstance(document, dict):
        raise TypeError("MET Norway response must be a JSON object")
    if document.get("type") != "Feature":
        raise TypeError("MET Norway response type must be Feature")
    properties = document.get("properties")
    if not isinstance(properties, dict):
        raise TypeError("MET Norway response is missing properties")
    timeseries = properties.get("timeseries")
    if not isinstance(timeseries, list) or not timeseries:
        raise TypeError("MET Norway response timeseries must be a non-empty list")
    if len(timeseries) > MAX_FORECAST_HOURS:
        raise ValueError(
            f"MET Norway response has {len(timeseries)} timesteps; at most {MAX_FORECAST_HOURS}"
        )
    latitude, longitude, altitude = _response_coordinates(document, location)

    # Validate the whole series before yielding anything.
    valid_times = []
    for index, step in enumerate(timeseries):
        if not isinstance(step, dict):
            raise TypeError(f"MET Norway timeseries[{index}] must be an object")
        valid_times.append(_utc_timestamp(step.get("time"), f"MET Norway timeseries[{index}].time"))
    if len(set(valid_times)) != len(valid_times):
        raise ValueError("MET Norway timeseries contains duplicate forecast-valid times")

    provider = {"name": PROVIDER_NAME, "url": PROVIDER_URL, "license": LICENSE}
    for index, step in enumerate(timeseries):
        data = step.get("data")
        if not isinstance(data, dict):
            raise TypeError(f"MET Norway timeseries[{index}] is missing data")
        weather = _instant_details(data, index)
        weather.update(
            {
                f"precipitation_amount_{period}": amount
                for period, amount in _precipitation_amounts(data, index).items()
            }
        )
        valid_at = valid_times[index]
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
            "requested_altitude": location.altitude,
            "latitude": latitude,
            "longitude": longitude,
            "altitude": altitude,
            "provider": dict(provider),
            "units": dict(INSTANT_VARIABLE_UNITS),
            "symbol_code": _symbol_code(data, index),
            "weather": weather,
        }


def fetch_forecasts(
    locations: Sequence[Location] = DEFAULT_LOCATIONS,
    *,
    timeout: float = 30,
    request_delay: float = DEFAULT_REQUEST_DELAY,
    fetched_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch explicit locations one request each and yield hourly forecast revisions."""
    requested_locations = validate_locations(locations)
    timeout = _finite_number(timeout, "timeout")
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if isinstance(request_delay, bool) or not isinstance(request_delay, (int, float)):
        raise ValueError("request_delay must be a non-negative number")
    if not math.isfinite(request_delay) or request_delay < 0:
        raise ValueError("request_delay must be a non-negative number")

    collection_time = _collection_timestamp(fetched_at)
    for index, location in enumerate(requested_locations):
        if index and request_delay:
            time.sleep(request_delay)
        document = _request_document(location, timeout)
        yield from _normalize_location_response(document, location, collection_time)


def _location_argument(value: str) -> Location:
    parts = value.split(",")
    if len(parts) != 5:
        raise argparse.ArgumentTypeError(
            "location must be ID,NAME,LATITUDE,LONGITUDE,ALTITUDE_METRES"
        )
    location_id, name, latitude, longitude, altitude = parts
    try:
        location = Location(location_id, name, float(latitude), float(longitude), int(altitude))
        return validate_locations((location,))[0]
    except (TypeError, ValueError) as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--location",
        action="append",
        type=_location_argument,
        metavar="ID,NAME,LATITUDE,LONGITUDE,ALTITUDE_METRES",
        help="forecast point; repeat for multiple points (defaults to the maintained location list)",
    )
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY,
        help="seconds to sleep between per-location requests (default: %(default)s)",
    )
    args = parser.parse_args(argv)

    try:
        records = fetch_forecasts(
            locations=args.location or DEFAULT_LOCATIONS,
            timeout=args.timeout,
            request_delay=args.request_delay,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
