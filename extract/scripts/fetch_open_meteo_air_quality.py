#!/usr/bin/env python3
"""Fetch bounded hourly air-quality analyses and forecasts from Open-Meteo.

Each request explicitly selects locations, hourly variables, UTC, and a
bounded day window. Open-Meteo answers a multi-location request as a JSON
list (a single-location request as a single object), so the five-location
default basket costs one API call per run. Every emitted row represents one
location and one valid-at hour; ``fetched_at`` identifies the collection,
so each daily run is a distinct revision of the same location-hour grid.

Upstream is the CAMS (Copernicus Atmosphere Monitoring Service) air-quality
models: the global composition forecast updates roughly every 12 hours and
the European forecast roughly every 24 hours, so a daily collection is well
below the data's change rate.

**Verified live 2026-10-05**: batched five-location request over
2026-10-04..2026-10-07 returned a 5-document JSON list, 96 hourly timestamps,
all nine variables present and finite at the sample hours. Two fetches 10
seconds apart were identical except ``generationtime_ms`` (server compute
time per request, not data), confirming sub-hourly stability between model
runs. The response carries no model-issuance timestamp, so issuance time
must not be presented as provider data.

Format: JSON. ``hourly`` maps each requested variable to a list aligned
with ``hourly.time``; ``hourly_units`` names each variable's unit; each
location document also echoes ``latitude``, ``longitude``, ``elevation``,
``utc_offset_seconds`` (always 0 for a ``timezone=UTC`` request), and
``timezone`` (always ``GMT`` for such a request). ``initializationtime`` is
documented but absent from current responses.

Endpoints:
    https://air-quality-api.open-meteo.com/v1/air-quality
        latitude/longitude are comma-separated coordinate lists; ``hourly``
        is a comma-separated variable list; the window is bounded with
        ``start_date``/``end_date`` (UTC).

Quirks that will cost someone an afternoon:
    * ``generationtime_ms`` is per-request server computation time, not a
      data revision marker; two identical-data responses differ on it.
    * The API silently picks the European (hourly) or global (3-hourly
      native) CAMS domain per location. Values are model gridded
      concentrations, not station measurements; the ``european_aqi``
      family is defined for the European domain and ``us_aqi`` for the
      global one, and both are returned for every location.
    * Pollen variables (``alder_pollen`` et al.) are Europe-only and
      seasonal; they are deliberately not in the maintained variable list.

Etiquette: keyless, no registration. The non-commercial public tier
documents limits of 600 calls/minute, 5,000 calls/hour, and 10,000
calls/day; this extractor makes one batched call per daily run. Weather
data are licensed CC BY 4.0; attribute Open-Meteo
(https://open-meteo.com/) and the CAMS/Copernicus Atmosphere Monitoring
Service upstream. Stdlib only.
"""
import argparse
import hashlib
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

SOURCE = "open_meteo_air_quality"
API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
PROVIDER_URL = "https://open-meteo.com/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

MAX_LOCATIONS = 20
MAX_WINDOW_DAYS = 7
DEFAULT_PAST_DAYS = 1
DEFAULT_FORECAST_DAYS = 2

AIR_QUALITY_VARIABLES = (
    "pm2_5",
    "pm10",
    "nitrogen_dioxide",
    "sulphur_dioxide",
    "carbon_monoxide",
    "ozone",
    "dust",
    "european_aqi",
    "us_aqi",
)
VARIABLE_ALLOWLIST = frozenset(AIR_QUALITY_VARIABLES)
LOCATION_ID_RE = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")


@dataclass(frozen=True)
class Location:
    """One explicitly configured air-quality point."""

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
        raise TypeError(f"{field} must be a number, got {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{field} must be finite, got {value!r}")
    return number


def validate_locations(locations: Sequence[Location]) -> tuple[Location, ...]:
    """Validate the request's explicit, bounded location list."""
    if not 1 <= len(locations) <= MAX_LOCATIONS:
        raise ValueError(
            f"location count must be between 1 and {MAX_LOCATIONS}, got {len(locations)}"
        )
    seen_ids: set[str] = set()
    normalized: list[Location] = []
    for index, location in enumerate(locations):
        if not isinstance(location.id, str) or not LOCATION_ID_RE.fullmatch(location.id):
            raise ValueError(f"location {index} id must match {LOCATION_ID_RE.pattern}")
        if location.id in seen_ids:
            raise ValueError(f"duplicate location id {location.id!r}")
        seen_ids.add(location.id)
        if not isinstance(location.name, str) or not location.name.strip():
            raise ValueError(f"location {location.id} name must be a non-empty string")
        latitude = _finite_number(location.latitude, f"location {location.id} latitude")
        longitude = _finite_number(location.longitude, f"location {location.id} longitude")
        if not -90.0 <= latitude <= 90.0:
            raise ValueError(f"location {location.id} latitude out of range: {latitude}")
        if not -180.0 <= longitude <= 180.0:
            raise ValueError(
                f"location {location.id} longitude out of range: {longitude}"
            )
        normalized.append(
            Location(location.id, location.name.strip(), latitude, longitude)
        )
    return tuple(normalized)


def validate_variables(variables: Sequence[str]) -> tuple[str, ...]:
    """Validate an explicit, duplicate-free subset of maintained variables."""
    if not 1 <= len(variables) <= len(AIR_QUALITY_VARIABLES):
        raise ValueError(
            f"variable count must be between 1 and {len(AIR_QUALITY_VARIABLES)}, "
            f"got {len(variables)}"
        )
    requested: list[str] = []
    for variable in variables:
        if variable not in VARIABLE_ALLOWLIST:
            raise ValueError(f"unsupported air-quality variable {variable!r}")
        if variable in requested:
            raise ValueError(f"duplicate variable {variable!r}")
        requested.append(variable)
    return tuple(requested)


def _positive_number(value: Any, field: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or float(value) <= 0
    ):
        raise ValueError(f"{field} must be a positive number")
    return float(value)


def _utc_timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{field} must be a non-empty string")
    parsed = datetime.fromisoformat(value)
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
    locations: Sequence[Location],
    variables: Sequence[str],
    start_date: str,
    end_date: str,
    timeout: float,
) -> Any:
    parameters = {
        "latitude": ",".join(str(location.latitude) for location in locations),
        "longitude": ",".join(str(location.longitude) for location in locations),
        "hourly": ",".join(variables),
        "start_date": start_date,
        "end_date": end_date,
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
        raise ValueError(f"Open-Meteo response {field} out of range: {value}")
    return value


def _validate_unit(value: Any, variable: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Open-Meteo response is missing a unit for {variable}")
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
    if not isinstance(times, list) or not times:
        raise TypeError("Open-Meteo hourly.time must be a non-empty list")

    # Validate every configured unit and complete series before yielding anything.
    units = {variable: _validate_unit(hourly_units.get(variable), variable) for variable in variables}
    series: dict[str, list[Any]] = {}
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
        raise ValueError("Open-Meteo hourly.time contains duplicate valid times")

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
        "dataset": "CAMS air quality forecast (auto domain selection)",
    }
    for index, valid_at in enumerate(valid_times):
        reading = {variable: series[variable][index] for variable in variables}
        yield {
            "id": _stable_revision_id(location.id, valid_at, fetched_at),
            "source": SOURCE,
            "fetched_at": fetched_at,
            "valid_at": valid_at,
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
            "air_quality": reading,
        }


def fetch_air_quality(
    locations: Sequence[Location] = DEFAULT_LOCATIONS,
    variables: Sequence[str] = AIR_QUALITY_VARIABLES,
    *,
    past_days: int = DEFAULT_PAST_DAYS,
    forecast_days: int = DEFAULT_FORECAST_DAYS,
    timeout: float = 30,
    fetched_at: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch explicit locations in one bounded batch and yield hourly readings."""
    requested_locations = validate_locations(locations)
    requested_variables = validate_variables(variables)
    for field, days in (("past_days", past_days), ("forecast_days", forecast_days)):
        if isinstance(days, bool) or not isinstance(days, int):
            raise ValueError(f"{field} must be an integer")
        if not 0 <= days <= MAX_WINDOW_DAYS:
            raise ValueError(f"{field} must be between 0 and {MAX_WINDOW_DAYS}")
    if past_days + forecast_days < 1:
        raise ValueError("past_days and forecast_days may not both be zero")
    timeout = _positive_number(timeout, "timeout")

    now = datetime.now(timezone.utc)
    start_date = (now - timedelta(days=past_days)).date().isoformat()
    end_date = (now + timedelta(days=forecast_days)).date().isoformat()
    collection_time = _collection_timestamp(fetched_at)
    document = _request_document(
        requested_locations, requested_variables, start_date, end_date, timeout
    )
    responses = _response_documents(document, len(requested_locations))
    for location, response in zip(requested_locations, responses):
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
        help="air-quality point; repeat for multiple points (defaults to the maintained location list)",
    )
    parser.add_argument(
        "--variable",
        action="append",
        choices=AIR_QUALITY_VARIABLES,
        help="hourly variable; repeat for multiple variables (defaults to the maintained list)",
    )
    parser.add_argument("--past-days", type=int, default=DEFAULT_PAST_DAYS)
    parser.add_argument("--forecast-days", type=int, default=DEFAULT_FORECAST_DAYS)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args(argv)

    locations = args.location or DEFAULT_LOCATIONS
    variables = args.variable or AIR_QUALITY_VARIABLES
    try:
        records = list(
            fetch_air_quality(
                locations=locations,
                variables=variables,
                past_days=args.past_days,
                forecast_days=args.forecast_days,
                timeout=args.timeout,
            )
        )
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    print(
        SUMMARY_PREFIX
        + json.dumps(
            {
                "source": SOURCE,
                "records": len(records),
                "locations": len(locations),
                "variables": list(variables),
                "past_days": args.past_days,
                "forecast_days": args.forecast_days,
                "requests": 1,
            },
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
