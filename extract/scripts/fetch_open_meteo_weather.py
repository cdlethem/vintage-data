#!/usr/bin/env python3
"""Fetch a bounded, revision-preserving Open-Meteo hourly forecast feed.

Open-Meteo does not expose a forecast issuance timestamp in this endpoint.  Each
run therefore records one UTC collection timestamp as both ``fetched_at`` and
``forecast_issued_at``.  It is part of every record's ID, so the same forecast
valid time collected by later runs remains a distinct revision rather than an
upsert candidate.

The locations and hourly variables below are deliberately explicit and bounded.
One request batches every configured location; no user input can expand the
geographic or variable scope.  Stdlib only.
"""
import argparse
import json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API_URL = "https://api.open-meteo.com/v1/forecast"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
REQUEST_TIMEOUT_SECONDS = 30
MAX_LOCATIONS = 10
MAX_FORECAST_DAYS = 7
FORECAST_DAYS = 3

# Keep this bounded configuration in source control: all locations are requested
# in one Open-Meteo batch and carry their configured identifiers into every row.
LOCATIONS = (
    {"location_id": "madison_wi_us", "name": "Madison, Wisconsin, US", "latitude": 43.0731, "longitude": -89.4012},
    {"location_id": "chicago_il_us", "name": "Chicago, Illinois, US", "latitude": 41.8781, "longitude": -87.6298},
    {"location_id": "milwaukee_wi_us", "name": "Milwaukee, Wisconsin, US", "latitude": 43.0389, "longitude": -87.9065},
)
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation_probability",
    "precipitation",
    "weather_code",
    "wind_speed_10m",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _valid_time(value: object) -> str:
    """Return an Open-Meteo UTC hourly time in an unambiguous ISO-8601 form."""
    if not isinstance(value, str):
        raise ValueError("Open-Meteo hourly time must be a string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"invalid Open-Meteo hourly time: {value!r}") from error
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    return parsed.isoformat(timespec="seconds") + "Z"


def validate_configuration(locations=LOCATIONS, hourly_variables=HOURLY_VARIABLES, forecast_days=FORECAST_DAYS):
    """Reject accidental scope expansion or malformed explicit configuration."""
    if not 1 <= len(locations) <= MAX_LOCATIONS:
        raise ValueError(f"configure between 1 and {MAX_LOCATIONS} locations")
    if not 1 <= len(hourly_variables) <= len(HOURLY_VARIABLES):
        raise ValueError(f"configure between 1 and {len(HOURLY_VARIABLES)} hourly variables")
    if len(set(hourly_variables)) != len(hourly_variables):
        raise ValueError("hourly variables must be unique")
    if not 1 <= forecast_days <= MAX_FORECAST_DAYS:
        raise ValueError(f"forecast_days must be between 1 and {MAX_FORECAST_DAYS}")

    location_ids = set()
    for location in locations:
        location_id = location.get("location_id")
        if not isinstance(location_id, str) or not location_id:
            raise ValueError("each location needs a non-empty location_id")
        if location_id in location_ids:
            raise ValueError(f"duplicate location_id: {location_id}")
        location_ids.add(location_id)
        for coordinate in ("latitude", "longitude"):
            value = location.get(coordinate)
            if not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError(f"{location_id} has an invalid {coordinate}")
        if not -90 <= location["latitude"] <= 90 or not -180 <= location["longitude"] <= 180:
            raise ValueError(f"{location_id} has coordinates outside the valid range")


def build_url(locations=LOCATIONS, hourly_variables=HOURLY_VARIABLES, forecast_days=FORECAST_DAYS) -> str:
    """Build the one bounded batched request used by a run."""
    validate_configuration(locations, hourly_variables, forecast_days)
    query = urllib.parse.urlencode({
        "latitude": ",".join(str(location["latitude"]) for location in locations),
        "longitude": ",".join(str(location["longitude"]) for location in locations),
        "hourly": ",".join(hourly_variables),
        "forecast_days": forecast_days,
        "timezone": "UTC",
    })
    return f"{API_URL}?{query}"


def _get(url: str, timeout: int = REQUEST_TIMEOUT_SECONDS):
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def rows_from_response(response, locations=LOCATIONS, hourly_variables=HOURLY_VARIABLES, fetched_at=None):
    """Normalize one batched response while retaining every forecast revision."""
    validate_configuration(locations, hourly_variables)
    if response == []:
        return []
    if isinstance(response, dict):
        response = [response]
    if not isinstance(response, list):
        raise ValueError("Open-Meteo response must be an object or list of objects")
    if len(response) != len(locations):
        raise ValueError("Open-Meteo response count does not match requested locations")

    fetched_at = fetched_at or _utc_now()
    rows = []
    for location, forecast in zip(locations, response):
        if not isinstance(forecast, dict):
            raise ValueError("Open-Meteo forecast response must contain objects")
        hourly = forecast.get("hourly")
        if not isinstance(hourly, dict):
            raise ValueError(f"Open-Meteo response has no hourly data for {location['location_id']}")
        times = hourly.get("time")
        if not isinstance(times, list):
            raise ValueError(f"Open-Meteo hourly time is not a list for {location['location_id']}")
        units = forecast.get("hourly_units")
        if not isinstance(units, dict):
            raise ValueError(f"Open-Meteo response has no hourly units for {location['location_id']}")

        values = {}
        for variable in hourly_variables:
            series = hourly.get(variable)
            if not isinstance(series, list) or len(series) != len(times):
                raise ValueError(f"Open-Meteo {variable} series does not match hourly time")
            values[variable] = series
        for index, raw_time in enumerate(times):
            valid_time = _valid_time(raw_time)
            rows.append({
                "source": "open_meteo_weather",
                "id": f"open_meteo_weather:{location['location_id']}:{fetched_at}:{valid_time}",
                "fetched_at": fetched_at,
                "forecast_issued_at": fetched_at,
                "forecast_valid_at": valid_time,
                "location_id": location["location_id"],
                "location_name": location.get("name"),
                "latitude": location["latitude"],
                "longitude": location["longitude"],
                "provider": "Open-Meteo",
                "provider_model": forecast.get("model"),
                "provider_generationtime_ms": forecast.get("generationtime_ms"),
                "provider_elevation_m": forecast.get("elevation"),
                "provider_timezone": forecast.get("timezone"),
                "provider_timezone_abbreviation": forecast.get("timezone_abbreviation"),
                "provider_utc_offset_seconds": forecast.get("utc_offset_seconds"),
                "units": {variable: units.get(variable) for variable in hourly_variables},
                **{variable: values[variable][index] for variable in hourly_variables},
            })
    return rows


def fetch_forecast(locations=LOCATIONS, hourly_variables=HOURLY_VARIABLES, forecast_days=FORECAST_DAYS, timeout=REQUEST_TIMEOUT_SECONDS, fetched_at=None):
    """Fetch the configured batch and return revision-preserving normalized rows."""
    url = build_url(locations, hourly_variables, forecast_days)
    return rows_from_response(_get(url, timeout), locations, hourly_variables, fetched_at)


def main():
    parser = argparse.ArgumentParser(description="Fetch bounded Open-Meteo hourly forecast revisions as JSON Lines.")
    parser.parse_args()
    for row in fetch_forecast():
        print(json.dumps(row, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
