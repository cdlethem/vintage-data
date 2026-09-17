#!/usr/bin/env python3
"""Fetch bounded daily point observations from the NASA POWER API."""

import argparse
import hashlib
import json
import math
import os
import re
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

SOURCE = "nasa_power_daily"
API_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_PARAMETERS = ("T2M", "T2M_MAX", "T2M_MIN", "PRECTOTCORR")
DEFAULT_LATITUDE = 38.9072
DEFAULT_LONGITUDE = -77.0369
COMMUNITIES = frozenset({"AG", "RE", "SB"})
TIME_STANDARDS = frozenset({"UTC", "LST"})
MAX_REQUEST_DAYS = 31
PARAMETER_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")
DATE_RE = re.compile(r"^\d{8}$")


def _finite_number(value, field_name):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be a finite number")
    if not math.isfinite(value):
        raise ValueError(f"{field_name} must be a finite number")
    return value


def _request_date(value, field_name):
    if not isinstance(value, str) or not DATE_RE.fullmatch(value):
        raise ValueError(f"{field_name} must use YYYYMMDD")
    try:
        return datetime.strptime(value, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid YYYYMMDD date") from exc


def _parameter_names(value):
    if isinstance(value, str):
        names = value.split(",")
    else:
        try:
            names = list(value)
        except TypeError as exc:
            raise ValueError("parameters must be a comma-separated string or sequence") from exc
    normalized = []
    for name in names:
        if not isinstance(name, str) or not PARAMETER_RE.fullmatch(name.strip().upper()):
            raise ValueError(f"invalid NASA POWER parameter: {name!r}")
        candidate = name.strip().upper()
        if candidate not in normalized:
            normalized.append(candidate)
    if not normalized:
        raise ValueError("at least one parameter is required")
    return tuple(normalized)


def _validate_request(latitude, longitude, parameters, community, time_standard, start, end, timeout):
    latitude = _finite_number(latitude, "latitude")
    longitude = _finite_number(longitude, "longitude")
    if not -90 <= latitude <= 90:
        raise ValueError("latitude must be between -90 and 90")
    if not -180 <= longitude <= 180:
        raise ValueError("longitude must be between -180 and 180")

    parameters = _parameter_names(parameters)
    community = str(community).strip().upper()
    if community not in COMMUNITIES:
        raise ValueError(f"community must be one of: {', '.join(sorted(COMMUNITIES))}")
    time_standard = str(time_standard).strip().upper()
    if time_standard not in TIME_STANDARDS:
        raise ValueError(f"time_standard must be one of: {', '.join(sorted(TIME_STANDARDS))}")

    start_date = _request_date(start, "start")
    end_date = _request_date(end, "end")
    if start_date > end_date:
        raise ValueError("start must not be later than end")
    if (end_date - start_date).days + 1 > MAX_REQUEST_DAYS:
        raise ValueError(f"request span must not exceed {MAX_REQUEST_DAYS} days")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be a positive finite number")
    return latitude, longitude, parameters, community, time_standard, start_date, end_date


def _request_document(query, timeout):
    url = API_URL + "?" + urllib.parse.urlencode(query)
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def _stable_id(latitude, longitude, parameter, observation_date):
    identity = "|".join(
        (
            SOURCE,
            format(latitude, ".15g"),
            format(longitude, ".15g"),
            parameter,
            observation_date,
        )
    )
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _observation_value(value, fill_value, field_name):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric or null")
    if not math.isfinite(value):
        raise ValueError(f"{field_name} must be finite or null")
    if value == fill_value:
        return None
    return value


def fetch_daily(
    latitude=DEFAULT_LATITUDE,
    longitude=DEFAULT_LONGITUDE,
    parameters=DEFAULT_PARAMETERS,
    community="AG",
    time_standard="UTC",
    start=None,
    end=None,
    *,
    timeout=30,
):
    """Return normalized records for one bounded NASA POWER point request."""
    if start is None or end is None:
        raise ValueError("start and end are required")
    (
        latitude,
        longitude,
        parameters,
        community,
        time_standard,
        start_date,
        end_date,
    ) = _validate_request(latitude, longitude, parameters, community, time_standard, start, end, timeout)

    query = {
        "parameters": ",".join(parameters),
        "community": community,
        "longitude": format(longitude, ".15g"),
        "latitude": format(latitude, ".15g"),
        "start": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d"),
        "format": "JSON",
        "time-standard": time_standard,
    }
    document = _request_document(query, timeout)
    if not isinstance(document, dict):
        raise TypeError("NASA POWER response must be a JSON object")

    header = document.get("header")
    if not isinstance(header, dict):
        raise TypeError("NASA POWER response is missing header object")
    fill_value = _finite_number(header.get("fill_value"), "NASA POWER header.fill_value")

    properties = document.get("properties")
    if not isinstance(properties, dict) or not isinstance(properties.get("parameter"), dict):
        raise TypeError("NASA POWER response is missing properties.parameter object")
    series_by_parameter = properties["parameter"]

    metadata_by_parameter = document.get("parameters")
    if not isinstance(metadata_by_parameter, dict):
        raise TypeError("NASA POWER response is missing parameters metadata object")

    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    request_metadata = {
        "latitude": latitude,
        "longitude": longitude,
        "parameters": list(parameters),
        "community": community,
        "time_standard": time_standard,
        "start": query["start"],
        "end": query["end"],
    }
    records = []
    for parameter in parameters:
        series = series_by_parameter.get(parameter)
        if not isinstance(series, dict):
            raise TypeError(f"NASA POWER response is missing series for parameter {parameter}")
        parameter_metadata = metadata_by_parameter.get(parameter)
        if not isinstance(parameter_metadata, dict):
            raise TypeError(f"NASA POWER response is missing metadata for parameter {parameter}")
        units = parameter_metadata.get("units")
        if not isinstance(units, str) or not units.strip():
            raise ValueError(f"NASA POWER metadata for parameter {parameter} is missing units")

        dated_values = []
        for date_key, raw_value in series.items():
            observation_date = _request_date(date_key, f"{parameter} observation date")
            if observation_date < start_date or observation_date > end_date:
                raise ValueError(f"{parameter} observation date {date_key} is outside the requested range")
            value = _observation_value(raw_value, fill_value, f"{parameter} observation {date_key}")
            dated_values.append((observation_date, date_key, value))

        for observation_date, date_key, value in sorted(dated_values):
            records.append(
                {
                    "id": _stable_id(latitude, longitude, parameter, date_key),
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "date": observation_date.isoformat(),
                    "parameter": parameter,
                    "value": value,
                    "units": units,
                    "fill_value": fill_value,
                    "latitude": latitude,
                    "longitude": longitude,
                    "request": dict(request_metadata),
                    "parameter_metadata": dict(parameter_metadata),
                    "header": dict(header),
                }
            )
    return records


def _utc_today():
    return datetime.now(timezone.utc).date()


def _resolved_range(start, end, smoke):
    if smoke:
        if start is not None or end is not None:
            raise ValueError("--smoke cannot be combined with --start or --end")
        previous_day = _utc_today() - timedelta(days=1)
        value = previous_day.strftime("%Y%m%d")
        return value, value
    if start is None or end is None:
        raise ValueError("--start and --end are required unless --smoke is used")
    return start, end


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--latitude", type=float, default=DEFAULT_LATITUDE)
    parser.add_argument("--longitude", type=float, default=DEFAULT_LONGITUDE)
    parser.add_argument("--parameters", default=",".join(DEFAULT_PARAMETERS))
    parser.add_argument("--community", choices=sorted(COMMUNITIES), default="AG")
    parser.add_argument("--time-standard", choices=sorted(TIME_STANDARDS), default="UTC")
    parser.add_argument("--start")
    parser.add_argument("--end")
    parser.add_argument("--smoke", action="store_true", help="fetch only the previous complete UTC day")
    parser.add_argument("--timeout", type=float, default=30.0)
    args = parser.parse_args(argv)

    try:
        start, end = _resolved_range(args.start, args.end, args.smoke)
        records = fetch_daily(
            latitude=args.latitude,
            longitude=args.longitude,
            parameters=args.parameters,
            community=args.community,
            time_standard=args.time_standard,
            start=start,
            end=end,
            timeout=args.timeout,
        )
        lines = [json.dumps(record, ensure_ascii=False, sort_keys=True, allow_nan=False) for record in records]
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
