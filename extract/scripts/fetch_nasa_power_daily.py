#!/usr/bin/env python3
"""Fetch a bounded NASA POWER Daily point time series as normalized NDJSON."""

import argparse
from datetime import date, datetime, timedelta, timezone
import json
import math
import os
import re
from typing import Any, Iterator, Mapping, Sequence
import urllib.parse
import urllib.request

SOURCE = "nasa_power_daily"
URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
MAX_RANGE_DAYS = 366
PARAMETER_RE = re.compile(r"[A-Z][A-Z0-9_]*")
DATE_KEY_RE = re.compile(r"\d{8}")
COMMUNITIES = ("AG", "RE", "SB")
TIME_STANDARDS = ("UTC", "LST")


def utc_now() -> datetime:
    """Return the current aware UTC time; isolated for deterministic invocation tests."""
    return datetime.now(timezone.utc)


def parse_date(value: str) -> date:
    """Parse an ISO or compact NASA request date."""
    for date_format in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value, date_format).date()
        except ValueError:
            pass
    raise ValueError(f"invalid date {value!r}; expected YYYY-MM-DD or YYYYMMDD")


def validate_date_range(start: date, end: date) -> None:
    """Validate inclusive bounds for one deliberately small request."""
    if not isinstance(start, date) or isinstance(start, datetime):
        raise TypeError("start must be a date")
    if not isinstance(end, date) or isinstance(end, datetime):
        raise TypeError("end must be a date")
    if start > end:
        raise ValueError("start must not be after end")
    days = (end - start).days + 1
    if days > MAX_RANGE_DAYS:
        raise ValueError(f"date range may span at most {MAX_RANGE_DAYS} inclusive days")


def validate_coordinate(value: float, name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    coordinate = float(value)
    if not math.isfinite(coordinate) or not minimum <= coordinate <= maximum:
        raise ValueError(f"{name} must be between {minimum:g} and {maximum:g}")
    return coordinate


def parse_parameters(value: str | Sequence[str]) -> tuple[str, ...]:
    """Return a validated, duplicate-free parameter tuple in requested order."""
    raw_parameters = value.split(",") if isinstance(value, str) else list(value)
    parameters = tuple(parameter.strip().upper() for parameter in raw_parameters)
    if not parameters or any(not parameter for parameter in parameters):
        raise ValueError("parameters must be a non-empty comma-separated list")
    invalid = [parameter for parameter in parameters if not PARAMETER_RE.fullmatch(parameter)]
    if invalid:
        raise ValueError(f"invalid NASA POWER parameter: {invalid[0]}")
    if len(set(parameters)) != len(parameters):
        raise ValueError("parameters must not contain duplicates")
    return parameters


def format_coordinate(value: float) -> str:
    """Produce a stable natural-ID representation without float padding."""
    rendered = format(value, ".8f").rstrip("0").rstrip(".")
    return "0" if rendered in ("-0", "") else rendered


def request_metadata(
    start: date,
    end: date,
    latitude: float,
    longitude: float,
    community: str,
    parameters: Sequence[str],
    output_format: str,
    time_standard: str,
) -> dict[str, Any]:
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "latitude": latitude,
        "longitude": longitude,
        "community": community,
        "parameters": list(parameters),
        "format": output_format,
        "time_standard": time_standard,
    }


def build_url(metadata: Mapping[str, Any]) -> str:
    query = urllib.parse.urlencode(
        {
            "start": str(metadata["start"]).replace("-", ""),
            "end": str(metadata["end"]).replace("-", ""),
            "latitude": metadata["latitude"],
            "longitude": metadata["longitude"],
            "community": metadata["community"],
            "parameters": ",".join(metadata["parameters"]),
            "format": metadata["format"],
            "time-standard": metadata["time_standard"],
        }
    )
    return f"{URL}?{query}"


def _require_object(value: Any, context: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"malformed NASA POWER response: {context} must be an object")
    return value


def parse_response(
    document: Any, metadata: Mapping[str, Any], fetched_at: str
) -> Iterator[dict[str, Any]]:
    """Validate a POWER response and emit one normalized record per UTC/local day."""
    root = _require_object(document, "root")
    properties = _require_object(root.get("properties"), "properties")
    parameter_data = _require_object(properties.get("parameter"), "properties.parameter")
    header = _require_object(root.get("header"), "header")
    parameter_metadata = _require_object(root.get("parameters"), "parameters")

    fill_value = header.get("fill_value")
    if isinstance(fill_value, bool) or not isinstance(fill_value, (int, float)):
        raise RuntimeError("malformed NASA POWER response: header.fill_value must be numeric")

    requested_parameters = tuple(metadata["parameters"])
    series_by_parameter: dict[str, Mapping[str, Any]] = {}
    units: dict[str, str] = {}
    date_keys: set[str] | None = None
    for parameter in requested_parameters:
        if parameter not in parameter_data:
            raise RuntimeError(
                f"malformed NASA POWER response: missing parameter series {parameter}"
            )
        series = _require_object(
            parameter_data[parameter], f"properties.parameter.{parameter}"
        )
        details = _require_object(
            parameter_metadata.get(parameter), f"parameters.{parameter}"
        )
        unit = details.get("units")
        if not isinstance(unit, str) or not unit:
            raise RuntimeError(
                f"malformed NASA POWER response: parameters.{parameter}.units must be a non-empty string"
            )
        current_date_keys = set(series)
        if date_keys is None:
            date_keys = current_date_keys
        elif current_date_keys != date_keys:
            raise RuntimeError(
                "malformed NASA POWER response: parameter series have inconsistent dates"
            )
        series_by_parameter[parameter] = series
        units[parameter] = unit

    start = parse_date(str(metadata["start"]))
    end = parse_date(str(metadata["end"]))
    latitude = validate_coordinate(metadata["latitude"], "latitude", -90, 90)
    longitude = validate_coordinate(metadata["longitude"], "longitude", -180, 180)
    community = str(metadata["community"])
    time_standard = str(metadata["time_standard"])

    for date_key in sorted(date_keys or (), key=str):
        if not isinstance(date_key, str) or not DATE_KEY_RE.fullmatch(date_key):
            raise RuntimeError(
                f"malformed NASA POWER response: invalid observation date {date_key!r}"
            )
        try:
            observation_date = parse_date(date_key)
        except ValueError as error:
            raise RuntimeError(
                f"malformed NASA POWER response: invalid observation date {date_key!r}"
            ) from error
        if observation_date < start or observation_date > end:
            raise RuntimeError(
                f"malformed NASA POWER response: observation date {date_key} is outside request bounds"
            )

        values: dict[str, float | int | None] = {}
        for parameter in requested_parameters:
            value = series_by_parameter[parameter][date_key]
            if value == fill_value:
                values[parameter] = None
            elif isinstance(value, bool) or not isinstance(value, (int, float)):
                raise RuntimeError(
                    f"malformed NASA POWER response: {parameter}[{date_key}] must be numeric"
                )
            elif not math.isfinite(float(value)):
                raise RuntimeError(
                    f"malformed NASA POWER response: {parameter}[{date_key}] must be finite"
                )
            else:
                values[parameter] = value

        natural_id = ":".join(
            (
                community,
                format_coordinate(latitude),
                format_coordinate(longitude),
                time_standard,
                date_key,
            )
        )
        yield {
            "id": natural_id,
            "source": SOURCE,
            "fetched_at": fetched_at,
            "date": observation_date.isoformat(),
            "latitude": latitude,
            "longitude": longitude,
            "community": community,
            "time_standard": time_standard,
            "values": values,
            "units": dict(units),
            "fill_value": fill_value,
            "request": dict(metadata),
        }


def fetch_nasa_power_daily(
    start: date,
    end: date,
    *,
    latitude: float,
    longitude: float,
    community: str,
    parameters: str | Sequence[str],
    output_format: str,
    time_standard: str,
    timeout: int = 30,
) -> Iterator[dict[str, Any]]:
    """Issue one bounded point request and yield normalized daily observations."""
    validate_date_range(start, end)
    latitude = validate_coordinate(latitude, "latitude", -90, 90)
    longitude = validate_coordinate(longitude, "longitude", -180, 180)
    community = community.upper()
    if community not in COMMUNITIES:
        raise ValueError(f"community must be one of {', '.join(COMMUNITIES)}")
    requested_parameters = parse_parameters(parameters)
    output_format = output_format.upper()
    if output_format != "JSON":
        raise ValueError("format must be JSON for NDJSON normalization")
    time_standard = time_standard.upper()
    if time_standard not in TIME_STANDARDS:
        raise ValueError(f"time standard must be one of {', '.join(TIME_STANDARDS)}")
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer")

    metadata = request_metadata(
        start,
        end,
        latitude,
        longitude,
        community,
        requested_parameters,
        output_format,
        time_standard,
    )
    request = urllib.request.Request(
        build_url(metadata),
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        method="GET",
    )
    fetched_at = utc_now().isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    yield from parse_response(document, metadata, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch one bounded NASA POWER Daily point request as NDJSON."
    )
    parser.add_argument("--start", help="first date, inclusive (YYYY-MM-DD or YYYYMMDD)")
    parser.add_argument("--end", help="last date, inclusive (YYYY-MM-DD or YYYYMMDD)")
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="fetch only the previous complete UTC day; cannot be combined with date bounds",
    )
    parser.add_argument("--latitude", required=True, type=float)
    parser.add_argument("--longitude", required=True, type=float)
    parser.add_argument("--community", required=True, type=str.upper, choices=COMMUNITIES)
    parser.add_argument(
        "--parameters", required=True, help="comma-separated NASA POWER parameter names"
    )
    parser.add_argument("--format", required=True, type=str.upper, choices=("JSON",))
    parser.add_argument(
        "--time-standard",
        required=True,
        type=str.upper,
        choices=TIME_STANDARDS,
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    args = parser.parse_args(argv)

    try:
        if args.smoke:
            if args.start is not None or args.end is not None:
                raise ValueError("--smoke cannot be combined with --start or --end")
            start = end = utc_now().date() - timedelta(days=1)
        else:
            if args.start is None or args.end is None:
                raise ValueError("provide both --start and --end, or use --smoke")
            start = parse_date(args.start)
            end = parse_date(args.end)
        validate_date_range(start, end)
        if args.timeout <= 0:
            raise ValueError("timeout must be a positive integer")
        parameters = parse_parameters(args.parameters)
    except (TypeError, ValueError) as error:
        parser.error(str(error))

    for record in fetch_nasa_power_daily(
        start,
        end,
        latitude=args.latitude,
        longitude=args.longitude,
        community=args.community,
        parameters=parameters,
        output_format=args.format,
        time_standard=args.time_standard,
        timeout=args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
