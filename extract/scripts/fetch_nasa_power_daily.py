#!/usr/bin/env python3
"""Fetch bounded daily observations for one NASA POWER point as NDJSON.

The POWER daily point endpoint returns every requested day in one response; it
has no pagination.  This extractor deliberately accepts one location only so a
scheduled source cannot accidentally request a global grid.
"""

import argparse
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation


SOURCE = "nasa_power_daily"
ENDPOINT = "https://power.larc.nasa.gov/api/temporal/daily/point"
MAX_DAYS = 366
USER_AGENT = "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DATE_FORMATS = ("%Y-%m-%d", "%Y%m%d")
PARAMETER_NAME = re.compile(r"^[A-Z][A-Z0-9_]*$")


class PowerError(RuntimeError):
    """A request or response that cannot safely produce normalized records."""


def parse_day(value):
    """Return a calendar date from a POWER or ISO day string."""
    if not isinstance(value, str):
        raise PowerError("dates must be strings in YYYY-MM-DD or YYYYMMDD format")
    for pattern in DATE_FORMATS:
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            pass
    raise PowerError("invalid date %r; use YYYY-MM-DD or YYYYMMDD" % value)


def canonical_coordinate(value, name, minimum, maximum):
    """Validate a coordinate and return its JSON number and stable text form."""
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise PowerError("%s must be a decimal number" % name) from None
    if not decimal.is_finite() or not minimum <= decimal <= maximum:
        raise PowerError("%s must be between %s and %s" % (name, minimum, maximum))
    text = format(decimal.normalize(), "f")
    if text == "-0":
        text = "0"
    return float(decimal), text


def parse_parameters(value):
    """Parse a comma-separated, ordered set of POWER parameter names."""
    names = []
    for raw_name in value.split(","):
        name = raw_name.strip().upper()
        if not PARAMETER_NAME.fullmatch(name):
            raise PowerError("invalid POWER parameter %r" % raw_name)
        if name not in names:
            names.append(name)
    if not names:
        raise PowerError("at least one POWER parameter is required")
    return names


def build_request(latitude, longitude, start, end, community, parameters,
                  response_format="JSON", time_standard="UTC"):
    """Validate arguments and return the request metadata used in every row."""
    latitude_value, latitude_text = canonical_coordinate(
        latitude, "latitude", Decimal("-90"), Decimal("90")
    )
    longitude_value, longitude_text = canonical_coordinate(
        longitude, "longitude", Decimal("-180"), Decimal("180")
    )
    start_day = parse_day(start)
    end_day = parse_day(end)
    if end_day < start_day:
        raise PowerError("end date must not precede start date")
    if (end_day - start_day).days + 1 > MAX_DAYS:
        raise PowerError("date range exceeds the %d-day request bound" % MAX_DAYS)

    community = community.strip().upper()
    if not re.fullmatch(r"[A-Z]{2,10}", community):
        raise PowerError("community must be an uppercase POWER community code")
    time_standard = time_standard.strip().upper()
    if time_standard not in {"UTC", "LST"}:
        raise PowerError("time standard must be UTC or LST")
    response_format = response_format.strip().upper()
    if response_format != "JSON":
        raise PowerError("only JSON response format can be normalized to NDJSON")

    parameter_names = parse_parameters(parameters)
    return {
        "latitude": latitude_value,
        "longitude": longitude_value,
        "latitude_id": latitude_text,
        "longitude_id": longitude_text,
        "start": start_day.strftime("%Y%m%d"),
        "end": end_day.strftime("%Y%m%d"),
        "community": community,
        "parameters": parameter_names,
        "format": response_format,
        "time_standard": time_standard,
    }


def request_url(request_metadata):
    """Build the one unpaginated POWER daily point request URL."""
    query = {
        "parameters": ",".join(request_metadata["parameters"]),
        "community": request_metadata["community"],
        "longitude": request_metadata["longitude_id"],
        "latitude": request_metadata["latitude_id"],
        "start": request_metadata["start"],
        "end": request_metadata["end"],
        "format": request_metadata["format"],
        "time-standard": request_metadata["time_standard"],
    }
    return "%s?%s" % (ENDPOINT, urllib.parse.urlencode(query))


def fetch_document(request_metadata, timeout=60, opener=urllib.request.urlopen):
    """Fetch and decode one POWER response, exposing HTTP failures explicitly."""
    request = urllib.request.Request(
        request_url(request_metadata), headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    try:
        with opener(request, timeout=timeout) as response:
            status = getattr(response, "status", 200)
            if status == 429:
                raise PowerError("NASA POWER rate limit reached (HTTP 429)")
            if status < 200 or status >= 300:
                raise PowerError("NASA POWER returned HTTP %s" % status)
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        error.close()
        if error.code == 429:
            raise PowerError("NASA POWER rate limit reached (HTTP 429)") from error
        raise PowerError("NASA POWER returned HTTP %s" % error.code) from error
    except urllib.error.URLError as error:
        raise PowerError("NASA POWER request failed: %s" % error.reason) from error
    except OSError as error:
        raise PowerError("NASA POWER request failed: %s" % error) from error
    try:
        document = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PowerError("NASA POWER returned malformed JSON") from error
    if not isinstance(document, dict):
        raise PowerError("NASA POWER response must be a JSON object")
    return document


def normalize_response(document, request_metadata, fetched_at=None):
    """Yield one normalized record for each returned location/day observation."""
    if not isinstance(document, dict):
        raise PowerError("NASA POWER response must be a JSON object")
    header = document.get("header")
    properties = document.get("properties")
    if not isinstance(header, dict) or not isinstance(properties, dict):
        raise PowerError("NASA POWER response is missing header or properties")
    parameter_data = properties.get("parameter")
    parameter_metadata = document.get("parameters")
    if not isinstance(parameter_data, dict) or not isinstance(parameter_metadata, dict):
        raise PowerError("NASA POWER response is missing parameter data or metadata")

    requested = request_metadata["parameters"]
    units = {}
    for name in requested:
        values = parameter_data.get(name)
        metadata = parameter_metadata.get(name)
        if not isinstance(values, dict) or not isinstance(metadata, dict):
            raise PowerError("NASA POWER response is missing requested parameter %s" % name)
        if "units" not in metadata:
            raise PowerError("NASA POWER metadata is missing units for %s" % name)
        units[name] = metadata["units"]

    try:
        start_day = parse_day(request_metadata["start"])
        end_day = parse_day(request_metadata["end"])
    except KeyError as error:
        raise PowerError("request metadata is missing %s" % error.args[0]) from error

    dates = set()
    for name in requested:
        for raw_day in parameter_data[name]:
            day = parse_day(raw_day)
            if day < start_day or day > end_day:
                raise PowerError("NASA POWER returned a day outside the request range")
            dates.add(day)

    fill_value = header.get("fill_value")
    if "fill_value" not in header:
        raise PowerError("NASA POWER response is missing fill_value")
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()
    public_request = {
        key: request_metadata[key]
        for key in ("latitude", "longitude", "start", "end", "community", "parameters", "format", "time_standard")
    }
    for day in sorted(dates):
        power_day = day.strftime("%Y%m%d")
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": "%s:%s:%s:%s:%s:%s" % (
                SOURCE,
                request_metadata["community"],
                request_metadata["time_standard"],
                request_metadata["latitude_id"],
                request_metadata["longitude_id"],
                power_day,
            ),
            "date": day.isoformat(),
            "location": {
                "latitude": request_metadata["latitude"],
                "longitude": request_metadata["longitude"],
            },
            "parameters": {name: parameter_data[name].get(power_day) for name in requested},
            "units": units,
            "fill_value": fill_value,
            "request": public_request,
        }


def fetch_daily(latitude, longitude, start, end, community, parameters,
                response_format="JSON", time_standard="UTC", timeout=60,
                opener=urllib.request.urlopen, fetched_at=None):
    """Fetch the bounded point range. The endpoint returns all days in one page."""
    request_metadata = build_request(
        latitude, longitude, start, end, community, parameters, response_format, time_standard
    )
    document = fetch_document(request_metadata, timeout=timeout, opener=opener)
    yield from normalize_response(document, request_metadata, fetched_at=fetched_at)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--latitude", required=True)
    parser.add_argument("--longitude", required=True)
    parser.add_argument("--start", help="inclusive date: YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--end", help="inclusive date: YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--community", default="RE")
    parser.add_argument("--parameters", required=True, help="comma-separated POWER parameters")
    parser.add_argument("--format", default="JSON", dest="response_format")
    parser.add_argument("--time-standard", choices=("UTC", "LST"), default="UTC")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument(
        "--smoke", action="store_true",
        help="fetch the previous complete UTC day without persistent state",
    )
    args = parser.parse_args(argv)
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.smoke:
        if args.start or args.end:
            parser.error("--smoke cannot be combined with --start or --end")
        smoke_day = datetime.now(timezone.utc).date() - timedelta(days=1)
        args.start = args.end = smoke_day.isoformat()
    elif not args.start or not args.end:
        parser.error("--start and --end are required unless --smoke is used")

    try:
        records = fetch_daily(
            args.latitude, args.longitude, args.start, args.end, args.community,
            args.parameters, args.response_format, args.time_standard, args.timeout,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    except PowerError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
