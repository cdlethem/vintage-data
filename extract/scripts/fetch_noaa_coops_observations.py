#!/usr/bin/env python3
"""Fetch bounded NOAA CO-OPS water-level observations or predictions.

The CO-OPS Data API reports station-local readings.  This extractor preserves the
requested datum, units, time-zone basis, API quality code, and raw flag string so
values are not detached from the information needed to interpret them.  It uses
one bounded API request; the endpoint does not expose cursor pagination.

Stdlib only.
"""

import argparse
import hashlib
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import date as Date
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

SOURCE = "noaa_coops_observations"
API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
DEFAULT_STATION = "9414290"  # San Francisco, CA
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

PRODUCTS = frozenset(("water_level", "predictions"))
DATUMS_BY_PRODUCT = {
    "water_level": frozenset(("MLLW", "MHW", "MTL", "MSL", "STND", "NAVD", "IGLD", "LWD", "HAT")),
    "predictions": frozenset(("MLLW", "MHW", "MTL", "MSL", "STND", "NAVD", "IGLD", "LWD", "HAT")),
}
UNITS = frozenset(("metric", "english"))
TIME_ZONES = frozenset(("gmt", "lst", "lst_ldt"))
MAX_DAYS_BY_PRODUCT = {"water_level": 31, "predictions": 365}
_STATION = re.compile(r"^\d{7}$")
_MISSING_VALUES = frozenset(("", "null", "none", "nan"))


class NoaaCoopsError(ValueError):
    """A request or response that cannot be interpreted without ambiguity."""


def _parse_day(value: str, field: str) -> Date:
    if not isinstance(value, str):
        raise NoaaCoopsError(f"{field} must be YYYY-MM-DD or YYYYMMDD")
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            pass
    raise NoaaCoopsError(f"{field} must be YYYY-MM-DD or YYYYMMDD")


def resolve_date_range(
    observation_date: Optional[str] = None,
    begin_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: str = "water_level",
    today: Optional[Date] = None,
) -> Tuple[Date, Date]:
    """Resolve a single day or inclusive range and enforce CO-OPS API bounds."""
    if observation_date is not None and (begin_date is not None or end_date is not None):
        raise NoaaCoopsError("date cannot be combined with begin_date or end_date")
    if (begin_date is None) != (end_date is None):
        raise NoaaCoopsError("begin_date and end_date must be supplied together")

    if observation_date is not None:
        begin = end = _parse_day(observation_date, "date")
    elif begin_date is not None:
        begin = _parse_day(begin_date, "begin_date")
        end = _parse_day(end_date, "end_date")
    else:
        # A completed UTC day avoids a permanently partial daily snapshot.
        begin = end = (today or datetime.now(timezone.utc).date()) - timedelta(days=1)

    if end < begin:
        raise NoaaCoopsError("end_date must not precede begin_date")
    max_days = MAX_DAYS_BY_PRODUCT[product]
    if (end - begin).days + 1 > max_days:
        raise NoaaCoopsError(f"{product} requests are limited to {max_days} days")
    return begin, end


def build_request_url(
    station: str,
    observation_date: Optional[str] = None,
    begin_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: str = "water_level",
    datum: str = "MLLW",
    units: str = "metric",
    time_zone: str = "gmt",
    today: Optional[Date] = None,
) -> str:
    """Validate arguments and build a deterministic CO-OPS Data API URL."""
    if not isinstance(station, str) or not _STATION.fullmatch(station):
        raise NoaaCoopsError("station must be a seven-digit NOAA CO-OPS station ID")
    if product not in PRODUCTS:
        raise NoaaCoopsError(f"unsupported product {product!r}; supported products: {', '.join(sorted(PRODUCTS))}")
    if not isinstance(datum, str) or datum.upper() not in DATUMS_BY_PRODUCT[product]:
        allowed = ", ".join(sorted(DATUMS_BY_PRODUCT[product]))
        raise NoaaCoopsError(f"datum {datum!r} is not valid for {product}; use one of: {allowed}")
    if units not in UNITS:
        raise NoaaCoopsError("units must be metric or english")
    if time_zone not in TIME_ZONES:
        raise NoaaCoopsError("time_zone must be gmt, lst, or lst_ldt")

    begin, end = resolve_date_range(observation_date, begin_date, end_date, product, today)
    params = {
        "application": SOURCE,
        "begin_date": begin.strftime("%Y%m%d"),
        "datum": datum.upper(),
        "end_date": end.strftime("%Y%m%d"),
        "format": "json",
        "product": product,
        "station": station,
        "time_zone": time_zone,
        "units": units,
    }
    return f"{API_URL}?{urllib.parse.urlencode(params)}"


def _number(value: Any, field: str) -> Optional[float]:
    if value is None or (isinstance(value, str) and value.strip().lower() in _MISSING_VALUES):
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as error:
        raise NoaaCoopsError(f"invalid {field} value {value!r}") from error


def _timestamp(value: Any, time_zone: str) -> str:
    if not isinstance(value, str):
        raise NoaaCoopsError("response row is missing timestamp t")
    text = value.strip()
    for pattern in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, pattern)
            normalized = parsed.isoformat(timespec="seconds")
            return f"{normalized}Z" if time_zone == "gmt" else normalized
        except ValueError:
            pass
    raise NoaaCoopsError(f"invalid response timestamp {value!r}")


def _record_id(station: str, product: str, datum: str, units: str, time_zone: str, timestamp: str) -> str:
    key = ":".join((SOURCE, station, product, datum, units, time_zone, timestamp))
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _response_error(payload: Mapping[str, Any]) -> Optional[str]:
    error = payload.get("error")
    if isinstance(error, Mapping):
        return str(error.get("message") or error.get("Message") or error)
    if error:
        return str(error)
    return None


def normalize_response(
    payload: Mapping[str, Any],
    station: str,
    product: str,
    datum: str,
    units: str,
    time_zone: str,
) -> List[Dict[str, Any]]:
    """Normalize one CO-OPS response while retaining quality and station context."""
    if not isinstance(payload, Mapping):
        raise NoaaCoopsError("CO-OPS response must be a JSON object")
    message = _response_error(payload)
    if message:
        raise NoaaCoopsError(f"CO-OPS API error: {message}")

    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        raise NoaaCoopsError("CO-OPS response is missing station metadata")
    response_station = str(metadata.get("id", station))
    if response_station != station:
        raise NoaaCoopsError(f"response station {response_station!r} does not match requested station {station!r}")

    data_key = "data" if product == "water_level" else "predictions"
    rows = payload.get(data_key)
    if not isinstance(rows, list):
        raise NoaaCoopsError(f"CO-OPS response is missing {data_key} array")

    station_metadata = dict(metadata)
    records = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise NoaaCoopsError("CO-OPS response contains a non-object data row")
        timestamp = _timestamp(row.get("t"), time_zone)
        record = {
            "id": _record_id(station, product, datum, units, time_zone, timestamp),
            "source": SOURCE,
            "station": station,
            "station_metadata": station_metadata,
            "timestamp": timestamp,
            "product": product,
            "datum": datum,
            "units": units,
            "time_zone": time_zone,
            "value": _number(row.get("v"), "water level"),
            "standard_error": _number(row.get("s"), "standard error"),
            "flags": row.get("f"),
            "quality_code": row.get("q"),
        }
        if product == "predictions":
            record["prediction_type"] = row.get("type")
        records.append(record)
    return records


def fetch_observations(
    station: str = DEFAULT_STATION,
    observation_date: Optional[str] = None,
    begin_date: Optional[str] = None,
    end_date: Optional[str] = None,
    product: str = "water_level",
    datum: str = "MLLW",
    units: str = "metric",
    time_zone: str = "gmt",
    timeout: int = 60,
    opener: Optional[Callable[..., Any]] = None,
) -> List[Dict[str, Any]]:
    """Fetch and normalize one API-bounded station request."""
    url = build_request_url(station, observation_date, begin_date, end_date, product, datum, units, time_zone)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    open_request = opener or urllib.request.urlopen
    try:
        with open_request(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NoaaCoopsError(f"unable to fetch CO-OPS response: {error}") from error
    return normalize_response(payload, station, product, datum.upper(), units, time_zone)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", default=DEFAULT_STATION, help="seven-digit NOAA CO-OPS station ID")
    dates = parser.add_mutually_exclusive_group()
    dates.add_argument("--date", dest="observation_date", help="one UTC calendar date: YYYY-MM-DD or YYYYMMDD")
    dates.add_argument("--begin-date", help="inclusive range start: YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--end-date", help="inclusive range end; required with --begin-date")
    parser.add_argument("--product", choices=sorted(PRODUCTS), default="water_level")
    parser.add_argument("--datum", default="MLLW", help="vertical datum, for example MLLW")
    parser.add_argument("--units", choices=sorted(UNITS), default="metric")
    parser.add_argument("--time-zone", choices=sorted(TIME_ZONES), default="gmt")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    try:
        records = fetch_observations(
            station=args.station,
            observation_date=args.observation_date,
            begin_date=args.begin_date,
            end_date=args.end_date,
            product=args.product,
            datum=args.datum,
            units=args.units,
            time_zone=args.time_zone,
            timeout=args.timeout,
        )
    except NoaaCoopsError as error:
        parser.error(str(error))
    for record in records:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
