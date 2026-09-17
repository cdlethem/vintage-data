#!/usr/bin/env python3
"""Fetch bounded NOAA CO-OPS water-level observations and predictions.

The NOAA Data API returns timestamps without offsets. This extractor deliberately requests
only GMT and emits UTC-aware timestamps; ``lst`` and ``lst_ldt`` are rejected because their
fall-back-hour timestamps cannot be normalized without station time-zone metadata and a fold
indicator. Requests are limited to 31 days and paced when multiple products are fetched.

NOAA CO-OPS data are United States government works. Users should attribute NOAA/NOS/CO-OPS
and the station identified in each record. Stdlib only.
"""

import argparse
import hashlib
import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

SOURCE = "noaa_coops_observations"
API_URL = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
APPLICATION = "vintage_data"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_STATION = "9414290"
MAX_REQUEST_SPAN = timedelta(days=31)

# NOAA Data API water-level datums. Availability is station-specific; the API's
# station-specific error is surfaced with request context rather than guessed locally.
WATER_LEVEL_DATUMS = frozenset(
    {"CRD", "DTL", "IGLD", "LWD", "MHHW", "MHW", "MLLW", "MLW", "MSL", "MTL", "NAVD", "STND"}
)
PRODUCT_DATUMS = {
    "water_level": WATER_LEVEL_DATUMS,
    "predictions": WATER_LEVEL_DATUMS,
}
UNITS = frozenset({"english", "metric"})
MISSING_NUMBERS = frozenset({"", "-", "--", "nan", "null", "none"})


class NOAAAPIError(ValueError):
    """A contextualized error returned by the NOAA Data API."""


def _utc_datetime(value, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name} must not be empty")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        for candidate in (text, text.replace(" ", "T", 1)):
            try:
                parsed = datetime.fromisoformat(candidate)
                break
            except ValueError:
                parsed = None
        if parsed is None:
            try:
                parsed = datetime.strptime(text, "%Y%m%d %H:%M")
            except ValueError as exc:
                raise ValueError(f"{field_name} is not a valid date/time: {value!r}") from exc
    else:
        raise TypeError(f"{field_name} must be a datetime or string")

    # A timestamp without an offset is interpreted as GMT only because local time
    # zones are rejected before request construction.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _api_datetime(value: datetime) -> str:
    return value.strftime("%Y%m%d %H:%M")


def _response_timestamp(value) -> str:
    parsed = _utc_datetime(value, "response timestamp")
    return parsed.isoformat(timespec="seconds")


def _number(value, field_name: str):
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} is not numeric: {value!r}")
    if isinstance(value, str):
        compact = value.strip()
        if compact.lower() in MISSING_NUMBERS:
            return None
        compact = compact.replace(",", "")
    elif isinstance(value, (int, float)):
        compact = value
    else:
        raise ValueError(f"{field_name} is not numeric: {value!r}")
    try:
        number = float(compact)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} is not numeric: {value!r}") from exc
    if not math.isfinite(number):
        raise ValueError(f"{field_name} is not finite: {value!r}")
    return number


def _validated_request(
    station,
    product: str,
    datum: str,
    units: str,
    time_zone: str,
    begin_date,
    end_date,
    timeout: float,
    retries: int,
):
    station = str(station).strip()
    if not station:
        raise ValueError("station must not be empty")

    product = str(product).strip().lower()
    if product not in PRODUCT_DATUMS:
        supported = ", ".join(sorted(PRODUCT_DATUMS))
        raise ValueError(f"unsupported product {product!r}; expected one of: {supported}")

    datum = str(datum).strip().upper()
    if datum not in PRODUCT_DATUMS[product]:
        accepted = ", ".join(sorted(PRODUCT_DATUMS[product]))
        raise ValueError(f"datum {datum!r} is not valid for product {product!r}; expected one of: {accepted}")

    units = str(units).strip().lower()
    if units not in UNITS:
        raise ValueError(f"unsupported units {units!r}; expected english or metric")

    time_zone = str(time_zone).strip().lower()
    if time_zone != "gmt":
        raise ValueError(
            f"unsupported time_zone {time_zone!r}; only 'gmt' is supported because local NOAA timestamps are ambiguous"
        )

    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if isinstance(retries, bool) or retries < 0:
        raise ValueError("retries must be a non-negative integer")

    begin = _utc_datetime(begin_date, "begin_date")
    end = _utc_datetime(end_date, "end_date")
    if begin >= end:
        raise ValueError("begin_date must be earlier than end_date")
    if end - begin > MAX_REQUEST_SPAN:
        raise ValueError("request span must not exceed 31 days")

    return station, product, datum, units, time_zone, begin, end


def _request_document(parameters, timeout: float, retries: int, retry_delay: float):
    if retry_delay < 0:
        raise ValueError("retry_delay must be non-negative")
    url = API_URL + "?" + urllib.parse.urlencode(parameters)
    request = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": USER_AGENT})

    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            retryable = exc.code == 429 or 500 <= exc.code < 600
            if not retryable or attempt == retries:
                raise
        except (TimeoutError, urllib.error.URLError):
            if attempt == retries:
                raise
        if retry_delay:
            time.sleep(retry_delay * (2**attempt))
    raise AssertionError("unreachable")


def _api_error(document, station: str, product: str, datum: str):
    error = document.get("error")
    if error is None:
        return
    if isinstance(error, dict):
        message = error.get("message") or json.dumps(error, sort_keys=True)
    else:
        message = str(error)
    raise NOAAAPIError(
        f"NOAA CO-OPS rejected station {station} product {product} datum {datum}: {message}"
    )


def _station_metadata(document, requested_station: str):
    metadata = document.get("metadata")
    if metadata is None:
        return {"id": requested_station}
    if not isinstance(metadata, dict):
        raise TypeError("NOAA response metadata must be an object")

    normalized = dict(metadata)
    metadata_id = normalized.get("id")
    if metadata_id is not None and str(metadata_id) != requested_station:
        raise ValueError(
            f"NOAA response station {metadata_id!r} does not match requested station {requested_station!r}"
        )
    normalized["id"] = requested_station
    for key in ("lat", "lon"):
        if key in normalized:
            normalized[key] = _number(normalized[key], f"station metadata {key}")
    return normalized


def _stable_id(station: str, product: str, datum: str, timestamp: str) -> str:
    identity = f"{SOURCE}|{station}|{product}|{datum}|{timestamp}".encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def fetch_observations(
    station=DEFAULT_STATION,
    product="water_level",
    begin_date=None,
    end_date=None,
    *,
    datum="MLLW",
    units="metric",
    time_zone="gmt",
    timeout=30,
    retries=2,
    retry_delay=1.0,
):
    """Yield normalized records for one bounded station/product request."""
    if begin_date is None or end_date is None:
        raise ValueError("begin_date and end_date are required")
    (
        station,
        product,
        datum,
        units,
        time_zone,
        begin,
        end,
    ) = _validated_request(station, product, datum, units, time_zone, begin_date, end_date, timeout, retries)

    parameters = {
        "product": product,
        "application": APPLICATION,
        "begin_date": _api_datetime(begin),
        "end_date": _api_datetime(end),
        "station": station,
        "time_zone": time_zone,
        "units": units,
        "format": "json",
        "datum": datum,
    }
    document = _request_document(parameters, timeout, retries, retry_delay)
    if not isinstance(document, dict):
        raise TypeError("NOAA response must be a JSON object")
    _api_error(document, station, product, datum)

    rows_key = "data" if product == "water_level" else "predictions"
    rows = document.get(rows_key)
    if not isinstance(rows, list):
        raise TypeError(f"NOAA {product} response is missing {rows_key!r} list")

    station_metadata = _station_metadata(document, station)
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise TypeError(f"NOAA {product} row {index} must be an object")
        if not row.get("t"):
            raise ValueError(f"NOAA {product} row {index} is missing timestamp 't'")
        timestamp = _response_timestamp(row["t"])
        value = _number(row.get("v"), f"{product} row {index} value")
        standard_error = _number(row.get("s"), f"{product} row {index} standard error")
        record = {
            "id": _stable_id(station, product, datum, timestamp),
            "source": SOURCE,
            "station_id": station,
            "station": dict(station_metadata),
            "station_name": station_metadata.get("name"),
            "latitude": station_metadata.get("lat"),
            "longitude": station_metadata.get("lon"),
            "product": product,
            "timestamp": timestamp,
            "value": value,
            "units": units,
            "datum": datum,
            "standard_error": standard_error,
            "flags": row.get("f"),
            "quality_code": row.get("q"),
            "fetched_at": fetched_at,
        }
        yield record


def _window(begin_date, end_date, lookback_hours: float):
    if (begin_date is None) != (end_date is None):
        raise ValueError("begin_date and end_date must be supplied together")
    if begin_date is not None:
        return begin_date, end_date
    if lookback_hours <= 0 or lookback_hours > MAX_REQUEST_SPAN.total_seconds() / 3600:
        raise ValueError("lookback_hours must be greater than 0 and no more than 744")
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    return end - timedelta(hours=lookback_hours), end


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", default=DEFAULT_STATION)
    parser.add_argument("--product", dest="products", action="append", choices=sorted(PRODUCT_DATUMS))
    parser.add_argument("--datum", default="MLLW")
    parser.add_argument("--units", default="metric", choices=sorted(UNITS))
    parser.add_argument("--time-zone", default="gmt")
    parser.add_argument("--begin-date")
    parser.add_argument("--end-date")
    parser.add_argument("--lookback-hours", type=float, default=24.0)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--request-delay", type=float, default=1.0)
    args = parser.parse_args(argv)

    try:
        begin, end = _window(args.begin_date, args.end_date, args.lookback_hours)
        if args.request_delay < 0:
            raise ValueError("request_delay must be non-negative")
        products = args.products or ["water_level", "predictions"]
        for product_index, product in enumerate(products):
            if product_index and args.request_delay:
                time.sleep(args.request_delay)
            for record in fetch_observations(
                args.station,
                product,
                begin,
                end,
                datum=args.datum,
                units=args.units,
                time_zone=args.time_zone,
                timeout=args.timeout,
                retries=args.retries,
                retry_delay=args.request_delay,
            ):
                print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
