#!/usr/bin/env python3
"""Portugal IPMA — 5-day daily weather forecast for every district.

IPMA publishes a 5-day daily forecast (min/max temperature, precipitation
probability, predicted wind, weather type) for each of the 35 mainland
districts plus Azores and Madeira, revised twice a day with the 00UTC model
run (published around 10:00 UTC) and the 12UTC run (published around 20:00
UTC). Each run snapshots the complete current forecast set, so successive
snapshots recover how the forecast for a given lead day was revised. The
weather-type and daily wind-speed class lookup tables are fetched alongside
and decoded into English labels; the ``-99`` "no data" code decodes to null.

IPMA open-data terms: free personal and public use without profit or
offensive purpose, source attribution required. No key or registration.

Stdlib only.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Sequence

SOURCE = "ipma_city_forecasts"
BASE_URL = "https://api.ipma.pt/open-data"
DISTRICTS_URL = f"{BASE_URL}/distrits-islands.json"
WEATHER_TYPES_URL = f"{BASE_URL}/weather-type-classe.json"
WIND_CLASSES_URL = f"{BASE_URL}/wind-speed-daily-classe.json"
CITY_FORECAST_URL = f"{BASE_URL}/forecast/meteorology/cities/daily/{{id}}.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
NO_DATA_CODE = -99
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 300
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 10.0
MAX_RETRY_DELAY = 120.0


class FetchError(RuntimeError):
    """A request that exhausted its retries, or a malformed source document."""


def _check_bounds(timeout: int, retries: int, retry_delay: float) -> None:
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= MAX_TIMEOUT:
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")
    if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= MAX_RETRIES:
        raise ValueError(f"retries must be between 0 and {MAX_RETRIES}")
    if (
        isinstance(retry_delay, bool)
        or not isinstance(retry_delay, (int, float))
        or not 0 <= retry_delay <= MAX_RETRY_DELAY
    ):
        raise ValueError(f"retry_delay must be between 0 and {MAX_RETRY_DELAY}")


def get_json(
    url: str, timeout: int, retries: int, retry_delay: float
) -> tuple[Any, int, int]:
    """GET one JSON document, retrying only transient HTTP failures.

    Returns ``(document, attempts, retries_made)``.
    """
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response), attempt + 1, attempt
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in RETRIABLE_STATUSES or attempt == retries:
                break
            print(
                f"IPMA request to {url} failed with HTTP {exc.code}; "
                f"retry {attempt + 1}/{retries} in {retry_delay:g}s",
                file=sys.stderr,
            )
            time.sleep(retry_delay)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt == retries:
                break
            print(
                f"IPMA request to {url} failed with {exc}; "
                f"retry {attempt + 1}/{retries} in {retry_delay:g}s",
                file=sys.stderr,
            )
            time.sleep(retry_delay)
    raise FetchError(f"IPMA request to {url} failed after {retries + 1} attempt(s): {last_error}")


def _data_list(document: Any, what: str) -> list[dict]:
    if not isinstance(document, dict):
        raise FetchError(f"IPMA {what} response is not a JSON object")
    rows = document.get("data")
    if not isinstance(rows, list) or not rows:
        raise FetchError(f"IPMA {what} response has no non-empty data list")
    return rows


def fetch_districts(timeout: int, retries: int, retry_delay: float) -> tuple[list[dict], int, int]:
    """Fetch the 35 forecast districts and validate location identifiers."""
    document, attempts, made = get_json(DISTRICTS_URL, timeout, retries, retry_delay)
    rows = _data_list(document, "districts")
    seen: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise FetchError("IPMA district entry is not a JSON object")
        location_id = row.get("globalIdLocal")
        if isinstance(location_id, bool) or not isinstance(location_id, int):
            raise FetchError(f"IPMA district is missing integer globalIdLocal: {row!r}")
        if not row.get("local"):
            raise FetchError(f"IPMA district {location_id} is missing its local name")
        if location_id in seen:
            raise FetchError(f"IPMA district list repeats globalIdLocal {location_id}")
        seen.add(location_id)
    return rows, attempts, made


def fetch_lookups(
    timeout: int, retries: int, retry_delay: float
) -> tuple[dict[str, str], dict[str, str], int, int]:
    """Fetch the weather-type and wind-speed class code tables."""
    weather_document, w_attempts, w_made = get_json(
        WEATHER_TYPES_URL, timeout, retries, retry_delay
    )
    weather_types: dict[str, str] = {}
    for row in _data_list(weather_document, "weather types"):
        if isinstance(row, dict) and row.get("descWeatherTypeEN"):
            weather_types[str(row.get("idWeatherType"))] = str(row["descWeatherTypeEN"])
    wind_document, m_attempts, m_made = get_json(
        WIND_CLASSES_URL, timeout, retries, retry_delay
    )
    wind_classes: dict[str, str] = {}
    for row in _data_list(wind_document, "wind speed classes"):
        if isinstance(row, dict) and row.get("descClassWindSpeedDailyEN"):
            wind_classes[str(row.get("classWindSpeed"))] = str(row["descClassWindSpeedDailyEN"])
    return weather_types, wind_classes, w_attempts + m_attempts, w_made + m_made


def decode_label(table: dict[str, str], code: Any) -> str | None:
    """Decode one IPMA class code; the -99 no-data code and unknown codes are null."""
    if code is None or isinstance(code, bool):
        return None
    if code == NO_DATA_CODE:
        return None
    return table.get(str(code))


def normalize_row(
    row: Any,
    location: dict,
    weather_types: dict[str, str],
    wind_classes: dict[str, str],
    fetched_at: str,
    data_update: Any,
) -> dict[str, Any]:
    """Validate one forecast row and stamp the run envelope; source values pass through."""
    if not isinstance(row, dict):
        raise FetchError("IPMA forecast row is not a JSON object")
    forecast_date = row.get("forecastDate")
    if not isinstance(forecast_date, str) or len(forecast_date) != 10:
        raise FetchError(f"IPMA forecast row is missing a valid forecastDate: {row!r}")
    for field in ("tMin", "tMax"):
        if field not in row:
            raise FetchError(
                f"IPMA forecast row for {forecast_date} is missing {field}"
            )
    location_id = location["globalIdLocal"]
    record = dict(row)
    record.update(
        {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{location_id}:{forecast_date}",
            "local": location["local"],
            "id_distrito": location.get("idDistrito"),
            "id_regiao": location.get("idRegiao"),
            "id_area_aviso": location.get("idAreaAviso"),
            "data_update": data_update if isinstance(data_update, str) else None,
            "weather_type_en": decode_label(weather_types, row.get("idWeatherType")),
            "wind_speed_class_en": decode_label(wind_classes, row.get("classWindSpeed")),
        }
    )
    return record


def fetch_city_forecast(
    location: dict,
    weather_types: dict[str, str],
    wind_classes: dict[str, str],
    fetched_at: str,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> tuple[list[dict], int, int]:
    """Fetch one district's multi-day forecast; returns (records, attempts, retries_made)."""
    url = CITY_FORECAST_URL.format(id=location["globalIdLocal"])
    document, attempts, made = get_json(url, timeout, retries, retry_delay)
    rows = _data_list(document, f"forecast for {location['local']}")
    data_update = document.get("dataUpdate")
    return (
        [
            normalize_row(row, location, weather_types, wind_classes, fetched_at, data_update)
            for row in rows
        ],
        attempts,
        made,
    )


def _emit_summary(
    health: str,
    completeness: str,
    requests_attempted: int,
    requests_succeeded: int,
    request_retries: int,
    partitions_attempted: int,
    partitions_succeeded: int,
    partition_failures: list[dict],
    records: int,
) -> None:
    payload = {
        "health": health,
        "completeness": completeness,
        "requests": {
            "attempted": requests_attempted,
            "succeeded": requests_succeeded,
            "failed": requests_attempted - requests_succeeded,
            "retries": request_retries,
        },
        "partitions": {
            "attempted": partitions_attempted,
            "succeeded": partitions_succeeded,
            "failed": partitions_attempted - partitions_succeeded,
            "failures": partition_failures,
        },
        "records": records,
    }
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def run(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                        help="per-request timeout in seconds")
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES,
                        help="retries per request for transient HTTP failures")
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY,
                        help="seconds to wait between retries")
    args = parser.parse_args(argv)
    try:
        _check_bounds(args.timeout, args.retries, args.retry_delay)
    except ValueError as exc:
        parser.error(str(exc))

    fetched_at = datetime.now(timezone.utc).isoformat()
    requests_attempted = 0
    requests_succeeded = 0
    request_retries = 0

    def add_usage(attempts: int, made: int, successes: int) -> None:
        nonlocal requests_attempted, requests_succeeded, request_retries
        requests_attempted += attempts
        requests_succeeded += successes
        request_retries += made

    try:
        districts, attempts, made = fetch_districts(
            args.timeout, args.retries, args.retry_delay
        )
        add_usage(attempts, made, 1)
        weather_types, wind_classes, attempts, made = fetch_lookups(
            args.timeout, args.retries, args.retry_delay
        )
        add_usage(attempts, made, 2)
    except FetchError as exc:
        print(str(exc), file=sys.stderr)
        _emit_summary(
            "failed", "failed", requests_attempted, requests_succeeded, request_retries,
            partitions_attempted=0, partitions_succeeded=0, partition_failures=[], records=0,
        )
        return 1

    records: list[dict] = []
    partition_failures: list[dict] = []
    for location in districts:
        location_id = location["globalIdLocal"]
        try:
            city_records, attempts, made = fetch_city_forecast(
                location, weather_types, wind_classes, fetched_at,
                args.timeout, args.retries, args.retry_delay,
            )
        except Exception as exc:  # one district must not sink the whole run
            partition_failures.append({"name": str(location_id), "error": str(exc)[:240]})
            continue
        add_usage(attempts, made, 1)
        records.extend(city_records)

    for record in records:
        print(json.dumps(record, ensure_ascii=False))

    failed = len(partition_failures)
    if not records:
        health = "failed"
        completeness = "failed"
        exit_code = 1
    elif failed:
        health = "degraded"
        completeness = "partial"
        exit_code = 0
    else:
        health = "healthy"
        completeness = "complete"
        exit_code = 0
    _emit_summary(
        health, completeness, requests_attempted, requests_succeeded, request_retries,
        partitions_attempted=len(districts), partitions_succeeded=len(districts) - failed,
        partition_failures=partition_failures, records=len(records),
    )
    return exit_code


if __name__ == "__main__":
    sys.exit(run())
