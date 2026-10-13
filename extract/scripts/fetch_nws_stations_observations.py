#!/usr/bin/env python3
"""Fetch latest hourly NWS surface observations for curated US stations as NDJSON.

The National Weather Service open API publishes the most recent METAR-style
observation per station at
``https://api.weather.gov/stations/<id>/observations/latest``. It is open
data, free for any purpose, and needs no key; a descriptive User-Agent is
required. The data is US Government work (public domain).

This fetcher polls a curated list of 24 major US ASOS stations, one request
per station, paced by a minimum interval (default 1.0 s). Each run stamps
every emitted record with the same UTC ``fetched_at`` and uses the station's
own observation timestamp as the source-natural id component.

Per-station staleness guard: the state file records the last emitted
observation time per station. A run skips a station whose latest observation
timestamp is not newer than the stored one, so consecutive runs that observe
no change emit no duplicate rows. State is published atomically only after
all output for the run has been written and flushed, so a failed run never
advances the watermark. Partial runs (some stations failing) exit 0 with
``health: "partial"`` evidence; a run in which every station fails exits
non-zero.

Stdlib only.
"""

import argparse
import json
import os
import pathlib
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, TextIO

SOURCE = "nws_stations_observations"
BASE = "https://api.weather.gov"
# Curated major US ASOS stations (ICAO ids), verified live 2026-10-13.
# Growth stays a script edit; each station costs one request per run.
STATIONS = (
    "KSEA",  # Seattle, WA
    "KPDX",  # Portland, OR
    "KSFO",  # San Francisco, CA
    "KLAX",  # Los Angeles, CA
    "KSAN",  # San Diego, CA
    "KLAS",  # Las Vegas, NV
    "KPHX",  # Phoenix, AZ
    "KSLC",  # Salt Lake City, UT
    "KDEN",  # Denver, CO
    "KOKC",  # Oklahoma City, OK
    "KDFW",  # Dallas/Fort Worth, TX
    "KIAH",  # Houston, TX
    "KMSY",  # New Orleans, LA
    "KTPA",  # Tampa, FL
    "KMIA",  # Miami, FL
    "KATL",  # Atlanta, GA
    "KMSP",  # Minneapolis, MN
    "KMCI",  # Kansas City, MO
    "KORD",  # Chicago, IL
    "KCLT",  # Charlotte, NC
    "KBNA",  # Nashville, TN
    "KDCA",  # Washington, DC
    "KJFK",  # New York, NY
    "KBOS",  # Boston, MA
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
DEFAULT_TIMEOUT = 30
DEFAULT_MIN_INTERVAL = 1.0
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

# source field -> (output field, expected WMO unit code)
OBSERVATION_FIELDS = {
    "temperature": ("temperature_c", "wmoUnit:degC"),
    "dewpoint": ("dewpoint_c", "wmoUnit:degC"),
    "windChill": ("wind_chill_c", "wmoUnit:degC"),
    "heatIndex": ("heat_index_c", "wmoUnit:degC"),
    "maxTemperatureLast24Hours": ("max_temperature_24h_c", "wmoUnit:degC"),
    "minTemperatureLast24Hours": ("min_temperature_24h_c", "wmoUnit:degC"),
    "windDirection": ("wind_direction_deg", "wmoUnit:degree_(angle)"),
    "windSpeed": ("wind_speed_kmh", "wmoUnit:km_h-1"),
    "windGust": ("wind_gust_kmh", "wmoUnit:km_h-1"),
    "barometricPressure": ("barometric_pressure_pa", "wmoUnit:Pa"),
    "seaLevelPressure": ("sea_level_pressure_pa", "wmoUnit:Pa"),
    "visibility": ("visibility_m", "wmoUnit:m"),
    "precipitationLast3Hours": ("precipitation_3h_mm", "wmoUnit:mm"),
    "relativeHumidity": ("relative_humidity_pct", "wmoUnit:percent"),
}


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects before urllib can issue a destination request."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


HTTP_OPENER = urllib.request.build_opener(NoRedirectHandler())


def timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} is not an ISO-8601 timestamp: {value!r}") from error
    if parsed.tzinfo is None:
        raise ValueError(f"{field} must carry a UTC offset")
    return parsed.astimezone(timezone.utc)


def quantified_value(value: object, field: str, unit: str) -> float | int | None:
    """Validate one {value, unitCode} observation field; None when absent."""
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a unit-quantified object")
    number = value.get("value")
    if number is not None and not isinstance(number, (int, float)):
        raise ValueError(f"{field}.value must be a number")
    unit_code = value.get("unitCode")
    if unit_code is not None and unit_code != unit:
        raise ValueError(f"{field}.unitCode is {unit_code!r}, expected {unit!r}")
    return number


def present_weather_list(value: object, field: str) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("weather"), str)
        for item in value
    ):
        raise ValueError(f"{field} must be a list of objects with a string 'weather' code")
    return value


def normalize_observation(document: object, station_id: str) -> dict:
    """Validate and normalize one /observations/latest document."""
    if not isinstance(document, dict):
        raise ValueError("NWS observation document must be an object")
    if document.get("stationId") != station_id:
        raise ValueError(
            f"stationId {document.get('stationId')!r} does not match requested {station_id}"
        )
    observation_time = parse_timestamp(document.get("timestamp"), "timestamp")
    record = {
        "source": SOURCE,
        "id": f"{station_id}@{timestamp_text(observation_time)}",
        "station_id": station_id,
        "observation_time": timestamp_text(observation_time),
    }
    station_name = document.get("stationName")
    if station_name is not None and not isinstance(station_name, str):
        raise ValueError("stationName must be a string")
    record["station_name"] = station_name
    text_description = document.get("textDescription")
    if text_description is not None and not isinstance(text_description, str):
        raise ValueError("textDescription must be a string")
    record["text_description"] = text_description
    for source_field, (out_field, unit) in OBSERVATION_FIELDS.items():
        record[out_field] = quantified_value(document.get(source_field), source_field, unit)
    record["present_weather"] = present_weather_list(
        document.get("presentWeather"), "presentWeather"
    )
    cloud_layers = document.get("cloudLayers")
    if cloud_layers is None:
        record["cloud_layers"] = []
    elif (
        not isinstance(cloud_layers, list)
        or any(not isinstance(layer, dict) for layer in cloud_layers)
    ):
        raise ValueError("cloudLayers must be a list of objects")
    else:
        record["cloud_layers"] = cloud_layers
    raw_message = document.get("rawMessage")
    if raw_message is not None and not isinstance(raw_message, str):
        raise ValueError("rawMessage must be a string")
    record["raw_message"] = raw_message
    return record


def default_state() -> dict:
    return {"version": STATE_VERSION, "source": SOURCE, "last_observation": {}}


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def validate_state(document: object, path: pathlib.Path) -> dict:
    if not isinstance(document, dict):
        raise ValueError(f"state file {path} must contain a JSON object")
    if document.get("version") != STATE_VERSION:
        raise ValueError(f"state file {path} has version {document.get('version')!r}")
    if document.get("source") != SOURCE:
        raise ValueError(f"state file {path} is for source {document.get('source')!r}")
    last = document.get("last_observation")
    if not isinstance(last, dict) or any(
        not isinstance(k, str) or not isinstance(v, str) for k, v in last.items()
    ):
        raise ValueError(f"state file {path} has a malformed last_observation map")
    return document


def load_state(path: pathlib.Path) -> dict:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default_state()
    return validate_state(document, path)


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish state after stdout has accepted the complete run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{SOURCE}-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(state, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def fetch_latest(url: str, timeout: int) -> object:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/ld+json; q=0.9, application/json",
        },
    )
    try:
        with HTTP_OPENER.open(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        if 400 <= error.code < 500:
            # A 4xx means this station does not exist (or is misconfigured in
            # our list); surface it per-station without a retryable error.
            raise
        raise


def run(
    *,
    stations: tuple[str, ...] = STATIONS,
    path: pathlib.Path | None,
    output: TextIO,
    timeout: int = DEFAULT_TIMEOUT,
    min_interval: float = DEFAULT_MIN_INTERVAL,
    transport: Callable[[str, int], object] = fetch_latest,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    if not stations:
        raise ValueError("station list must not be empty")
    if timeout <= 0:
        raise ValueError("timeout must be a positive number of seconds")
    if min_interval < 0:
        raise ValueError("min_interval must not be negative")
    fetched_at = now()
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at = fetched_at.astimezone(timezone.utc)
    fetched_at_text = timestamp_text(fetched_at)

    state = default_state() if path is None else load_state(path)
    last_observation = dict(state["last_observation"])

    records = []
    failures: list[str] = []
    stale = 0
    new_last = dict(last_observation)
    deadline = None
    for position, station_id in enumerate(stations):
        if deadline is not None:
            wait = deadline - monotonic()
            if wait > 0:
                sleep(wait)
        deadline = monotonic() + min_interval
        url = f"{BASE}/stations/{urllib.parse.quote(station_id)}/observations/latest"
        try:
            document = transport(url, timeout)
            record = normalize_observation(document, station_id)
        except Exception as error:
            failures.append(f"{station_id}: {error}")
            continue
        observation_dt = parse_timestamp(record["observation_time"], "observation_time")
        previous = last_observation.get(station_id)
        if previous is not None:
            previous_dt = parse_timestamp(previous, f"state last_observation[{station_id}]")
            if observation_dt <= previous_dt:
                stale += 1
                continue
        record["fetched_at"] = fetched_at_text
        records.append(record)
        new_last[station_id] = record["observation_time"]

    payload = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records
    )
    if payload:
        written = output.write(payload)
    state_change = {}
    if path is not None:
        changed = {
            k: v for k, v in new_last.items() if last_observation.get(k) != v
        }
        if changed:
            save_state(path, {**state, "last_observation": new_last})
        state_change = changed

    if failures and not records and not stale:
        raise RuntimeError(f"all {len(failures)} station requests failed: {failures}")

    succeeded = len(records) + stale
    summary = {
        "health": "healthy" if not failures else "partial",
        "completeness": "complete" if not failures else "partial",
        "requests": {
            "attempted": len(stations),
            "succeeded": succeeded,
            "failed": len(failures),
        },
        "stations_failed": failures,
        "state_change": state_change,
        "metrics": {
            "stations_ok": succeeded,
            "stations_stale_skipped": stale,
            "records": len(records),
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return len(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="state file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="do not read or update persistent state")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument(
        "--min-interval",
        type=float,
        default=DEFAULT_MIN_INTERVAL,
        help=f"minimum seconds between station requests (default {DEFAULT_MIN_INTERVAL})",
    )
    arguments = parser.parse_args()
    if arguments.no_state and arguments.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    try:
        count = run(
            path=None if arguments.no_state else state_path(arguments.state_file),
            output=sys.stdout,
            timeout=arguments.timeout,
            min_interval=arguments.min_interval,
        )
    except ValueError as error:
        parser.error(str(error))
    print(
        f"Collected {count} NWS station observations "
        f"({len(STATIONS)} stations polled)",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
