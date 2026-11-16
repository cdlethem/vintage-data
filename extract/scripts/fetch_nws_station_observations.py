#!/usr/bin/env python3
"""Snapshot new National Weather Service ASOS surface observations as NDJSON.

Each run polls a fixed basket of major U.S. metro weather stations. The NWS
``/stations/{id}/observations`` endpoint is keyless and returns the station's
most recent reports (newest first) as a GeoJSON FeatureCollection. A per-station
watermark (the newest emitted observation timestamp, kept in the state file)
bounds each run: a station is replayed only for timestamps strictly newer than
its watermark, never older than the ``--lookback-hours`` floor, so a paused
source resumes with a bounded catch-up instead of a multi-day backfill. State
is committed atomically after every record has been written, so an aborted run
re-emits at most the last report per station (at-least-once; RAW is insert-only).

Diagnostics and the ``VINTAGE_RUN_SUMMARY`` line go to stderr; NDJSON goes to
stdout. Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

SOURCE = "nws_station_observations"
BASE = "https://api.weather.gov/stations"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1

# Fixed basket of major U.S. metro ASOS stations; each reports roughly hourly.
DEFAULT_STATIONS = (
    "KATL",  # Hartsfield-Jackson Atlanta International
    "KBOS",  # Boston Logan International
    "KDEN",  # Denver International
    "KDFW",  # Dallas/Fort Worth International
    "KIAH",  # George Bush Intercontinental (Houston)
    "KJFK",  # John F. Kennedy International (New York)
    "KLAS",  # Harry Reid International (Las Vegas)
    "KLAX",  # Los Angeles International
    "KMIA",  # Miami International
    "KMSP",  # Minneapolis–Saint Paul International
    "KNYC",  # New York City, Central Park
    "KPHL",  # Philadelphia International
    "KPHX",  # Phoenix Sky Harbor International
    "KSEA",  # Seattle–Tacoma International
    "KSFO",  # San Francisco International
)

STATION_PATTERN = r"^[A-Z0-9]{4}\Z"
DEFAULT_LOOKBACK_HOURS = 48.0
MAX_LOOKBACK_HOURS = 216.0  # the endpoint keeps ~9 days; cap well inside it
DEFAULT_LIMIT = 60
MAX_LIMIT = 154
DEFAULT_TIMEOUT = 30.0
MAX_TIMEOUT = 120.0
MAX_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 2.0
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_FAILURE_CHARS = 240


class NwsError(RuntimeError):
    """A request or response cannot satisfy the extractor contract."""


class SummaryEmissionError(OSError):
    """The reserved run-summary line could not be fully emitted."""


def state_path(explicit: str | os.PathLike[str] | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict[str, Any]:
    """Return the per-station watermark map; a missing file means first run."""
    if not path.exists():
        return {"version": STATE_VERSION, "stations": {}}
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read state file {path}: {exc}") from exc
    if not isinstance(document, dict) or document.get("version") != STATE_VERSION:
        raise ValueError(f"state file {path} has an unsupported format")
    stations = document.get("stations")
    if not isinstance(stations, dict):
        raise ValueError(f"state file {path} has invalid stations")
    for station, watermark in stations.items():
        if not isinstance(station, str) or not isinstance(watermark, str):
            raise ValueError(f"state file {path} has an invalid station watermark")
    return document


def save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(state, output, sort_keys=True, separators=(",", ":"))
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _bounded_float(value: Any, name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be a number between {minimum} and {maximum}")
    return float(value)


def normalize_station(value: str) -> str:
    station = str(value).strip().upper()
    if not re.fullmatch(STATION_PATTERN, station):
        raise ValueError(f"invalid station identifier: {value!r}")
    return station


def _match_station(station: str) -> bool:
    return re.fullmatch(STATION_PATTERN, station) is not None


def _failure_message(exc: BaseException) -> str:
    message = " ".join(str(exc).split())
    return message[:MAX_FAILURE_CHARS]


def _parse_timestamp(value: Any) -> datetime | None:
    """Parse an ISO-8601 timestamp into aware UTC, or None when malformed."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _metric_value(properties: dict[str, Any], field: str) -> int | float | None:
    node = properties.get(field)
    if not isinstance(node, dict):
        return None
    value = node.get("value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return value


def normalize_feature(feature: Any, fetched_at: str) -> dict[str, Any]:
    """Validate one NWS observation feature and normalize its key fields."""
    if not isinstance(feature, dict) or feature.get("type") != "Feature":
        raise NwsError("NWS observation is not a GeoJSON Feature")
    properties = feature.get("properties")
    if not isinstance(properties, dict):
        raise NwsError("NWS observation has no properties")
    station_id = properties.get("stationId")
    if not isinstance(station_id, str) or not _match_station(station_id):
        raise NwsError(f"NWS observation has no usable stationId: {station_id!r}")
    timestamp_raw = properties.get("timestamp")
    timestamp = _parse_timestamp(timestamp_raw)
    if timestamp is None:
        raise NwsError(f"NWS observation has no usable timestamp: {timestamp_raw!r}")
    geometry = feature.get("geometry")
    coordinates = geometry.get("coordinates") if isinstance(geometry, dict) else None
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise NwsError("NWS observation has no usable Point geometry")
    for coordinate in coordinates[:2]:
        if isinstance(coordinate, bool) or not isinstance(coordinate, (int, float)):
            raise NwsError("NWS observation has non-numeric coordinates")
    present_weather = properties.get("presentWeather")
    if present_weather is None:
        present_weather = []
    if not isinstance(present_weather, list) or not all(
        isinstance(entry, str) for entry in present_weather
    ):
        raise NwsError("NWS observation has non-string presentWeather entries")
    return {
        "source": SOURCE,
        "id": f"{station_id}/{timestamp_raw}",
        "fetched_at": fetched_at,
        "station_id": station_id,
        "station_name": properties.get("stationName"),
        "timestamp": timestamp.isoformat(),
        "lat": coordinates[1],
        "lon": coordinates[0],
        "elevation_m": _metric_value(properties, "elevation"),
        "temperature_c": _metric_value(properties, "temperature"),
        "dewpoint_c": _metric_value(properties, "dewpoint"),
        "relative_humidity_pct": _metric_value(properties, "relativeHumidity"),
        "wind_speed_kmh": _metric_value(properties, "windSpeed"),
        "wind_direction_deg": _metric_value(properties, "windDirection"),
        "wind_gust_kmh": _metric_value(properties, "windGust"),
        "barometric_pressure_pa": _metric_value(properties, "barometricPressure"),
        "sea_level_pressure_pa": _metric_value(properties, "seaLevelPressure"),
        "visibility_m": _metric_value(properties, "visibility"),
        "precipitation_last_hour_mm": _metric_value(properties, "precipitationLastHour"),
        "text_description": properties.get("textDescription"),
        "present_weather": present_weather,
        "raw_message": properties.get("rawMessage"),
    }


def _request_document(url: str, timeout: float, metrics: dict[str, Any]) -> Any:
    """Fetch one JSON document with one bounded retry for transient failures."""
    last_error: Exception | None = None
    for attempt in range(MAX_ATTEMPTS):
        if attempt:
            time.sleep(RETRY_DELAY_SECONDS)
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/geo+json, application/json", "User-Agent": USER_AGENT},
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = response.read(MAX_RESPONSE_BYTES + 1)
                if len(payload) > MAX_RESPONSE_BYTES:
                    raise NwsError(f"NWS response exceeds {MAX_RESPONSE_BYTES} bytes")
        except urllib.error.HTTPError as exc:
            if exc.code in RETRYABLE_STATUS and attempt + 1 < MAX_ATTEMPTS:
                last_error = exc
                continue
            raise NwsError(f"NWS request failed with HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            if attempt + 1 < MAX_ATTEMPTS:
                last_error = exc
                continue
            raise NwsError(f"NWS request failed: {exc}") from exc
        try:
            document = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise NwsError("NWS response is not valid UTF-8 JSON") from exc
        metrics["requests"]["succeeded"] += 1
        metrics["requests"]["payload_bytes"] += len(payload)
        return document
    raise NwsError(f"NWS request failed after {MAX_ATTEMPTS} attempts: {last_error}")


def fetch_station_observations(
    station: str, limit: int, timeout: float, metrics: dict[str, Any]
) -> list[Any]:
    """Return one station's most recent observation features, newest first."""
    url = f"{BASE}/{station}/observations?limit={limit}"
    document = _request_document(url, timeout, metrics)
    if not isinstance(document, dict) or document.get("type") != "FeatureCollection":
        raise NwsError("NWS observations response is not a FeatureCollection")
    features = document.get("features")
    if not isinstance(features, list):
        raise NwsError("NWS observations response has no features list")
    return features


def collect_observations(
    stations: Sequence[str],
    state: dict[str, Any],
    *,
    now: datetime,
    lookback_hours: float,
    limit: int,
    timeout: float,
    request_delay: float,
    metrics: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Collect one bounded run; return (records, next_state)."""
    now = now.astimezone(timezone.utc)
    fetched_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    floor = now - timedelta(hours=lookback_hours)
    records: list[dict[str, Any]] = []
    next_stations: dict[str, str] = dict(state.get("stations", {}))
    for index, station in enumerate(stations):
        if index:
            time.sleep(request_delay)
        metrics["partitions"]["attempted"] += 1
        metrics["requests"]["attempted"] += 1
        try:
            features = fetch_station_observations(station, limit, timeout, metrics)
        except Exception as exc:
            metrics["requests"]["failed"] += 1
            metrics["partitions"]["failed"] += 1
            metrics["partitions"]["failures"].append(
                {"partition": station, "error": _failure_message(exc)}
            )
            continue
        metrics["partitions"]["succeeded"] += 1
        watermark = _parse_timestamp(next_stations.get(station))
        cutoff = max(floor, watermark) if watermark is not None else floor
        newest: datetime | None = None
        for feature in features:
            try:
                record = normalize_feature(feature, fetched_at)
            except NwsError:
                # A single malformed report must not sink the station's run.
                continue
            observed = _parse_timestamp(record["timestamp"])
            if observed is None or observed <= cutoff:
                continue
            records.append(record)
            if newest is None or observed > newest:
                newest = observed
        if newest is not None:
            next_stations[station] = newest.isoformat()
    return records, {"version": STATE_VERSION, "stations": next_stations}


def build_summary(
    *,
    records: int,
    stations_total: int,
    stations_failed: int,
    requests: dict[str, int],
    failures: list[dict[str, str]] | None = None,
    failed: bool = False,
    error: str | None = None,
) -> dict[str, Any]:
    if failed:
        health = "failed"
        completeness = "failed"
    elif stations_failed == 0:
        health = "healthy"
        completeness = "complete"
    elif stations_total - stations_failed > 0:
        health = "degraded"
        completeness = "partial"
    else:
        health = "failed"
        completeness = "failed"
    partitions: dict[str, Any] = {
        "attempted": stations_total,
        "succeeded": stations_total - stations_failed,
        "failed": stations_failed,
    }
    if failures:
        partitions["failures"] = failures
    summary: dict[str, Any] = {
        "health": health,
        "completeness": completeness,
        "requests": requests,
        "partitions": partitions,
        "records": records,
        "stations": stations_total,
    }
    if error:
        summary["error"] = error
    return summary


def _emit_summary(err: Any, summary: dict[str, Any]) -> None:
    try:
        line = SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":"), sort_keys=True) + "\n"
        err.write(line)
        err.flush()
    except Exception as exc:
        raise SummaryEmissionError(_failure_message(exc)) from exc


def _partitions_from_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    partitions = metrics.get("partitions", {})
    attempted = partitions.get("attempted", 0)
    failed = partitions.get("failed", 0)
    block: dict[str, Any] = {
        "attempted": attempted,
        "succeeded": attempted - failed,
        "failed": failed,
    }
    failures = partitions.get("failures")
    if failures:
        block["failures"] = failures
    return block


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--station", action="append", default=[], help="explicit station; repeatable")
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=DEFAULT_LOOKBACK_HOURS,
        help="first-run and catch-up window in hours (default %(default)s)",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="reports to request per station")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="per-request timeout in seconds")
    parser.add_argument("--request-delay", type=float, default=1.0, help="pause between stations in seconds")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="ignore and do not write the watermark (one-off pull)")
    return parser


def main(argv: Sequence[str] | None = None, *, out: Any = None, err: Any = None) -> int:
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    args = _parser().parse_args(list(argv) if argv is not None else None)
    if args.no_state and args.state_file:
        print("--no-state and --state-file cannot be combined", file=err)
        return 2
    try:
        stations = [normalize_station(s) for s in (args.station or DEFAULT_STATIONS)]
        lookback_hours = _bounded_float(args.lookback_hours, "lookback-hours", 1.0, MAX_LOOKBACK_HOURS)
        limit = int(args.limit)
        if limit < 1 or limit > MAX_LIMIT:
            raise ValueError(f"limit must be an integer between 1 and {MAX_LIMIT}")
        timeout = _bounded_float(args.timeout, "timeout", 1.0, MAX_TIMEOUT)
    except ValueError as exc:
        print(str(exc), file=err)
        return 2

    metrics: dict[str, Any] = {
        "requests": {"attempted": 0, "succeeded": 0, "failed": 0, "payload_bytes": 0},
        "partitions": {"attempted": 0, "succeeded": 0, "failed": 0, "failures": []},
    }
    state: dict[str, Any] = {"version": STATE_VERSION, "stations": {}}
    state_file = None if args.no_state else state_path(args.state_file)
    try:
        if state_file is not None:
            state = load_state(state_file)
        records, next_state = collect_observations(
            stations,
            state,
            now=datetime.now(timezone.utc),
            lookback_hours=lookback_hours,
            limit=limit,
            timeout=timeout,
            request_delay=max(0.0, float(args.request_delay)),
            metrics=metrics,
        )
        for record in records:
            out.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        out.flush()
    except Exception as exc:
        partitions = _partitions_from_metrics(metrics)
        summary = build_summary(
            records=0,
            stations_total=len(stations),
            stations_failed=partitions["failed"],
            requests=metrics["requests"],
            failures=partitions.get("failures"),
            failed=True,
            error=_failure_message(exc),
        )
        try:
            _emit_summary(err, summary)
        except SummaryEmissionError:
            pass
        return 1

    partitions = _partitions_from_metrics(metrics)
    if partitions["failed"] == len(stations) and len(stations) > 0:
        # Every station failed: nothing was collected and the source is down.
        summary = build_summary(
            records=len(records),
            stations_total=len(stations),
            stations_failed=partitions["failed"],
            requests=metrics["requests"],
            failures=partitions.get("failures"),
            failed=True,
            error="all stations failed",
        )
        try:
            _emit_summary(err, summary)
        except SummaryEmissionError:
            pass
        return 1

    summary = build_summary(
        records=len(records),
        stations_total=len(stations),
        stations_failed=partitions["failed"],
        requests=metrics["requests"],
        failures=partitions.get("failures"),
    )
    try:
        _emit_summary(err, summary)
    except SummaryEmissionError:
        return 1
    if state_file is not None:
        try:
            save_state(state_file, next_state)
        except Exception as exc:
            print(f"state write failed: {exc}", file=err)
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
