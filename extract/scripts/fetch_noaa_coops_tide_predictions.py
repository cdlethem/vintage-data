#!/usr/bin/env python3
"""NOAA CO-OPS — daily high/low tide predictions for major U.S. ports.

Harmonic tide predictions (high/low water on the MLLW datum, GMT, metres) for
a curated set of major ports, from NOAA's keyless CO-OPS Data Retrieval API.
Predictions for each day are issued in advance and are stable once the day has
passed, so a per-station date watermark makes runs gap-free and idempotent: a
run emits one record per high/low water per station, and a missed day is
caught up on the next run without re-emitting already-emitted dates.

United States government work; attribute NOAA/NOS/Center for Operational
Oceanographic Products and Services and the named station. Predictions are
stable once the day has passed, so poll at most once per day.

Stdlib only.
"""

import argparse
import json
import os
import pathlib
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

SOURCE = "noaa_coops_tide_predictions"
BASE = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
MAX_TIMEOUT = 300
MAX_RETRIES = 5
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
TIDE_TYPES = frozenset({"HH", "H", "L", "LL"})
TIDE_TIME = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$")

# Curated major ports, verified against the CO-OPS metadata API
# (stations.json?type=tidepredictions) on 2026-10-07.
STATIONS = {
    "8518750": {"name": "NEW YORK (The Battery)", "state": "NY", "lat": 40.70055389404297, "lng": -74.01416778564453},
    "8465692": {"name": "Lighthouse Point, New Haven Harbor", "state": "CT", "lat": 41.2517, "lng": -72.905},
    "8452660": {"name": "NEWPORT", "state": "RI", "lat": 41.5043333, "lng": -71.3261389},
    "8443970": {"name": "BOSTON", "state": "MA", "lat": 42.35388888888889, "lng": -71.05027777777778},
    "8423745": {"name": "Portsmouth NH", "state": "NH", "lat": 43.0783, "lng": -70.7517},
    "8418150": {"name": "PORTLAND", "state": "ME", "lat": 43.65805555555556, "lng": -70.24416666666667},
    "8413320": {"name": "BAR HARBOR", "state": "ME", "lat": 44.39219444444444, "lng": -68.20427777777778},
    "8550714": {"name": "Wilmington Marine Terminal", "state": "DE", "lat": 39.7183, "lng": -75.52},
    "8574680": {"name": "Baltimore, Fort McHenry", "state": "MD", "lat": 39.266693115234375, "lng": -76.57830810546875},
    "8638610": {"name": "HAMPTON ROADS (Sewells Point)", "state": "VA", "lat": 36.9427778, "lng": -76.3286111},
    "8665530": {"name": "CHARLESTON (Customhouse Wharf)", "state": "SC", "lat": 32.7808333, "lng": -79.9236111},
    "8654400": {"name": "CAPE HATTERAS FISHING PIER", "state": "NC", "lat": 35.965, "lng": -75.485},
    "8724580": {"name": "KEY WEST", "state": "FL", "lat": 24.555700302124023, "lng": -81.80789947509766},
    "8723080": {"name": "Haulover Pier, N. Miami Beach", "state": "FL", "lat": 25.9033, "lng": -80.12},
    "8726668": {"name": "Tampa, Hooker Point, Hillsborough Bay", "state": "FL", "lat": 27.91830062866211, "lng": -82.44499969482422},
    "8729840": {"name": "PENSACOLA", "state": "FL", "lat": 30.366666666666667, "lng": -87.2},
    "8733821": {"name": "Point Clear, Mobile Bay", "state": "AL", "lat": 30.48663888888889, "lng": -87.93452777777778},
    "8741041": {"name": "Dock E, Port of Pascagoula", "state": "MS", "lat": 30.366666666666667, "lng": -88.56666666666667},
    "8770542": {"name": "Buffalo Bayou, Houston Ship Channel", "state": "TX", "lat": 29.761899948120117, "lng": -95.34429931640625},
    "8775870": {"name": "Corpus Christi, Bob Hall Pier", "state": "TX", "lat": 27.579999923706055, "lng": -97.21669769287111},
    "9414290": {"name": "SAN FRANCISCO (Golden Gate)", "state": "CA", "lat": 37.80630555555555, "lng": -122.46588888888889},
    "9410660": {"name": "LOS ANGELES (Outer Harbor)", "state": "CA", "lat": 33.72, "lng": -118.272},
    "9410135": {"name": "South San Diego Bay", "state": "CA", "lat": 32.629100799560555, "lng": -117.10780334472656},
    "9447130": {"name": "SEATTLE (Madison St.), Elliott Bay", "state": "WA", "lat": 47.60263888888889, "lng": -122.33930555555556},
    "1612340": {"name": "HONOLULU", "state": "HI", "lat": 21.303333333333334, "lng": -157.86452777777778},
}


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the per-station watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit)
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {"version", "source", "stations": {station_id: "YYYY-MM-DD"}}."""
    state = {"version": STATE_VERSION, "source": SOURCE, "stations": {}}
    if not path.exists():
        return state
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"state file {path} is unreadable: {exc}") from exc
    stations = document.get("stations")
    if not isinstance(stations, dict):
        raise ValueError(f"state file {path} is missing the stations map")
    for station_id, last_date in stations.items():
        if not isinstance(last_date, str) or not TIDE_TIME.match(last_date + " 00:00"):
            raise ValueError(f"state file {path} has a bad watermark for {station_id!r}")
    state["stations"] = {k: v for k, v in stations.items()}
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, sort_keys=True, indent=1) + "\n", encoding="utf-8")
    os.replace(staged, path)


def station_url(station_id: str, begin: date, end: date) -> str:
    params = {
        "station": station_id,
        "product": "predictions",
        "interval": "hilo",
        "datum": "MLLW",
        "time_zone": "gmt",
        "units": "metric",
        "application": "vintage-data",
        "begin_date": begin.strftime("%Y%m%d"),
        "end_date": end.strftime("%Y%m%d"),
        "format": "json",
    }
    return f"{BASE}?{urllib.parse.urlencode(params)}"


def normalize_prediction(entry: object, station_id: str, meta: dict, fetched_at: str) -> dict:
    if not isinstance(entry, dict):
        raise ValueError("prediction entry is not an object")
    tide_time = entry.get("t")
    if not isinstance(tide_time, str) or not TIDE_TIME.match(tide_time):
        raise ValueError(f"bad tide time {tide_time!r}")
    height = entry.get("v")
    if isinstance(height, bool) or not isinstance(height, (int, float, str)) or not str(height).strip():
        raise ValueError("missing height")
    try:
        height_m = float(str(height).strip())
    except ValueError:
        raise ValueError(f"non-numeric height {height!r}")
    tide_type = entry.get("type")
    if tide_type not in TIDE_TYPES:
        raise ValueError(f"unknown tide type {tide_type!r}")
    iso_time = tide_time.replace(" ", "T") + "+00:00"
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{station_id}|{iso_time}",
        "station": station_id,
        "station_name": meta["name"],
        "state": meta["state"],
        "lat": meta["lat"],
        "lng": meta["lng"],
        "tide_time": iso_time,
        "height_m": height_m,
        "type": tide_type,
        "datum": "MLLW",
    }


class StationFetchError(RuntimeError):
    def __init__(self, station_id: str, detail: str):
        super().__init__(f"station {station_id}: {detail}")
        self.station_id = station_id
        self.detail = detail


def fetch_station(station_id: str, begin: date, end: date, timeout: int,
                  retries: int, delay: float, fetched_at: str) -> tuple[list[dict], int]:
    """Fetch one station's predictions for [begin, end], chunked to the API's
    per-request date window; retry 429/5xx with bounded backoff."""
    meta = STATIONS[station_id]
    records: list[dict] = []
    retries_used = 0
    cursor = begin
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=91))
        url = station_url(station_id, cursor, chunk_end)
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        last_error: Exception | None = None
        for attempt in range(retries + 1):
            try:
                with urllib.request.urlopen(request, timeout=timeout) as response:
                    document = json.load(response)
                break
            except urllib.error.HTTPError as exc:
                if exc.code in RETRIABLE_STATUSES and attempt < retries:
                    last_error = exc
                    retries_used += 1
                    time.sleep(min(2 ** (attempt + 1), 10))
                    continue
                raise StationFetchError(station_id, f"HTTP {exc.code}") from exc
            except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
                if attempt < retries:
                    last_error = exc
                    retries_used += 1
                    time.sleep(min(2 ** (attempt + 1), 10))
                    continue
                raise StationFetchError(station_id, f"{type(exc).__name__}: {exc}") from exc
        if not isinstance(document, dict) or not isinstance(document.get("predictions"), list):
            raise StationFetchError(station_id, "response is missing the predictions list")
        for entry in document["predictions"]:
            try:
                records.append(normalize_prediction(entry, station_id, meta, fetched_at))
            except ValueError as exc:
                raise StationFetchError(station_id, str(exc)) from exc
        cursor = chunk_end + timedelta(days=1)
    if delay > 0:
        time.sleep(delay)
    return records, retries_used


def parse_stations(spec: str) -> list[str]:
    if not spec:
        return list(STATIONS)
    stations = [s.strip() for s in spec.split(",") if s.strip()]
    unknown = [s for s in stations if s not in STATIONS]
    if unknown:
        raise ValueError(f"unknown station id(s) {unknown}; valid ids: {sorted(STATIONS)}")
    if len(set(stations)) != len(stations):
        raise ValueError("duplicate station ids in --stations")
    return stations


def run(stations: list[str], today: date, state: dict | None,
        lookback_days: int, timeout: int, retries: int, delay: float) -> tuple[list[dict], dict, dict]:
    """Fetch due dates for every station; return (records, new_state, report).

    ``state`` is None in --no-state mode: each station gets the fixed
    lookback window and nothing is written. The watermark for a station only
    advances when that station's whole date window was fetched successfully.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict] = []
    report = {"requests": 0, "retries": 0, "partitions": [], "records": 0}
    new_stations = dict(state.get("stations", {})) if state else {}
    for station_id in stations:
        if state:
            watermark = state.get("stations", {}).get(station_id)
            begin = date.fromisoformat(watermark) + timedelta(days=1) if watermark else today - timedelta(days=lookback_days)
        else:
            begin = today - timedelta(days=lookback_days)
        if begin > today:
            continue  # already up to date (e.g. second run on the same UTC day)
        try:
            station_records, station_retries = fetch_station(
                station_id, begin, today, timeout, retries, delay, fetched_at
            )
        except StationFetchError as exc:
            report["partitions"].append({"station": station_id, "error": exc.detail})
            continue
        records.extend(station_records)
        new_stations[station_id] = today.isoformat()
        report["requests"] += 1
        report["retries"] += station_retries
        report["records"] += len(station_records)
    attempted = len(stations)
    failed = len(report["partitions"])
    health = "failed" if failed == attempted else ("degraded" if failed else "healthy")
    completeness = "failed" if failed == attempted else ("partial" if failed else "complete")
    summary = {
        "health": health,
        "completeness": completeness,
        "requests": {"attempted": report["requests"], "succeeded": report["requests"],
                     "failed": failed, "retries": report["retries"]},
        "partitions": {"attempted": attempted, "succeeded": attempted - failed,
                       "failed": failed, "failures": report["partitions"]},
        "metrics": {"records": len(records), "stations_total": attempted},
    }
    if state is not None:
        state = {**state, "stations": new_stations}
    return records, state if state is not None else None, summary


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stations", help="comma-separated station ids; default: all curated ports")
    parser.add_argument("--lookback-days", type=int, default=2,
                        help="baseline window for stations with no watermark")
    parser.add_argument("--today", help="override the run date (YYYY-MM-DD, UTC) for reconciliation runs")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=2,
                        help="retries per station request on 429/5xx")
    parser.add_argument("--request-delay", type=float, default=1.0,
                        help="seconds to sleep between station requests")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if not 1 <= args.timeout <= MAX_TIMEOUT:
        parser.error(f"--timeout must be in 1..{MAX_TIMEOUT}")
    if not 0 <= args.retries <= MAX_RETRIES:
        parser.error(f"--retries must be in 0..{MAX_RETRIES}")
    if args.request_delay < 0:
        parser.error("--request-delay must be non-negative")
    if args.lookback_days < 0:
        parser.error("--lookback-days must be non-negative")

    try:
        stations = parse_stations(args.stations or "")
        today = (date.fromisoformat(args.today) if args.today
                 else datetime.now(timezone.utc).date())
        state_file = None if args.no_state else state_path(args.state_file)
        state = None if state_file is None else load_state(state_file)
        original = dict(state.get("stations", {})) if state is not None else None
        records, state, summary = run(
            stations, today, state, args.lookback_days,
            args.timeout, args.retries, args.request_delay,
        )
    except (ValueError, OSError) as exc:
        print(f"noaa_coops_tide_predictions: {exc}", file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    if state_file is not None and state is not None and state["stations"] != original:
        save_state(state_file, state)
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return 0 if summary["partitions"]["failed"] < summary["partitions"]["attempted"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
