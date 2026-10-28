#!/usr/bin/env python3
"""Deutscher Wetterdienst (DWD) synoptic station surface observations (keyless).

DWD publishes the current synoptic surface observation of every reporting
station as ``<station>-BEOB.csv`` under
``https://opendata.dwd.de/weather/weather_reports/poi/``.  Each file is a
semicolon-separated table: an English parameter header row, a unit row, a
German label row, then one row per observation (hourly, UTC) covering roughly
the last 24 hours.  The directory is republished about every ten minutes, so
the underlying data changes hourly.

This fetcher lists the directory, downloads every station file, and emits one
record per station per observation hour.  It is stateful: a per-station
watermark (last observation seen) prevents the overlapping 24-hour windows
from being re-emitted on later runs.  ``--no-state`` emits the complete
current window without reading or writing state.  Stations that fail to
download keep their old watermark, so their data is retried on the next run
instead of being lost.

Access is free and needs no registration; DWD's open data terms apply (the
server stores the client IP address for a maximum of 7 days):
https://www.dwd.de/EN/service/legal_notice/legal_notice_node.html
Attribution: "Deutscher Wetterdienst (DWD), synoptic station observations,
https://opendata.dwd.de/weather/weather_reports/poi/".

The unit row of each CSV (e.g. "Grad C", "km/h", "hPa", "W/m2") is part of
the source format, not repeated per record; the parameter names in records
are the English header names from row one.

Standard library only.
"""
from __future__ import annotations

import argparse
import html
import json
import os
import pathlib
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "dwd_station_observations"
LISTING_URL = "https://opendata.dwd.de/weather/weather_reports/poi/"
FILE_URL = "https://opendata.dwd.de/weather/weather_reports/poi/{name}"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
STATION_FILE = re.compile(r"^(\d{5})-BEOB\.csv$")
DATE_CELL = re.compile(r"^(\d{2})\.(\d{2})\.(\d{2})$")
TIME_CELL = re.compile(r"^(\d{2}):(\d{2})$")
MISSING = "---"
MAX_FAILURE_SAMPLES = 20
RETRYABLE_HTTP = frozenset({429, 500, 502, 503, 504})


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        with path.open(encoding="utf-8") as handle:
            state = json.load(handle)
    except FileNotFoundError:
        return {"version": STATE_VERSION, "source": SOURCE, "stations": {}}
    if not isinstance(state, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if state.get("version") != STATE_VERSION or state.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: incompatible version or source")
    stations = state.get("stations")
    if not isinstance(stations, dict):
        raise ValueError(f"malformed state in {path}: stations must be an object")
    for station, watermark in stations.items():
        if not isinstance(station, str) or not isinstance(watermark, str):
            raise ValueError(f"malformed state in {path}: station watermarks must map str to ISO-8601 str")
        datetime.fromisoformat(watermark)
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish watermarks only after stdout has accepted every record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def http_get(url: str, timeout: float, retries: int, retry_delay: float) -> bytes:
    """GET with bounded retries; only transport errors and 429/5xx are retried."""
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in RETRYABLE_HTTP or attempt == retries:
                raise RuntimeError(f"GET {url} failed: HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as exc:
            last = exc
            if attempt == retries:
                break
        time.sleep(retry_delay)
    raise RuntimeError(f"GET {url} failed after {retries + 1} attempts: {last}")


def parse_listing(body: bytes) -> list[str]:
    """Station ids from the DWD directory listing, sorted for a stable run order."""
    text = body.decode("utf-8", errors="replace")
    stations = set()
    for name in re.findall(r'href="([^"]+)"', text):
        match = STATION_FILE.match(html.unescape(name))
        if match:
            stations.add(match.group(1))
    return sorted(stations)


def parse_observed(date_cell: str, time_cell: str, station: str) -> datetime:
    date_match = DATE_CELL.match(date_cell.strip())
    if not date_match:
        raise ValueError(f"station {station}: unparseable observation date {date_cell!r}")
    day, month, year = int(date_match[1]), int(date_match[2]), int(date_match[3])
    if year >= 70:
        year += 1900
    else:
        year += 2000
    time_match = TIME_CELL.match(time_cell.strip())
    if not time_match:
        raise ValueError(f"station {station}: unparseable observation time {time_cell!r}")
    try:
        # DWD publishes these files in UTC (header row: "Uhrzeit (UTC)").
        return datetime(year, month, day, int(time_match[1]), int(time_match[2]), tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError(f"station {station}: invalid observation datetime {date_cell!r} {time_cell!r}") from exc


def parse_value(raw: str):
    value = raw.strip()
    if value == "" or value == MISSING:
        return None
    if "," in value:
        try:
            return float(value.replace(",", "."))
        except ValueError:
            return value
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def parse_station_csv(body: bytes, station: str):
    """Return (parameter_names, [(observed, {param: value})]) from one station file."""
    lines = [line.rstrip("\r\n") for line in body.decode("utf-8", errors="replace").splitlines()]
    lines = [line for line in lines if line.strip()]
    if len(lines) < 4:
        raise ValueError(f"station {station}: file has {len(lines)} lines, expected header, unit, label, and data rows")
    header_cells = lines[0].split(";")
    if len(header_cells) < 3:
        raise ValueError(f"station {station}: header has no parameters")
    if not header_cells[0].strip().lower().startswith("surface observation"):
        raise ValueError(f"station {station}: unexpected first header cell {header_cells[0]!r}")
    if header_cells[1].strip() != "Parameter description":
        raise ValueError(f"station {station}: unexpected second header cell {header_cells[1]!r}")
    parameters = [cell.strip() for cell in header_cells[2:]]
    if not all(parameters):
        raise ValueError(f"station {station}: header has no parameters")
    rows = []
    for line in lines[3:]:
        cells = line.split(";")
        if len(cells) != 2 + len(parameters):
            raise ValueError(f"station {station}: row has {len(cells)} cells, expected {2 + len(parameters)}")
        observed = parse_observed(cells[0], cells[1], station)
        rows.append((observed, {parameters[i]: parse_value(cells[2 + i]) for i in range(len(parameters))}))
    return parameters, rows


def run_summary(
    *,
    records: int,
    stations_attempted: int,
    stations_succeeded: int,
    station_failures: list[dict],
    requests: dict,
    failed: bool = False,
    error: str | None = None,
) -> dict:
    complete = not failed and not station_failures
    summary = {
        "health": "failed" if failed else ("degraded" if station_failures else "healthy"),
        "completeness": "failed" if failed else ("partial" if station_failures else "complete"),
        "requests": requests,
        "partitions": {
            "attempted": stations_attempted,
            "succeeded": stations_succeeded,
            "failed": len(station_failures),
            "failures": station_failures[:MAX_FAILURE_SAMPLES],
            "failures_truncated": max(0, len(station_failures) - MAX_FAILURE_SAMPLES),
        },
        "metrics": {"records_emitted": records, "stations_attempted": stations_attempted},
    }
    if error is not None:
        summary["metrics"]["error"] = error
    return summary


def emit_summary(err, summary: dict) -> None:
    err.write(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")) + "\n")
    err.flush()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off full pull)")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--retry-delay", type=float, default=5.0)
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0 or args.retries < 0 or args.retry_delay < 0:
        parser.error("--timeout must be positive; --retries and --retry-delay must not be negative")

    out, err = sys.stdout, sys.stderr
    requests = {"attempted": 0, "succeeded": 0, "failed": 0, "payload_bytes": 0}

    def request(url: str) -> bytes:
        requests["attempted"] += 1
        try:
            body = http_get(url, args.timeout, args.retries, args.retry_delay)
        except Exception:
            requests["failed"] += 1
            raise
        requests["succeeded"] += 1
        requests["payload_bytes"] += len(body)
        return body

    emitted = 0
    stations: list[str] = []
    stations_succeeded = 0
    station_failures: list[dict] = []
    next_watermarks: dict[str, datetime] = {}
    state_path_value = None
    try:
        state_path_value = None if args.no_state else state_path(args.state_file)
        watermarks: dict[str, datetime] = {}
        if state_path_value is not None:
            for station, watermark in load_state(state_path_value)["stations"].items():
                watermarks[station] = datetime.fromisoformat(watermark)

        stations = parse_listing(request(LISTING_URL))
        if not stations:
            raise RuntimeError("DWD poi listing returned no station files")

        fetched_at = datetime.now(timezone.utc)
        next_watermarks = dict(watermarks)

        for station in stations:
            try:
                body = request(FILE_URL.format(name=f"{station}-BEOB.csv"))
                _, rows = parse_station_csv(body, station)
            except Exception as exc:
                station_failures.append({"station": station, "error": str(exc)[:200]})
                continue
            stations_succeeded += 1
            if not rows:
                continue
            newest = max(observed for observed, _ in rows)
            previous_newest = next_watermarks.get(station)
            if previous_newest is None or newest > previous_newest:
                next_watermarks[station] = newest
            for observed, values in rows:
                previous = watermarks.get(station)
                if previous is not None and observed <= previous:
                    continue
                record = {
                    "source": SOURCE,
                    "fetched_at": fetched_at.isoformat(),
                    "id": f"{station}|{observed.isoformat()}",
                    "station": station,
                    "observed_at": observed.isoformat(),
                }
                record.update(values)
                out.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
                emitted += 1
        out.flush()
    except Exception as exc:
        emit_summary(err, run_summary(
            records=emitted,
            stations_attempted=len(stations),
            stations_succeeded=stations_succeeded,
            station_failures=station_failures,
            requests=requests,
            failed=True,
            error=str(exc),
        ))
        return 1

    if stations_succeeded == 0 or stations_succeeded * 2 < len(stations):
        failed = True
    else:
        failed = False

    emit_summary(err, run_summary(
        records=emitted,
        stations_attempted=len(stations),
        stations_succeeded=stations_succeeded,
        station_failures=station_failures,
        requests=requests,
        failed=failed,
    ))

    if state_path_value is not None:
        try:
            save_state(state_path_value, {
                "version": STATE_VERSION,
                "source": SOURCE,
                "stations": {station: value.isoformat() for station, value in next_watermarks.items()},
            })
        except Exception as exc:
            emit_summary(err, run_summary(
                records=emitted,
                stations_attempted=len(stations),
                stations_succeeded=stations_succeeded,
                station_failures=station_failures,
                requests=requests,
                failed=True,
                error=f"state write failed: {exc}",
            ))
            return 1

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
