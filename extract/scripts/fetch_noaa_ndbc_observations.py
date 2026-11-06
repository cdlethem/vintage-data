#!/usr/bin/env python3
"""Snapshot NOAA NDBC realtime surface observations as NDJSON.

The National Data Buoy Center publishes one text file per surface station
(moored buoys, drifting buoys, coastal surface stations) under
https://www.ndbc.noaa.gov/data/realtime2/. Each file carries a two-line
header followed by observations, newest first:

    #YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE
    2026 11 06 05 00 200 10.0 12.0   5.9    16   9.6 268 1008.5  14.7  13.9  14.3   MM -1.7    MM

``MM`` means "no value". Each run lists the directory once, then fetches the
head of every station file with a bounded HTTP range request (the latest
observation is the first data line) and emits one NDJSON record per station:

    {"source": ..., "id": "<station>", "fetched_at": "<one per run>",
     "observation_time": "2026-11-06T05:00:00Z",
     "wdir": 200.0, "wspd": 10.0, ... , "tide": null}

Field units are the NDBC standard: directions in degrees true (360 means
"no direction"), speeds in m/s, wave height in m, periods in s, pressures
in hPa, temperatures in degC, visibility in nmi, tide in ft. Units are
documented in the source YAML; the file header is not repeated per record.

Station files stop being updated when a station is offline, so
``observation_time`` can lag ``fetched_at``; the true observation time is
kept verbatim rather than dropping the station.

Transient per-station HTTP failures are retried once; a station that still
fails is reported in the run summary instead of failing the whole run,
unless the failures are broad enough to indicate an upstream outage.
Stateless snapshot: no state file. Stdlib only.
"""

import argparse
import calendar
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "noaa_ndbc_observations"
BASE = "https://www.ndbc.noaa.gov/data/realtime2/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
MAX_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 2
RETRIABLE_HTTP_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})

# The latest observation is the first data line, a few tens of bytes after
# the two-line header; a small head keeps each station request cheap.
HEAD_BYTES = 4096
MAX_LISTING_BYTES = 10 * 1024 * 1024

# Apache-style directory listing; real station ids are 4-7 uppercase
# alphanumerics (moored buoys 46005, partner stations 32ST0, coastal
# stations KATP, drifters 4403585). "ship_obs.txt" and sort links are
# directory artifacts and never match.
STATION_FILE_RE = re.compile(r'href="([A-Z0-9]{4,7})\.txt"')

# (name, min, max, max_inclusive) for the 14 observation fields, in file
# order after the 5 timestamp tokens. Ranges are generous corruption guards,
# not curation: MM -> null, anything outside a plausible range fails the
# station for this run and is reported in the summary. Directions admit
# 360, which NDBC transmits as "no direction" (calm wind, no wave
# direction), often alongside a non-zero speed.
FIELDS = (
    ("wdir", 0.0, 360.0, True),    # wind direction, degrees true
    ("wspd", 0.0, 100.0, True),    # wind speed, m/s
    ("gust", 0.0, 150.0, True),    # gust, m/s
    ("wvht", 0.0, 50.0, True),     # significant wave height, m
    ("dpd", 0.0, 100.0, True),     # dominant wave period, s
    ("apd", 0.0, 100.0, True),     # average wave period, s
    ("mwd", 0.0, 360.0, True),     # mean wave direction, degrees true
    ("pres", 850.0, 1100.0, True), # station pressure, hPa
    ("atmp", -60.0, 60.0, True),   # air temperature, degC
    ("wtmp", -30.0, 45.0, True),   # water temperature, degC
    ("dewp", -60.0, 50.0, True),   # dew point, degC
    ("vis", 0.0, 100.0, True),     # visibility, nmi
    ("ptdy", -10.0, 10.0, True),   # pressure tendency, hPa
    ("tide", -100.0, 100.0, True), # tide, ft
)
TOKEN_COUNT = 5 + len(FIELDS)

SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
SUMMARY_MAX_BYTES = 64 * 1024
MAX_PARTITION_FAILURES = 100
MAX_ERROR_CHARS = 240
MAX_FUTURE_SKEW_HOURS = 24


class MalformedObservation(ValueError):
    """A station head that parses but violates the NDBC layout."""


class StationFetchError(RuntimeError):
    """A station fetch failed after its transport retries."""


def _digits(token: str, width: int) -> int | None:
    """Parse a fixed-width ASCII decimal token; None when it is not one."""
    if len(token) != width or any(ch not in "0123456789" for ch in token):
        return None
    return int(token)


def _parse_observation_time(tokens, now: datetime) -> str:
    year = _digits(tokens[0], 4)
    month = _digits(tokens[1], 2)
    day = _digits(tokens[2], 2)
    hour = _digits(tokens[3], 2)
    minute = _digits(tokens[4], 2)
    if None in (year, month, day, hour, minute):
        raise MalformedObservation(
            f"timestamp tokens {tokens[:5]!r} are not fixed-width decimal"
        )
    if not 1 <= month <= 12:
        raise MalformedObservation(f"month {month} out of range")
    if not 1 <= day <= calendar.monthrange(year, month)[1]:
        raise MalformedObservation(f"day {day} out of range for {year:04d}-{month:02d}")
    if not 0 <= hour <= 23:
        raise MalformedObservation(f"hour {hour} out of range")
    if not 0 <= minute <= 59:
        raise MalformedObservation(f"minute {minute} out of range")
    observed = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    if (observed - now).total_seconds() > MAX_FUTURE_SKEW_HOURS * 3600:
        raise MalformedObservation(
            f"observation time {observed.isoformat()} is more than "
            f"{MAX_FUTURE_SKEW_HOURS}h in the future"
        )
    return observed.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_field(name: str, token: str) -> float | None:
    if token == "MM":
        return None
    value = float(token)
    if not math.isfinite(value):
        raise MalformedObservation(f"{name}={token!r} is not finite")
    return value


def parse_station_head(station_id: str, text: str, now: datetime) -> dict:
    """Validate one station head and return the latest observation fields."""
    lines = text.splitlines()
    first_data = 0
    while first_data < len(lines) and lines[first_data].startswith("#"):
        first_data += 1
    if first_data >= len(lines):
        raise MalformedObservation("no data lines after the header")

    tokens = lines[first_data].split()
    if len(tokens) != TOKEN_COUNT:
        raise MalformedObservation(
            f"row has {len(tokens)} tokens, expected {TOKEN_COUNT}"
        )
    for token in tokens:
        if token == "MM":
            continue
        try:
            float(token)
        except ValueError:
            raise MalformedObservation(f"non-numeric token {token!r}") from None

    observation_time = _parse_observation_time(tokens, now)
    fields = {}
    for (name, lo, hi, hi_inclusive), token in zip(FIELDS, tokens[5:]):
        value = _parse_field(name, token)
        if value is not None:
            if value < lo or value > hi or (value == hi and not hi_inclusive):
                raise MalformedObservation(
                    f"{name}={value} outside plausible range [{lo}, {hi}]"
                )
        fields[name] = value
    return {"observation_time": observation_time, **fields}


def _read_head(url: str, timeout: int, max_bytes: int = HEAD_BYTES) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Range": f"bytes=0-{max_bytes - 1}",
        },
    )
    # A 200 means the server ignored the range: read only the head and stop.
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read(max_bytes)


def _get_with_retry(url: str, timeout: int, stats: dict, max_bytes: int = HEAD_BYTES) -> bytes:
    """Fetch one URL with a single retry on transient transport errors."""
    for attempt in range(MAX_ATTEMPTS):
        if attempt:
            stats["retries"] += 1
            time.sleep(RETRY_DELAY_SECONDS)
        stats["requests_attempted"] += 1
        try:
            data = _read_head(url, timeout, max_bytes)
            stats["requests_succeeded"] += 1
            return data
        except urllib.error.HTTPError as exc:
            if exc.code in RETRIABLE_HTTP_STATUS and attempt < MAX_ATTEMPTS - 1:
                continue
            raise StationFetchError(f"HTTP {exc.code} for {url}") from exc
        except OSError as exc:
            # Timeouts, resets, refused connections: transient, worth one retry.
            if attempt < MAX_ATTEMPTS - 1:
                continue
            raise StationFetchError(f"{type(exc).__name__} for {url}: {exc}") from exc
    raise AssertionError("unreachable")


def list_stations(timeout: int, stats: dict) -> list[str]:
    """Return the sorted station ids that publish a realtime .txt file."""
    data = _get_with_retry(BASE, timeout, stats, MAX_LISTING_BYTES + 1)
    if len(data) > MAX_LISTING_BYTES:
        raise ValueError(f"directory listing exceeds {MAX_LISTING_BYTES} bytes")
    text = data.decode("utf-8", "replace")
    stations = sorted(set(STATION_FILE_RE.findall(text)))
    if not stations:
        raise ValueError("directory listing contained no station files")
    return stations


def fetch_station(
    station_id: str, timeout: int, stats: dict, now: datetime
) -> dict:
    """Fetch and validate one station head; one record, newest observation."""
    data = _get_with_retry(BASE + f"{station_id}.txt", timeout, stats)
    return parse_station_head(station_id, data.decode("utf-8", "replace"), now)


def _failure_list(failures: list[dict], budget_for: dict) -> list[dict]:
    """Bound the partition-failure detail list to the summary byte cap."""
    out: list[dict] = []
    budget = SUMMARY_MAX_BYTES - len(
        json.dumps(budget_for, separators=(",", ":")).encode("utf-8")
    )
    for failure in sorted(failures, key=lambda f: f["station"]):
        if len(out) >= MAX_PARTITION_FAILURES:
            break
        size = len(json.dumps(failure, separators=(",", ":")).encode("utf-8"))
        size += bool(out)  # comma between entries
        if size > budget:
            continue
        out.append(failure)
        budget -= size
    return out


def _emit_summary(
    health: str,
    completeness: str,
    records: int,
    stations: list[str],
    failures: list[dict],
    stats: dict,
) -> None:
    failed = len(failures)
    total = len(stations)
    payload = {
        "health": health,
        "completeness": completeness,
        "records": records,
        "requests": {
            "attempted": stats["requests_attempted"],
            "succeeded": stats["requests_succeeded"],
            "retries": stats["retries"],
        },
        "partitions": {
            "attempted": total,
            "succeeded": total - failed,
            "failed": failed,
            "failures": [],
        },
    }
    payload["partitions"]["failures"] = _failure_list(failures, payload)
    print(
        SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")),
        file=sys.stderr,
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Snapshot the latest NDBC surface observation per station."
    )
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)
    if not 1 <= args.timeout <= MAX_TIMEOUT:
        parser.error(f"--timeout must be between 1 and {MAX_TIMEOUT} seconds")

    now = datetime.now(timezone.utc)
    fetched_at = now.isoformat()
    stats = {"requests_attempted": 0, "requests_succeeded": 0, "retries": 0}

    try:
        stations = list_stations(args.timeout, stats)
    except Exception as exc:
        print(f"ndbc listing failed: {exc}", file=sys.stderr)
        _emit_summary("failed", "failed", 0, [], [], stats)
        return 1

    records = 0
    failures: list[dict] = []
    for station_id in stations:
        try:
            observation = fetch_station(station_id, args.timeout, stats, now)
        except Exception as exc:
            failures.append(
                {
                    "station": station_id,
                    "error": str(exc)[:MAX_ERROR_CHARS],
                }
            )
            continue
        print(
            json.dumps(
                {
                    "source": SOURCE,
                    "id": station_id,
                    "fetched_at": fetched_at,
                    **observation,
                },
                ensure_ascii=False,
            )
        )
        records += 1

    total = len(stations)
    failed = len(failures)
    broad = failed == total or (failed >= 3 and failed / total >= 0.8)
    health = "failed" if broad else ("degraded" if failed else "healthy")
    completeness = "failed" if broad else ("partial" if failed else "complete")
    _emit_summary(health, completeness, records, stations, failures, stats)
    if broad:
        print(
            f"ndbc broad failure: {failed}/{total} stations failed",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
