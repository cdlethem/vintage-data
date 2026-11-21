#!/usr/bin/env python3
"""Snapshot live ADS-B aircraft positions around major international hubs.

adsb.fi's open data API (no key, no login) returns the aircraft currently
within a radius of a point, up to 250 nautical miles. Each run queries a fixed
set of hub airports once per hub, with at least one second between requests to
respect the public 1 request/second limit. Records preserve each aircraft's
raw object, keyed by hub and Mode-S hex code.

A hub that keeps failing after its retries makes the run partial and is
reported in the structured ``VINTAGE_RUN_SUMMARY`` line; a run in which every
hub fails is a hard failure.

Rows lacking a plain Mode-S hex code (adsb.fi marks synthetic identifiers with a
leading ``~``) or valid coordinates are dropped and counted in the summary; a
hub whose response has no usable row at all is treated as failed.

Stdlib only.
"""

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
import re
import sys
import time
from typing import Any, Iterator, Sequence
import urllib.error
import urllib.request


SOURCE = "adsbfi_hubs"
BASE_URL = "https://opendata.adsb.fi/api/v3"
HUBS: tuple[tuple[str, str, float, float], ...] = (
    ("EGLL", "London Heathrow", 51.4700, -0.4543),
    ("KJFK", "New York John F. Kennedy", 40.6413, -73.7781),
    ("KSFO", "San Francisco", 37.6213, -122.3790),
    ("OMDB", "Dubai", 25.2532, 55.3657),
    ("EDDF", "Frankfurt", 50.0379, 8.5622),
    ("WSSS", "Singapore Changi", 1.3644, 103.9915),
    ("RJTT", "Tokyo Haneda", 35.5494, 139.7798),
    ("YSYD", "Sydney", -33.9399, 151.1753),
)
DEFAULT_RADIUS_NM = 50
MAX_RADIUS_NM = 250
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 5
MAX_RETRY_DELAY = 300
DEFAULT_MIN_INTERVAL = 1.0
MAX_MIN_INTERVAL = 60
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
HEX_PATTERN = re.compile(r"^[0-9A-Fa-f]{1,8}$")
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
MAX_FAILURE_ERROR_CHARS = 240
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


@dataclass
class HubResult:
    icao: str
    succeeded: bool
    records: int
    error: str | None = None


def _is_finite_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if not isinstance(value, (int, float)):
        return False
    return math.isfinite(value)


def build_url(lat: float, lon: float, radius_nm: int) -> str:
    return f"{BASE_URL}/lat/{lat:.4f}/lon/{lon:.4f}/dist/{radius_nm}"


def normalize_aircraft(aircraft: Any, icao: str, hub_name: str,
                       radius_nm: int, fetched_at: str, seen: set[str]) -> dict[str, Any] | None:
    """Validate one adsb.fi aircraft object without changing source values.

    Returns ``None`` for rows with no usable stable identity: rows without a
    plain Mode-S hex (e.g. adsb.fi's ``~``-prefixed synthetic identifiers) and
    rows with invalid coordinates. Only a duplicated clean hex is an error.
    """
    if not isinstance(aircraft, dict):
        return None

    hex_code = aircraft.get("hex")
    if not isinstance(hex_code, str) or not HEX_PATTERN.match(hex_code):
        return None
    hex_key = hex_code.upper()
    if hex_key in seen:
        raise ValueError(f"{icao} response lists Mode-S hex {hex_key} twice")
    seen.add(hex_key)

    lat = aircraft.get("lat")
    lon = aircraft.get("lon")
    if not _is_finite_number(lat) or not -90 <= lat <= 90:
        return None
    if not _is_finite_number(lon) or not -180 <= lon <= 180:
        return None

    return {
        "source": SOURCE,
        "id": f"{icao}-{hex_key}",
        "fetched_at": fetched_at,
        "hub": icao,
        "hub_name": hub_name,
        "radius_nm": radius_nm,
        "aircraft": aircraft,
    }


def _validate_document(icao: str, document: Any) -> list[Any]:
    if not isinstance(document, dict):
        raise ValueError(f"{icao} response is not a JSON object")
    aircraft = document.get("ac")
    if not isinstance(aircraft, list):
        raise ValueError(f"{icao} response is missing its aircraft list")
    return aircraft


def _fetch_hub(
    icao: str,
    hub_name: str,
    lat: float,
    lon: float,
    radius_nm: int,
    timeout: int,
    retries: int,
    retry_delay: float,
    fetched_at: str,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch one hub snapshot, retrying only transient failures."""
    url = build_url(lat, lon, radius_nm)
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    seen: set[str] = set()
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ValueError(
                    f"{icao} response exceeds {MAX_RESPONSE_BYTES} bytes"
                )
            document = json.loads(body)
            aircraft = _validate_document(icao, document)
            rows: list[dict[str, Any]] = []
            dropped = 0
            for row in aircraft:
                record = normalize_aircraft(
                    row, icao, hub_name, radius_nm, fetched_at, seen
                )
                if record is None:
                    dropped += 1
                else:
                    rows.append(record)
            if not rows and aircraft:
                raise ValueError(
                    f"{icao} response has no rows with a usable Mode-S hex code"
                )
            return rows, dropped
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in RETRIABLE_STATUSES or attempt == retries:
                break
            print(
                f"adsbfi_hubs: {icao} request failed with HTTP {exc.code}; "
                f"retry {attempt + 1}/{retries} in {retry_delay:g}s",
                file=sys.stderr,
            )
            time.sleep(retry_delay)
        except ValueError as exc:
            # Malformed or oversized payloads will not heal; fail the hub.
            raise
        except (urllib.error.URLError, OSError) as exc:
            last_error = exc
            if attempt == retries:
                break
            print(
                f"adsbfi_hubs: {icao} request failed ({exc.__class__.__name__}: "
                f"{exc}); retry {attempt + 1}/{retries} in {retry_delay:g}s",
                file=sys.stderr,
            )
            time.sleep(retry_delay)
    raise RuntimeError(f"{icao}: {last_error}")


def fetch_hubs(
    icaos: Sequence[str] = tuple(icao for icao, *_ in HUBS),
    radius_nm: int = DEFAULT_RADIUS_NM,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    min_interval: float = DEFAULT_MIN_INTERVAL,
) -> Iterator[dict[str, Any]]:
    """Snapshot every hub once, pacing requests and reporting partial runs."""
    if (
        isinstance(radius_nm, bool)
        or not isinstance(radius_nm, int)
        or not 1 <= radius_nm <= MAX_RADIUS_NM
    ):
        raise ValueError(f"radius must be between 1 and {MAX_RADIUS_NM} NM")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")
    if (
        isinstance(retries, bool)
        or not isinstance(retries, int)
        or not 0 <= retries <= MAX_RETRIES
    ):
        raise ValueError(f"retries must be between 0 and {MAX_RETRIES}")
    if (
        isinstance(retry_delay, bool)
        or not isinstance(retry_delay, (int, float))
        or not math.isfinite(retry_delay)
        or not 0 <= retry_delay <= MAX_RETRY_DELAY
    ):
        raise ValueError(
            f"retry_delay must be a finite number between 0 and {MAX_RETRY_DELAY}"
        )
    if (
        isinstance(min_interval, bool)
        or not isinstance(min_interval, (int, float))
        or not math.isfinite(min_interval)
        or not 0 <= min_interval <= MAX_MIN_INTERVAL
    ):
        raise ValueError(
            f"min_interval must be a finite number between 0 and {MAX_MIN_INTERVAL}"
        )

    known = {icao: (name, lat, lon) for icao, name, lat, lon in HUBS}
    selected = []
    for icao in icaos:
        if icao not in known:
            raise ValueError(
                f"unknown hub {icao!r}; expected one of {', '.join(known)}"
            )
        selected.append(icao)

    fetched_at = datetime.now(timezone.utc).isoformat()
    dropped_total = 0
    results: list[HubResult] = []
    for index, icao in enumerate(selected):
        name, lat, lon = known[icao]
        if index:
            time.sleep(min_interval)
        try:
            rows, dropped = _fetch_hub(
                icao, name, lat, lon, radius_nm, timeout, retries,
                retry_delay, fetched_at,
            )
        except Exception as exc:  # noqa: BLE001 - one hub must not kill the run
            results.append(HubResult(icao, False, 0, str(exc)[:MAX_FAILURE_ERROR_CHARS]))
            print(
                f"adsbfi_hubs: skipping hub {icao}: {exc}", file=sys.stderr
            )
            continue
        results.append(HubResult(icao, True, len(rows)))
        dropped_total += dropped
        yield from rows

    attempted = len(results)
    failed = sum(1 for r in results if not r.succeeded)
    broad_failure = attempted == 0 or failed == attempted
    payload = {
        "health": "failed" if broad_failure else ("degraded" if failed else "healthy"),
        "completeness": "failed" if broad_failure else ("partial" if failed else "complete"),
        "records": sum(r.records for r in results),
        "partitions": {
            "attempted": attempted,
            "succeeded": attempted - failed,
            "failed": failed,
            "failures": [
                {"hub": r.icao, "error": r.error} for r in results if not r.succeeded
            ],
        },
        "metrics": {
            "hubs_attempted": attempted,
            "hubs_succeeded": attempted - failed,
            "hubs_failed": failed,
            "aircraft_records": sum(r.records for r in results),
            "dropped_rows": dropped_total,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)
    if broad_failure:
        raise RuntimeError(
            f"all {attempted} adsb.fi hub requests failed"
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--hubs",
        default=",".join(icao for icao, *_ in HUBS),
        help="comma-separated hub ICAO codes (default: all)",
    )
    parser.add_argument("--radius", type=int, default=DEFAULT_RADIUS_NM,
                        help=f"search radius in NM, 1-{MAX_RADIUS_NM}")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY)
    parser.add_argument("--min-interval", type=float, default=DEFAULT_MIN_INTERVAL,
                        help="seconds between hub requests (default 1)")
    args = parser.parse_args(argv)

    icaos = tuple(p.strip().upper() for p in args.hubs.split(",") if p.strip())
    try:
        records = list(
            fetch_hubs(
                icaos,
                radius_nm=args.radius,
                timeout=args.timeout,
                retries=args.retries,
                retry_delay=args.retry_delay,
                min_interval=args.min_interval,
            )
        )
    except ValueError as exc:
        print(f"adsbfi_hubs: {exc}", file=sys.stderr)
        return 2
    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
