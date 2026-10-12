#!/usr/bin/env python3
"""MET Norway — hourly location forecasts for ten major Norwegian cities.

Verified live 2026-10-12 through the official keyless locationforecast 2.0 API:
no key, no registration, identifying User-Agent required. The national model is
re-run about every 10 minutes, so each hourly poll captures a fresh forecast
snapshot. One NDJSON record is emitted per city per forecast hour (86 hours),
keyed by city and forecast time.

Licence: NLOD 2.0 and CC-BY 4.0; attribution to MET Norway is required.
Coordinates are rounded to 4 decimals as the service's terms request.

Stdlib only.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "met_norway"
BASE = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
MAX_DELAY_SECONDS = 5.0
MAX_TIMEOUT = 120

# slug, lat, lon, altitude (m); coordinates at 4 decimals per the service ToS.
CITIES = (
    ("oslo", 59.9139, 10.7522, 25),
    ("bergen", 60.3913, 5.3221, 12),
    ("trondheim", 63.4305, 10.3951, 47),
    ("stavanger", 58.9700, 5.7331, 8),
    ("tromso", 69.6492, 18.9553, 10),
    ("alesund", 62.4722, 6.1497, 13),
    ("bodo", 67.2804, 14.4054, 35),
    ("kristiansand", 58.1492, 8.0052, 2),
    ("haugesund", 59.4136, 5.2771, 12),
    ("narvik", 68.4314, 17.4293, 5),
)


def _details(data, period):
    """Return the period's instant details dict, tolerating missing blocks."""
    block = data.get(period)
    if not isinstance(block, dict):
        return {}
    details = block.get("details")
    return details if isinstance(details, dict) else {}


def _summary_code(data, period):
    block = data.get(period)
    if not isinstance(block, dict):
        return None
    summary = block.get("summary")
    if not isinstance(summary, dict):
        return None
    return summary.get("symbol_code")


def _validate(document, slug):
    """Return (timeseries, model_run_time) or raise on a malformed document."""
    if not isinstance(document, dict):
        raise ValueError(f"{slug}: response is not a JSON object")
    properties = document.get("properties")
    if not isinstance(properties, dict):
        raise ValueError(f"{slug}: response is missing properties")
    rows = properties.get("timeseries")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{slug}: response has no timeseries")
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("time"), str):
            raise ValueError(f"{slug}: timeseries entry is missing a string time")
    meta = properties.get("meta")
    updated_at = meta.get("updated_at") if isinstance(meta, dict) else None
    if not isinstance(updated_at, str):
        raise ValueError(f"{slug}: response is missing meta.updated_at")
    return rows, updated_at


def fetch_city(slug, lat, lon, altitude, *, timeout, fetched_at):
    """Yield one record per forecast hour for one city."""
    query = urllib.parse.urlencode(
        {
            "lat": f"{round(lat, 4):.4f}",
            "lon": f"{round(lon, 4):.4f}",
            "altitude": altitude,
        }
    )
    request = urllib.request.Request(
        f"{BASE}?{query}", headers={"User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows, model_run_time = _validate(document, slug)
    for row in rows:
        data = row.get("data")
        if not isinstance(data, dict):
            data = {}
        record = {
            "source": SOURCE,
            "id": f"{slug}:{row['time']}",
            "fetched_at": fetched_at,
            "city": slug,
            "lat": round(lat, 4),
            "lon": round(lon, 4),
            "altitude_m": altitude,
            "model_run_time": model_run_time,
            "time": row["time"],
            "air_temperature": data.get("instant", {}).get("details", {}).get("air_temperature"),
            "air_pressure_at_sea_level": data.get("instant", {}).get("details", {}).get("air_pressure_at_sea_level"),
            "cloud_area_fraction": data.get("instant", {}).get("details", {}).get("cloud_area_fraction"),
            "relative_humidity": data.get("instant", {}).get("details", {}).get("relative_humidity"),
            "wind_speed": data.get("instant", {}).get("details", {}).get("wind_speed"),
            "wind_from_direction": data.get("instant", {}).get("details", {}).get("wind_from_direction"),
            "symbol_code_1h": _summary_code(data, "next_1_hours"),
            "symbol_code_6h": _summary_code(data, "next_6_hours"),
            "symbol_code_12h": _summary_code(data, "next_12_hours"),
            "precipitation_amount_1h": _details(data, "next_1_hours").get("precipitation_amount"),
            "precipitation_amount_6h": _details(data, "next_6_hours").get("precipitation_amount"),
            "precipitation_amount_12h": _details(data, "next_12_hours").get("precipitation_amount"),
        }
        yield record


def run(timeout=30, delay=0.5, sleep=time.sleep):
    """Fetch every city, printing records to stdout; return (succeeded, failures).

    A per-city failure is logged to stderr and reported in the emitted run
    summary so a partial snapshot still lands; a run where every city failed
    raises after emitting a failed summary.
    """
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 0 < timeout <= MAX_TIMEOUT:
        raise ValueError(f"timeout must be an integer in 1-{MAX_TIMEOUT}, got {timeout!r}")
    if delay < 0:
        raise ValueError(f"delay must be non-negative, got {delay!r}")
    delay = min(delay, MAX_DELAY_SECONDS)
    fetched_at = datetime.now(timezone.utc).isoformat()
    failures = []
    succeeded = 0
    for index, (slug, lat, lon, altitude) in enumerate(CITIES):
        if index:
            sleep(delay)
        try:
            for record in fetch_city(slug, lat, lon, altitude, timeout=timeout, fetched_at=fetched_at):
                print(json.dumps(record, ensure_ascii=False))
            succeeded += 1
        except Exception as exc:  # noqa: BLE001 - per-city isolation by design
            failures.append({"city": slug, "error": str(exc)[:200]})
            print(f"met_norway: {slug} failed: {exc}", file=sys.stderr)
    if not succeeded:
        _emit_summary(0, len(CITIES), failures, health="failed", completeness="failed")
        raise RuntimeError(
            "all cities failed: "
            + "; ".join(f"{f['city']} ({f['error']})" for f in failures)
        )
    _emit_summary(succeeded, len(CITIES), failures)
    return succeeded, failures


def _emit_summary(succeeded, attempted, failures, *, health=None, completeness=None):
    if health is None:
        health = "healthy" if not failures else "degraded"
    if completeness is None:
        completeness = "complete" if not failures else "partial"
    summary = {
        "health": health,
        "completeness": completeness,
        "requests": {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": len(failures),
        },
        "cities_failed": [f["city"] for f in failures],
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--timeout", type=int, default=30, help="per-request timeout in seconds")
    parser.add_argument("--delay", type=float, default=0.5, help="pause between city requests, seconds")
    args = parser.parse_args()
    run(timeout=args.timeout, delay=args.delay)


if __name__ == "__main__":
    main()
