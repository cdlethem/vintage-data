#!/usr/bin/env python3
"""Safecast — crowd-sourced radiation measurements, keyless.

Lead #102: born after Fukushima, global coverage, citizen seismology's radiation
cousin. Every measurement is one reading from a volunteer's Geiger counter (fixed
station or mobile bGeigie device), with a timestamp and geopoint.

**Verified live 2026-09-03.** The catalog-plausible `order=`/`limit=` params were
tried first and **silently ignored** — a `limit=5` request returned 25 rows anyway,
and rows were not chronologically ordered (top result dated 2022). The real,
documented filter is `since`/`until` (added-to-database time) or `captured_after`/
`captured_before` (measurement time): `api.safecast.org/measurements.json?
since=2026-09-02+00:00` — **200, 8,187 bytes, 25 real measurements**, top result
`captured_at: 2026-09-02T00:00:00.035Z`, `unit: cpm`, `value: 15.0` — one day before
the probe.

Endpoint: `https://api.safecast.org/measurements.json?since=YYYY-MM-DD+HH:MM`
    since=/until=            filters on when the measurement was added to the DB
    captured_after=/captured_before=   filters on when the measurement was taken
    latitude=&longitude=&distance=      geographic radius filter
    Page size is fixed at 25 per request regardless of a `limit` param (see quirks);
    paginate with repeated `since=` bumped past the last seen timestamp.

Quirks that will cost someone an afternoon:
    * **`order=` and `limit=` are silently ignored — confirmed live.** A request for
      `limit=5` returned 25 rows; a request implying newest-first order returned a
      measurement from 2022. Use the documented `since=`/`captured_after=` filters
      instead of assuming generic sort/limit query params work, and don't trust an
      unfiltered request to be either recent or short.
    * **`unit` is not always a radiation unit.** Verified live across two samples:
      alongside `cpm` (counts per minute, raw Geiger output), real records carry
      `unit: "celcius"` (a temperature reading posted through the same measurements
      endpoint, misspelled by the device firmware, not this script) and
      `unit: "status"` (a device health value, not a radiation reading at all). A
      naive consumer expecting every row to be radiation data will silently average
      temperatures and status codes into a dose-rate series. `fetch_recent()`
      defaults to returning everything with `unit` intact so you can filter; use
      `radiation_only=True` to keep only `cpm`/`usv` rows.
    * Many fields are legitimately null per measurement — `location_name`,
      `sensor_id`, `station_id`, `channel_id`, `height` are frequently absent
      depending on device type (fixed station vs mobile bGeigie).
    * `devicetype_id` can itself be a **packed diagnostic string**
      (`"DeviceID:10022,Temperature:35.6,Battery Voltage:8.21,..."`) rather than a
      simple identifier — verified live; don't assume it's always a short id.

Etiquette: keyless, no published rate limit found. This is volunteer-run citizen
science infrastructure — self-impose 1 req/s and cache aggressively; a measurement
once captured never changes.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://api.safecast.org/measurements.json"


def _get(params):
    req = urllib.request.Request(f"{BASE}?{urllib.parse.urlencode(params)}",
                                 headers={"User-Agent": USER_AGENT,
                                         "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


RADIATION_UNITS = {"cpm", "usv"}


def fetch_recent(hours: int = 48, radiation_only: bool = False):
    """Measurements added to the database in the last `hours` hours. Page size is
    fixed at 25 by the API regardless of any limit param -- see docstring.
    radiation_only=True drops non-radiation rows (unit 'celcius', 'status', etc --
    real device telemetry mixed into this endpoint, verified live)."""
    now = datetime.now(timezone.utc)
    since = (now - timedelta(hours=hours)).strftime("%Y-%m-%d+%H:%M")
    doc = _get({"since": since})
    fetched_at = now.isoformat()
    for m in doc or []:
        unit = m.get("unit")
        if radiation_only and unit not in RADIATION_UNITS:
            continue
        yield {
            "source": "safecast",
            "fetched_at": fetched_at,
            "id": m.get("id"),
            "value": m.get("value"),
            "unit": unit,
            "is_radiation": unit in RADIATION_UNITS,
            "captured_at": m.get("captured_at"),
            "latitude": m.get("latitude"),
            "longitude": m.get("longitude"),
            "device_id": m.get("device_id"),
            "height": m.get("height"),
        }


if __name__ == "__main__":
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 48
    for rec in fetch_recent(hours=hours):
        print(json.dumps(rec, ensure_ascii=False))
