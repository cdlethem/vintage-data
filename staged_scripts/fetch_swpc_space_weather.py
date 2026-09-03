#!/usr/bin/env python3
"""NOAA SWPC — live space weather (keyless, plain JSON files).

services.swpc.noaa.gov/json/ is a directory of continuously refreshed JSON
files — no API ceremony at all, just GET and parse. The sun does not care
about your sprint schedule: solar wind updates every minute, X-ray flux every
minute, the planetary K-index every 3 hours, and flares/CMEs arrive on their
own dramatic timeline. Aurora nowcast grids update ~every 5 minutes (relevant
at Madison's latitude during strong storms, pleasingly).

High-confidence known endpoints (SWPC has served these paths for years);
not live-fetched in this session, so smoke-test each file once and prune the
FEEDS dict to what you actually want. If a path 404s, browse the parent
directory listing — SWPC occasionally reorganizes.

'Current' per run = latest rows of each feed newer than your stored watermark.

Stdlib only.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone

HOST = "https://services.swpc.noaa.gov"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"

# feed name -> (path, time field). Verify paths on first run.
FEEDS = {
    "kp_index":    ("/products/noaa-planetary-k-index.json", 0),   # array-of-arrays, header row
    "solar_wind":  ("/products/solar-wind/plasma-1-day.json", 0),  # array-of-arrays, header row
    "xray_flux":   ("/json/goes/primary/xrays-1-day.json", "time_tag"),
    "flares":      ("/json/solar_probabilities.json", None),       # daily forecast probabilities
}


def _get(path: str):
    req = urllib.request.Request(HOST + path, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def fetch_feed(name: str):
    """Yield normalized rows for one feed. Handles both SWPC JSON shapes:
    (a) array-of-arrays with a header row, (b) array of dicts."""
    path, timefield = FEEDS[name]
    data = _get(path)
    now = datetime.now(timezone.utc).isoformat()
    if isinstance(data, list) and data and isinstance(data[0], list):
        header, rows = data[0], data[1:]
        for row in rows:
            rec = dict(zip(header, row))
            yield {"source": f"swpc_{name}", "fetched_at": now,
                   "ts": row[timefield] if isinstance(timefield, int) else None,
                   **rec}
    elif isinstance(data, list):
        for rec in data:
            yield {"source": f"swpc_{name}", "fetched_at": now,
                   "ts": rec.get(timefield) if isinstance(timefield, str) else None,
                   **rec}
    else:
        yield {"source": f"swpc_{name}", "fetched_at": now, **data}


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "kp_index"
    for rec in fetch_feed(name):
        print(json.dumps(rec, ensure_ascii=False))
