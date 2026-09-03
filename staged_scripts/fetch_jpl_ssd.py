#!/usr/bin/env python3
"""NASA/JPL SSD-CNEOS — fireballs + asteroid close approaches (keyless, JSON).

Two feeds from the same server:
  * fireball.api  — US government sensor detections of bright meteors
    entering the atmosphere (energy, lat/lon, altitude, velocity vector).
    Sparse but delightful: a handful of new events per month. Verified live
    2026-09-02 (latest event 2026-08-15).
  * cad.api       — asteroid/comet close approaches to Earth. Denser and
    forward-looking: upcoming approaches appear and their nominal distances
    get REFINED as orbits improve, so re-polling captures revision history —
    an unusual and interesting kind of time series.

Fair-use policy (per ssd-api.jpl.nasa.gov): ONE request at a time, no
concurrency; check the `signature.version` field each run and alert if it
changes (formats can change without notice).

Response format quirk: columnar — a `fields` array of names plus rows of
values; zip them together (done below).

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
EXPECTED_VERSIONS = {"fireball": "1.2"}  # alert if signature.version drifts


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def _rows(data: dict):
    fields = data.get("fields", [])
    for row in data.get("data", []):
        yield dict(zip(fields, row))


def fetch_fireballs(date_min: str | None = None, limit: int = 50):
    """Fireball events, newest first. date_min = 'YYYY-MM-DD' watermark."""
    params = {"limit": limit, "vel-comp": "true"}
    if date_min:
        params["date-min"] = date_min
    data = _get("https://ssd-api.jpl.nasa.gov/fireball.api?"
                + urllib.parse.urlencode(params))
    ver = (data.get("signature") or {}).get("version")
    if ver != EXPECTED_VERSIONS["fireball"]:
        print(f"WARNING: fireball.api version changed to {ver}", file=sys.stderr)
    now = datetime.now(timezone.utc).isoformat()
    for r in _rows(data):
        yield {
            "source": "jpl_fireball",
            "fetched_at": now,
            "id": r.get("date"),               # event UTC timestamp is the key
            "energy_e10J": _f(r.get("energy")),
            "impact_energy_kt": _f(r.get("impact-e")),
            "lat": _signed(r.get("lat"), r.get("lat-dir"), "S"),
            "lon": _signed(r.get("lon"), r.get("lon-dir"), "W"),
            "alt_km": _f(r.get("alt")),
            "vel_kms": _f(r.get("vel")),
        }


def fetch_close_approaches(days_ahead: int = 30, dist_max: str = "10LD"):
    """Upcoming close approaches within `dist_max` (lunar distances)."""
    params = {"date-min": "now", "date-max": f"+{days_ahead}",
              "dist-max": dist_max, "sort": "date"}
    data = _get("https://ssd-api.jpl.nasa.gov/cad.api?"
                + urllib.parse.urlencode(params))
    now = datetime.now(timezone.utc).isoformat()
    for r in _rows(data):
        yield {
            "source": "jpl_close_approach",
            "fetched_at": now,                  # snapshot; distances get refined
            "id": f"{r.get('des')}@{r.get('cd')}",
            "designation": r.get("des"),
            "approach_time": r.get("cd"),
            "dist_au": _f(r.get("dist")),
            "vel_kms": _f(r.get("v_rel")),
            "abs_magnitude_h": _f(r.get("h")),  # proxy for size
        }


def _f(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _signed(val, hemi, negative_hemi):
    f = _f(val)
    if f is None:
        return None
    return -f if hemi == negative_hemi else f


if __name__ == "__main__":
    for rec in fetch_fireballs(limit=20):
        print(json.dumps(rec, ensure_ascii=False))
