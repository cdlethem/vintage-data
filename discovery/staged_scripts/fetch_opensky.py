#!/usr/bin/env python3
"""OpenSky Network — live ADS-B aircraft state vectors, keyless with generous limits.

Lead #56: one of the best physical-world firehoses available. Every state vector is a
snapshot of a real aircraft's position, altitude, speed and heading, refreshed
continuously as ADS-B receivers worldwide report in.

**Verified live 2026-09-03** — `opensky-network.org/api/states/all?lamin=45.8&
lomin=5.9&lamax=47.8&lomax=10.5` (a Switzerland-sized bounding box) returned **200,
11,278 bytes, 88 aircraft**, `time: 1788461584` (a fresh Unix epoch matching the
request). An unbounded global query (no lat/lon params) returned **1,655,473 bytes,
12,590+ aircraft** in one pull — the anonymous endpoint really does return the whole
globe if you let it.

Endpoint: `https://opensky-network.org/api/states/all?lamin=&lomin=&lamax=&lomax=`
    Omit the bounding box for the whole world (large — 1.6+ MB observed); supply
    `lamin/lomin/lamax/lomax` (a lat/lon box) to scope to a region.

Quirks that will cost someone an afternoon:
    * **Each state is a positional array, not a keyed object** — 17 fields per
      aircraft in a fixed order: `[icao24, callsign, origin_country, time_position,
      last_contact, longitude, latitude, baro_altitude, on_ground, velocity,
      true_track, vertical_rate, sensors, geo_altitude, squawk, spi,
      position_source]`. Getting the index wrong silently mis-labels every record.
    * **`callsign` is padded with trailing spaces** to a fixed width (`"TVF8603 "`,
      confirmed live) — strip it, or string comparisons and joins will silently fail.
    * Many fields are legitimately `null` per aircraft, not missing data — `sensors`
      is null unless you're querying with sensor-specific auth, and `vertical_rate`/
      `geo_altitude` are frequently null for aircraft on the ground or without the
      right avionics.
    * `on_ground: true` aircraft can still report a `velocity` (taxiing) but
      `baro_altitude`/`geo_altitude` are typically null then — don't treat a null
      altitude as an error.
    * `/api/status` (a plausible health-check path) **404s** — not a real endpoint on
      this API.

Etiquette: keyless with generous anonymous limits (OpenSky documents roughly 100
requests per rolling 24h window for anonymous use, more for registered accounts).
Self-impose accordingly; prefer a bounded box over the unbounded global query if you
only need one region, both to reduce payload size and to be a better citizen of a
free, volunteer-receiver-fed network.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://opensky-network.org/api/states/all"
FIELDS = ["icao24", "callsign", "origin_country", "time_position", "last_contact",
         "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
         "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
         "spi", "position_source"]


def _get(params):
    url = BASE + (f"?{urllib.parse.urlencode(params)}" if params else "")
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_states(bbox: tuple[float, float, float, float] | None = None):
    """bbox is (lamin, lomin, lamax, lomax). None fetches the whole world (large)."""
    now = datetime.now(timezone.utc).isoformat()
    params = {}
    if bbox:
        lamin, lomin, lamax, lomax = bbox
        params = {"lamin": lamin, "lomin": lomin, "lamax": lamax, "lomax": lomax}
    doc = _get(params)
    snapshot_time = doc.get("time")
    for state in doc.get("states") or []:
        rec = dict(zip(FIELDS, state))
        callsign = (rec.get("callsign") or "").strip() or None
        yield {
            "source": "opensky",
            "fetched_at": now,
            "id": f"{rec.get('icao24')}:{snapshot_time}",
            "icao24": rec.get("icao24"),
            "callsign": callsign,
            "origin_country": rec.get("origin_country"),
            "longitude": rec.get("longitude"),
            "latitude": rec.get("latitude"),
            "baro_altitude_m": rec.get("baro_altitude"),
            "on_ground": rec.get("on_ground"),
            "velocity_m_s": rec.get("velocity"),
            "true_track_deg": rec.get("true_track"),
            "vertical_rate_m_s": rec.get("vertical_rate"),
            "snapshot_time": snapshot_time,
        }


if __name__ == "__main__":
    if len(sys.argv) >= 5:
        bbox = tuple(float(x) for x in sys.argv[1:5])
    else:
        bbox = (45.8, 5.9, 47.8, 10.5)  # Switzerland, a small default region
    for rec in fetch_states(bbox):
        print(json.dumps(rec, ensure_ascii=False))
