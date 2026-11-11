#!/usr/bin/env python3
"""Open Waters AIS — live vessel traffic snapshot for a bounded sea area.

Keyless maritime counterpart to the opensky aircraft source. The Open Waters
AIS network aggregates volunteer and commercial AIS receivers and serves a
snapshot of every vessel reported inside a bounding box as a GeoJSON
FeatureCollection of Point features (vessels, aids to navigation, base
stations), each carrying position, course, speed, navigation status and the
static particulars (name, IMO, flag, size).

Endpoint: `https://ais.openwaters.io/v1/vessels?bbox=minlat,minlon,maxlat,maxlon&max_age=15m`
    Anonymous access is documented: no token needed, 120 requests/minute per
    address, anonymous boxes capped at 100 square degrees (or 10 MMSIs).
    `max_age` keeps only vessels seen within the window, so a snapshot never
    mixes in days-old positions.

**Verified live 2026-11-11** — Dover Strait box (51.0, 0.5, 51.5, 1.5)
returned **200, 34,879 bytes, 70 features** with fresh `seen` timestamps;
mid-Atlantic empty box returned **200, `features: []`**; an oversized box
returned **400 "bbox not allowed for this key"** (plain text, not JSON).

Quirks that will cost someone an afternoon:
    * `properties.source` and `properties.station` name the *receiving
      station*, not the data provider — renaming to `receiver` to avoid
      colliding with the envelope's `source` key.
    * `eta` is a compact local string like `"11-11 09:01"` (day-month HH:MM,
      no year, receiver-local time) — keep it verbatim, do not parse it.
    * `sog`/`cog`/`heading`/`length`/`beam`/`draught` units are AIS
      conventions (knots, degrees, metres); many are null per vessel.
    * `id` is MMSI + the vessel's own `seen` time, so one vessel across runs
      yields a new id each snapshot, like opensky's icao24:snapshot pair.

Etiquette: one request every five minutes (288/day) is negligible against the
120 requests/minute anonymous limit; the default box is 0.5 square degrees,
far under the 100 square degree cap. Attribution strings accompany every
response and are required by the service.

Stdlib only.
"""
import argparse
import errno
import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "openwaters_ais"
BASE = "https://ais.openwaters.io/v1/vessels"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
# Strait of Dover — one of the busiest shipping lanes in the world; 0.5 square
# degrees, ~70 vessels and ~35 KB observed per 15-minute snapshot (2026-11-11).
DEFAULT_BBOX = "51.0,0.5,51.5,1.5"
DEFAULT_MAX_AGE = "15m"

MAX_ATTEMPTS = 3
RETRY_DELAYS = (1, 2)


def is_timeout_error(error):
    """Return whether *error* is a real timeout, including urllib wrappers."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, (TimeoutError, socket.timeout)):
            return True
        if isinstance(error, OSError) and error.errno == errno.ETIMEDOUT:
            return True
        if isinstance(error, urllib.error.URLError):
            error = error.reason
        else:
            return False
    return False


def fetch_response(request, timeout):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", "replace")
        except Exception as error:
            if not is_timeout_error(error):
                raise
            if attempt == MAX_ATTEMPTS:
                raise TimeoutError(
                    f"Open Waters AIS request timed out after {attempt} attempts"
                ) from error
            time.sleep(RETRY_DELAYS[attempt - 1])


def parse_bbox(value: str) -> dict:
    parts = [float(p) for p in value.split(",")]
    if len(parts) != 4:
        raise ValueError(f"bbox must be minlat,minlon,maxlat,maxlon: {value!r}")
    minlat, minlon, maxlat, maxlon = parts
    if not (-90 <= minlat <= maxlat <= 90 and -180 <= minlon <= maxlon <= 180):
        raise ValueError(f"bbox out of range: {value!r}")
    return {"bbox": value, "max_age": DEFAULT_MAX_AGE}


def fetch_vessels(bbox: str = DEFAULT_BBOX, timeout: int = 30):
    """Yield one record per feature in the snapshot for *bbox*."""
    now = datetime.now(timezone.utc).isoformat()
    params = parse_bbox(bbox)
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    request = urllib.request.Request(
        url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"}
    )
    text = fetch_response(request, timeout)
    try:
        doc = json.loads(text)
    except json.JSONDecodeError as error:
        raise ValueError(f"Open Waters AIS returned non-JSON body: {text[:200]!r}") from error
    if not isinstance(doc, dict) or not isinstance(doc.get("features"), list):
        raise ValueError("Open Waters AIS response is missing the features list")
    for feature in doc["features"]:
        geometry = feature.get("geometry")
        props = feature.get("properties")
        if not isinstance(geometry, dict) or geometry.get("type") != "Point" \
                or not isinstance(geometry.get("coordinates"), list) \
                or len(geometry["coordinates"]) != 2:
            raise ValueError(f"feature without a Point geometry: {feature!r}"[:300])
        if not isinstance(props, dict) or "mmsi" not in props or "seen" not in props:
            raise ValueError(f"feature missing mmsi or seen: {feature!r}"[:300])
        longitude, latitude = geometry["coordinates"]
        yield {
            "source": SOURCE,
            "fetched_at": now,
            "id": f"{props['mmsi']}:{props['seen']}",
            "mmsi": props["mmsi"],
            "name": props.get("name"),
            "callsign": props.get("callsign"),
            "imo": props.get("imo"),
            "flag": props.get("flag"),
            "type": props.get("type"),
            "kind": props.get("kind"),
            "nav_status": props.get("nav_status"),
            "sog_knots": props.get("sog"),
            "cog_deg": props.get("cog"),
            "heading_deg": props.get("heading"),
            "latitude": latitude,
            "longitude": longitude,
            "draught_m": props.get("draught"),
            "length_m": props.get("length"),
            "beam_m": props.get("beam"),
            "destination": props.get("destination"),
            "eta": props.get("eta"),
            "seen": props["seen"],
            "msg_type": props.get("msg_type"),
            "receiver": props.get("source"),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bbox", default=DEFAULT_BBOX,
                        help="minlat,minlon,maxlat,maxlon (default: Strait of Dover)")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_vessels(args.bbox, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
