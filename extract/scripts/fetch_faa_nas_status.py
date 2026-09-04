#!/usr/bin/env python3
"""FAA NAS Status — ground stops, ground delays, and airport pacing, keyless.

Lead #39: very spiky and weather-coupled without being a weather API. Ground delay
programs and stops are how the FAA manages the air traffic system in real time — a
JFK ground delay today is a downstream effect of weather, volume, or equipment, not
weather data itself.

**Verified live 2026-09-03** — three different endpoints, three different formats:
  * `nasstatus.faa.gov/api/airport-status-information` — **200, 4,582 bytes, XML** —
    `Update_Time: Thu Sep 3 17:07:46 2026 GMT`, real ground delays: JFK (avg 1h50m,
    max 3h28m, "airport volume"), SFO ("low ceilings").
  * `nasstatus.faa.gov/api/pacing-airports` — **200, 4,174 bytes, JSON, 40 airports**
    currently being paced (STL, DFW, MIA, LGA, ...) with lat/lon and timezone.
  * `nasstatus.faa.gov/api/airport-events` — **200, 22,711 bytes, JSON, 16 airports**
    with per-airport `groundStop`/`groundDelay` detail. BOS carried a real active
    ground delay, `sourceTimeStamp: 2026-09-03T14:04:39Z`, ~3 hours before the probe,
    "low ceilings", avg delay 136 min.

Endpoints (base `https://nasstatus.faa.gov/api`):
    /airport-status-information    XML: current ground delay programs + ground stops
    /pacing-airports                 JSON: airports currently being metered/paced
    /airport-events                  JSON: per-airport groundStop/groundDelay detail
    (`/restrictions` was tried and 404'd — not a real path)

Quirks that will cost someone an afternoon:
    * **Three endpoints, three formats** — one is XML (`xml.etree.ElementTree`
      needed), two are JSON. Don't assume a uniform content type across this API.
    * **`groundStop` and `groundDelay` are independently nullable** on each airport in
      `/airport-events` — verified live: BOS had a real `groundDelay` object with
      `groundStop: null`, while other airports had both null (tracked but currently
      unaffected). Check each field's presence separately rather than assuming an
      airport entry means an active condition.
    * The XML `Update_Time` uses a **non-standard, locale-dependent date string**
      (`"Thu Sep 3 17:07:46 2026 GMT"`) rather than ISO 8601 — parse with an explicit
      format string (`%a %b %d %H:%M:%S %Y %Z`), don't assume `fromisoformat` works.
    * `sourceTimeStamp` (when the FAA's system recorded the event) and `createdAt`/
      `updatedAt` (this specific record's lifecycle) are distinct fields on
      `groundDelay`/`groundStop` objects — `sourceTimeStamp` is the one that tracks
      the underlying real-world event.
    * `avgDelay`/`maxDelay` are in **minutes as a number** in the JSON endpoints, but
      the XML endpoint's equivalent fields are **pre-formatted strings** ("1 hour and
      50 minutes") — the same information, incompatible representations depending on
      which endpoint you use.

Etiquette: keyless, no published rate limit found. This reflects live, safety-relevant
air traffic operations — poll at a reasonable interval (a few minutes; delay programs
don't change second to second) rather than hammering it.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://nasstatus.faa.gov/api"


def _get_bytes(path):
    req = urllib.request.Request(f"{BASE}/{path}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.read()


def fetch_events():
    """Per-airport ground stop / ground delay status. The richest of the three."""
    now = datetime.now(timezone.utc).isoformat()
    doc = json.loads(_get_bytes("airport-events"))
    for a in doc:
        gs = a.get("groundStop")
        gd = a.get("groundDelay")
        yield {
            "source": "faa_nas_status",
            "fetched_at": now,
            "id": a.get("airportId"),
            "airport": a.get("airportId"),
            "has_ground_stop": gs is not None,
            "has_ground_delay": gd is not None,
            "ground_stop_reason": (gs or {}).get("impactingCondition"),
            "ground_delay_reason": (gd or {}).get("impactingCondition"),
            "ground_delay_avg_min": (gd or {}).get("avgDelay"),
            "ground_delay_max_min": (gd or {}).get("maxDelay"),
            "source_timestamp": (gd or gs or {}).get("sourceTimeStamp"),
        }


def fetch_pacing_airports():
    """Airports currently being metered/paced, with location."""
    now = datetime.now(timezone.utc).isoformat()
    doc = json.loads(_get_bytes("pacing-airports"))
    for a in doc:
        yield {
            "source": "faa_nas_status_pacing",
            "fetched_at": now,
            "id": a.get("airportId"),
            "airport": a.get("airportId"),
            "is_pacing": a.get("isPacing"),
            "timezone": a.get("timezone"),
            "latitude": a.get("latitude"),
            "longitude": a.get("longitude"),
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "events"
    fn = fetch_pacing_airports if mode == "pacing" else fetch_events
    for rec in fn():
        print(json.dumps(rec, ensure_ascii=False))
