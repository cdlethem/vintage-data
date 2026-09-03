#!/usr/bin/env python3
"""Smithsonian Global Volcanism Program — eruption database, keyless WFS/GeoJSON.

Lead #26: low frequency but high drama; a good slow-moving join dimension against
seismic data.

**Verified live 2026-09-03**:
  * `webservices.volcano.si.edu/geoserver/GVP-VOTW/wfs?service=WFS&version=2.0.0&
    request=GetFeature&typeName=GVP-VOTW:E3WebApp_Eruptions1960&outputFormat=
    application/json&count=3` — **200, 1,772 bytes, `totalFeatures: 2248`** eruptions
    since 1960, real GeoJSON features.
  * The same query with **`sortBy=StartDate+D`** (descending) — **200, 2,779 bytes,
    5 features** — top result Kikai, Japan, `StartDate: 20251229` — the most recent
    eruption in the record, genuinely close to the probe date.
  * **`maxfeatures=3` (lowercase, no separating capital) silently returned the
    entire dataset** — one query with that parameter came back **8.9 MB, 11,089
    features**, ignoring the limit entirely — confirmed and explained in quirks.

Endpoint: `https://webservices.volcano.si.edu/geoserver/GVP-VOTW/wfs`
    service=WFS&version=2.0.0&request=GetFeature&outputFormat=application/json
    typeName=GVP-VOTW:E3WebApp_Eruptions1960   (or ...Smithsonian_VOTW_Holocene_Eruptions
                                               for the full Holocene record, much larger)
    count=N              row limit — the correct WFS 2.0 parameter name
    sortBy=StartDate+D     D=descending, A=ascending — space-encoded as `+` in the URL

Quirks that will cost someone an afternoon:
    * **The row-limit parameter is `count`, not `maxfeatures`, in WFS 2.0** — this is
      a real, silent trap: `maxfeatures=3` (the WFS 1.x parameter name, wrong case for
      2.0) returned the *entire* 2,248-row or 11,089-row dataset instead of erroring
      or limiting, confirmed live twice at different scales (1.15 MB and 8.9 MB
      responses). The correctly-cased `count=3` limited properly. Always use `count`.
    * `sortBy` needs a direction suffix and the space before it must be `+`-encoded
      in the URL (`sortBy=StartDate+D`) — without it you get the database's default
      (apparently insertion) order, not chronological.
    * `StartDate`/`EndDate` are **packed `YYYYMMDD` strings**, with separate
      `StartDateYear`/`Month`/`Day` integer fields alongside them — pick one
      representation and stick with it rather than mixing.
    * `ContinuingEruption` is a **stringified boolean** (`"True"`/`"False"`), not a
      real JSON bool — and it can read `"True"` on a record whose `EndDate` is
      already in the past, so don't treat it as a reliable "still erupting right now"
      signal without cross-checking the date.
    * Two typeNames cover very different scales: `E3WebApp_Eruptions1960` (2,248
      rows, since 1960) is the practical "recent activity" table; the full
      `Smithsonian_VOTW_Holocene_Eruptions` typeName returns 11,000+ rows spanning
      the Holocene — fetching it unfiltered is an 8.9 MB response.

Etiquette: keyless, no published rate limit found. This is a small institutional
service (Smithsonian) — cache aggressively (eruption records are historical once
entered) and avoid unfiltered full-table pulls given the 8.9 MB size observed.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
BASE = "https://webservices.volcano.si.edu/geoserver/GVP-VOTW/wfs"


def _get(params):
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp)


def fetch_recent_eruptions(count: int = 25):
    """Most recent eruptions first. `count`, not `maxfeatures`, is the real limit
    parameter -- see docstring for the trap."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get({"service": "WFS", "version": "2.0.0", "request": "GetFeature",
               "typeName": "GVP-VOTW:E3WebApp_Eruptions1960",
               "outputFormat": "application/json", "count": count,
               "sortBy": "StartDate D"})
    for f in doc.get("features") or []:
        p = f.get("properties") or {}
        yield {
            "source": "smithsonian_volcanism",
            "fetched_at": now,
            "id": p.get("Activity_ID"),
            "volcano_number": p.get("VolcanoNumber"),
            "volcano_name": p.get("VolcanoName"),
            "start_date": p.get("StartDate"),
            "end_date": p.get("EndDate"),
            "continuing": p.get("ContinuingEruption") == "True",
            "explosivity_index_max": p.get("ExplosivityIndexMax"),
            "latitude": p.get("LatitudeDecimal"),
            "longitude": p.get("LongitudeDecimal"),
        }


if __name__ == "__main__":
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 25
    for rec in fetch_recent_eruptions(count=count):
        print(json.dumps(rec, ensure_ascii=False))
