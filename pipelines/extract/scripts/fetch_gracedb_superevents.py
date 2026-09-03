#!/usr/bin/env python3
"""GraceDB — LIGO/Virgo/KAGRA gravitational-wave candidate events (keyless).

'Current' per run = publicly exposed superevents, newest first. During
observing runs, candidates appear within *minutes* of detection and then
accrete annotations (classification probabilities: BBH / BNS / terrestrial,
false-alarm rate, sky maps). Re-polling captures how a candidate's
classification evolves — retractions included, which is a fantastic story
for a dataset.

Docs-verified 2026-09-02 against https://gracedb.ligo.org/documentation/rest.html
(API root https://gracedb.ligo.org/api/, browsable; public superevents are
visible without credentials). My sandboxed fetcher was robots-blocked, so
smoke-test once from your machine. If you want push instead of poll, the same
alerts flow over GCN Kafka (free registration at gcn.nasa.gov).

Query syntax note: the search endpoint accepts GraceDB query strings, e.g.
`public` or `public is_gw: True`, via the ?query= parameter.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API = "https://gracedb.ligo.org/api"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"

GPS_EPOCH_UNIX = 315964800  # 1980-01-06; GraceDB t_0 is GPS seconds
LEAP_OFFSET = 18            # GPS-UTC offset (valid as of 2026; revisit on new leap second)


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def gps_to_utc_iso(gps: float | None):
    if gps is None:
        return None
    unix = gps + GPS_EPOCH_UNIX - LEAP_OFFSET
    return datetime.fromtimestamp(unix, tz=timezone.utc).isoformat()


def fetch_superevents(query: str = "public", count: int = 25):
    params = urllib.parse.urlencode({"query": query, "count": count})
    data = _get(f"{API}/superevents/?{params}")
    now = datetime.now(timezone.utc).isoformat()
    for s in data.get("superevents", []):
        labels = s.get("labels") or []
        yield {
            "source": "gracedb",
            "fetched_at": now,               # snapshot; labels/status evolve
            "id": s.get("superevent_id"),    # e.g. S260901ab
            "event_time_utc": gps_to_utc_iso(s.get("t_0")),
            "gps_t0": s.get("t_0"),
            "far_hz": s.get("far"),          # false-alarm rate
            "preferred_event": s.get("preferred_event"),
            "category": s.get("category"),
            "labels": labels,                # e.g. ADVOK / RETRACTED / EM_READY
            "retracted": "RETRACTED" in labels,
            "links": (s.get("links") or {}).get("self"),
        }


if __name__ == "__main__":
    q = sys.argv[1] if len(sys.argv) > 1 else "public"
    for rec in fetch_superevents(q):
        print(json.dumps(rec, ensure_ascii=False))
