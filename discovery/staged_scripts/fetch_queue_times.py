#!/usr/bin/env python3
"""Queue-Times — live theme park ride wait times worldwide (keyless).

Sector: **venue operations / queueing theory.** Untouched by anything else in
this catalog, and one of the purest operational-telemetry feeds available free.

**What it is:** live wait times and open/closed status for every ride at 80+
amusement parks — Disney, Universal, Merlin, Six Flags, Cedar Fair, SeaWorld —
refreshed every 5 minutes, with history back to 2014.

**Why it's genuinely interesting rather than just fun:**
  * It is a **real M/M/c queueing system with observable queue lengths.** Wait
    time is the dependent variable; ride capacity, park hours, weather,
    holidays, and ride breakdowns are the independent ones. You can fit actual
    queueing models against real-world data, which is hard to do free.
  * **`is_open` flipping to false mid-day is an unplanned outage.** You're
    watching mechanical reliability in real time across hundreds of machines.
    Mean time between failures per ride, by park, by season — nobody publishes
    that, but you can derive it.
  * **Demand redistributes.** When a headliner goes down, waits at neighbouring
    rides spike. That's an observable substitution effect within a closed
    system, which is a lovely natural experiment.
  * Cross-park comparison at the same instant separates operator behaviour
    from demand: same brand, different continents, same timestamp.

**Verified live 2026-09-03T00:30Z** — GET
`https://queue-times.com/parks/7/queue_times.json` returned Disney Hollywood
Studios with per-ride waits timestamped ~1 minute earlier (Toy Story Mania 25
min, Rock 'n' Roller Coaster 40 min, Rise of the Resistance closed).

Endpoints:
  https://queue-times.com/parks.json                 all parks, grouped by operator
  https://queue-times.com/parks/{id}/queue_times.json live waits for one park

**Terms — this one has a real obligation:** the API is free but *requires* you
to display "Powered by Queue-Times.com" linking to https://queue-times.com/
prominently in any app or service that surfaces the data. Honour it. They also
ask for Patreon support if you can afford it; it's a small project.

Cadence note: data refreshes **every ~5 minutes**, so polling faster is pure
waste. Use `last_updated` to dedupe — consecutive polls often return identical
timestamps.

Stdlib only.
"""
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

BASE = "https://queue-times.com"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
ATTRIBUTION = "Powered by Queue-Times.com"   # REQUIRED in any user-facing output
POLITE_DELAY = 1.0


def _get(path: str):
    time.sleep(POLITE_DELAY)
    req = urllib.request.Request(f"{BASE}/{path}",
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp)


def fetch_parks():
    """All parks, flattened out of their operator groups."""
    now = datetime.now(timezone.utc).isoformat()
    for group in _get("parks.json"):
        for p in group.get("parks", []):
            yield {
                "source": "queue_times_park",
                "fetched_at": now,
                "id": p.get("id"),
                "park": p.get("name"),
                "operator": group.get("name"),     # Disney, Merlin, Cedar Fair...
                "country": p.get("country"),
                "continent": p.get("continent"),
                "latitude": p.get("latitude"),
                "longitude": p.get("longitude"),
                "timezone": p.get("timezone"),
            }


def fetch_waits(park_id: int, park_name: str | None = None):
    """Live per-ride waits for one park.

    Response shape gotcha: rides live under `lands[].rides`, but parks without
    land groupings put them in a top-level `rides` array instead. Handle both
    or you'll silently get zero rows for some parks.
    """
    data = _get(f"parks/{park_id}/queue_times.json")
    now = datetime.now(timezone.utc).isoformat()

    def emit(ride, land_id, land_name):
        return {
            "source": "queue_times_ride",
            "fetched_at": now,             # snapshot time = the series axis
            "id": f"{park_id}:{ride.get('id')}:{ride.get('last_updated')}",
            "park_id": park_id,
            "park": park_name,
            "land_id": land_id,
            "land": land_name,
            "ride_id": ride.get("id"),
            "ride": ride.get("name"),
            "is_open": ride.get("is_open"),      # False mid-day = outage
            "wait_minutes": ride.get("wait_time"),
            "last_updated": ride.get("last_updated"),  # dedupe on this
        }

    for land in data.get("lands", []) or []:
        for ride in land.get("rides", []) or []:
            yield emit(ride, land.get("id"), land.get("name"))
    for ride in data.get("rides", []) or []:      # ungrouped parks
        yield emit(ride, None, None)


def fetch_all_parks_snapshot(park_ids=None, limit: int | None = None):
    """One synchronized sweep across many parks — enables cross-park
    comparison at the same instant."""
    parks = list(fetch_parks())
    if park_ids:
        parks = [p for p in parks if p["id"] in set(park_ids)]
    if limit:
        parks = parks[:limit]
    for p in parks:
        try:
            yield from fetch_waits(p["id"], p["park"])
        except Exception as e:                 # one bad park shouldn't kill the sweep
            print(f"WARN park {p['id']} ({p['park']}): {e}", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "parks":
        gen = fetch_parks()
    elif len(sys.argv) > 1:
        gen = fetch_waits(int(sys.argv[1]))
    else:
        gen = fetch_waits(7, "Disney Hollywood Studios")
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
    print(f"# {ATTRIBUTION}", file=sys.stderr)
