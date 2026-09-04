#!/usr/bin/env python3
"""Jolpica-F1 — Formula 1 results, lap times and pit stops (keyless).

Sector: international motorsport.

**Read this first, it will save you an afternoon:** the API everyone still
links to for F1 data — **Ergast (ergast.com) — was SHUT DOWN at the end of
2024.** Most tutorials, Stack Overflow answers, and blog posts you'll find are
pointing at a dead endpoint. Jolpica-F1 is the community-built, drop-in
compatible successor at `https://api.jolpi.ca/ergast/f1/`, and it's what
FastF1 and the rest of the ecosystem migrated to.

Docs-verified 2026-09-02 against the jolpica/jolpica-f1 repo and the FastF1
documentation, both of which confirm the Ergast shutdown and the drop-in
replacement path.

**Why it's a good time series:** lap times are a per-lap event stream across a
two-hour race — tyre degradation shows up directly as lap-time drift, and pit
stops are discrete interventions in that series. It's a clean natural
experiment structure. Championship standings after each round give you a
season-long trajectory per driver and constructor.

Endpoints (all under https://api.jolpi.ca/ergast/f1/):
  /current/last/results.json        most recent race result
  /{season}/{round}/laps.json       LAP TIMES — the dense one, paginate it
  /{season}/{round}/pitstops.json   pit stops
  /{season}/{round}/qualifying.json
  /current/driverStandings.json     |  /current/constructorStandings.json
  /current.json                     season calendar

Quirks:
  * Responses are wrapped in the legacy Ergast envelope: `MRData` at the root,
    then a table object whose key varies per endpoint (`RaceTable`,
    `StandingsTable`, ...). The helper below unwraps it.
  * **Default `limit` is 30 and lap data is huge** — a single race is
    thousands of rows. Always paginate with `limit`/`offset` (limit caps at
    100 for many endpoints; check `MRData.total`).
  * `season=current` and `round=last` are supported magic values.
  * Jolpica is **volunteer-run on donations** (~$45/month hosting). There are
    published rate limits and Terms of Use — respect them, cache aggressively,
    and never re-fetch a completed historical race you already have. Past
    seasons are immutable; fetch once and store.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.jolpi.ca/ergast/f1"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(path: str, limit: int = 100, offset: int = 0):
    url = f"{BASE}/{path}?limit={limit}&offset={offset}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp).get("MRData", {})   # legacy Ergast envelope


def _paginate(path: str, table_key: str, limit: int = 100, max_pages: int = 50):
    offset = 0
    for _ in range(max_pages):
        md = _get(path, limit=limit, offset=offset)
        table = md.get(table_key) or {}
        races = table.get("Races") or []
        if not races:
            break
        yield md, races
        offset += limit
        if offset >= int(md.get("total", 0)):
            break


def fetch_lap_times(season: str = "current", rnd: str = "last"):
    """Per-lap times for every driver. The dense stream — expect thousands."""
    now = datetime.now(timezone.utc).isoformat()
    for _md, races in _paginate(f"{season}/{rnd}/laps.json", "RaceTable"):
        for race in races:
            for lap in race.get("Laps", []):
                for t in lap.get("Timings", []):
                    yield {
                        "source": "f1_lap",
                        "fetched_at": now,
                        "id": f"{race.get('season')}:{race.get('round')}:"
                              f"{t.get('driverId')}:{lap.get('number')}",
                        "season": race.get("season"),
                        "round": race.get("round"),
                        "race": race.get("raceName"),
                        "date": race.get("date"),
                        "lap": int(lap.get("number")) if lap.get("number") else None,
                        "driver": t.get("driverId"),
                        "position": t.get("position"),
                        "time": t.get("time"),      # e.g. "1:32.412"
                    }


def fetch_pit_stops(season: str = "current", rnd: str = "last"):
    now = datetime.now(timezone.utc).isoformat()
    for _md, races in _paginate(f"{season}/{rnd}/pitstops.json", "RaceTable"):
        for race in races:
            for p in race.get("PitStops", []):
                yield {
                    "source": "f1_pitstop",
                    "fetched_at": now,
                    "id": f"{race.get('season')}:{race.get('round')}:"
                          f"{p.get('driverId')}:{p.get('stop')}",
                    "season": race.get("season"),
                    "round": race.get("round"),
                    "race": race.get("raceName"),
                    "driver": p.get("driverId"),
                    "stop": p.get("stop"),
                    "lap": p.get("lap"),
                    "time_of_day": p.get("time"),
                    "duration_s": p.get("duration"),
                }


def fetch_driver_standings(season: str = "current"):
    now = datetime.now(timezone.utc).isoformat()
    md = _get(f"{season}/driverStandings.json")
    lists = (md.get("StandingsTable") or {}).get("StandingsLists") or []
    for sl in lists:
        for d in sl.get("DriverStandings", []):
            drv = d.get("Driver") or {}
            cons = (d.get("Constructors") or [{}])[0]
            yield {
                "source": "f1_driver_standings",
                "fetched_at": now,
                "id": f"{sl.get('season')}:{sl.get('round')}:{drv.get('driverId')}",
                "season": sl.get("season"),
                "round": sl.get("round"),
                "driver": drv.get("driverId"),
                "driver_name": f"{drv.get('givenName','')} {drv.get('familyName','')}".strip(),
                "nationality": drv.get("nationality"),
                "constructor": cons.get("name"),
                "position": d.get("position"),
                "points": d.get("points"),
                "wins": d.get("wins"),
            }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "standings"
    gen = {"laps": fetch_lap_times, "pits": fetch_pit_stops,
           "standings": fetch_driver_standings}[mode]()
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
