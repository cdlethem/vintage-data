#!/usr/bin/env python3
"""UK Carbon Intensity — grid CO2 forecast AND actual, half-hourly (keyless).

Official API from Great Britain's National Energy System Operator. Every
half-hour settlement period carries **both a forecast and, once settled, an
actual** — in the same record. That is a rare and valuable property: you get
a free, continuously-refreshing forecast-scoring dataset without having to
archive predictions yourself and join them later. Most forecast APIs make you
do that work; this one hands you the pair.

Verified live 2026-09-02: `/intensity` returned the 16:00–16:30Z period with
forecast=101, actual=134, index="moderate". A 33% miss, right there in the
sample — which is itself a hint that forecast error is worth modeling.

Also available and worth pulling:
  /intensity/date            all periods for today
  /generation                current fuel mix (wind/solar/nuclear/gas/imports)
  /regional                  per-region intensity + fuel mix for GB
  /intensity/{from}/{to}     historical range
  /intensity/{from}/fw48h    forward 48-hour forecast

'Current' per run = the current settlement period plus, ideally, a re-pull of
the last ~24h so you capture `actual` values as they land for periods you
previously only had forecasts for. **Upsert on the `from` timestamp; don't
append blindly**, or you'll get duplicate rows for the same period with
different actuals.

Licence CC BY 4.0, and there are formal Terms of Use — read them if this goes
anywhere beyond a prototype.

Stdlib only.
"""
import argparse
import json
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://api.carbonintensity.org.uk"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
HEADERS = {"Accept": "application/json", "User-Agent": USER_AGENT}


def _get(path: str):
    req = urllib.request.Request(BASE + path, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def fetch_current():
    yield from _rows(_get("/intensity"), "national")


def fetch_recent(hours_back: int = 24):
    """Re-pull recent periods so late-arriving `actual` values get captured."""
    now = datetime.now(timezone.utc)
    lo = _iso(now - timedelta(hours=hours_back))
    hi = _iso(now)
    yield from _rows(_get(f"/intensity/{lo}/{hi}"), "national")
 
def fetch_range(start: str, end: str):
    """Fetch one bounded historical range (the API returns half-hour periods)."""
    yield from _rows(_get(f"/intensity/{start}/{end}"), "national")



def fetch_forward_48h():
    """Pure forecast, 48h ahead. Store these and score them later."""
    now = _iso(datetime.now(timezone.utc))
    yield from _rows(_get(f"/intensity/{now}/fw48h"), "forecast_48h")


def fetch_generation_mix():
    data = _get("/generation")
    d = data.get("data") or {}
    now = datetime.now(timezone.utc).isoformat()
    rec = {"source": "carbon_intensity_genmix", "fetched_at": now,
           "id": d.get("from"), "from": d.get("from"), "to": d.get("to")}
    for f in d.get("generationmix", []):
        rec[f"pct_{f.get('fuel')}"] = f.get("perc")
    yield rec


def _rows(data: dict, kind: str):
    now = datetime.now(timezone.utc).isoformat()
    for p in data.get("data", []):
        it = p.get("intensity") or {}
        yield {
            "source": f"carbon_intensity_{kind}",
            "fetched_at": now,
            "id": p.get("from"),          # settlement period start = upsert key
            "from": p.get("from"),
            "to": p.get("to"),
            "forecast": it.get("forecast"),
            "actual": it.get("actual"),   # None until the period settles
            "index": it.get("index"),     # very low / low / moderate / high...
        }
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", nargs="?", default="current",
                        choices=("current", "recent", "forward", "mix", "range"))
    parser.add_argument("start", nargs="?")
    parser.add_argument("end", nargs="?")
    args = parser.parse_args()
    if args.mode == "range":
        if not args.start or not args.end:
            parser.error("range requires START and END in API ISO format")
        gen = fetch_range(args.start, args.end)
    else:
        gen = {"current": fetch_current, "recent": fetch_recent,
               "forward": fetch_forward_48h, "mix": fetch_generation_mix}[args.mode]()
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
