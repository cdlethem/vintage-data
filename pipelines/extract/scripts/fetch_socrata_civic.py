#!/usr/bin/env python3
"""Socrata / SODA — live civic operations feeds across hundreds of cities.

Sector: **emergency operations and municipal telemetry.** New to this catalog,
and the highest-leverage entry in it: **one client unlocks hundreds of
government open-data portals**, because they nearly all run the same Socrata
(now Tyler Data & Insights) platform with an identical query API.

**The striking one: live 911 dispatch.** Seattle publishes *Seattle Real Time
Fire 911 Calls* (`kzjm-xkqj` on data.seattle.gov), described by the city as
"Seattle Fire Department 911 dispatches, updated every 5 minutes", with call
type, address, and coordinates. San Francisco, Baltimore and others publish
comparable feeds. This is a city's emergency response system as an event
stream — incident type, location, timestamp — and it is simply public.

**What you can build that nobody does:**
  * **Incident-type time series by neighbourhood.** Cardiac calls, fires, and
    MVAs have completely different diurnal and seasonal shapes.
  * **Heat and emergency load.** Join call volume against a temperature series
    and the relationship is stark — this is one of the cleanest public
    demonstrations of climate-health coupling available.
  * **311 vs 911.** Many cities publish both; the ratio of "annoyance" to
    "emergency" per neighbourhood is a genuinely novel civic metric.
  * Event-driven surges: a storm, a power cut, a holiday.

**SODA query language is the real prize.** Unlike most APIs here you get
SQL-ish operators server-side: `$where`, `$select`, `$group`, `$order`,
`$limit`, plus aggregate functions. You can make the server do
`SELECT type, count(*) ... GROUP BY type` instead of paging a million rows.

Verification status 2026-09-03: **DOCS/KNOWN.** The Seattle dataset ID and its
5-minute cadence are confirmed from the city's own catalog entry; my sandbox
couldn't fetch the resource endpoint directly. SODA itself is a long-stable,
heavily documented interface. Smoke-test each dataset once.

**Field names are NOT standard across cities.** Every portal names its columns
differently (`datetime` vs `call_date` vs `received_dttm`). Always fetch one
row first and inspect before writing a normalizer — the `discover_columns`
helper below does exactly that.

Etiquette: anonymous requests work but are throttled and share a pool. A **free
app token** (no approval needed, just registration) raises your limit
substantially and is the polite choice for anything recurring. Pass it as the
`X-App-Token` header.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
APP_TOKEN = None      # free, no approval; raises rate limits. Set it.

# A starter map of live-ish civic feeds. Verify each before trusting.
FEEDS = {
    "seattle_fire_911": ("data.seattle.gov", "kzjm-xkqj"),   # updated ~5 min
    "sf_fire_calls":    ("data.sfgov.org", "nuek-vuh3"),
    "nyc_311":          ("data.cityofnewyork.us", "erm2-nwe9"),
    "chicago_crime":    ("data.cityofchicago.org", "ijzp-q8t2"),
    "baltimore_911":    ("data.baltimorecity.gov", "xviu-ezkt"),
}


def _get(domain: str, dataset: str, **params):
    url = f"https://{domain}/resource/{dataset}.json"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = {"User-Agent": USER_AGENT}
    if APP_TOKEN:
        headers["X-App-Token"] = APP_TOKEN
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def discover_columns(domain: str, dataset: str):
    """RUN THIS FIRST for any new dataset. Column names differ per city and
    guessing them is the main way this integration silently returns nothing."""
    rows = _get(domain, dataset, **{"$limit": 1})
    return sorted(rows[0].keys()) if rows else []


def fetch_recent(domain: str, dataset: str, time_column: str,
                 since_iso: str | None = None, limit: int = 1000,
                 pages: int = 1, source_name: str = "socrata",
                 id_column: str | None = None):
    """Rows newer than `since_iso`, newest first. `time_column` varies per city
    — get it from discover_columns().

    The fallback id columns below are the ones 911 dispatch datasets use. Other
    Socrata domains name theirs differently — CDC's wastewater datasets on
    `data.cdc.gov` use `record_id` — and without `id_column` the envelope's `id`
    comes back null. Pass it explicitly whenever you leave civic data."""
    offset = 0
    for _ in range(pages):
        params = {"$limit": min(limit, 50000), "$offset": offset,
                  "$order": f"{time_column} DESC"}
        if since_iso:
            # SODA floating timestamps: no trailing Z, quote the literal
            params["$where"] = f"{time_column} > '{since_iso}'"
        rows = _get(domain, dataset, **params)
        if not rows:
            break
        now = datetime.now(timezone.utc).isoformat()
        for r in rows:
            yield {
                "source": source_name,
                "fetched_at": now,
                "id": (r.get(id_column) if id_column else None)
                      or r.get("incident_number") or r.get("unique_key")
                      or r.get("id") or r.get("cad_number"),
                "ts": r.get(time_column),
                "raw": r,          # keep raw: schemas differ wildly per city
            }
        offset += len(rows)


def fetch_aggregate(domain: str, dataset: str, group_by: str,
                    where: str | None = None, limit: int = 500):
    """Let the SERVER aggregate. Returns counts per category — tiny payload,
    and the right way to build a daily time series without paging raw rows."""
    params = {"$select": f"{group_by}, count(*) AS n",
              "$group": group_by, "$order": "n DESC", "$limit": limit}
    if where:
        params["$where"] = where
    now = datetime.now(timezone.utc).isoformat()
    for r in _get(domain, dataset, **params):
        yield {
            "source": f"socrata_agg_{group_by}",
            "fetched_at": now,
            "id": r.get(group_by),
            "category": r.get(group_by),
            "count": int(r.get("n", 0)),
        }


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "seattle_fire_911"
    domain, dataset = FEEDS[name]
    cols = discover_columns(domain, dataset)
    print(json.dumps({"feed": name, "domain": domain,
                      "dataset": dataset, "columns": cols}), file=sys.stderr)
    # pick a plausible time column from what we discovered
    tcol = next((c for c in ("datetime", "call_date", "received_dttm",
                             "created_date", "date") if c in cols), None)
    if tcol:
        for i, rec in enumerate(fetch_recent(domain, dataset, tcol,
                                             limit=20, source_name=name)):
            print(json.dumps(rec, ensure_ascii=False))
            if i >= 19:
                break
