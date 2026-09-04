#!/usr/bin/env python3
"""UK Police API — street-level crime, outcomes, and stop-and-search (keyless).

Sector: law enforcement / criminal justice. Not covered anywhere else here.

data.police.uk publishes every recorded street-level crime in England, Wales
and Northern Ireland, geocoded to an anonymised nearby point, with a category
and — crucially — an **outcome that updates over time**. It also publishes
stop-and-search records including self-defined ethnicity, object of search,
and result.

Why it's a good pipeline subject beyond the obvious:
  * **Outcomes are the time series.** A crime is first published with no
    outcome, then later gains one ("under investigation" -> "charged" ->
    "unable to prosecute"). Re-poll past months and you capture the justice
    system's decision latency, which is a real and rarely-built dataset.
  * Categories plus geography plus month gives a clean panel structure that's
    excellent for practicing joins and aggregation.
  * Stop-and-search data is one of the few public datasets that lets you
    examine institutional behavior rather than just events.

Verification status 2026-09-02: KNOWN. The API has been public and keyless
for over a decade, but I did not fetch it during this session (sandbox host
restrictions). Confirm endpoint shapes on first run.

Endpoints (base https://data.police.uk/api):
  /crimes-street/all-crime?lat=&lng=&date=YYYY-MM   crimes near a point
  /crimes-street/all-crime?poly=lat,lng:lat,lng:... crimes in a polygon
  /outcomes-at-location?...                          outcome updates
  /stops-street?lat=&lng=&date=YYYY-MM               stop and search
  /crimes-street-dates                               which months exist
  /forces                                            list of police forces

Quirks that will bite you:
  * **This is MONTHLY, not real-time.** Data lands roughly two months in
    arrears. Poll once a day at most; the interesting churn is revisions to
    *past* months, not new rows today.
  * Always call `/crimes-street-dates` first — it tells you which months are
    actually available, and it changes when a new batch publishes. That's
    your real watermark.
  * Locations are **anonymised to a nearby map point**, not the true address.
    Never present them as precise. This is a deliberate privacy control and
    treating it otherwise is both wrong and misleading.
  * Polygon queries are POST-friendly and have a size ceiling; large areas
    return 503. Slice geographically.
  * Open Government Licence — attribution required.

Stdlib only.
"""
import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://data.police.uk/api"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(path: str, **params):
    url = f"{BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise


def available_months():
    """Which months have data. Use as the watermark; it moves on publication."""
    return [d.get("date") for d in _get("crimes-street-dates")]


def fetch_crimes(lat: float, lng: float, month: str | None = None):
    """Street-level crimes near a point for one month (YYYY-MM)."""
    params = {"lat": lat, "lng": lng}
    if month:
        params["date"] = month
    fetched_at = datetime.now(timezone.utc).isoformat()
    for c in _get("crimes-street/all-crime", **params):
        loc = c.get("location") or {}
        street = loc.get("street") or {}
        outcome = c.get("outcome_status") or {}
        yield {
            "source": "uk_police_crime",
            "fetched_at": fetched_at,      # snapshot; outcomes change later
            "id": c.get("persistent_id") or c.get("id"),
            "month": c.get("month"),
            "category": c.get("category"),
            "lat": loc.get("latitude"),
            "lng": loc.get("longitude"),
            "street": street.get("name"),   # anonymised nearby point, NOT an address
            "location_type": c.get("location_type"),
            "outcome": outcome.get("category"),        # None until decided
            "outcome_date": outcome.get("date"),
        }


def fetch_stops(lat: float, lng: float, month: str | None = None):
    """Stop-and-search records near a point for one month."""
    params = {"lat": lat, "lng": lng}
    if month:
        params["date"] = month
    fetched_at = datetime.now(timezone.utc).isoformat()
    for s in _get("stops-street", **params):
        loc = s.get("location") or {}
        yield {
            "source": "uk_police_stop_search",
            "fetched_at": fetched_at,
            "id": f"{s.get('datetime')}|{loc.get('latitude')}|{loc.get('longitude')}",
            "datetime": s.get("datetime"),
            "type": s.get("type"),
            "object_of_search": s.get("object_of_search"),
            "outcome": s.get("outcome"),
            "age_range": s.get("age_range"),
            "gender": s.get("gender"),
            "self_defined_ethnicity": s.get("self_defined_ethnicity"),
            "legislation": s.get("legislation"),
            "lat": loc.get("latitude"),
            "lng": loc.get("longitude"),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lat", type=float, default=52.629729)   # Leicester
    parser.add_argument("--lng", type=float, default=-1.131592)
    parser.add_argument("--month", help="YYYY-MM; omit for the newest published month")
    parser.add_argument("--stops", action="store_true",
                        help="stop-and-search instead of street crime")
    args = parser.parse_args()

    month = args.month
    if not month:
        months = available_months()
        print(json.dumps({"available_months": months[:3], "count": len(months)}),
              file=sys.stderr)
        month = months[0] if months else None
    fetch = fetch_stops if args.stops else fetch_crimes
    for rec in fetch(args.lat, args.lng, month):
        print(json.dumps(rec, ensure_ascii=False))


if __name__ == "__main__":
    main()
