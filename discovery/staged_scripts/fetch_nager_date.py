#!/usr/bin/env python3
"""Nager.Date — public holidays for ~100+ countries, keyless.

Lead #53 in DATA_SOURCE_IDEAS.md, filed as "a join dimension, not a source": nearly
every other series in this catalog has holiday seasonality (job postings, retail fuel
prices, transit ridership, lottery sales, hospital ED visits), and this is the small,
boring, disproportionately useful table that lets you control for it.

**Verified live 2026-09-03** — `date.nager.at/api/v3/AvailableCountries` returned
**204 countries**; `PublicHolidays/2026/US` returned 17 holidays for the US including
state-observed ones; `NextPublicHolidays/US` returned the next 13 upcoming, headed by
Labor Day 2026-09-07. All keyless, no rate limit published or observed.

**Why it belongs in a "rapidly changing" catalog despite being a slow-moving table:**
it isn't the source that moves — it's the join. `fetched_at` on each pull lets you track
when a country's holiday calendar was *revised* (governments add/move holidays with
real lead time, sometimes mid-year), which is itself a small, honest signal.

Endpoints (base `https://date.nager.at/api/v3`):
    /AvailableCountries              all supported country codes (204 as of this check)
    /PublicHolidays/{year}/{cc}      one country, one year — the workhorse
    /NextPublicHolidays/{cc}         the next 1-2 years of holidays from today
    /IsTodayPublicHoliday/{cc}       boolean-ish check for today specifically
    /CountryInfo/{cc}                region metadata

Quirks:
    * `counties` is non-null only for holidays observed in a subset of a country's
      regions (US state holidays, German Bundesländer) — an ISO 3166-2 subdivision
      list. `null` means nationwide.
    * `fixed: false` does not mean "moves every year" in the floating-holiday sense —
      it means the *date* isn't hardcoded to the same month/day forever (e.g. it could
      be law-changed), distinct from `types` which says Public/Bank/School/etc.
    * Years far in the past or future for small countries can return an empty list
      (`200` with `[]`), not a 404 — treat as "no holidays on file", not an error.
    * **The same holiday can appear twice on the same date for the same country**,
      once per `types` value or once per county subset. Verified live: US 2026 lists
      Good Friday twice (`types: [Public]` for a 10-state list, `types: [Optional]`
      for Texas alone) and Columbus Day twice (`types: [Public]` state-observed,
      `types: [Bank]` nationwide). `date + name` alone is not a unique key — the id
      below includes `types` and a `counties` fingerprint to disambiguate.
    * No API key, no documented rate limit. Be a good citizen: this is a small free
      project. Cache per (year, country) — a past year's list never changes.

Stdlib only.
"""
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://date.nager.at/api/v3"


def _row_id(country_code, r):
    """date+name collides when the same holiday has two `types` rows (e.g. a Public
    version for some states and an Optional/Bank version for others) — verified live
    in US 2026 (Good Friday, Columbus Day). Disambiguate with types + a counties
    fingerprint rather than assuming date+name is unique."""
    types = ",".join(sorted(r.get("types") or []))
    counties = ",".join(sorted(r.get("counties") or [])) or "ALL"
    return f"{country_code}:{r['date']}:{r['name']}:{types}:{counties}"


def _get(path):
    req = urllib.request.Request(f"{BASE}{path}", headers={"User-Agent": USER_AGENT,
                                                            "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def available_countries():
    """[{countryCode, name}, ...] — 204 as of 2026-09-03."""
    return _get("/AvailableCountries")


def fetch_holidays(country_code: str, year: int):
    """One country, one year. Empty list is a valid answer, not an error."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        rows = _get(f"/PublicHolidays/{year}/{country_code}")
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return
        raise
    for r in rows:
        yield {
            "source": "nager_date",
            "fetched_at": now,
            "id": _row_id(country_code, r),
            "country_code": country_code,
            "year": year,
            "date": r["date"],
            "name": r["name"],
            "local_name": r.get("localName"),
            "types": r.get("types") or [],
            "global": r.get("global"),
            "counties": r.get("counties"),
            "fixed": r.get("fixed"),
            "launch_year": r.get("launchYear"),
        }


def fetch_upcoming(country_code: str):
    """The next 1-2 years of holidays from today — good for a 'what's coming' poll."""
    now = datetime.now(timezone.utc).isoformat()
    for r in _get(f"/NextPublicHolidays/{country_code}"):
        yield {
            "source": "nager_date",
            "fetched_at": now,
            "id": _row_id(country_code, r),
            "country_code": country_code,
            "date": r["date"],
            "name": r["name"],
            "local_name": r.get("localName"),
            "types": r.get("types") or [],
            "global": r.get("global"),
            "counties": r.get("counties"),
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "upcoming"
    if mode == "countries":
        print(json.dumps(available_countries(), ensure_ascii=False))
    elif mode == "year":
        cc = sys.argv[2] if len(sys.argv) > 2 else "US"
        yr = int(sys.argv[3]) if len(sys.argv) > 3 else datetime.now().year
        for rec in fetch_holidays(cc, yr):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        cc = sys.argv[2] if len(sys.argv) > 2 else "US"
        for rec in fetch_upcoming(cc):
            print(json.dumps(rec, ensure_ascii=False))
