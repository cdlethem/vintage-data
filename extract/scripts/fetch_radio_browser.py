#!/usr/bin/env python3
"""Radio Browser — every internet radio station on Earth, with live popularity.

Sector: music / culture, and it is **natively international** — which makes it
the single best answer to "give me something non-English." Stations carry a
`countrycode` and a `language` field, so you can slice the entire dataset by
language and watch what Tamil, Portuguese, or Farsi listeners are tuning into.
Roughly 45,000+ stations.

**Why it's a real time series and not just a directory:** each station carries
`clickcount` (total tune-ins), `clicktrend` (recent direction), `votes`, and
`lastchecktime`. Those move continuously. Poll a slice daily and you get:
  * a **listening-popularity series per station and per country**
  * station **uptime/death** tracking via `lastcheckok` — internet radio has
    surprisingly high churn, and watching stations go dark is its own dataset
  * genre drift over time via the `tags` field

Cross-cultural analysis is where this gets fun: compare tag vocabulary across
language communities, or watch how a global event changes tune-in patterns by
country. Very little of this has been done publicly.

Docs-verified 2026-09-02 against docs.radio-browser.info and the API host
index; my sandboxed fetcher was robots-blocked, so smoke-test once from your
own machine. Field list below is taken from the official docs.

**Two etiquette rules that are unusually important here** (both stated by the
project itself):
  1. **Do NOT hardcode a single server.** The project runs a mirror pool and
     asks clients to discover hosts rather than pin one. Resolve the mirror
     list from https://all.api.radio-browser.info/json/servers and pick one.
     The script does this, with a documented fallback.
  2. **Send a descriptive User-Agent.** It's a volunteer-run community
     service; anonymous hammering is how free resources die.

Also: do NOT use the `id` or `country` fields — the docs explicitly deprecate
both. Use `stationuuid` and `countrycode` instead.

Stdlib only.
"""
import json
import os
import random
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SERVER_LIST = "https://all.api.radio-browser.info/json/servers"
FALLBACK = "https://de1.api.radio-browser.info"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

_server = None


def _pick_server() -> str:
    """Resolve a mirror rather than pinning one, as the project requests."""
    global _server
    if _server:
        return _server
    try:
        req = urllib.request.Request(SERVER_LIST,
                                     headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=20) as resp:
            hosts = [h.get("name") for h in json.load(resp) if h.get("name")]
        if hosts:
            _server = "https://" + random.choice(hosts)
            return _server
    except Exception:
        pass
    _server = FALLBACK
    return _server


def _get(path: str, **params):
    url = f"{_pick_server()}/json/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp)


def _norm(s: dict, now: str) -> dict:
    return {
        "source": "radio_browser",
        "fetched_at": now,             # snapshot; clicks/votes move
        "id": s.get("stationuuid"),    # NOT `id` — deprecated by the project
        "name": s.get("name"),
        "country_code": s.get("countrycode"),   # NOT `country` — deprecated
        "state": s.get("state"),
        "language": s.get("language"),
        "language_codes": s.get("languagecodes"),
        "tags": [t for t in (s.get("tags") or "").split(",") if t],
        "codec": s.get("codec"),
        "bitrate": s.get("bitrate"),
        "homepage": s.get("homepage"),
        # the moving values — this is your time series
        "clickcount": s.get("clickcount"),
        "clicktrend": s.get("clicktrend"),
        "votes": s.get("votes"),
        "click_timestamp": s.get("clicktimestamp_iso8601"),
        # liveness — stations die constantly
        "last_check_ok": bool(s.get("lastcheckok")),
        "last_check_time": s.get("lastchecktime_iso8601"),
        "last_change_time": s.get("lastchangetime_iso8601"),
        "geo_lat": s.get("geo_lat"),
        "geo_lon": s.get("geo_long"),
    }


def fetch_by_country(country_code: str, limit: int = 500, hide_broken: bool = True):
    """All stations for an ISO country code, e.g. 'JP', 'BR', 'IN', 'DE'."""
    rows = _get(f"stations/bycountrycodeexact/{country_code}",
                limit=limit, hidebroken=str(hide_broken).lower(),
                order="clickcount", reverse="true")
    now = datetime.now(timezone.utc).isoformat()
    for s in rows:
        yield _norm(s, now)


def fetch_by_language(language: str, limit: int = 500):
    """Stations by language name, e.g. 'tamil', 'portuguese', 'arabic'."""
    rows = _get("stations/search", language=language, limit=limit,
                hidebroken="true", order="clickcount", reverse="true")
    now = datetime.now(timezone.utc).isoformat()
    for s in rows:
        yield _norm(s, now)


def fetch_trending(limit: int = 200):
    """Stations whose listenership is moving fastest right now."""
    rows = _get("stations/search", limit=limit, hidebroken="true",
                order="clicktrend", reverse="true")
    now = datetime.now(timezone.utc).isoformat()
    for s in rows:
        yield _norm(s, now)


def fetch_country_totals():
    """Station counts per country — a compact daily row set."""
    now = datetime.now(timezone.utc).isoformat()
    for c in _get("countrycodes"):
        yield {"source": "radio_browser_countries", "fetched_at": now,
               "id": c.get("name"), "country_code": c.get("name"),
               "station_count": c.get("stationcount")}


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "trending"
    if mode == "country":
        gen = fetch_by_country(sys.argv[2] if len(sys.argv) > 2 else "JP", limit=50)
    elif mode == "language":
        gen = fetch_by_language(sys.argv[2] if len(sys.argv) > 2 else "tamil", limit=50)
    elif mode == "totals":
        gen = fetch_country_totals()
    else:
        gen = fetch_trending(limit=50)
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
