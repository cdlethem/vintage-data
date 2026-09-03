#!/usr/bin/env python3
"""MusicBrainz — the open music encyclopedia, keyless with a strict rate rule.

Lead #66 (and #31 in the earlier sweep, same source). New releases entered continuously
by a global community of editors. Same flavor as Wikimedia recent changes but for music
metadata: artist credits, release groups, track counts, and country/date of release.

**Verified live 2026-09-03** — `musicbrainz.org/ws/2/release/?query=date:2026-09-02
&fmt=json&limit=10` returned **200, 12,123 bytes, count: 1363** matching releases,
first result "RAVEPOP" by r u s s e l b u c k, an Album release-group with 10 tracks
on Vinyl.

Endpoint (base `https://musicbrainz.org/ws/2`):
    /release/?query=<lucene-query>&fmt=json&limit=N&offset=N
        Lucene-syntax search. `date:YYYY-MM-DD` matches releases indexed with that
        date; `date:[2026-09-01 TO 2026-09-02]` for a range.
    /release-group/?query=...&fmt=json     search by release-group (album) instead
    /artist/?query=...&fmt=json             search artists

Quirks that will cost someone an afternoon:
    * **`date` on a release is very often partial** — a real "date:2026-09-02" query
      hit returned a release whose own `date` field is just `"2026"` (year only, no
      month/day). MusicBrainz indexes releases by whatever date precision the editor
      supplied; do not assume the returned `date` string matches your query's
      precision, and do not assume it parses as a full ISO date.
    * `release-events` carries its own `date` (often also partial) plus an `area` —
      for a global release this is `"[Worldwide]"` / country code `XW`, not a real
      country. Filter `XW` out if you want a country-level release map.
    * `artist-credit` is a **list**, because a release can credit multiple artists
      (collaborations, "feat." credits) — never assume `artist-credit[0]` is the whole
      story for multi-artist releases.
    * **The strict rule**: MusicBrainz asks for **one request per second, no more**,
      and requires a real User-Agent identifying your application (they will block
      generic/browser User-Agents). This script enforces the pacing itself, the same
      pattern as `fetch_codeforces.py` for its own documented limit — do not rely on a
      comment reminding you; a burst of unthrottled requests gets your IP blocked.

Etiquette: keyless but rate-limited to 1 req/s, enforced in code below. Real contact
info in the User-Agent is not optional here — MusicBrainz explicitly reserves the
right to block generic ones.

Stdlib only.
"""
import json
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: cdlethem@gmail.com)"
BASE = "https://musicbrainz.org/ws/2"
_MIN_INTERVAL = 1.0  # MusicBrainz's documented limit: 1 request/second, strictly
_last_request = [0.0]


def _get(url):
    wait = _MIN_INTERVAL - (time.monotonic() - _last_request[0])
    if wait > 0:
        time.sleep(wait)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        doc = json.load(resp)
    _last_request[0] = time.monotonic()
    return doc


def search_releases(query: str, limit: int = 25, offset: int = 0):
    """Lucene-syntax query against MusicBrainz's release index. Rate-limited to 1/s."""
    now = datetime.now(timezone.utc).isoformat()
    params = urllib.parse.urlencode({"query": query, "fmt": "json",
                                     "limit": limit, "offset": offset})
    doc = _get(f"{BASE}/release/?{params}")
    for r in doc.get("releases") or []:
        credits = r.get("artist-credit") or []
        rg = r.get("release-group") or {}
        yield {
            "source": "musicbrainz",
            "fetched_at": now,
            "id": r["id"],
            "title": r.get("title"),
            "artists": [c.get("name") for c in credits if isinstance(c, dict) and c.get("name")],
            "date": r.get("date"),
            "country": r.get("country"),
            "status": r.get("status"),
            "release_group_type": rg.get("primary-type"),
            "track_count": r.get("track-count"),
            "score": r.get("score"),
        }


def fetch_recent(date: str, limit: int = 25):
    """Convenience: releases indexed with the given YYYY-MM-DD date."""
    yield from search_releases(f"date:{date}", limit=limit)


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for rec in fetch_recent(date):
        print(json.dumps(rec, ensure_ascii=False))
