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
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://musicbrainz.org/ws/2"
_MIN_INTERVAL = 1.0  # MusicBrainz's documented limit: 1 request/second, strictly
_MAX_ATTEMPTS = 3
_FALLBACK_RETRY_DELAYS = (1.0, 2.0)
_MIN_RETRY_DELAY = 1.0
_MAX_RETRY_DELAY = 30.0
_MAX_RETRY_WAIT = 60.0
_REQUEST_TIMEOUT = 30
_last_request = [None]


def _retry_after_delay(value, now):
    """Return a bounded Retry-After delay, or None for an unusable value."""
    if value is None:
        return None
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, IndexError, OverflowError):
            return None
        if retry_at is None:
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        delay = (retry_at - now).total_seconds()
    if not math.isfinite(delay):
        return None
    return min(_MAX_RETRY_DELAY, max(_MIN_RETRY_DELAY, delay))


def _get(url):
    """Fetch one release query with 503-only retries and strict request-start pacing.

    A query has at most three HTTP attempts. Retry-After (delta-seconds or HTTP-date)
    is clamped to 1--30 seconds; retry waiting is capped at 60 seconds per query.
    With Airflow's two task retries, this bounds a query to nine attempts and 180
    seconds of retry waiting across task attempts. Each task has at most three
    30-second HTTP calls plus 60 seconds of retry waiting, within ten minutes.
    """
    retry_wait = 0.0
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        retry_delay = 0.0
        if attempt > 1:
            retry_delay = _retry_after_delay(retry_after, datetime.now(timezone.utc))
            if retry_delay is None:
                retry_delay = _FALLBACK_RETRY_DELAYS[attempt - 2]
            retry_delay = min(retry_delay, _MAX_RETRY_WAIT - retry_wait)
            retry_wait += retry_delay
        target = time.monotonic() + retry_delay
        if _last_request[0] is not None:
            target = max(target, _last_request[0] + _MIN_INTERVAL)
        wait = target - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        # Record the start, including failures, before opening the connection.
        _last_request[0] = time.monotonic()
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                    "Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as error:
            if error.code != 503:
                error.close()
                raise
            if attempt == _MAX_ATTEMPTS:
                error.close()
                raise RuntimeError(
                    f"MusicBrainz HTTP 503 after {_MAX_ATTEMPTS} attempts"
                ) from None
            retry_after = error.headers.get("Retry-After") if error.headers else None
            error.close()


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
