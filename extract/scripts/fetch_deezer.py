#!/usr/bin/env python3
"""Deezer public API — keyless charts and editorial new-release lists.

Lead #69. Deezer's global chart (`chart/0/tracks`) and per-genre charts give a live,
ranked, keyless view of what's playing, sliceable by genre and (via editorial lists)
by country. Complements Radio Browser and iTunes charts already in this catalog with a
third, differently-weighted popularity signal.

**Verified live 2026-09-03**:
  * `api.deezer.com/chart/0/tracks?limit=15` — **200, 31,778 bytes, 15 tracks**, #1
    "Dracula (with JENNIE)" by Tame Impala, `rank: 982905`.
  * `api.deezer.com/chart/132/tracks?limit=5` — **200, 10,562 bytes, 5 tracks** — genre
    id 132 confirms per-genre charts work identically.
  * `api.deezer.com/editorial/0/releases?limit=10` — **200** but **`data: []`** — a
    live-but-empty result, not an error (see quirks).
  * `api.deezer.com/chart/999999/tracks` (bogus genre id) — **200, `data: []`** — same
    silent-empty behavior, confirming this API never 404s on a bad id, it just returns
    nothing.

Endpoints (base `https://api.deezer.com`):
    /chart/{genre_id}/tracks     ranked tracks; genre_id=0 is the global "all genres" chart
    /chart/{genre_id}            tracks+albums+artists+playlists in one call
    /editorial/{genre_id}/releases   new releases for that genre (0 = all)
    /genre                       the list of valid genre_id values

Quirks:
    * **A bad or empty-result genre_id returns `200` with `data: []`, never a 404 or
      an error field.** Verified with genre 999999. Don't treat an empty chart as a
      broken request; it may simply mean that genre has nothing charting right now.
    * `rank` is Deezer's own internal popularity score (large opaque integer, ~10^6
      range observed) — it is not the same axis as chart `position`; use `position`
      for "where in the Top N" and `rank` only as a relative popularity magnitude.
    * `preview` URLs are signed, time-limited CDN links (`hdnea=exp=...`) — they expire;
      don't cache them past a session, re-fetch instead.
    * `explicit_content_lyrics` and `explicit_content_cover` are small integer codes,
      not booleans — `0` seen live means "not flagged," but the API defines other codes
      for "unknown"/"edited" that are worth mapping if you use this field.

Etiquette: keyless, Deezer publishes a documented rate limit of 50 requests / 5 seconds
per IP — comfortably above anything a daily chart poll needs. Self-impose well under
that; charts are worth polling at most hourly.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://api.deezer.com"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_chart(genre_id: int = 0, limit: int = 25):
    """Ranked tracks for one genre. genre_id=0 is the global chart. Empty is valid."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(f"{BASE}/chart/{genre_id}/tracks?limit={limit}")
    for position, t in enumerate(doc.get("data") or [], start=1):
        artist = t.get("artist") or {}
        album = t.get("album") or {}
        yield {
            "source": "deezer_chart",
            "fetched_at": now,
            "id": t["id"],
            "genre_id": genre_id,
            "position": position,
            "rank": t.get("rank"),
            "title": t.get("title"),
            "artist": artist.get("name"),
            "artist_id": artist.get("id"),
            "album": album.get("title"),
            "duration_s": t.get("duration"),
            "explicit_lyrics": t.get("explicit_lyrics"),
            "url": t.get("link"),
        }


def fetch_releases(genre_id: int = 0, limit: int = 25):
    """New releases for a genre. genre_id=0 is all genres. Can be a valid empty list."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(f"{BASE}/editorial/{genre_id}/releases?limit={limit}")
    for r in doc.get("data") or []:
        artist = r.get("artist") or {}
        yield {
            "source": "deezer_release",
            "fetched_at": now,
            "id": r["id"],
            "genre_id": genre_id,
            "title": r.get("title"),
            "artist": artist.get("name"),
            "release_date": r.get("release_date"),
            "record_type": r.get("record_type"),
            "url": r.get("link"),
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "chart"
    genre = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    fn = fetch_releases if mode == "releases" else fetch_chart
    for rec in fn(genre):
        print(json.dumps(rec, ensure_ascii=False))
