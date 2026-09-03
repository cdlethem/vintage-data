#!/usr/bin/env python3
"""ListenBrainz — consented, published music listening histories.

Sector: open behavioural telemetry (media consumption). **Ethically the
cleanest source in this category**, and that is the reason to prefer it.

Every other per-user dataset in this catalog is public as a side effect of how
the platform works. ListenBrainz is different: users **deliberately install a
client to publish their own playback log** to an open, non-commercial database
run by the MetaBrainz Foundation. The whole point of the project is that
listening data should belong to listeners and be openly reusable. Data is
released under CC0.

**This is literally the media-telemetry event stream** that Spotify keeps
private: what was played, by whom, at what timestamp, from which client.

Analogues to product analytics:
  * `listens` = playback events, the raw interaction log
  * `listened_at` = session reconstruction (cluster with a gap threshold)
  * `recording_msid` / MBIDs = content identity, joinable to MusicBrainz
  * repeat plays = engagement depth; first-ever play = content discovery
  * long gaps = churn; the global firehose = platform-level DAU

**The `/1/listens` global firehose** (all users' recent listens) is the
aggregate entry point and needs no individual targeting at all — start there.

Verification status 2026-09-02: **KNOWN**. The API has been public and stable
for years and read endpoints are documented as not requiring a token, but I
did not fetch it this session (sandbox host restrictions). A free token
(listenbrainz.org, user settings) raises limits and is polite for volume work.

Endpoints (base https://api.listenbrainz.org):
  /1/user/{user}/listens?count=&max_ts=&min_ts=   per-user history
  /1/user/{user}/listen-count                     total listens
  /1/user/{user}/playing-now                      currently playing
  /1/listens                                      GLOBAL recent firehose
  /1/stats/sitewide/artists                       aggregate charts

Quirks:
  * Paginate **backwards in time** with `max_ts` (a Unix seconds cursor),
    not with an offset. `count` caps at 1000.
  * `listened_at` is Unix **seconds**.
  * MBIDs may be absent when a listen wasn't matched to MusicBrainz — treat
    `recording_mbid` as optional and fall back to the raw artist/track strings.
  * Rate limits are returned in `X-RateLimit-*` response headers; read them.

Even though users opted in, prefer aggregate analysis and pseudonymise
identifiers unless you have a specific, defensible reason not to.

Stdlib only.
"""
import hashlib
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.listenbrainz.org/1"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
TOKEN = None            # optional; raises rate limits
PSEUDONYMISE = True


def _get(path: str, **params):
    url = f"{BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = {"User-Agent": USER_AGENT}
    if TOKEN:
        headers["Authorization"] = f"Token {TOKEN}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp), dict(resp.headers)


def _uid(user: str) -> str:
    if not PSEUDONYMISE:
        return user
    return "lb_" + hashlib.sha256(user.encode()).hexdigest()[:16]


def _norm(listen: dict, now: str, user: str | None = None) -> dict:
    meta = listen.get("track_metadata") or {}
    info = meta.get("additional_info") or {}
    mbid = meta.get("mbid_mapping") or {}
    ts = listen.get("listened_at")
    who = listen.get("user_name") or user or ""
    return {
        "source": "listenbrainz",
        "fetched_at": now,
        "id": f"{who}:{ts}",
        "user": _uid(who) if who else None,
        "listened_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                       if ts else None,
        "artist": meta.get("artist_name"),
        "track": meta.get("track_name"),
        "release": meta.get("release_name"),
        "recording_mbid": (info.get("recording_mbid")
                           or mbid.get("recording_mbid")),   # often absent
        "artist_mbids": info.get("artist_mbids") or mbid.get("artist_mbids"),
        "client": info.get("submission_client"),   # which app reported it
        "duration_ms": info.get("duration_ms"),
    }


def fetch_user_listens(user: str, count: int = 100, max_ts: int | None = None,
                       pages: int = 1):
    """One user's history, paginating BACKWARDS via max_ts."""
    now = datetime.now(timezone.utc).isoformat()
    cursor = max_ts
    for _ in range(pages):
        params = {"count": min(count, 1000)}
        if cursor:
            params["max_ts"] = cursor
        payload, _hdrs = _get(f"user/{user}/listens", **params)
        listens = (payload.get("payload") or {}).get("listens", [])
        if not listens:
            break
        for l in listens:
            yield _norm(l, now, user)
        cursor = min(l.get("listened_at") for l in listens if l.get("listened_at"))


def fetch_global_firehose(count: int = 100):
    """All users' recent listens. Aggregate; no individual targeting."""
    now = datetime.now(timezone.utc).isoformat()
    payload, _ = _get("listens", count=min(count, 1000))
    for l in (payload.get("payload") or {}).get("listens", []):
        yield _norm(l, now)


def fetch_listen_count(user: str):
    payload, _ = _get(f"user/{user}/listen-count")
    return (payload.get("payload") or {}).get("count")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gen = fetch_user_listens(sys.argv[1], count=50)
    else:
        gen = fetch_global_firehose(count=50)
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
