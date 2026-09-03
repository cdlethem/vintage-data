#!/usr/bin/env python3
"""Lichess API — keyless, generous, and streaming-friendly.

Lead #34. Live top-rated games per variant, and the full tournament lifecycle
(scheduled -> running -> finished), all keyless. Millions of fully structured moves
per day flow through this platform; this script covers the two lowest-friction
snapshot endpoints rather than the NDJSON game-stream API, which is a natural next step
if you want per-move granularity instead of per-tournament/per-channel snapshots.

**Verified live 2026-09-03**:
  * `lichess.org/api/tv/channels` — **200, 2,175 bytes** — the current top game in
    every variant (bullet, blitz, chess960, antichess, atomic, ...), e.g. `bullet`
    channel led by a 2927-rated GM. Re-polling this is a clean snapshot pattern: the
    leading game changes as play continues and finishes.
  * `lichess.org/api/tournament` — **200, 63,089 bytes** — `created: 101`,
    `started: 14`, `finished: 20` tournaments, each carrying start/finish epoch-ms,
    time control, variant, and current player count.

Endpoints (base `https://lichess.org/api`):
    /tv/channels        current best game per variant — a dict keyed by variant name
    /tournament          arena tournaments split into created/started/finished arrays
    /tournament/{id}      one tournament's detail (standings, results)
    /games/user/{name}    that user's games as streamed NDJSON (not covered here)

Quirks:
    * **`tv/channels` is a dict keyed by variant name, not a list** — `bullet`,
      `blitz`, `chess960`, `antichess`, `atomic`, `horde`, `racingKings`, `crazyhouse`,
      `ultraBullet`, `bot`, `computer`, `best` (best game across all variants). Iterate
      `.items()`, don't expect an array.
    * **`tournament` groups by lifecycle status into three top-level arrays**
      (`created`/`started`/`finished`) rather than one flat list with a status field —
      verified live with 101/14/20 real entries in each. Tag which bucket a row came
      from; `nbPlayers` only means "current" for `started`, and is 0/near-0 for
      not-yet-begun `created` entries.
    * `startsAt`/`finishesAt` are **epoch milliseconds**, not seconds or ISO strings —
      divide by 1000 before feeding to most datetime constructors.
    * `secondsToStart` is a snapshot-relative countdown, stale the moment you cache
      the response — recompute from `startsAt` and `fetched_at` if you need "time until
      start" later, don't trust the field itself after the fact.

Etiquette: keyless. Lichess publishes a general API rate-limit guideline of roughly 1
request/second sustained per client with brief bursts tolerated; this script's two
endpoints are cheap and infrequent, but self-impose that pace if you add more calls.

Stdlib only.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
BASE = "https://lichess.org/api"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_tv():
    """Current leading game per variant. Dict keyed by variant, not a list."""
    now = datetime.now(timezone.utc).isoformat()
    for variant, g in _get(f"{BASE}/tv/channels").items():
        user = g.get("user") or {}
        yield {
            "source": "lichess_tv",
            "fetched_at": now,
            "id": f"{variant}:{g.get('gameId')}",
            "variant": variant,
            "game_id": g.get("gameId"),
            "player": user.get("name"),
            "title": user.get("title"),
            "rating": g.get("rating"),
            "color": g.get("color"),
        }


def fetch_tournaments():
    """Arena tournaments, tagged with which lifecycle bucket they came from."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(f"{BASE}/tournament")
    for status in ("created", "started", "finished"):
        for t in doc.get(status) or []:
            perf = t.get("perf") or {}
            variant = t.get("variant") or {}
            yield {
                "source": "lichess_tournament",
                "fetched_at": now,
                "id": t["id"],
                "status": status,
                "name": t.get("fullName"),
                "system": t.get("system"),
                "variant": variant.get("key"),
                "perf": perf.get("key"),
                "rated": t.get("rated"),
                "clock_limit_s": (t.get("clock") or {}).get("limit"),
                "clock_increment_s": (t.get("clock") or {}).get("increment"),
                "starts_at_ms": t.get("startsAt"),
                "finishes_at_ms": t.get("finishesAt"),
                "nb_players": t.get("nbPlayers"),
            }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "tv"
    fn = fetch_tournaments if mode == "tournament" else fetch_tv
    for rec in fn():
        print(json.dumps(rec, ensure_ascii=False))
