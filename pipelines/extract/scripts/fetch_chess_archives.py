#!/usr/bin/env python3
"""Chess.com public API — per-user game archives with move-level timing.

Sector: open behavioural telemetry (session-level).

If Codeforces is the "attempt log", chess is the **session log**. Every game a
player has ever played is public, organised into monthly archives, with:
  * exact start/end timestamps  → session length, time-of-day habits
  * rating before each game     → skill trajectory at game granularity
  * time control chosen         → which "feature" the user engages with
  * result and termination      → outcome, plus *how* it ended (resign,
    timeout, abandonment — the last one is a rage-quit signal)
  * PGN with clock annotations  → **per-move time spent**, i.e. genuine
    interaction-level timing inside a session

That last one is the remarkable part. `[%clk 0:02:31.4]` tags in the PGN give
you how long a person deliberated on each individual decision. Very few public
datasets contain per-decision latency for millions of real users.

**Telemetry analogues:** monthly archive list = retention by month (a gap is
churn, a later archive is resurrection); games per day = DAU/engagement;
rating delta = progression; termination reason = failure taxonomy; time
control mix = feature adoption.

Verification status 2026-09-02: **KNOWN** — the `api.chess.com/pub/` interface
is long-stable and keyless, but I did not fetch it this session (sandbox host
restrictions). Confirm field names on first run.

Endpoints:
  /pub/player/{u}                      profile
  /pub/player/{u}/stats                current ratings per time control
  /pub/player/{u}/games/archives       LIST of monthly archive URLs  <- start here
  /pub/player/{u}/games/{YYYY}/{MM}    every game that month, with PGN
  /pub/leaderboards                    global boards (aggregate, no target)

Etiquette and terms:
  * Chess.com asks for a descriptive User-Agent with contact info and applies
    per-IP throttling. Serial requests are tolerated; parallel bursts get you
    blocked. Fetch archives one at a time.
  * Archives for **past months are immutable** — fetch once, cache forever,
    and only re-poll the current month. Re-crawling history is the main way
    people get rate-limited here.
  * Lichess (lichess.org/api) is the fully open-source equivalent and is
    friendlier for bulk work — it streams NDJSON and explicitly supports
    exporting a user's whole game history. Prefer it if you need volume.

**See the ethics note in the catalog.** Pseudonymise by default.

Stdlib only.
"""
import hashlib
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.chess.com/pub"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
POLITE_DELAY = 0.5
PSEUDONYMISE = True

CLK = re.compile(r"\[%clk\s+(\d+):(\d{2}):(\d{2}(?:\.\d+)?)\]")


def _get(path: str):
    time.sleep(POLITE_DELAY)
    req = urllib.request.Request(f"{BASE}/{path}",
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def _uid(username: str) -> str:
    if not PSEUDONYMISE:
        return username
    return "cc_" + hashlib.sha256(username.lower().encode()).hexdigest()[:16]


def list_archives(username: str):
    """Monthly archive URLs. The gaps in this list ARE the churn signal."""
    return _get(f"player/{username}/games/archives").get("archives", [])


def parse_move_times(pgn: str | None):
    """Extract per-move remaining clock (seconds) from PGN [%clk] tags, then
    difference them into time-spent-per-move. Returns [] if unannotated."""
    if not pgn:
        return []
    remaining = []
    for h, m, s in CLK.findall(pgn):
        remaining.append(int(h) * 3600 + int(m) * 60 + float(s))
    # even indices = one player, odd = the other; diff within each side
    spent = []
    for side in (0, 1):
        seq = remaining[side::2]
        for i in range(1, len(seq)):
            spent.append(round(seq[i - 1] - seq[i], 1))
    return spent


def fetch_month(username: str, year: int, month: int, include_pgn: bool = False):
    """All games for one user-month, normalized."""
    data = _get(f"player/{username}/games/{year}/{month:02d}")
    now = datetime.now(timezone.utc).isoformat()
    me = username.lower()
    for g in data.get("games", []):
        white, black = g.get("white") or {}, g.get("black") or {}
        is_white = (white.get("username", "").lower() == me)
        mine, theirs = (white, black) if is_white else (black, white)
        start, end = g.get("start_time"), g.get("end_time")
        times = parse_move_times(g.get("pgn"))
        yield {
            "source": "chesscom_game",
            "fetched_at": now,
            "id": g.get("uuid"),
            "user": _uid(username),
            "color": "white" if is_white else "black",
            "start": datetime.fromtimestamp(start, tz=timezone.utc).isoformat()
                     if start else None,
            "end": datetime.fromtimestamp(end, tz=timezone.utc).isoformat()
                   if end else None,
            "duration_s": (end - start) if (start and end) else None,
            "time_class": g.get("time_class"),     # bullet/blitz/rapid/daily
            "time_control": g.get("time_control"),
            "rated": g.get("rated"),
            "my_rating": mine.get("rating"),       # skill at this moment
            "opp_rating": theirs.get("rating"),
            "result": mine.get("result"),          # win / resigned / timeout...
            "opp_result": theirs.get("result"),
            "rules": g.get("rules"),
            "eco": g.get("eco"),
            "n_moves_timed": len(times),
            "median_move_time_s": (sorted(times)[len(times) // 2]
                                   if times else None),
            "max_move_time_s": max(times) if times else None,
            "pgn": g.get("pgn") if include_pgn else None,
        }


def fetch_history(username: str, max_months: int | None = None):
    """Walk every archive. Past months are immutable — cache them."""
    archives = list_archives(username)
    if max_months:
        archives = archives[-max_months:]
    for url in archives:
        y, m = url.rstrip("/").split("/")[-2:]
        yield from fetch_month(username, int(y), int(m))


if __name__ == "__main__":
    user = sys.argv[1] if len(sys.argv) > 1 else "erik"
    for rec in fetch_history(user, max_months=1):
        print(json.dumps(rec, ensure_ascii=False))
