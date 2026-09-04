#!/usr/bin/env python3
"""NHL — official league API, keyless, same philosophy as MLB Stats API (#22).

Lead #59: `api-web.nhle.com` publishes schedule, scores and (per NHL's own
documentation, not independently verified this session — see below) play-by-play and
shift charts. Same shape as the MLB source already in this catalog: the league gives
away the events.

**Verified live 2026-09-03**:
  * `api-web.nhle.com/v1/schedule/now` — **200, 76,591 bytes** — real upcoming games,
    including betting odds per game, TV broadcast info, venues.
  * `api-web.nhle.com/v1/score/now` — **200, 15,427 bytes** — `currentDate:
    2026-09-29`, `numberOfGames: 5` that day.
  * **No game was in progress this session** (probed during the off-season/
    pre-season gap) — the documented live play-by-play endpoint
    (`/v1/gamecenter/{gameId}/play-by-play`) was **not exercised against a real
    in-progress game**, so its shape is not independently confirmed here. The
    schedule/score endpoints above are the confirmed-live part of this verdict.

Endpoints (base `https://api-web.nhle.com/v1`):
    /schedule/now                       upcoming games, this week and next
    /score/now                           scoreboard for the nearest game day
    /gamecenter/{gameId}/play-by-play    per-event play-by-play (documented, not
                                         independently verified this session)
    /gamecenter/{gameId}/boxscore         box score for one game

Quirks that will cost someone an afternoon:
    * **`/score/now`'s `currentDate` is not today's calendar date — it's the
      nearest date that has games scheduled.** Verified live: probed on 2026-09-03
      (off-season), and the response's `currentDate` was `2026-09-29`, the next date
      with games, not the actual probe date. Don't assume `currentDate` means "today".
    * Team names are split across `commonName`/`placeName`/`abbrev`, each itself an
      object with a `default` key (and sometimes `fr` for French) rather than a
      plain string — `game["awayTeam"]["commonName"]["default"]` for "Panthers", not
      a flat field.
    * `gameState` values (`"FUT"` for future, and others for live/final per NHL's
      docs) tell you where a game is in its lifecycle — filter on this rather than
      assuming every entry in `schedule/now` is upcoming.
    * Odds (`awayTeam.odds`/`homeTeam.odds`) are embedded per-team, from multiple
      providers, each with its own `providerId` — a betting-market signal riding
      along with the schedule data, worth keeping distinct from game state.

Etiquette: keyless, no published rate limit found. Self-impose a reasonable interval
(schedule/score data doesn't need sub-minute polling except during live games).

Stdlib only.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://api-web.nhle.com/v1"


def _get(path):
    req = urllib.request.Request(f"{BASE}/{path}", headers={"User-Agent": USER_AGENT,
                                                            "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_schedule():
    """Upcoming games, this week and next."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get("schedule/now")
    for week in doc.get("gameWeek") or []:
        for g in week.get("games") or []:
            away = g.get("awayTeam") or {}
            home = g.get("homeTeam") or {}
            yield {
                "source": "nhl_schedule",
                "fetched_at": now,
                "id": g.get("id"),
                "date": week.get("date"),
                "start_time_utc": g.get("startTimeUTC"),
                "game_state": g.get("gameState"),
                "venue": (g.get("venue") or {}).get("default"),
                "away_team": (away.get("commonName") or {}).get("default"),
                "away_team_abbrev": away.get("abbrev"),
                "home_team": (home.get("commonName") or {}).get("default"),
                "home_team_abbrev": home.get("abbrev"),
            }


def fetch_scoreboard():
    """Scoreboard for the nearest game day (see docstring: not necessarily today)."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get("score/now")
    current_date = doc.get("currentDate")
    for week in doc.get("gameWeek") or []:
        yield {
            "source": "nhl_score",
            "fetched_at": now,
            "id": f"{week.get('date')}",
            "date": week.get("date"),
            "current_date": current_date,
            "number_of_games": week.get("numberOfGames"),
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "schedule"
    fn = fetch_scoreboard if mode == "score" else fetch_schedule
    for rec in fn():
        print(json.dumps(rec, ensure_ascii=False))
