#!/usr/bin/env python3
"""MLB Stats API — schedule, live games, and PITCH-BY-PITCH data (keyless).

Sector: sports. The surprise of this round: the leagues themselves publish
far richer free data than any betting site will give you. Betting outfits
guard *odds*; they don't guard play-by-play, because the league gives it away.

**Why this is the best free sports source I found:** the live game feed goes
down to the individual pitch. Every pitch carries type, speed, spin,
release point, and the resulting count. That's a genuinely dense event stream
during a game — thousands of rows per matchup — and it is free, keyless, and
official.

Time-series angles:
  * **In-game state** — poll `/feed/live` during a game and you get win
    probability and leverage evolving pitch by pitch.
  * **`timecode` replay** — the live feed accepts a timecode parameter, so you
    can re-request a past moment in a game. That's an unusual and very handy
    property: you can rebuild a game's state history without having polled it
    at the time.
  * Schedule + probable pitchers changes daily in the run-up to each game.

Verified live 2026-09-02: GET https://statsapi.mlb.com/api/v1/teams?sportId=1
returned all 30 clubs with 2026 season and venue data, no key, no auth.

Endpoints:
  /api/v1/schedule?sportId=1&date=YYYY-MM-DD   (hydrate=team,linescore,probablePitcher)
  /api/v1/game/{gamePk}/linescore              inning-by-inning + game state
  /api/v1/game/{gamePk}/boxscore               team + player box
  /api/v1.1/game/{gamePk}/feed/live            THE FIREHOSE (note: v1.1, not v1)
  /api/v1/standings?leagueId=103,104
  /api/v1/teams?sportId=1  |  /api/v1/people/{id}

Quirks:
  * The live feed is **v1.1**, everything else is **v1**. Easy to get wrong.
  * `sportId=1` is MLB; other ids cover minors, college, and international
    leagues — the same API gives you Japanese and Dominican winter ball.
  * The live feed payload is very large. Pull `liveData.plays.allPlays` and
    discard the rest unless you need it.
  * **Licensing matters here.** Every response carries an MLB Advanced Media
    copyright notice pointing at their terms. This is fine for a personal
    prototype; do not build a commercial product on it without reading them.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

BASE = "https://statsapi.mlb.com/api"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(path: str, **params):
    url = f"{BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def fetch_schedule(day: str | None = None, sport_id: int = 1):
    """Games for a date, with live status and probable pitchers."""
    day = day or date.today().isoformat()
    data = _get("v1/schedule", sportId=sport_id, date=day,
                hydrate="team,linescore,probablePitcher")
    now = datetime.now(timezone.utc).isoformat()
    for d in data.get("dates", []):
        for g in d.get("games", []):
            teams = g.get("teams") or {}
            home, away = teams.get("home") or {}, teams.get("away") or {}
            status = g.get("status") or {}
            yield {
                "source": "mlb_schedule",
                "fetched_at": now,          # snapshot; status/score evolve
                "id": g.get("gamePk"),
                "game_date": g.get("gameDate"),
                "status": status.get("detailedState"),
                "abstract_status": status.get("abstractGameState"),
                "home_team": (home.get("team") or {}).get("name"),
                "away_team": (away.get("team") or {}).get("name"),
                "home_score": home.get("score"),
                "away_score": away.get("score"),
                "venue": (g.get("venue") or {}).get("name"),
                "home_probable": ((home.get("probablePitcher") or {}).get("fullName")),
                "away_probable": ((away.get("probablePitcher") or {}).get("fullName")),
            }


def fetch_plays(game_pk: int, timecode: str | None = None):
    """Pitch-by-pitch events for one game. THIS is the dense stream.

    `timecode` (YYYYMMDD_HHMMSS) replays the feed as of a past moment.
    """
    params = {"timecode": timecode} if timecode else {}
    data = _get(f"v1.1/game/{game_pk}/feed/live", **params)   # note v1.1
    now = datetime.now(timezone.utc).isoformat()
    live = data.get("liveData") or {}
    for play in (live.get("plays") or {}).get("allPlays", []):
        res = play.get("result") or {}
        about = play.get("about") or {}
        matchup = play.get("matchup") or {}
        count = play.get("count") or {}
        for ev in play.get("playEvents", []):
            details = ev.get("details") or {}
            pitch = ev.get("pitchData") or {}
            coords = pitch.get("coordinates") or {}
            if not ev.get("isPitch"):
                continue
            yield {
                "source": "mlb_pitch",
                "fetched_at": now,
                "id": f"{game_pk}:{about.get('atBatIndex')}:{ev.get('index')}",
                "game_pk": game_pk,
                "ts": ev.get("startTime"),
                "inning": about.get("inning"),
                "half": about.get("halfInning"),
                "batter": (matchup.get("batter") or {}).get("fullName"),
                "pitcher": (matchup.get("pitcher") or {}).get("fullName"),
                "pitch_type": (details.get("type") or {}).get("description"),
                "call": (details.get("call") or {}).get("description"),
                "start_speed": pitch.get("startSpeed"),
                "end_speed": pitch.get("endSpeed"),
                "spin_rate": (pitch.get("breaks") or {}).get("spinRate"),
                "zone": pitch.get("zone"),
                "px": coords.get("pX"),
                "pz": coords.get("pZ"),
                "balls": count.get("balls"),
                "strikes": count.get("strikes"),
                "outs": count.get("outs"),
                "at_bat_result": res.get("event"),
            }


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        for rec in fetch_plays(int(sys.argv[1])):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        for rec in fetch_schedule():
            print(json.dumps(rec, ensure_ascii=False))
