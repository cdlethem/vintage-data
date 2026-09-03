#!/usr/bin/env python3
"""OpenLigaDB — German, community-run, fully keyless football/handball results.

Lead #62: Bundesliga and other German leagues with match-day results. A good
non-English-primary source where the field names themselves are German
(`resultName: "Halbzeit"` = half-time).

**Verified live 2026-09-03**. The catalog blurb's implied league shortcut
`bundesliga` returned an **empty table** (`getbltable/bundesliga/2025` -> `[]`) — the
real shortcut is `bl1`. `api.openligadb.de/getmatchdata/bl1/2026` returned **200,
257,398 bytes, 306 matches** for the 2026/2027 Bundesliga season, including a real
finished match (Bayern Munich 1-0 Stuttgart at half-time, 2026-08-28) five days before
the probe.

Endpoint (base `https://api.openligadb.de`):
    /getmatchdata/{leagueShortcut}/{season}    all matches for a league-season
    /getbltable/{leagueShortcut}/{season}       current standings table
    /getavailableleagues                         every league this API covers (829
                                                 entries observed live — far more
                                                 than just German football)

Quirks that will cost someone an afternoon:
    * **League shortcuts are not the obvious names.** `bundesliga` is empty;
      `bl1` is the real 1. Bundesliga shortcut (`bl2` for 2. Bundesliga). Verified
      live: the wrong guess returned `200` with an empty array, not an error — always
      cross-check a shortcut against `/getavailableleagues` rather than guessing from
      the league's common name.
    * **`/getlastchangedate/{league}` (no season) 404s** — this endpoint needs a
      season suffix or a different shape than the plain league name; use
      `matchIsFinished`/`lastUpdateDateTime` on individual matches instead for a
      recency signal.
    * Field and value names are **German** — `resultName: "Halbzeit"` (half-time),
      `resultDescription` in German prose. This is a genuine non-English source, not
      an English API with German team names.
    * **`matchResults`' final-score entry is `resultTypeKind: "After90Minutes"`,
      not `"FinalResult"`** — a plausible-looking guess at the kind name silently
      returns no match and every score comes back `null` even on a finished game.
      Verified live: a real finished match's results carried only `HalfTime` and
      `After90Minutes` entries (German names `Halbzeit`/`Endergebnis`), no
      `FinalResult` kind at all. The script's first version used the wrong kind name
      and a test now locks in the correct one.
    * `getavailableleagues` covers 829 leagues, not just German football — Handball,
      other countries' leagues, and lower German divisions are all in the same API.

Etiquette: keyless, community-run (not an official Bundesliga API) — no published
rate limit, self-impose gentle polling out of respect for a volunteer-run service.

Stdlib only.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
BASE = "https://api.openligadb.de"


def _get(path):
    req = urllib.request.Request(f"{BASE}/{path}", headers={"User-Agent": USER_AGENT,
                                                            "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_matches(league: str = "bl1", season: int | None = None):
    """Matches for one league-season. league is a shortcut like 'bl1' (1.
    Bundesliga) or 'bl2' -- NOT the obvious full name, see docstring."""
    season = season or datetime.now().year
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(f"getmatchdata/{league}/{season}")
    for m in doc or []:
        team1 = m.get("team1") or {}
        team2 = m.get("team2") or {}
        final = next((r for r in (m.get("matchResults") or [])
                     if r.get("resultTypeKind") == "After90Minutes"), None)
        yield {
            "source": "openligadb",
            "fetched_at": now,
            "id": m.get("matchID"),
            "league": m.get("leagueName"),
            "match_datetime": m.get("matchDateTimeUTC") or m.get("matchDateTime"),
            "is_finished": m.get("matchIsFinished"),
            "team1": team1.get("teamName"),
            "team2": team2.get("teamName"),
            "team1_score": (final or {}).get("pointsTeam1"),
            "team2_score": (final or {}).get("pointsTeam2"),
            "last_updated": m.get("lastUpdateDateTime"),
        }


if __name__ == "__main__":
    league = sys.argv[1] if len(sys.argv) > 1 else "bl1"
    season = int(sys.argv[2]) if len(sys.argv) > 2 else None
    for rec in fetch_matches(league, season):
        print(json.dumps(rec, ensure_ascii=False))
