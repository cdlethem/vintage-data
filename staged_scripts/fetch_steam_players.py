#!/usr/bin/env python3
"""Steam — keyless per-appid current player counts.

Lead #25: poll a basket of games for clean daily/weekly rhythms plus release-day
spikes. No key needed for this one endpoint, unlike most of the Steam Web API.

**Verified live 2026-09-03** — `api.steampowered.com/ISteamUserStats/
GetNumberOfCurrentPlayers/v1/?appid=730` (Counter-Strike 2) returned **200,
{"response": {"player_count": 1171851, "result": 1}}** — over a million concurrent
players, a genuinely large real-time number. A bogus appid
(`?appid=999999999`) returned **404** with body `{"response": {"result": 42}}`.

Endpoint: `https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid={id}`
    No key required for this specific call (many other Steam Web API endpoints do
    need one — this is a documented exception).

Quirks that will cost someone an afternoon:
    * **Check `response.result`, not just the HTTP status.** A bad appid still comes
      back as valid JSON, `response.result: 42` (failure) instead of `1` (success),
      wrapped in a 404 — verified live. If you only check for a JSON parse success
      you'll silently record a player count of `None`/missing for a typo'd appid
      rather than catching the error.
    * `player_count` is only ever provided for the single `appid` you asked about —
      there is no bulk/multi-app endpoint here. Polling "a basket of games" means one
      request per game, sequentially or with your own concurrency.
    * Some appids (DLC, tools, delisted games) return `result: 1` with a
      `player_count` of exactly `0` rather than erroring — a real zero, not a sign
      the request failed.

Etiquette: keyless for this endpoint, no published rate limit, but it's Valve's
production API serving real players — poll a handful of games at a reasonable
interval (every few minutes is plenty for a daily-rhythm chart), not in a tight loop.

Stdlib only.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
BASE = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

# A few well-known appids to make this runnable out of the box.
APPS = {
    "cs2": 730,
    "dota2": 570,
    "pubg": 578080,
}


def _get(appid):
    req = urllib.request.Request(f"{BASE}?appid={appid}",
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=15) as resp:
        status = resp.status
        return status, json.load(resp)


def fetch_player_count(appid: int):
    """One game's current concurrent player count. Returns None if the appid or
    request failed -- check response.result, not just HTTP status."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        _, doc = _get(appid)
    except Exception as e:
        import urllib.error
        if isinstance(e, urllib.error.HTTPError):
            try:
                doc = json.loads(e.read())
            except Exception:
                doc = {"response": {"result": 0}}
        else:
            raise
    r = doc.get("response") or {}
    ok = r.get("result") == 1
    yield {
        "source": "steam_players",
        "fetched_at": now,
        "id": appid,
        "appid": appid,
        "ok": ok,
        "player_count": r.get("player_count") if ok else None,
    }


def fetch_basket(appids):
    for appid in appids:
        yield from fetch_player_count(appid)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        ids = [int(x) for x in sys.argv[1:]]
    else:
        ids = list(APPS.values())
    for rec in fetch_basket(ids):
        print(json.dumps(rec, ensure_ascii=False))
