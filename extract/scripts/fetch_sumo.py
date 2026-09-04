#!/usr/bin/env python3
"""sumo-api.com — professional sumo wrestlers, rankings and tournaments, keyless.

Lead #61: delightfully niche. Rikishi (wrestler) profiles and current rank, plus
basho (tournament) schedules and division champions (yusho). Six tournaments a year,
deep historical depth, and almost nobody has modelled it.

**Verified live 2026-09-03**:
  * `sumo-api.com/api/rikishis` — **200, 259,228 bytes, total: 601** wrestlers, each
    with `currentRank` and `updatedAt` — top record `updatedAt: 2026-08-30T21:30:59Z`,
    4 days before the probe.
  * `sumo-api.com/api/basho/202607` — **200, 1,674 bytes** — the July 2026 basho, real
    division champions (`yusho`) including Makuuchi winner Aonishiki.
  * `sumo-api.com/api/basho/202609` — **200, 98 bytes** — the *next* basho,
    `startDate: 2026-09-13`, 10 days after the probe, with an empty `yusho` list
    since it hasn't been contested yet — correctly distinguishing past results from
    a scheduled future tournament.
  * `sumo-api.com/api/ranks` — **400** — not a real endpoint path (see quirks).

Endpoint (base `https://sumo-api.com/api`):
    /rikishis?limit=N&skip=N        paginated wrestler roster, `total` in the envelope
    /basho/{YYYYMM}                   one tournament by year+month (basho are held in
                                      Jan/Mar/May/Jul/Sep/Nov — odd months only)
    /rikishi/{id}                      one wrestler's detail

Quirks that will cost someone an afternoon:
    * **`/api/ranks` (a plausible-looking endpoint name) returns 400**, not a rank
      list — confirmed live. Current rank lives on each rikishi record
      (`currentRank: "Maegashira 6 East"`) instead of a separate ranks endpoint.
    * **`basho` id is `YYYYMM`, and only odd months exist** — Jan, Mar, May, Jul,
      Sep, Nov (`202609`, not `202608` or `202610`). Requesting an even month or a
      basho that hasn't been announced yet returns a real 404, not a graceful empty
      response.
    * **A future/unstarted basho returns real metadata (`startDate`/`endDate`) with
      an empty `yusho` list**, not an error — confirmed live for September 2026,
      which starts 10 days after this session's probe. Empty `yusho` means "not
      contested yet," not "no data for this tournament."
    * `rikishis` is paginated with `total`/`limit`/`skip` in the envelope, not a bare
      array — the 601 wrestlers returned in one page suggests the default limit is
      generous, but check `total` vs `len(records)` before assuming you got everyone.
    * Names carry both `shikonaEn` (romanized) and `shikonaJp` (Japanese script,
      often with era/generation markers like `　新大`) — keep both.

Etiquette: keyless, no published rate limit found. Self-impose gentle polling; roster
and basho data change on a tournament cadence (every two months), not continuously.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://sumo-api.com/api"


def _get(path):
    req = urllib.request.Request(f"{BASE}/{path}", headers={"User-Agent": USER_AGENT,
                                                            "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_rikishis():
    """Current wrestler roster with rank."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get("rikishis")
    for r in doc.get("records") or []:
        yield {
            "source": "sumo_rikishi",
            "fetched_at": now,
            "id": r.get("id"),
            "shikona_en": r.get("shikonaEn"),
            "shikona_jp": r.get("shikonaJp"),
            "current_rank": r.get("currentRank"),
            "heya": r.get("heya"),
            "height_cm": r.get("height"),
            "weight_kg": r.get("weight"),
            "debut": r.get("debut"),
            "updated_at": r.get("updatedAt"),
        }


def fetch_basho(year_month: str):
    """One tournament, e.g. '202609'. Basho are only held in odd months. A future
    tournament returns real dates with an empty yusho list, not an error."""
    now = datetime.now(timezone.utc).isoformat()
    b = _get(f"basho/{year_month}")
    yield {
        "source": "sumo_basho",
        "fetched_at": now,
        "id": b.get("date"),
        "start_date": b.get("startDate"),
        "end_date": b.get("endDate"),
        "contested": bool(b.get("yusho")),
        "champions": [{"division": y.get("type"), "wrestler": y.get("shikonaEn")}
                     for y in b.get("yusho") or []],
    }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "rikishis"
    if mode == "basho":
        ym = sys.argv[2] if len(sys.argv) > 2 else "202609"
        for rec in fetch_basho(ym):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        for rec in fetch_rikishis():
            print(json.dumps(rec, ensure_ascii=False))
