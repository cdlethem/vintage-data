#!/usr/bin/env python3
"""speedrun.com — newly submitted and verified speedruns (keyless).

A delightful and almost completely unmined dataset. Humans compete to finish
video games faster, submit video proof, and volunteer moderators verify each
run. What you get per run: a duration in seconds, a game, a category, a
player, a platform, a submission timestamp, and a verification timestamp.

Why it makes a genuinely interesting time series:
  * **World records are a monotone decreasing staircase.** Per (game,
    category), record times only ever fall. Modeling the approach to a
    theoretical minimum is a real and pretty problem — records improve in
    bursts after route discoveries, then plateau for years.
  * **Submission-to-verification latency** is a human moderation queue you can
    measure. It spikes around game releases and community events.
  * Volume per game is an attention signal that responds to remasters,
    anniversaries, and marathon events like AGDQ.

Docs-verified 2026-09-02 against the official docs repo
(github.com/speedruncomorg/api, version1). The docs state most of the API is
read-only and anonymous — no key, no credentials. My sandboxed fetcher was
URL-restricted, so smoke-test once from your own machine.

Extraction notes:
  * Base https://www.speedrun.com/api/v1. Collections default to 20 items;
    `max` goes to 200. Page with `offset`.
  * `orderby=submitted&direction=desc` gives the newest-first feed. Also
    accepts `orderby=verify-date` — often the better watermark, since old runs
    can be verified long after submission.
  * `status=verified` filters out the pending/rejected queue. Drop it if you
    WANT the moderation queue (that's the latency dataset).
  * **`date` is when the run happened and is frequently null**; `submitted` is
    when it was uploaded and is also null for very old runs. Use whichever is
    present and record which one you used.
  * Times live under `times`: `primary_t` is the canonical duration in
    seconds (float) — use it. `primary` is an ISO-8601 duration string.
  * Use `embed=game,category,players,platform` to avoid N+1 lookups; the API
    returns ephemeral abbreviations, so prefer stable IDs.
  * Original content is CC-BY-NC 4.0. Non-commercial use only — relevant if
    this ever leaves your prototype.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://www.speedrun.com/api/v1"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(path: str, **params):
    url = f"{BASE}/{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                               "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp)


def fetch_recent_runs(max_items: int = 200, status: str | None = "verified",
                      orderby: str = "submitted", pages: int = 1):
    """Newest-first run feed. status=None includes the pending moderation queue."""
    offset = 0
    for _ in range(pages):
        params = {"orderby": orderby, "direction": "desc",
                  "max": min(max_items, 200), "offset": offset,
                  "embed": "game,category,players,platform"}
        if status:
            params["status"] = status
        data = _get("runs", **params)
        rows = data.get("data", [])
        if not rows:
            break
        fetched_at = datetime.now(timezone.utc).isoformat()
        for r in rows:
            yield normalize(r, fetched_at)
        offset += len(rows)


def _embedded_name(run: dict, key: str):
    """Embedded resources arrive as {'data': {...}} (or a list for players)."""
    blob = (run.get(key) or {})
    d = blob.get("data") if isinstance(blob, dict) else None
    if isinstance(d, dict):
        names = d.get("names")
        if isinstance(names, dict):
            return names.get("international")
        return d.get("name") or d.get("id")
    return None


def normalize(r: dict, fetched_at: str) -> dict:
    times = r.get("times") or {}
    status = r.get("status") or {}
    players_blob = (r.get("players") or {}).get("data")
    players = []
    if isinstance(players_blob, list):
        for p in players_blob:
            nm = p.get("names") or {}
            players.append(nm.get("international") or p.get("name") or p.get("id"))
    return {
        "source": "speedrun",
        "fetched_at": fetched_at,
        "id": r.get("id"),
        "game": _embedded_name(r, "game"),
        "category": _embedded_name(r, "category"),
        "platform": _embedded_name(r, "platform"),
        "players": players,
        "run_date": r.get("date"),            # when played; often null
        "submitted": r.get("submitted"),      # when uploaded; often null
        "verify_date": status.get("verify-date"),
        "status": status.get("status"),       # new / verified / rejected
        "examiner": status.get("examiner"),
        "duration_s": times.get("primary_t"),  # canonical seconds (float)
        "duration_iso": times.get("primary"),
        "emulated": r.get("system", {}).get("emulated"),
        "video": bool((r.get("videos") or {}).get("links")),
        "url": (r.get("weblink")),
    }


if __name__ == "__main__":
    status = None if (len(sys.argv) > 1 and sys.argv[1] == "queue") else "verified"
    for rec in fetch_recent_runs(max_items=20, status=status):
        print(json.dumps(rec, ensure_ascii=False))
