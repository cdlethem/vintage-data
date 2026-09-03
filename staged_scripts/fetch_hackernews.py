#!/usr/bin/env python3
"""Hacker News — story score trajectories (keyless, officially no rate limit).

The canonical "snapshot the same entity repeatedly" source, and the cleanest
teaching example of that pattern in this whole catalog.

Three modes:
  1. fetch_updates()   — /v0/updates.json returns the items and profiles that
     CHANGED since HN last flushed. This is the efficient per-run poll: you
     get a changed-set, not the whole world. Verified live.
  2. fetch_new_stories() — /v0/newstories.json, up to 500 newest ids.
  3. fetch_items(ids)  — hydrate ids into full records with CURRENT score and
     descendant (comment) count.

**Why this is a good dataset:** a story's score is a monotonically-ish
increasing counter that you can only observe by sampling. Poll `newstories`
every few minutes, hydrate, and you reconstruct the full rise-and-plateau
curve for every submission — including the ones that die at 2 points, which
are exactly the ones no public dataset bothers to keep. Title text plus
score trajectory plus time-of-day is a genuinely rich modeling problem.

Verified live 2026-09-02: /v0/updates.json returned ~85 changed item ids and
28 changed profiles.

Quirks:
  * Official docs state there is **no rate limit**, but be decent — one
    request per item is the API's design, so hydrating 500 stories is 500
    calls. Sleep between them and cache immutable fields (by, time, title).
  * Deleted/dead items return `null` or carry `deleted`/`dead` flags — the
    script skips nulls rather than crashing.
  * `time` is Unix seconds. `descendants` is the comment count and is absent
    on comments themselves.
  * `?print=pretty` is optional and only affects whitespace.

Stdlib only.
"""
import json
import sys
import time as _time
import urllib.request
from datetime import datetime, timezone

BASE = "https://hacker-news.firebaseio.com/v0"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def _get(path: str):
    req = urllib.request.Request(f"{BASE}/{path}",
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_updates():
    """Items and profiles that changed since the last flush. Cheap per-run poll."""
    data = _get("updates.json")
    return data.get("items", []), data.get("profiles", [])


def fetch_new_stories(limit: int = 100):
    return _get("newstories.json")[:limit]


def fetch_top_stories(limit: int = 100):
    return _get("topstories.json")[:limit]


def fetch_items(ids, sleep: float = 0.05):
    """Hydrate item ids into normalized records with current score."""
    now = datetime.now(timezone.utc).isoformat()
    for i in ids:
        item = _get(f"item/{i}.json")
        if not item:                      # deleted items come back as null
            continue
        yield {
            "source": "hackernews",
            "fetched_at": now,            # snapshot time = the time axis
            "id": item.get("id"),
            "type": item.get("type"),     # story / comment / job / poll
            "by": item.get("by"),
            "created": datetime.fromtimestamp(
                item["time"], tz=timezone.utc).isoformat()
                if item.get("time") else None,
            "title": item.get("title"),
            "url": item.get("url"),
            "score": item.get("score"),          # the value that moves
            "descendants": item.get("descendants"),  # comment count
            "dead": bool(item.get("dead")),
            "deleted": bool(item.get("deleted")),
            "text": item.get("text"),
        }
        if sleep:
            _time.sleep(sleep)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "new"
    if mode == "updates":
        items, profiles = fetch_updates()
        print(json.dumps({"changed_items": len(items),
                          "changed_profiles": len(profiles)}))
        ids = items[:20]
    elif mode == "top":
        ids = fetch_top_stories(20)
    else:
        ids = fetch_new_stories(20)
    for rec in fetch_items(ids):
        print(json.dumps(rec, ensure_ascii=False))
