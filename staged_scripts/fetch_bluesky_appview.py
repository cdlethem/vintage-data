#!/usr/bin/env python3
"""Bluesky public AppView — fresh social text + engagement snapshots (keyless).

Two complementary extraction modes:
  1. fetch_author_feed(handle): recent posts by an account, WITH live
     like/repost/reply/quote/bookmark counts. Re-polling the same posts each
     run gives you engagement *trajectories* — a great time series that almost
     nobody builds because it requires repeated snapshots.
  2. fetch_search(query): keyword search sorted latest. NOTE: auth policy on
     the search endpoint has flip-flopped during 2025-2026 (public host
     sometimes 403s; api.bsky.app sometimes works unauthenticated; a free app
     password always works). Treat 401/403 as "add auth", not "broken".

Verified live 2026-09-02: GET
https://public.api.bsky.app/xrpc/app.bsky.feed.getAuthorFeed?actor=jay.bsky.team
returned current posts (Aug 2026) with full engagement counts, no auth.

For a true firehose (every public post in real time, zero auth), use the
Jetstream websocket instead — see IDEAS.md; that needs a ws client, so it is
not covered by this polling script.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

APPVIEW = "https://public.api.bsky.app/xrpc"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def _get(method: str, **params):
    url = f"{APPVIEW}/{method}?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _norm_post(p: dict, now: str) -> dict:
    rec = p.get("record") or {}
    return {
        "source": "bluesky",
        "fetched_at": now,          # snapshot time — key for engagement series
        "id": p.get("uri"),
        "author": (p.get("author") or {}).get("handle"),
        "created": rec.get("createdAt"),
        "text": rec.get("text"),
        "langs": rec.get("langs") or [],
        "likes": p.get("likeCount"),
        "reposts": p.get("repostCount"),
        "replies": p.get("replyCount"),
        "quotes": p.get("quoteCount"),
        "bookmarks": p.get("bookmarkCount"),
    }


def fetch_author_feed(handle: str, limit: int = 50):
    now = datetime.now(timezone.utc).isoformat()
    data = _get("app.bsky.feed.getAuthorFeed", actor=handle, limit=limit)
    for item in data.get("feed", []):
        yield _norm_post(item.get("post") or {}, now)


def fetch_search(query: str, limit: int = 50):
    """May require a free app password depending on current policy."""
    now = datetime.now(timezone.utc).isoformat()
    data = _get("app.bsky.feed.searchPosts", q=query, limit=limit, sort="latest")
    for p in data.get("posts", []):
        yield _norm_post(p, now)


if __name__ == "__main__":
    handle = sys.argv[1] if len(sys.argv) > 1 else "bsky.app"
    for rec in fetch_author_feed(handle):
        print(json.dumps(rec, ensure_ascii=False))
