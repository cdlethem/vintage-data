#!/usr/bin/env python3
"""Stack Exchange API — new questions across all sites, keyless with a quota.

Lead #77 (and #21 in the earlier sweep, same source): `/questions` is a live event
log — new questions asked, timestamped, with tags. Titles and bodies are a large
technical text corpus with built-in topic tags. The per-user angle (`/users/{id}/
timeline`, `/users/{id}/reputation-history`) is a natural follow-up for the
behavioural-telemetry section of this catalog.

**Verified live 2026-09-03** — `api.stackexchange.com/2.3/questions?order=desc&
sort=creation&site=stackoverflow&pagesize=5` returned **200, 3,706 bytes, 5
questions**, top question "Duckdb database connection", `creation_date` **382
seconds (6.4 minutes) before the probe**. `quota_remaining: 299` in the response —
confirming the anonymous daily quota is real and being tracked per-IP.

Endpoint: `https://api.stackexchange.com/2.3/questions?order=desc&sort=creation&
site={site}&pagesize=N&page=N`
    `site=` is required — `stackoverflow`, `serverfault`, `superuser`,
    `math.stackexchange`, etc. (hundreds of sites on the same API, another protocol-
    client-shaped source, though this script covers questions specifically).
    `filter=` can request additional fields (e.g. question body) at the cost of
    quota; the default filter is fields-lean.

Quirks that will cost someone an afternoon:
    * **The anonymous daily quota is small and shared per-IP, not per-script** —
      `quota_remaining` in every response tells you exactly how much is left (299
      after this session's first call, out of a default 300/day). Track it and back off
      rather than discovering the quota exhausted mid-run; a burst of automated
      pulls against one site can eat the whole day's quota in a few requests if you
      request pages naively.
    * `creation_date`/`last_activity_date` are **Unix epoch seconds**, not ISO
      strings or milliseconds.
    * `owner` can be an unregistered/deleted user — `user_type` is `"unregistered"`
      in that case, and `reputation`/`link` may be absent; don't assume every
      question has a fully-populated owner.
    * `has_more: true` means there are more pages beyond what you requested — proper
      pagination needs `page=`, not just a bigger `pagesize` (which is capped, 100 max).
    * Different Stack Exchange sites can have very different question volumes —
      `stackoverflow` produces a new question every few minutes; a niche site might
      go hours between questions, which is a real signal about that community, not
      a broken query.

Etiquette: keyless, documented quota of roughly 300 requests/day per IP anonymously
(10,000/day with a free registered app key, which this script doesn't use to stay
keyless). Self-impose accordingly and watch `quota_remaining`.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://api.stackexchange.com/2.3/questions"


def _get(site, pagesize, page):
    params = {"order": "desc", "sort": "creation", "site": site,
             "pagesize": pagesize, "page": page}
    req = urllib.request.Request(f"{BASE}?{urllib.parse.urlencode(params)}",
                                 headers={"User-Agent": USER_AGENT,
                                         "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_recent(site: str = "stackoverflow", pagesize: int = 25, page: int = 1):
    """Newest questions on one Stack Exchange site, newest first. Watch
    quota_remaining in each response -- the anonymous quota is small and shared."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(site, pagesize, page)
    quota_remaining = doc.get("quota_remaining")
    for q in doc.get("items") or []:
        owner = q.get("owner") or {}
        yield {
            "source": f"stackexchange_{site}",
            "fetched_at": now,
            "id": q.get("question_id"),
            "title": q.get("title"),
            "tags": q.get("tags") or [],
            "creation_date": q.get("creation_date"),
            "owner_display_name": owner.get("display_name"),
            "owner_user_type": owner.get("user_type"),
            "score": q.get("score"),
            "view_count": q.get("view_count"),
            "answer_count": q.get("answer_count"),
            "is_answered": q.get("is_answered"),
            "link": q.get("link"),
            "quota_remaining": quota_remaining,
        }


if __name__ == "__main__":
    site = sys.argv[1] if len(sys.argv) > 1 else "stackoverflow"
    pagesize = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    for rec in fetch_recent(site, pagesize=pagesize):
        print(json.dumps(rec, ensure_ascii=False))
