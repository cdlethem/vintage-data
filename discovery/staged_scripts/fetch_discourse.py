#!/usr/bin/env python3
"""Discourse forums — a protocol client covering hundreds of technical and hobbyist
communities, no key.

Lead #57 (and #86 for per-user activity, same protocol). Every Discourse instance
exposes `/latest.json` with no authentication — a distributed, opt-in alternative to
scraping Reddit, spanning everything from official product forums to hobbyist
communities, all speaking the identical API.

**Verified live 2026-09-03** against two unrelated instances, confirming the protocol
claim rather than assuming it:
  * `meta.discourse.org/latest.json?order=created&limit=10` — **200, 73,749 bytes** —
    top topic "Integrating Discourse Voice with LiveKit", `created_at:
    2026-09-03T15:19:01.323Z`, essentially live.
  * `community.openai.com/latest.json?order=created&limit=5` — **200, 51,246 bytes** —
    same response shape on a completely different, unrelated forum.

Endpoint (base is the forum's own domain): `https://{forum}/latest.json?order=created&limit=N&page=N`
    Also: `/c/{category-slug}/{category_id}.json` for one category,
    `/t/{topic_id}.json` for one topic's full post history,
    `/u/{username}/activity.json` for one user's public activity (lead #86 — same
    protocol, different endpoint).

Quirks that will cost someone an afternoon:
    * **`posters` references users by id; the user objects aren't embedded in the
      topic.** Each topic's `posters` list carries `user_id` only — join against the
      top-level `users` array in the *same response* (`{"users": [...], "topic_list":
      {"topics": [...]}}`) to get a username/avatar. A per-topic fetch alone won't
      have it.
    * `tags` is a list of **objects** (`{id, name, slug}`), not bare strings —
      confirmed live on a real topic tagged `voice`.
    * `created_at` and `bumped_at` are separate fields and can differ by seconds even
      on a genuinely brand-new topic (a reply bumps `bumped_at` without changing
      `created_at`) — use `created_at` for "when was this topic opened", `bumped_at`
      for "when did it last get activity".
    * `excerpt` can contain literal `…` and other escaped entities from the post
      body — decode, don't display raw.
    * Not every Discourse instance allows anonymous `/latest.json` — some are
      login-walled by the operator's choice. A 403/redirect-to-login means that
      specific forum opted out, not that the protocol is broken.

Etiquette: keyless, no universal published rate limit (each instance can set its own
Cloudflare-level throttling). Poll gently — this is community infrastructure, often
volunteer-run even for large forums; a few requests per minute per forum is plenty.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(forum, path, **params):
    url = f"https://{forum}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_latest(forum: str = "meta.discourse.org", limit: int = 25):
    """Newest topics on one Discourse forum. `forum` is any Discourse instance's
    bare domain -- this works against hundreds of them unchanged."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(forum, "/latest.json", order="created", limit=limit)
    users_by_id = {u["id"]: u for u in doc.get("users") or []}
    for t in doc.get("topic_list", {}).get("topics", []):
        posters = t.get("posters") or []
        op = next((p for p in posters if "Original Poster" in (p.get("description") or "")),
                  posters[0] if posters else {})
        op_user = users_by_id.get(op.get("user_id"), {})
        yield {
            "source": f"discourse_{forum}",
            "fetched_at": now,
            "id": t.get("id"),
            "title": t.get("title"),
            "slug": t.get("slug"),
            "created_at": t.get("created_at"),
            "bumped_at": t.get("bumped_at"),
            "author": op_user.get("username"),
            "tags": [tag.get("name") for tag in (t.get("tags") or [])
                    if isinstance(tag, dict)],
            "reply_count": t.get("reply_count"),
            "views": t.get("views"),
            "url": f"https://{forum}/t/{t.get('slug')}/{t.get('id')}",
        }


if __name__ == "__main__":
    forum = sys.argv[1] if len(sys.argv) > 1 else "meta.discourse.org"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    for rec in fetch_latest(forum, limit=limit):
        print(json.dumps(rec, ensure_ascii=False))
