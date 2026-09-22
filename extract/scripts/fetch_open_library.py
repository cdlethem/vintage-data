#!/usr/bin/env python3
"""Open Library recent changes — keyless feed of edits to the world's book catalog.

Lead #30. Same flavor as Wikimedia recent changes (already in this catalog) but a much
smaller, more tractable universe: every edit to Open Library's open book database —
new books, cover uploads, author edits, and new accounts — as they happen.

**Verified live 2026-09-03** — `openlibrary.org/recentchanges.json?limit=10` returned
**200, 3,085 bytes, 10 events**, top event `timestamp: 2026-09-03T14:58:06Z` — the
same second as the probe. Four distinct event kinds observed in one page: `add-book`,
`edit-book`, `add-cover`, `new-account`.

Endpoint: `https://openlibrary.org/recentchanges.json?limit=N&offset=N`
    Optional filters: `/recentchanges/{kind}.json` for one kind only (e.g.
    `add-book.json`), or `/recentchanges/{YYYY}/{MM}/{DD}.json` for one day.

Quirks that will cost someone an afternoon:
    * **One event can touch multiple catalog entities at once.** A single `add-book`
      event's `changes` list carried three entries in one real event: a new author
      record, a new work record, and a new edition/book record, each with its own
      `key` and `revision`. Don't assume one event = one entity; count `len(changes)`
      if you want a "how many things changed" metric.
    * **`kind` values span more than books**: `new-account` events appear in the same
      feed, with `author.key` pointing at the newly created account itself (its first
      edit is creating its own permission records) — filter on `kind` if you only want
      bibliographic activity, not account creation.
    * A large share of real edit volume is bot activity (`author.key` like
      `/people/horncBot` observed repeatedly) — legitimate, credited imports, not
      spam, but worth separating from human edits if that distinction matters to you.
    * `ip` is consistently `null` in every event sampled — Open Library does not
      expose editor IPs through this feed, unlike some MediaWiki recent-changes feeds.
    * `data` is present but empty (`{}`) on every event type sampled here; don't build
      logic that depends on it being populated without checking a wider sample.

Etiquette: keyless, no published rate limit, run by the Internet Archive. Poll at a
cadence matched to real edit volume — this session saw ~10 events across a handful of
seconds, so every few minutes is reasonable for most use; don't poll sub-second.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://openlibrary.org/recentchanges.json"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _normalize_author(author):
    """Return the feed's author key/string, rejecting malformed values."""
    if author is None:
        return None
    if isinstance(author, dict):
        return author.get("key")
    if isinstance(author, str):
        return author
    raise ValueError("Open Library event author must be an object, string, or null")


def fetch_recent(limit: int = 100, kind: str | None = None):
    """Recent catalog edits, newest first. kind filters to one event type
    (add-book, edit-book, add-cover, new-account, ...)."""
    now = datetime.now(timezone.utc).isoformat()
    url = f"{BASE}?limit={limit}"
    if kind:
        url = f"https://openlibrary.org/recentchanges/{kind}.json?limit={limit}"
    for r in _get(url):
        changes = r.get("changes") or []
        yield {
            "source": "open_library",
            "fetched_at": now,
            "id": r.get("id"),
            "kind": r.get("kind"),
            "timestamp": r.get("timestamp"),
            "comment": r.get("comment"),
            "author": _normalize_author(r.get("author")),
            "n_changes": len(changes),
            "changed_keys": [c.get("key") for c in changes],
        }


if __name__ == "__main__":
    kind = sys.argv[1] if len(sys.argv) > 1 else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    for rec in fetch_recent(limit=limit, kind=kind):
        print(json.dumps(rec, ensure_ascii=False))
