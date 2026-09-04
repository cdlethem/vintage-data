#!/usr/bin/env python3
"""IETF Datatracker — internet standards being written, revised and promoted, live.

Lead #43: watch RFCs and drafts being written, revised, and promoted in real time.
Working group document events (state changes, new revisions, stream changes) are
logged individually and queryable by time — a genuinely live institutional feed.

**Verified live 2026-09-03** — `datatracker.ietf.org/api/v1/doc/docevent/?format=json
&time__gte=2026-09-03&limit=2` returned **200, 801 bytes, meta.total_count: 360**
events today, most recent `time: 2026-09-03T17:14:32Z` — minutes before the probe.
Event: `"Stream changed to None from ISE"` on `draft-vauban-x402-consolidated`.

Endpoint: `https://datatracker.ietf.org/api/v1/doc/docevent/?format=json&
time__gte=YYYY-MM-DD&limit=N&offset=N`
    Django Tastypie-style API: `time__gte` is a real watermark filter, `meta.next`
    gives a ready-to-follow pagination URL, `meta.total_count` the full match count.
    Related: `/api/v1/doc/document/?format=json&time__gte=...` for document-level
    (not event-level) changes; `/api/v1/group/group/?format=json&state__slug__
    in=active&type__slug__in=wg` for active working groups.

Quirks that will cost someone an afternoon:
    * **`by` and `doc` are relative API URIs, not resolved objects or names** —
      `"doc": "/api/v1/doc/document/draft-vauban-x402-consolidated/"`, not a bare
      draft name, and `"by": "/api/v1/person/person/2097/"`, not a person's name. A
      second request is needed if you want the human-readable draft name or author —
      this script extracts the draft slug from the URI path rather than making that
      extra round-trip, since the slug alone is usually enough to identify the draft.
    * **`desc` contains literal HTML tags** (`"Stream changed to <b>None</b> from
      ISE"`) — strip or render, don't display raw if building anything user-facing.
    * `time__gte` filters on a **date-or-datetime boundary you supply** — passing just
      a date (`2026-09-03`) is treated as midnight UTC that day; for a tighter
      recent-events poll, pass a full ISO datetime.
    * `/api/v1/event/` (without the `doc/` prefix) **404s** — the real path is nested
      under `doc/`, confirmed live; a plausible-looking shorter URL doesn't exist.
    * Pagination is real cursor-style via `meta.next` (a ready-to-use relative URL),
      not a page number you compute — follow it directly.

Etiquette: keyless, no published rate limit, but this is IETF's production
infrastructure for internet standards work — poll gently (event volume is on the
order of hundreds/day, not per-second) and prefer the time-filtered query over paging
through everything.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://datatracker.ietf.org/api/v1"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _draft_slug(doc_uri):
    """'/api/v1/doc/document/draft-foo/' -> 'draft-foo'."""
    if not doc_uri:
        return None
    return doc_uri.rstrip("/").rsplit("/", 1)[-1]


def fetch_events(since: str | None = None, limit: int = 50):
    """Document events (state changes, revisions, stream moves) since a date/datetime.
    Defaults to the last 24 hours."""
    since = since or (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc).isoformat()
    params = urllib.parse.urlencode({"format": "json", "time__gte": since, "limit": limit})
    doc = _get(f"{BASE}/doc/docevent/?{params}")
    for e in doc.get("objects") or []:
        yield {
            "source": "ietf_datatracker",
            "fetched_at": now,
            "id": e.get("id"),
            "event_type": e.get("type"),
            "draft": _draft_slug(e.get("doc")),
            "description": e.get("desc"),
            "revision": e.get("rev"),
            "time": e.get("time"),
        }


if __name__ == "__main__":
    since = sys.argv[1] if len(sys.argv) > 1 else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    for rec in fetch_events(since, limit=limit):
        print(json.dumps(rec, ensure_ascii=False))
