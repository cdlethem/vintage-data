#!/usr/bin/env python3
"""CrossRef — the firehose of newly registered DOIs (keyless, JSON).

Every new journal article, preprint, book chapter, and dataset that gets a
CrossRef DOI shows up here — thousands per hour, with titles, abstracts
(sometimes), subjects, and publishers. 'Current' per run = works whose DOI was
*created* inside a trailing window, via the from-created-date filter, which
supports hourly resolution: filter=from-created-date:2026-09-02T14.

Verified live 2026-09-02: server responded (with a 429 to my anonymous
sandbox fetcher, which is their documented polite-pool behavior). REQUIRED
etiquette: include a mailto in your User-Agent — that moves you into the
polite pool and rate limits become generous.

Quirks:
  * Use cursor=* deep paging, never page/offset, for large pulls.
  * created-date captures registrations of old back-catalog too (journal
    transfers). Filter by publication year if you only want new literature.
  * select= keeps payloads small; full records are huge.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://api.crossref.org/works"
MAILTO = "you@example.com"  # <-- REQUIRED: set a real address (polite pool)
USER_AGENT = f"my-pipeline-poc/0.1 (mailto:{MAILTO})"

SELECT = "DOI,title,abstract,created,type,publisher,container-title,subject,author"


def fetch_new_works(hours_back: int = 1, rows: int = 200, max_records: int = 2000):
    """Yield works registered in the last `hours_back` hours."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    flt = "from-created-date:" + cutoff.strftime("%Y-%m-%dT%H")
    cursor, fetched = "*", 0
    while fetched < max_records:
        params = urllib.parse.urlencode({
            "filter": flt, "select": SELECT,
            "rows": rows, "cursor": cursor, "mailto": MAILTO,
        })
        req = urllib.request.Request(f"{BASE}?{params}",
                                     headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=60) as resp:
            msg = json.load(resp)["message"]
        items = msg.get("items", [])
        if not items:
            break
        now = datetime.now(timezone.utc).isoformat()
        for it in items:
            fetched += 1
            yield normalize(it, now)
        cursor = msg.get("next-cursor")
        if not cursor:
            break


def normalize(it: dict, now: str) -> dict:
    created = (it.get("created") or {}).get("date-time")
    return {
        "source": "crossref",
        "fetched_at": now,
        "id": it.get("DOI"),
        "created": created,
        "type": it.get("type"),
        "title": (it.get("title") or [None])[0],
        "journal": (it.get("container-title") or [None])[0],
        "publisher": it.get("publisher"),
        "subjects": it.get("subject") or [],
        "n_authors": len(it.get("author") or []),
        "has_abstract": bool(it.get("abstract")),
        "abstract": it.get("abstract"),  # JATS-flavored XML string when present
    }


if __name__ == "__main__":
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    for rec in fetch_new_works(hours_back=hours):
        print(json.dumps(rec, ensure_ascii=False))
