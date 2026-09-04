#!/usr/bin/env python3
"""Gutendex — a keyless JSON API over the Project Gutenberg catalog.

Lead #38: "what is the world reading from the public domain this week" as a charming
little series, built on `download_count` per book. Not the ~80,000 book texts
themselves (those are the Gutenberg mirrors) but the catalog metadata that wraps them —
title, authors with birth/death years, subjects, bookshelves, and a per-book download
count that drifts as Gutendex refreshes its cache from Gutenberg's own logs.

**Verified live 2026-09-03** — `gutendex.com/books/?sort=popular` returned **200,
61,848 bytes, count: 79,296 total books**, page 1 of 32. Top result: *Pride and
Prejudice* (id 1342), `download_count: 198164`. A `?search=` query timed out from this
sandbox in one attempt; the sort/list path is the one confirmed working.

**Why `fetched_at` matters here**: `download_count` is a moving popularity signal.
Re-polling `?sort=popular` periodically and diffing rank/count by book id turns a static
library catalog into "what's trending in the public domain," with real seasonal spikes
(a book adapted into a film, an author's public-domain-day anniversary).

Endpoints (base `https://gutendex.com/books/`):
    ?sort=popular              paginated, 32/page, ordered by download_count desc
    ?search=<query>            full-text search across title/author
    ?ids=1342,84               fetch specific books by Gutenberg id
    ?languages=en,fr           filter by language code
    (all filters combine; `next`/`previous` in the response are ready-to-use URLs)

Quirks:
    * `summaries` is a **list of one AI-generated summary string**, not a scalar — the
      response itself says so ("This is an automatically generated summary."). Treat it
      as generated text, not editorial description.
    * `formats` is a dict keyed by MIME type, not a list — `formats["text/plain; charset=utf-8"]`
      is the plain-text download; the key literally includes `; charset=utf-8`.
    * `authors`/`translators`/`editors` are objects with `birth_year`/`death_year` that
      can be `null` (anonymous or undated works) — don't assume both are present.
    * Pagination is real cursor-style (`next` is a full URL, not a page-number
      convention you compute yourself) — follow it rather than incrementing `page=`.
    * A `?search=` request timed out once in this session; the site can be slow under
      load. Retry with backoff rather than assuming search is down.

Etiquette: keyless, no published rate limit; Gutendex is a small community project
mirroring Gutenberg's own catalog — poll gently (daily is plenty for a popularity
ranking) and don't hammer `?search=`.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://gutendex.com/books/"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp)


def _norm(book, now):
    return {
        "source": "gutendex",
        "fetched_at": now,
        "id": book["id"],
        "title": book.get("title"),
        "authors": [a.get("name") for a in book.get("authors") or []],
        "author_years": [(a.get("birth_year"), a.get("death_year"))
                         for a in book.get("authors") or []],
        "languages": book.get("languages") or [],
        "subjects": book.get("subjects") or [],
        "bookshelves": book.get("bookshelves") or [],
        "download_count": book.get("download_count"),
        "copyright": book.get("copyright"),
        "text_url": (book.get("formats") or {}).get("text/plain; charset=utf-8"),
    }


def fetch_popular(pages: int = 1):
    """Books ranked by download_count, most-downloaded first. One page = 32 books."""
    url = f"{BASE}?sort=popular"
    for _ in range(pages):
        if not url:
            return
        now = datetime.now(timezone.utc).isoformat()
        doc = _get(url)
        for b in doc["results"]:
            yield _norm(b, now)
        url = doc.get("next")


def fetch_search(query: str, pages: int = 1):
    url = f"{BASE}?search={urllib.parse.quote(query)}"
    for _ in range(pages):
        if not url:
            return
        now = datetime.now(timezone.utc).isoformat()
        doc = _get(url)
        for b in doc["results"]:
            yield _norm(b, now)
        url = doc.get("next")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "popular"
    if mode == "search":
        for rec in fetch_search(sys.argv[2] if len(sys.argv) > 2 else "darwin"):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        pages = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        for rec in fetch_popular(pages=pages):
            print(json.dumps(rec, ensure_ascii=False))
