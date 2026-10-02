#!/usr/bin/env python3
"""Internet Archive — new uploads across movies, audio, texts and software, keyless.

Lead #71: the scrape/advancedsearch API surfaces new uploads continuously across
concert recordings, software, books and film — one of the most culturally rich and
genuinely fast-moving arts sources available.

**Verified live 2026-09-03** — `archive.org/advancedsearch.php?q=mediatype:movies&
sort[]=addeddate+desc&rows=5&output=json&fl[]=identifier,title,addeddate,mediatype,
collection` returned **200, `numFound: 16,991,855`**, top result `addeddate:
2026-09-03T15:19:47Z` — **40 seconds before the probe.**

Endpoint: `https://archive.org/advancedsearch.php`
    q=mediatype:{type}          movies | audio | texts | software | image | etree
    sort[]=addeddate+desc         newest first (repeat sort[] for tiebreak fields)
    rows=N&start=N                 pagination
    output=json
    fl[]=field                     repeat for each field wanted (identifier, title,
                                   addeddate, mediatype, collection, creator, ...)

Quirks that will cost someone an afternoon:
    * **`collection` is a list, not a scalar** — an item is routinely filed under
      multiple collections at once (verified live: `["TV-TVRIN", "tvarchive"]`).
      Don't assume one collection per item.
    * **Query params use `[]` array syntax** (`sort[]=`, `fl[]=`) even for a single
      value — this is Solr's parameter convention leaking through, not optional
      decoration; omitting the brackets on `fl[]` silently returns the default field
      set instead of erroring.
    * `numFound` in the response is the *total* matching the query (millions, for a
      broad mediatype filter), not the number of rows returned — don't confuse the
      two when reporting "how many".
    * `identifier` is the stable, permanent per-item id (used to build the item's page
      URL as `archive.org/details/{identifier}`) — use it as the envelope id, not
      `title`, which is free text and not unique.
    * A large share of real-time volume is **automated TV-capture ingestion**
      (identifiers like `TVRIN_20260903_143000`, collection `tvarchive`) rather than
      human uploads — expect that mix rather than being surprised by it.

Etiquette: keyless, no published rate limit for advancedsearch, but the Internet
Archive is a nonprofit running on donations serving one of the largest libraries in
existence — poll gently (this endpoint moves fast enough that every few minutes is
plenty) and request only the fields you need via `fl[]` rather than the full record.

Stdlib only.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://archive.org/advancedsearch.php"
FIELDS = ["identifier", "title", "addeddate", "mediatype", "collection", "creator"]

MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def _get(query, rows=25, sort="addeddate desc"):
    """Fetch one search page, retrying transient failures with a finite bound.

    archive.org's TLS edge intermittently resets connections and times out
    handshakes (observed 2026-10-02: SSL handshake timeout in task log,
    connection reset on a live probe). Transient transport errors and 429/5xx
    retry with 1s/2s backoff; non-transient errors and exhausted attempts
    raise the final failure unchanged.
    """
    parts = [("q", query), ("rows", rows), ("output", "json")]
    parts += [("sort[]", sort)]
    parts += [("fl[]", f) for f in FIELDS]
    url = f"{BASE}?{urllib.parse.urlencode(parts)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            reason = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            reason = str(exc.reason if isinstance(exc, urllib.error.URLError) else exc)
        delay = RETRY_BASE_DELAY * (2 ** attempt)
        print(f"internet_archive: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)


def fetch_recent(mediatype: str = "movies", rows: int = 25):
    """Newest uploads for one mediatype: movies, audio, texts, software, image, etree."""
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(f"mediatype:{mediatype}", rows=rows)
    for d in doc.get("response", {}).get("docs", []):
        collections = d.get("collection")
        if isinstance(collections, str):
            collections = [collections]
        yield {
            "source": "internet_archive",
            "fetched_at": now,
            "id": d.get("identifier"),
            "title": d.get("title"),
            "mediatype": d.get("mediatype"),
            "collections": collections or [],
            "creator": d.get("creator"),
            "added_date": d.get("addeddate"),
            "url": f"https://archive.org/details/{d.get('identifier')}",
        }


if __name__ == "__main__":
    mediatype = sys.argv[1] if len(sys.argv) > 1 else "movies"
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    for rec in fetch_recent(mediatype, rows=rows):
        print(json.dumps(rec, ensure_ascii=False))
