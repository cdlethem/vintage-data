#!/usr/bin/env python3
"""Internet Archive Wayback CDX API — query what has been archived, and when.

Lead #32: a meta-internet dataset — you're measuring the act of preservation rather
than the content. Pairs well with Certificate Transparency (#14, also in this catalog)
for "new domain appears, gets archived, disappears" lifecycle work.

**Verified live 2026-09-03** — `web.archive.org/cdx/search/cdx?url=example.org&
matchType=domain&from=20260830&to=20260903&collapse=timestamp:8&showSkipCount=true&
output=json` returned **200, 4,579 bytes, 30 real capture records**, spanning
2026-08-30 through 2026-08-31 — inside the requested window, confirming both the date
filter and the response shape work as documented.

Endpoint: `https://web.archive.org/cdx/search/cdx?url=<url>&output=json&...`
    matchType=domain|host|prefix|exact   how broadly to match the url
    from=YYYYMMDD&to=YYYYMMDD              date-range filter — the reliable way to
                                           get "recent" captures (see quirks)
    collapse=timestamp:N                    dedupe captures to one per N-digit
                                           timestamp prefix (e.g. `timestamp:8` = one
                                           per day) so you don't get every near-
                                           duplicate re-crawl
    limit=N                                  cap the row count

Quirks that will cost someone an afternoon:
    * **The response is a JSON array of arrays, with the field names as the first
      row** — `[["urlkey","timestamp","original","mimetype","statuscode","digest",
      "length"], [actual, row, values, ...], ...]`. It is not a list of objects; the
      first element is a header row you must zip against the rest yourself.
    * **A negative `limit` (e.g. `limit=-2`) did not behave as "the N most recent"
      in this session** — it returned entries from 2022 while a positive-limit,
      date-ranged query for the same period returned entries from 2026. The exact
      semantics of negative limit were not fully resolved here; **use `from=`/`to=`
      for "recent captures" instead of relying on negative limit**, which is what
      this script does.
    * `original` is the archived URL **URL-encoded**, and can itself contain
      percent-encoded Unicode and even literal `<`/`>`/backslash sequences from
      malformed real-world URLs that got crawled — don't assume it's a clean URL
      ready to reuse without validation.
    * `statuscode` in the row is a **string**, and `"-"` (not a number) appears for
      revisit records that point at a prior identical capture rather than storing a
      fresh HTTP response.
    * `archive.org/wayback/available?url=...` is a different, simpler endpoint (one
      URL -> its single closest snapshot) — useful for a quick "has this been
      archived at all" check, but not for browsing capture history the way CDX does.

Etiquette: keyless, no published rate limit for CDX specifically, but this is shared
Internet Archive infrastructure — use `collapse=` to avoid pulling near-duplicate rows,
and prefer a narrow date range over an unbounded query.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
BASE = "https://web.archive.org/cdx/search/cdx"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_recent_captures(url: str, days: int = 7, match_type: str = "domain",
                          collapse_digits: int = 8):
    """Captures of `url` in the last `days` days. collapse_digits=8 dedupes to one
    row per day (YYYYMMDD prefix); use a bigger number for finer granularity."""
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).strftime("%Y%m%d")
    params = {"url": url, "matchType": match_type, "from": since,
             "to": now.strftime("%Y%m%d"), "collapse": f"timestamp:{collapse_digits}",
             "output": "json"}
    doc = _get(f"{BASE}?{urllib.parse.urlencode(params)}")
    if not doc or len(doc) < 2:
        return  # header-only or empty response: no captures in this window
    header, rows = doc[0], doc[1:]
    fetched_at = now.isoformat()
    for row in rows:
        rec = dict(zip(header, row))
        yield {
            "source": "wayback_cdx",
            "fetched_at": fetched_at,
            "id": f"{rec.get('urlkey')}:{rec.get('timestamp')}",
            "original_url": rec.get("original"),
            "captured_at": rec.get("timestamp"),  # YYYYMMDDhhmmss
            "status_code": rec.get("statuscode"),
            "mimetype": rec.get("mimetype"),
            "digest": rec.get("digest"),
            "length": rec.get("length"),
        }


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else "example.org"
    days = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    for rec in fetch_recent_captures(url, days=days):
        print(json.dumps(rec, ensure_ascii=False))
