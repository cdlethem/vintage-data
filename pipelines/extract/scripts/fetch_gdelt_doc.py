#!/usr/bin/env python3
"""GDELT DOC 2.0 — global news article firehose slices (keyless, JSON).

GDELT monitors worldwide online news in 65 machine-translated languages and
refreshes every 15 minutes. 'Current' per run = ArtList results for a query
over the last N minutes (min 15), sorted newest first, plus optionally a
TimelineVolRaw series (article counts per 15-min bucket) which is the cleanest
ready-made time series in the entire news space.

Docs-verified 2026-09-02 against the official spec at
https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/ (endpoint
api.gdeltproject.org/api/v2/doc/doc). The endpoint is explicitly public and
keyless; my sandboxed fetcher couldn't hit it only because of a blanket
robots rule, so smoke-test once from your own machine before wiring in.

Tips:
  * maxrecords caps at 250 per call; slide STARTDATETIME/ENDDATETIME windows
    to go deeper. Searchable window: trailing 3 months.
  * `tone` and `sourcecountry`/`sourcelang` query operators let you build
    sentiment-by-country time series with zero NLP of your own.
  * Very short single-word queries can be rejected; quote phrases.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://api.gdeltproject.org/api/v2/doc/doc"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def _get(params: dict):
    url = BASE + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_articles(query: str, minutes: int = 15, maxrecords: int = 250):
    """Newest articles matching `query` from the last `minutes` (>=15)."""
    data = _get({
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "sort": "DateDesc",
        "timespan": f"{max(minutes, 15)}min",
        "maxrecords": min(maxrecords, 250),
    })
    now = datetime.now(timezone.utc).isoformat()
    for a in data.get("articles", []):
        yield {
            "source": "gdelt_doc",
            "fetched_at": now,
            "id": a.get("url"),                      # URL is the natural key
            "published": a.get("seendate"),          # YYYYMMDDTHHMMSSZ
            "title": a.get("title"),
            "domain": a.get("domain"),
            "language": a.get("language"),
            "sourcecountry": a.get("sourcecountry"),
            "url": a.get("url"),
        }


def fetch_volume_timeline(query: str, timespan: str = "1d"):
    """15-min resolution article-count time series for `query`."""
    data = _get({
        "query": query, "mode": "TimelineVolRaw",
        "format": "json", "timespan": timespan,
    })
    now = datetime.now(timezone.utc).isoformat()
    for series in data.get("timeline", []):
        for point in series.get("data", []):
            yield {
                "source": "gdelt_timeline",
                "fetched_at": now,
                "query": query,
                "ts": point.get("date"),
                "count": point.get("value"),
                "norm": point.get("norm"),   # total articles GDELT saw that bucket
            }


if __name__ == "__main__":
    q = sys.argv[1] if len(sys.argv) > 1 else '"artificial intelligence"'
    for rec in fetch_articles(q, minutes=30):
        print(json.dumps(rec, ensure_ascii=False))
