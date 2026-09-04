#!/usr/bin/env python3
"""arXiv — new preprint submissions per category (keyless, Atom XML).

'Current' per run = papers with submittedDate inside a trailing window for a
category (e.g. cs.AI, astro-ph.HE). Titles + full abstracts make this a
first-class LLM-analysis corpus (topic drift, embedding trajectories,
"what is physics worried about this week").

Docs-verified 2026-09-02 against the official user manual
(https://info.arxiv.org/help/api/user-manual.html). Endpoint
export.arxiv.org/api/query is explicitly public; hard limit 3 req/sec,
recommended <= 1 req / 3 sec with pagination sleeps.

Quirks:
  * Response is Atom 1.0 XML, not JSON — parsed here with stdlib ElementTree.
  * New submissions are announced in daily batches (Sun-Thu ~20:00 ET), so a
    smart cadence is ~1-4 polls/day per category, not minutes.
  * date filter format: submittedDate:[YYYYMMDDHHMM TO YYYYMMDDHHMM] (GMT).

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

BASE = "http://export.arxiv.org/api/query"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
NS = {"a": "http://www.w3.org/2005/Atom", "arxiv": "http://arxiv.org/schemas/atom"}


def fetch_new_papers(category: str = "cs.AI", hours_back: int = 24,
                     max_results: int = 200):
    now = datetime.now(timezone.utc)
    lo = (now - timedelta(hours=hours_back)).strftime("%Y%m%d%H%M")
    hi = now.strftime("%Y%m%d%H%M")
    search = f"cat:{category} AND submittedDate:[{lo} TO {hi}]"
    params = urllib.parse.urlencode({
        "search_query": search,
        "sortBy": "submittedDate", "sortOrder": "descending",
        "start": 0, "max_results": max_results,
    })
    req = urllib.request.Request(f"{BASE}?{params}",
                                 headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        root = ET.fromstring(resp.read())
    fetched_at = now.isoformat()
    for entry in root.findall("a:entry", NS):
        yield parse_entry(entry, fetched_at)


def parse_entry(entry, fetched_at: str) -> dict:
    def text(tag):
        el = entry.find(tag, NS)
        return el.text.strip() if el is not None and el.text else None

    cats = [c.get("term") for c in entry.findall("a:category", NS)]
    primary = entry.find("arxiv:primary_category", NS)
    return {
        "source": "arxiv",
        "fetched_at": fetched_at,
        "id": text("a:id"),                       # http://arxiv.org/abs/XXXX.YYYYYvN
        "published": text("a:published"),
        "updated": text("a:updated"),
        "title": " ".join((text("a:title") or "").split()),
        "abstract": " ".join((text("a:summary") or "").split()),
        "authors": [a.findtext("a:name", None, NS)
                    for a in entry.findall("a:author", NS)],
        "primary_category": primary.get("term") if primary is not None else None,
        "categories": cats,
    }


if __name__ == "__main__":
    cat = sys.argv[1] if len(sys.argv) > 1 else "cs.AI"
    for rec in fetch_new_papers(category=cat):
        print(json.dumps(rec, ensure_ascii=False))
