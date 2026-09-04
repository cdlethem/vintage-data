#!/usr/bin/env python3
"""Federal Register — new regulatory documents (keyless, JSON).

'Current' per run = documents published on/after a given date (default: today),
plus optionally the public-inspection queue (documents posted before official
publication; this queue changes throughout the business day and is the most
'live' part of the source).

Verified live 2026-09-02: GET https://www.federalregister.gov/api/v1/documents.json
returned current documents with clean JSON.

Quirks (important for the ingestion agent):
  * HTTP 404 means "zero matches", NOT a dead endpoint. Treat as empty.
  * HTTP 400 appears once you page past ~2,000 results. Partition by date
    range instead of deep paging. per_page max is 100.
  * No API key. Be polite: identify yourself via User-Agent.

Stdlib only.
"""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone

BASE = "https://www.federalregister.gov/api/v1"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

FIELDS = [
    "document_number", "title", "type", "abstract", "publication_date",
    "agencies", "html_url", "pdf_url", "raw_text_url",
]


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.load(resp)
    except urllib.error.HTTPError as e:
        if e.code == 404:      # documented: no matches
            return {"results": []}
        raise


def fetch_documents(since: str, per_page: int = 100, max_pages: int = 5):
    """Yield normalized document records published on/after `since` (YYYY-MM-DD)."""
    params = {
        "per_page": per_page,
        "order": "newest",
        "conditions[publication_date][gte]": since,
    }
    for f in FIELDS:
        params.setdefault("fields[]", [])
    # fields[] needs repeated keys; build query manually
    query = urllib.parse.urlencode(
        [("per_page", per_page), ("order", "newest"),
         ("conditions[publication_date][gte]", since)]
        + [("fields[]", f) for f in FIELDS]
    )
    page = 1
    url = f"{BASE}/documents.json?{query}&page={page}"
    while url and page <= max_pages:
        data = _get(url)
        for r in data.get("results", []):
            yield normalize(r)
        url = data.get("next_page_url")
        page += 1


def fetch_public_inspection():
    """Documents currently on public inspection (pre-publication queue)."""
    data = _get(f"{BASE}/public-inspection-documents/current.json")
    for r in data.get("results", []):
        yield normalize(r, public_inspection=True)


def normalize(r: dict, public_inspection: bool = False) -> dict:
    return {
        "source": "federal_register",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "id": r.get("document_number"),
        "published": r.get("publication_date") or r.get("filed_at"),
        "doc_type": r.get("type"),
        "title": r.get("title"),
        "abstract": r.get("abstract"),
        "agencies": [a.get("slug") or a.get("name") for a in (r.get("agencies") or [])
                     if isinstance(a, dict)],
        "url": r.get("html_url"),
        "text_url": r.get("raw_text_url"),   # full text for LLM analysis
        "public_inspection": public_inspection,
    }


if __name__ == "__main__":
    since = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
    for rec in fetch_documents(since):
        print(json.dumps(rec, ensure_ascii=False))
