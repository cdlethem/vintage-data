#!/usr/bin/env python3
"""W3C API — specification metadata snapshots.

Repeated snapshots reveal standards publication, title changes, and lifecycle churn.
Verified live 2026-09-03 at ``https://api.w3.org/specifications``. This HAL response
puts the collection under ``_links.specifications`` rather than ``_embedded`` and uses
one-based pages. The published limit is 6,000 requests per IP per ten minutes; this
client makes one request and is intended for daily or weekly use. Observe W3C document
and data terms.

Stdlib only.
"""
import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "w3c_specifications"
URL = "https://api.w3.org/specifications"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_specifications(limit: int = 100, page: int = 1, timeout: int = 30):
    if limit <= 0:
        return
    query = urllib.parse.urlencode({"items": min(limit, 1000), "page": page})
    request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    rows = document.get("_links", {}).get("specifications")
    if not isinstance(rows, list):
        raise ValueError("W3C response is missing _links.specifications")
    for row in rows[:limit]:
        href = row.get("href")
        if not href:
            raise ValueError("W3C specification link is missing href")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": href})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--page", type=int, default=1)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_specifications(args.limit, args.page, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
