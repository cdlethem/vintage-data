#!/usr/bin/env python3
"""The Gazette — newest UK insolvency notices.

Notices form company-distress event streams; the feed also exposes its retained parsing
error count. Verified live 2026-09-03 at
``https://www.thegazette.co.uk/insolvency/notice/data.json``. Colon-bearing keys and
HTML notice content are source fields, not parsing mistakes. Individual notices link to
richer JSON-LD. The public feed is distinct from the paid tailored-data service. Poll
at most every 15–60 minutes and follow the notice-specific reuse rights.

Stdlib only.
"""
import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "uk_gazette_insolvency_notices"
URL = "https://www.thegazette.co.uk/insolvency/notice/data.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_notices(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    query = urllib.parse.urlencode({
        "results-page-size": max(2, min(limit, 100)),
        "results-page": 1,
    })
    # Sending an explicit Accept header currently makes this endpoint return HTTP 500.
    request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    rows = document.get("entry")
    if not isinstance(rows, list):
        raise ValueError("Gazette response is missing its entry list")
    for row in rows[:limit]:
        notice_id = row.get("id")
        if not notice_id:
            raise ValueError("Gazette notice is missing id")
        record = dict(row)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": notice_id,
            "feed_updated_at": document.get("updated"),
            "feed_total": document.get("f:total"),
            "feed_total_errors": document.get("f:total-errors"),
        })
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_notices(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
