#!/usr/bin/env python3
"""Netflix Top 10 — latest official weekly rankings.

Weekly rank, viewing hours, runtime, views, and cumulative chart tenure support demand
and release-lifecycle analysis. Verified live 2026-09-03 against Netflix's global and
country TSV downloads. Files contain full history in newest-week-first order; this
client streams only the latest week by default. Netflix data-use terms apply.

Stdlib only.
"""

import argparse
import csv
import io
import json
import urllib.request
from datetime import datetime, timezone

URLS = {
    "global": "https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv",
    "countries": "https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv",
}
SOURCE = "netflix_top10"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_rankings(scope: str = "global", limit: int = 1000, timeout: int = 60):
    if limit <= 0:
        return
    request = urllib.request.Request(URLS[scope], headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        modified = response.headers.get("Last-Modified")
        reader = csv.DictReader(
            io.TextIOWrapper(response, encoding="utf-8-sig"), delimiter="\t"
        )
        if not reader.fieldnames or not {"week", "weekly_rank", "show_title"}.issubset(
            reader.fieldnames
        ):
            raise ValueError("Netflix TSV is missing expected columns")
        newest_week = None
        for index, row in enumerate(reader):
            newest_week = newest_week or row["week"]
            if row["week"] != newest_week or index >= limit:
                break
            parts = [
                scope,
                row["week"],
                row.get("country_name", ""),
                row.get("category", ""),
                row["weekly_rank"],
                row["show_title"],
            ]
            record = dict(row)
            record.update(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": "|".join(parts),
                    "scope": scope,
                    "publisher_updated_at": modified,
                }
            )
            yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", choices=tuple(URLS), default="global")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_rankings(args.scope, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
