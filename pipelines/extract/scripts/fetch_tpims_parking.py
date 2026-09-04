#!/usr/bin/env python3
"""Midwest TPIMS — truck-parking availability across every live state feed.

Illinois and Minnesota publish the standard TPIMS dynamic shape at least every five
minutes; a bare run covers both. Explicit ``Unknown`` availability, stale timestamps, and
``trustData: false`` are preserved rather than coerced to zero. Both feeds verified live
2026-09-04. State DOT terms apply.

Per-feed failures are reported on stderr and skipped. Stdlib only.
"""

import argparse
import json
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "tpims_truck_parking"
FEEDS = {
    "illinois": "https://truckparking.travelmidwest.com/TPIMS_Dynamic.json",
    "minnesota": "https://iris.dot.state.mn.us/iris/TPIMS_dynamic",
}
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def _sites(region: str, url: str, fetched_at: str, limit: int, timeout: int):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        rows = json.load(response)
    if not isinstance(rows, list) or not rows:
        raise ValueError("TPIMS response is not a non-empty list")
    records = []
    for row in rows[:limit]:
        site_id = row.get("siteId")
        if not site_id or "reportedAvailable" not in row:
            raise ValueError("TPIMS row is missing siteId or reportedAvailable")
        record = dict(row)
        record.update(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{region}:{site_id}",
                "region": region,
            }
        )
        records.append(record)
    return records


def fetch_parking(regions=(), limit: int = 5000, timeout: int = 30):
    if limit <= 0:
        return
    regions = list(regions) or sorted(FEEDS)
    unknown = [region for region in regions if region not in FEEDS]
    if unknown:
        raise ValueError(f"unknown regions {unknown}; choose from {sorted(FEEDS)}")
    fetched_at = datetime.now(timezone.utc).isoformat()
    failures = 0
    for region in regions:
        try:
            records = _sites(region, FEEDS[region], fetched_at, limit, timeout)
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError, OSError, json.JSONDecodeError) as error:
            failures += 1
            print(f"{region}: {type(error).__name__}: {error}", file=sys.stderr)
            continue
        yield from records
    if failures == len(regions):
        raise RuntimeError(f"all {failures} TPIMS feeds failed")
    if failures:
        print(f"{failures} of {len(regions)} TPIMS feeds failed", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("regions", nargs="*", choices=sorted(FEEDS), help="omit to cover every region")
    parser.add_argument("--limit", type=int, default=5000, help="max sites per region")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_parking(args.regions, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
