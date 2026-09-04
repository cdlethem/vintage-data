#!/usr/bin/env python3
"""IRS — Exempt Organization Business Master File regional extracts.

Monthly snapshots reveal registrations and status or address changes. Verified live
2026-09-03 against the official ``eo1.csv`` partition. The national extract is split
across numbered regional files, selected with ``--region``. Compact date fields and EIN
leading zeroes deliberately remain strings. Bare execution emits the complete first
partition; ``--limit`` is intended only for smoke tests. US federal public data; fetch
monthly.

Stdlib only.
"""
import argparse
import csv
import io
import json
import urllib.request
from datetime import datetime, timezone

BASE_URL = "https://www.irs.gov/pub/irs-soi"
SOURCE = "irs_exempt_organizations"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_organizations(region: int = 1, limit: int | None = None, timeout: int = 60):
    if region not in (1, 2, 3, 4):
        raise ValueError("region must be 1, 2, 3, or 4")
    if limit is not None and limit < 0:
        raise ValueError("limit must be non-negative")
    url = f"{BASE_URL}/eo{region}.csv"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        modified = response.headers.get("Last-Modified")
        reader = csv.DictReader(
            io.TextIOWrapper(response, encoding="utf-8-sig", errors="replace", newline="")
        )
        if not reader.fieldnames or "EIN" not in reader.fieldnames:
            raise ValueError("IRS extract is missing the EIN column")
        for index, row in enumerate(reader):
            if limit is not None and index >= limit:
                break
            ein = row.get("EIN")
            if not ein:
                raise ValueError("IRS organization row is missing EIN")
            record = dict(row)
            record.update({
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": ein,
                "region": region,
                "publisher_updated_at": modified,
            })
            yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", type=int, choices=(1, 2, 3, 4), default=1)
    parser.add_argument("--limit", type=int, help="omit to emit the complete partition")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_organizations(args.region, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
