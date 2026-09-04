#!/usr/bin/env python3
"""Malaysia OpenDOSM — daily organ-donor pledges by state.

Withdrawals can revise historical counts downward, so repeated snapshots preserve a
revision series that the current file alone cannot show. Verified live 2026-09-03 at
the official ``organ_pledges_state.csv`` object. The full-history CSV is streamed and
only the newest requested rows are retained in memory. Data is CC BY 4.0; fetch once
after the publisher's daily update.

Stdlib only.
"""
import argparse
import csv
import io
import json
import urllib.request
from collections import deque
from datetime import datetime, timezone

SOURCE = "malaysia_organ_pledges_state"
URL = "https://storage.data.gov.my/healthcare/organ_pledges_state.csv"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_pledges(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        modified = response.headers.get("Last-Modified")
        reader = csv.DictReader(io.TextIOWrapper(response, encoding="utf-8-sig", newline=""))
        expected = {"date", "state", "pledges"}
        if not reader.fieldnames or not expected.issubset(reader.fieldnames):
            raise ValueError(f"organ pledge CSV is missing columns: {sorted(expected)}")
        rows = deque(reader, maxlen=limit)

    for row in rows:
        record = dict(row)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{row['date']}|{row['state']}",
            "publisher_updated_at": modified,
        })
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_pledges(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
