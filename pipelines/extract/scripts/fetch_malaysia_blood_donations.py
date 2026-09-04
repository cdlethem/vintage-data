#!/usr/bin/env python3
"""Malaysia OpenDOSM — daily blood donations by state and blood group.

Campaigns, holidays, group mix, and publisher backfills become measurable through
repeated snapshots. Verified live 2026-09-03 at the official
``blood_donations_state.csv`` object. Kuala Lumpur includes Putrajaya and mobile
campaigns around Selangor; Perlis and Labuan are absent. The full-history CSV is
streamed and only the newest requested rows are retained in memory. Data is CC BY 4.0;
fetch once after the 10:00 Asia/Kuala_Lumpur daily update.

Stdlib only.
"""
import argparse
import csv
import io
import json
import urllib.request
from collections import deque
from datetime import datetime, timezone

SOURCE = "malaysia_blood_donations_state"
URL = "https://storage.data.gov.my/healthcare/blood_donations_state.csv"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_donations(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        modified = response.headers.get("Last-Modified")
        reader = csv.DictReader(io.TextIOWrapper(response, encoding="utf-8-sig", newline=""))
        expected = {"date", "state", "blood_type", "donations"}
        if not reader.fieldnames or not expected.issubset(reader.fieldnames):
            raise ValueError(f"blood donation CSV is missing columns: {sorted(expected)}")
        rows = deque(reader, maxlen=limit)

    for row in rows:
        record = dict(row)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{row['date']}|{row['state']}|{row['blood_type']}",
            "publisher_updated_at": modified,
        })
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_donations(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
