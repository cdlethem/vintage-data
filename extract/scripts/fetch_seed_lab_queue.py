#!/usr/bin/env python3
"""Oregon State Seed Laboratory — public sample-processing queue.

The business-day page exposes received-date batches, sample counts, rush load, and
estimated completion windows. Daily snapshots reveal queue age and throughput. The
queue table was verified live 2026-09-04. Oregon State University website terms apply.

Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "oregon_seed_lab_queue"
URL = "https://seedlab.oregonstate.edu/turnaround-time"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
EXPECTED = [
    "Date Sample or Re-test Request Received",
    "Number Samples Received",
    "**Estimated Completion Day",
]


class Tables(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables, self.table, self.row, self.cell = [], None, None, None

    def handle_starttag(self, tag, attrs):
        if tag == "table": self.table = []
        elif tag == "tr" and self.table is not None: self.row = []
        elif tag in ("th", "td") and self.row is not None: self.cell = []
        elif tag == "br" and self.cell is not None: self.cell.append(" ")

    def handle_data(self, data):
        if self.cell is not None: self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split())); self.cell = None
        elif tag == "tr" and self.row is not None:
            if any(self.row): self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self.tables.append(self.table); self.table = None


def fetch_queue(limit: int = 100, timeout: int = 30):
    if limit <= 0: return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8", "replace")
    parser = Tables(); parser.feed(text)
    table = next((table for table in parser.tables if table and table[0] == EXPECTED), None)
    if table is None: raise ValueError("seed-lab page is missing the queue table")
    emitted = 0
    for cells in table[1:]:
        if emitted >= limit: break
        if len(cells) == 1:  # trailing footnote paragraph rendered as a row
            continue
        if len(cells) != 3 or not cells[0]: raise ValueError("seed queue row has an unexpected shape")
        count = cells[1].replace(",", "")
        emitted += 1
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": cells[0].lower().replace(" ", "-"),
            "received_date_or_queue": cells[0],
            "sample_count": int(count) if count.isdigit() else cells[1],
            "estimated_completion": cells[2],
        }
    if not emitted: raise ValueError("seed-lab queue table has no data rows")


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--limit", type=int, default=100); parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_queue(args.limit, args.timeout): print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__": main()
