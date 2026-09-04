#!/usr/bin/env python3
"""IMDb — contribution-processing queue depth and oldest pending item.

Roughly seventy editorial data types expose SLA class, oldest unprocessed date, and
queue depth, allowing backlog and SLA-breach trajectories. Verified live 2026-09-03 on
IMDb's public Data Processing Times page, which updates each business day. The script
selects the table by its exact headers so unrelated page structure cannot become data.
IMDb's site and non-commercial-use terms apply.

Stdlib only.
"""

import argparse
import json
import re
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "imdb_contribution_processing"
URL = "https://contribute.imdb.com/times"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
EXPECTED = ["Data Type", "SLA", "Oldest Item", "Unprocessed Items"]


class Tables(HTMLParser):
    def __init__(self):
        super().__init__()
        self.tables, self.table, self.row, self.cell = [], None, None, None

    def handle_starttag(self, tag, attrs):
        if tag == "table":
            self.table = []
        elif tag == "tr" and self.table is not None:
            self.row = []
        elif tag in ("th", "td") and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            if any(self.row):
                self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def fetch_processing_times(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8", "replace")
    match = re.search(r"Last updated on ([^.]+)\.", text)
    if not match:
        raise ValueError("IMDb page is missing its update date")
    publisher_updated_at = match.group(1)
    parser = Tables()
    parser.feed(text)
    table = next(
        (table for table in parser.tables if table and table[0] == EXPECTED), None
    )
    if table is None:
        raise ValueError("IMDb page is missing the processing-times table")
    for cells in table[1 : limit + 1]:
        if len(cells) != len(EXPECTED):
            raise ValueError("IMDb queue row has an unexpected shape")
        row = dict(zip(EXPECTED, cells))
        queue_depth = row["Unprocessed Items"].replace(",", "")
        row["Unprocessed Items"] = (
            int(queue_depth) if queue_depth.isdigit() else row["Unprocessed Items"]
        )
        row.update(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": row["Data Type"],
                "publisher_updated_at": publisher_updated_at,
            }
        )
        yield row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_processing_times(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
