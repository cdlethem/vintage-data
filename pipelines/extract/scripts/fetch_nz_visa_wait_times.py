#!/usr/bin/env python3
"""Immigration New Zealand — work-visa processing wait times.

The weekly table reports median processing time (labelled average) and the 80th
percentile over the preceding four weeks. Verified live 2026-09-03. This parser selects
the table by its exact three-column header so cookie and navigation tables cannot be
emitted as records. Preserve the source's working-day units and wording.

Stdlib only.
"""

import argparse
import json
import re
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "nz_work_visa_wait_times"
URL = "https://www.immigration.govt.nz/process-to-apply/waiting-for-a-visa/processing-a-visa-application/how-long-it-takes-to-process-an-application/work-visa-wait-times/"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
EXPECTED = ["Visa type", "Average wait time", "Most completed within"]


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


def fetch_wait_times(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8", "replace")
    modified = re.search(r'"dateModified"\s*:\s*"([^"]+)"', text)
    parser = Tables()
    parser.feed(text)
    table = next(
        (table for table in parser.tables if table and table[0] == EXPECTED), None
    )
    if table is None:
        raise ValueError("visa page is missing the work-visa wait-time table")
    for cells in table[1 : limit + 1]:
        if len(cells) != len(EXPECTED):
            raise ValueError("visa wait-time row has an unexpected shape")
        row = dict(zip(EXPECTED, cells))
        row.update(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": row["Visa type"],
                "publisher_updated_at": modified.group(1) if modified else None,
            }
        )
        yield row


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_wait_times(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
