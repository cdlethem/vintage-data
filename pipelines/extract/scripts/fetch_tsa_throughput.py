#!/usr/bin/env python3
"""U.S. TSA — daily national checkpoint passenger throughput.

Daily totals expose travel demand, holiday peaks, and disruption recovery. Verified live
2026-09-03 on TSA's official passenger-volume page, updated weekdays by 09:00. The
script selects only the two-column ``Date``/``Numbers`` table and preserves the HTTP
Last-Modified timestamp. U.S. federal public data.

Stdlib only.
"""

import argparse
import json
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "tsa_checkpoint_throughput"
URL = "https://www.tsa.gov/travel/passenger-volumes"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


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


def fetch_throughput(limit: int = 400, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        modified = response.headers.get("Last-Modified")
        text = response.read().decode("utf-8", "replace")
    parser = Tables()
    parser.feed(text)
    table = next(
        (table for table in parser.tables if table and table[0] == ["Date", "Numbers"]),
        None,
    )
    if table is None:
        raise ValueError("TSA page is missing the passenger-volume table")
    for cells in table[1 : limit + 1]:
        if len(cells) != 2:
            raise ValueError("TSA volume row has an unexpected shape")
        day = (
            datetime.strptime(cells[0], "%m/%d/%Y")
            .replace(tzinfo=timezone.utc)
            .date()
            .isoformat()
        )
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": day,
            "date": day,
            "passengers": int(cells[1].replace(",", "")),
            "publisher_updated_at": modified,
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=400)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_throughput(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
