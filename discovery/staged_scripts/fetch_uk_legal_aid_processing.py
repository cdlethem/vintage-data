#!/usr/bin/env python3
"""UK Legal Aid Agency — civil work processing frontiers.

Weekly tables publish the oldest processing date by work type and, for complex work,
the percentage of backlog older than that date. Verified live 2026-09-03 on GOV.UK.
The page contains unrelated tables; this parser accepts only tables whose headers include
``Current Processing Date`` and a work-type key. Data is under the Open Government
Licence.

Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "uk_legal_aid_processing"
URL = "https://www.gov.uk/guidance/civil-processing-dates"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


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


def fetch_processing_dates(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8", "replace")
    match = re.search(r"\(Last updated ([^)]+)\)", text)
    if not match:
        raise ValueError("Legal Aid page is missing its reporting date")
    parser = Tables()
    parser.feed(text)
    emitted = 0
    selected = [
        table
        for table in parser.tables
        if table
        and "Current Processing Date" in table[0]
        and table[0][0].endswith("Work type")
    ]
    if not selected:
        raise ValueError("Legal Aid page has no recognized processing-date tables")
    for table_index, table in enumerate(selected):
        headers = table[0]
        for cells in table[1:]:
            if len(cells) != len(headers):
                raise ValueError("Legal Aid row has an unexpected shape")
            row = dict(zip(headers, cells))
            work_type = cells[0]
            row.update(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": work_type,
                    "table": headers[0],
                    "publisher_updated_at": match.group(1),
                }
            )
            yield row
            emitted += 1
            if emitted >= limit:
                return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_processing_dates(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
