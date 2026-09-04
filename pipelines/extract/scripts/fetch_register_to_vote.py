#!/usr/bin/env python3
"""UK Register to Vote — rolling five-minute application counts.

The official performance page embeds all 288 bins from the preceding 24 hours, creating
a national civic-action pulse. Verified live 2026-09-03. The accessible table labels
bins with local clock times but omits dates; this client walks its newest-first rows and
resolves midnight crossings in Europe/London. Poll every 30 minutes rather than every
five minutes to avoid repeatedly downloading the full dashboard.

Stdlib only.
"""

import argparse
import json
import urllib.request
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

SOURCE = "uk_register_to_vote_usage"
URL = "https://www.registertovote.service.gov.uk/performance"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
UK_TIME = ZoneInfo("Europe/London")


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
            if self.cell is not None:
                self.row.append(" ".join("".join(self.cell).split()))
                self.cell = None
            if any(self.row):
                self.table.append(self.row)
            self.row = None
        elif tag == "table" and self.table is not None:
            self.tables.append(self.table)
            self.table = None


def fetch_usage(limit: int = 288, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    fetched_local = datetime.now(UK_TIME)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8", "replace")
    parser = Tables()
    parser.feed(text)
    table = next(
        (
            table
            for table in parser.tables
            if table and table[0] == ["Time", "Online applications"]
        ),
        None,
    )
    if table is None:
        raise ValueError("performance page is missing the live-usage table")

    current_date = fetched_local.date()
    previous_time = None
    for row in table[1 : limit + 1]:
        if len(row) != 2:
            raise ValueError("live-usage row has an unexpected shape")
        local_time = (
            datetime.strptime(row[0].lower(), "%I:%M%p")
            .replace(tzinfo=UK_TIME)
            .timetz()
        )
        if previous_time is not None and local_time > previous_time:
            current_date -= timedelta(days=1)
        previous_time = local_time
        published = datetime.combine(current_date, local_time, UK_TIME)
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": published.isoformat(),
            "published_at": published.isoformat(),
            "applications": int(row[1].replace(",", "")),
        }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=288)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_usage(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
