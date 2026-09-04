#!/usr/bin/env python3
"""Hong Kong FEHD — cremation-session availability for every crematorium and date.

The public 15-day view exposes, per date and crematorium, sessions available with the
count temporarily held under booking in parentheses, ``N/A`` for maintenance or
not-in-service, a daily total, and a publisher update timestamp. Snapshots capture
release-to-book depletion; only aggregates are collected and no booking flow is touched.
Verified live 2026-09-04.

The host requires TLS legacy renegotiation, so the client enables that option explicitly
(``OP_LEGACY_SERVER_CONNECT``); certificate verification stays on. FEHD terms apply and
the service limits repeated sessions, so poll only a few times a day.

Stdlib only.
"""

import argparse
import json
import os
import re
import ssl
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "hk_cremation_sessions"
URL = "https://app.fehd.gov.hk/cremview/viewAvailableSession.do?lang=en"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DATE_HEADER = "Date (dd/mm/yyyy)"
SESSION = re.compile(r"^(\d+)\s*\(\s*(\d+)\s*\)$")


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


def _context():
    context = ssl.create_default_context()
    context.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
    return context


def fetch_sessions(timeout: int = 30):
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout, context=_context()) as response:
        text = response.read().decode("utf-8", "replace")
    parser = Tables()
    parser.feed(text)
    # "Last updated:" and its timestamp are split across elements, so read the parsed cell.
    updated = next(
        (
            match.group(1)
            for table in parser.tables
            for row in table
            for cell in row
            if (match := re.fullmatch(r"Last updated:\s*([0-9/]+\s+[0-9:]+)", cell))
        ),
        None,
    )
    table = next((table for table in parser.tables if table and table[0][:1] == [DATE_HEADER]), None)
    if table is None or not updated:
        raise ValueError("FEHD page is missing the availability table or update time")
    headers = table[0]
    crematoria = headers[2:]
    emitted = 0
    for cells in table[1:]:
        if len(cells) != len(headers):
            raise ValueError("availability row has an unexpected shape")
        date, total = cells[0], cells[1]
        for crematorium, value in zip(crematoria, cells[2:]):
            match = SESSION.match(value)
            record = {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{date}:{crematorium}",
                "session_date": date,
                "crematorium": crematorium,
                "raw_value": value,
                "available_sessions": int(match.group(1)) if match else None,
                "sessions_under_booking": int(match.group(2)) if match else None,
                "service_state": "in_service" if match else value,
                "day_total_sessions": int(total) if total.isdigit() else total,
                "publisher_updated_at": updated,
            }
            emitted += 1
            yield record
    if not emitted:
        raise ValueError("availability table has no date rows")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_sessions(args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
