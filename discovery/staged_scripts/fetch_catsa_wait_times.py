#!/usr/bin/env python3
"""CATSA — security-checkpoint wait bands at every listed Canadian airport.

CATSA refreshes these pages every two minutes and derives waits from boarding-pass scans.
The published band is stored verbatim, including ``Not available``; no numeric midpoint is
invented. The national wait-times index listed 17 airports when verified live 2026-09-04,
and a bare run covers all of them. CATSA website terms apply.

Per-airport failures are reported on stderr and skipped. Stdlib only.
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "catsa_security_waits"
INDEX_URL = "https://www.catsa-acsta.gc.ca/en/current-wait-times"
BASE_URL = "https://www.catsa-acsta.gc.ca/en/airport/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
EXPECTED = ["Checkpoint name", "Wait time *"]


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


def _get(url: str, timeout: int) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", "replace")


def discover_airports(timeout: int = 30):
    slugs = dict.fromkeys(re.findall(r'href="/en/airport/([^"#?]+)"', _get(INDEX_URL, timeout)))
    if not slugs:
        raise ValueError("CATSA index exposed no airport pages; structure changed")
    return list(slugs)


def _checkpoints(slug: str, fetched_at: str, timeout: int):
    parser = Tables()
    parser.feed(_get(BASE_URL + slug, timeout))
    table = next((table for table in parser.tables if table and table[0] == EXPECTED), None)
    if table is None:
        raise ValueError("page is missing the checkpoint wait-time table")
    rows = []
    for cells in table[1:]:
        if len(cells) != 2 or not cells[0]:
            raise ValueError("checkpoint row has an unexpected shape")
        rows.append(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{slug}:{cells[0]}",
                "airport_slug": slug,
                "checkpoint": cells[0],
                "wait_time": cells[1],
            }
        )
    if not rows:
        raise ValueError("checkpoint table has no rows")
    return rows


def fetch_waits(slugs=(), timeout: int = 30):
    fetched_at = datetime.now(timezone.utc).isoformat()
    slugs = list(slugs) or discover_airports(timeout)
    failures = 0
    for slug in slugs:
        try:
            rows = _checkpoints(slug, fetched_at, timeout)
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError, OSError) as error:
            failures += 1
            print(f"{slug}: {type(error).__name__}: {error}", file=sys.stderr)
            continue
        yield from rows
    if failures == len(slugs):
        raise RuntimeError(f"all {failures} airport pages failed")
    if failures:
        print(f"{failures} of {len(slugs)} airport pages failed", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("airport_slugs", nargs="*", help="omit to cover every listed airport")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_waits(args.airport_slugs, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
