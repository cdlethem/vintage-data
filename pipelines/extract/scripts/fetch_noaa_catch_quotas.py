#!/usr/bin/env python3
"""NOAA Southeast catch shares — quota and landings for every share category and year.

The current report publishes, per fishery year, each system and share category's start-of-
year quota, in-season quota increase and its effective date, end-of-year quota, landings,
percentage landed, and quota remaining. Landings for the open year update as they load, so
snapshots yield depletion velocity while older years capture reporting backfill. Twenty
year sections were verified live 2026-09-04. US federal public data.

Administrative quota changes are distinct from catch: both are carried verbatim.

Stdlib only.
"""

import argparse
import json
import re
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "noaa_catch_share_quotas"
URL = "https://secatchshares.fisheries.noaa.gov/getQuotasAndCatchAllowancesReport"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
EXPECTED = [
    "System",
    "Share Category",
    "Start of Year Quota",
    "Quota Increase",
    "Quota Increase Date",
    "End of Year Quota",
    "Landings",
    "% Quota Landed",
    "Quota Remaining",
]
NUMERIC = ("Start of Year Quota", "Quota Increase", "End of Year Quota", "Landings", "% Quota Landed", "Quota Remaining")
YEAR_HEADING = re.compile(r"<h[1-5][^>]*>\s*(\d{4})\s*</h[1-5]>")


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


def _number(value: str):
    compact = value.replace(",", "").strip()
    if not compact:
        return None
    try:
        return int(compact) if compact.isdigit() else float(compact)
    except ValueError:
        return value


def fetch_quotas(years=(), timeout: int = 60):
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8", "replace")
    headings = YEAR_HEADING.findall(text)
    parser = Tables()
    parser.feed(text)
    tables = [table for table in parser.tables if table and table[0] == EXPECTED]
    if not tables or len(tables) != len(headings):
        raise ValueError(
            f"report structure changed: {len(headings)} year headings vs {len(tables)} quota tables"
        )
    wanted = {str(year) for year in years}
    emitted = 0
    for year, table in zip(headings, tables):
        if wanted and year not in wanted:
            continue
        for cells in table[1:]:
            if len(cells) != len(EXPECTED):
                raise ValueError("quota row has an unexpected shape")
            row = dict(zip(EXPECTED, cells))
            row.update({f"{name}_value": _number(row[name]) for name in NUMERIC})
            row.update(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": f"{year}:{row['System']}:{row['Share Category']}",
                    "fishery_year": int(year),
                }
            )
            emitted += 1
            yield row
    if not emitted:
        raise ValueError("no quota rows matched")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("years", nargs="*", type=int, help="omit to emit every published year")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_quotas(args.years, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
