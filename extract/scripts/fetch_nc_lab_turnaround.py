#!/usr/bin/env python3
"""North Carolina Agronomic Services — current laboratory turnaround times.

The public PALS table reports moving processing estimates across soil, plant, waste,
media, solution, and nematode workflows. Daily snapshots expose seasonal queue pressure.
Verified live 2026-09-04. North Carolina public-information terms apply. Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "nc_agronomic_lab_turnaround"
URL = "https://apps.ncagr.gov/PALS/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
EXPECTED = ["Lab", "Report Type", "Processing Time"]


class Table(HTMLParser):
    def __init__(self):
        super().__init__(); self.rows = []; self.row = None; self.cell = None
    def handle_starttag(self, tag, attrs):
        if tag == "tr": self.row = []
        elif tag in ("th", "td") and self.row is not None: self.cell = []
    def handle_data(self, data):
        if self.cell is not None: self.cell.append(data)
    def handle_endtag(self, tag):
        if tag in ("th", "td") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split())); self.cell = None
        elif tag == "tr" and self.row is not None:
            if any(self.row): self.rows.append(self.row)
            self.row = None


def fetch_turnaround(timeout: int = 30):
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8", "replace")
    parser = Table(); parser.feed(text)
    try: start = parser.rows.index(EXPECTED)
    except ValueError as error: raise ValueError("PALS page is missing its turnaround table") from error
    count = 0
    for cells in parser.rows[start + 1 :]:
        if len(cells) != 3: break
        lab, report_type, processing_time = cells
        match = re.fullmatch(r"(\d+)\s+(Day|Days|Week|Weeks)", processing_time, re.I)
        if not match: raise ValueError(f"unexpected processing time {processing_time!r}")
        value, unit = int(match.group(1)), match.group(2).lower()
        days = value * 7 if unit.startswith("week") else value
        yield {
            "source": SOURCE, "fetched_at": fetched_at,
            "id": f"{lab}:{report_type}", "lab": lab, "report_type": report_type,
            "processing_time": processing_time, "processing_days": days,
        }
        count += 1
    if not count: raise ValueError("PALS turnaround table has no data rows")


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--timeout", type=int, default=30); args = parser.parse_args()
    for record in fetch_turnaround(args.timeout): print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__": main()
