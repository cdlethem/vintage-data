#!/usr/bin/env python3
"""Rhode Island — current-month animal-shelter intake and outcome ledgers.

Fetches the publisher's aggregate, dog, cat, ferret, and other-animal reports in one run.
The cumulative tables and generation timestamps reveal daily flow and revisions without
collecting animal-level data. Verified live 2026-09-04. Rhode Island terms apply.

Stdlib only.
"""

import argparse
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser

SOURCE = "rhode_island_shelter_stats"
URL = "https://www.ri.gov/app/dem/shelter/reports/stats"
SPECIES = ("animals", "dogs", "cats", "ferrets", "others")
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


class Tables(HTMLParser):
    def __init__(self): super().__init__(); self.tables=[]; self.table=None; self.row=None; self.cell=None
    def handle_starttag(self, tag, attrs):
        if tag == "table": self.table=[]
        elif tag == "tr" and self.table is not None: self.row=[]
        elif tag in ("th", "td") and self.row is not None: self.cell=[]
    def handle_data(self, data):
        if self.cell is not None: self.cell.append(data)
    def handle_endtag(self, tag):
        if tag in ("th", "td") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split())); self.cell=None
        elif tag == "tr" and self.row is not None:
            if any(self.row): self.table.append(self.row)
            self.row=None
        elif tag == "table" and self.table is not None:
            self.tables.append(self.table); self.table=None


def fetch_stats(limit: int = 10000, timeout: int = 30):
    if limit <= 0: return
    fetched_at = datetime.now(timezone.utc).isoformat(); emitted = 0
    for species in SPECIES:
        query = urllib.parse.urlencode({"filter_by": species, "time_period": "current_month"})
        request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8", "replace")
        updated = re.search(r"generated at\s+([^<]+)", text, re.I)
        parser = Tables(); parser.feed(text)
        table = next((t for t in parser.tables if len(t) > 2 and t[1][:2] == ["Entity Name", "Status"]), None)
        if table is None or not updated: raise ValueError(f"{species}: report table or generation time missing")
        headers = table[1]
        for cells in table[2:]:
            if emitted >= limit: return
            if len(cells) != len(headers): continue
            row = dict(zip(headers, cells))
            entity = row.get("Entity Name")
            if not entity: continue
            for key, value in tuple(row.items()):
                compact = value.replace(",", "")
                if compact.isdigit(): row[key] = int(compact)
            row.update({"source": SOURCE, "fetched_at": fetched_at, "id": f"{species}:{entity}", "species": species, "publisher_updated_at": updated.group(1).strip()})
            emitted += 1; yield row


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--limit",type=int,default=10000); parser.add_argument("--timeout",type=int,default=30); args=parser.parse_args()
    for record in fetch_stats(args.limit,args.timeout): print(json.dumps(record,ensure_ascii=False))


if __name__ == "__main__": main()
