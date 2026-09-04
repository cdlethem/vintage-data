#!/usr/bin/env python3
"""Tokyo MOU — current-month port-state-control detentions.

Submits the public APCIS form's own month filter and extracts the detention table,
including release date and deficiency details. The list is updated in real time. Verified
live 2026-09-04. Tokyo MOU terms apply. Stdlib only.
"""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

SOURCE = "tokyo_mou_detentions"
URL = "https://apcis.tmou.org/isss/public_apcis.php?Mode=DetList"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
EXPECTED = ["IMO No.", "Ship Name", "Ship Flag", "Year of build", "Gross Tonnage", "Ship Type", "Classification society", "Related ROs", "Company", "Place of detention", "Date of detention", "Date of release", "Nature of deficiencies"]


class Tables(HTMLParser):
    def __init__(self): super().__init__(); self.tables=[]; self.table=None; self.row=None; self.cell=None
    def handle_starttag(self, tag, attrs):
        if tag == "table": self.table=[]
        elif tag == "tr" and self.table is not None: self.row=[]
        elif tag in ("th", "td") and self.row is not None: self.cell=[]
        elif tag == "br" and self.cell is not None: self.cell.append(" ")
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


def fetch_detentions(year=None, month=None, limit: int = 1000, timeout: int = 60):
    if limit <= 0: return
    now = datetime.now(ZoneInfo("Asia/Tokyo")); year = year or now.year; month = month or now.month
    payload = urllib.parse.urlencode({"Mode":"DetList","MOU":"","Auth":"","Src":"online","Type":"Auth","Month":f"{month:02d}","Year":str(year),"SaveFile":""}).encode()
    request = urllib.request.Request(URL, data=payload, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response: text=response.read().decode("utf-8","replace")
    parser=Tables(); parser.feed(text)
    table=next((t for t in parser.tables if t and t[0][1:] == EXPECTED),None)
    if table is None: raise ValueError("APCIS response is missing the detention table")
    for cells in table[1:limit+1]:
        if len(cells) != len(EXPECTED)+1: raise ValueError("detention row has an unexpected shape")
        row=dict(zip(EXPECTED,cells[1:])); imo=row["IMO No."]; detained=row["Date of detention"]
        if not imo or not detained: raise ValueError("detention row is missing IMO number or detention date")
        row.update({"source":SOURCE,"fetched_at":fetched_at,"id":f"{imo}:{detained}","query_year":year,"query_month":month})
        yield row


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--year",type=int); parser.add_argument("--month",type=int,choices=range(1,13)); parser.add_argument("--limit",type=int,default=1000); parser.add_argument("--timeout",type=int,default=60); args=parser.parse_args()
    for record in fetch_detentions(args.year,args.month,args.limit,args.timeout): print(json.dumps(record,ensure_ascii=False))


if __name__ == "__main__": main()
