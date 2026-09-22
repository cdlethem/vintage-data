#!/usr/bin/env python3
"""Tokyo MOU — current-month port-state-control detentions.

Submits the public APCIS form's own month filter and extracts the detention table,
including release date and deficiency details. The list is updated in real time. Verified
live 2026-09-04. Tokyo MOU terms apply. Stdlib only.
"""

import argparse
import errno
import json
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from zoneinfo import ZoneInfo

SOURCE = "tokyo_mou_detentions"
URL = "https://apcis.tmou.org/isss/public_apcis.php?Mode=DetList"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
EXPECTED = ["IMO No.", "Ship Name", "Ship Flag", "Year of build", "Gross Tonnage", "Ship Type", "Classification society", "Related ROs", "Company", "Place of detention", "Date of detention", "Date of release", "Nature of deficiencies"]

MAX_ATTEMPTS = 3
RETRY_DELAYS = (1, 2)


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


def is_timeout_error(error):
    """Return whether *error* is a real timeout, including urllib wrappers."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, (TimeoutError, socket.timeout)):
            return True
        if isinstance(error, OSError) and error.errno == errno.ETIMEDOUT:
            return True
        if isinstance(error, urllib.error.URLError):
            error = error.reason
        else:
            return False
    return False


def fetch_response(request, timeout):
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", "replace")
        except Exception as error:
            if not is_timeout_error(error):
                raise
            if attempt == MAX_ATTEMPTS:
                raise TimeoutError(
                    f"APCIS request timed out after {attempt} attempts (timeout)"
                ) from error
            time.sleep(RETRY_DELAYS[attempt - 1])


def fetch_detentions(year=None, month=None, limit: int = 1000, timeout: int = 60):
    if limit <= 0: return
    now = datetime.now(ZoneInfo("Asia/Tokyo")); year = year or now.year; month = month or now.month
    payload = urllib.parse.urlencode({"Mode":"DetList","MOU":"","Auth":"","Src":"online","Type":"Auth","Month":f"{month:02d}","Year":str(year),"SaveFile":""}).encode()
    request = urllib.request.Request(URL, data=payload, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    text = fetch_response(request, timeout)
    parser=Tables(); parser.feed(text)
    table=next((t for t in parser.tables if t and t[0][1:] == EXPECTED),None)
    if table is None: raise ValueError("APCIS response is missing the detention table")
    for cells in table[1:limit+1]:
        if len(cells) != len(EXPECTED) + 1: raise ValueError("detention row has an unexpected shape")
        row=dict(zip(EXPECTED,cells[1:])); imo=row["IMO No."]; detained=row["Date of detention"]
        if not imo or not detained: raise ValueError("detention row is missing IMO number or detention date")
        row.update({"source":SOURCE,"fetched_at":fetched_at,"id":f"{imo}:{detained}","query_year":year,"query_month":month})
        yield row


def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--year",type=int); parser.add_argument("--month",type=int,choices=range(1,13)); parser.add_argument("--limit",type=int,default=1000); parser.add_argument("--timeout",type=int,default=60); args=parser.parse_args()
    for record in fetch_detentions(args.year,args.month,args.limit,args.timeout): print(json.dumps(record,ensure_ascii=False))


if __name__ == "__main__": main()
