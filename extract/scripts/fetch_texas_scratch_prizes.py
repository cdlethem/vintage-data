#!/usr/bin/env python3
"""Texas Lottery — all scratch-ticket prize-level inventories.

The official CSV reports every current game's printed and claimed prizes by level plus
its as-of date. Daily snapshots expose depletion and game closure. Verified live
2026-09-04. Texas Lottery terms apply; this is inventory data, not gambling advice.

Stdlib only.
"""

import argparse
import csv
import io
import json
import os
import re
import urllib.request
from datetime import datetime, timezone

SOURCE = "texas_scratch_prizes"
URL = "https://www.texaslottery.com/export/sites/lottery/Games/Scratch_Offs/scratchoff.csv"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
EXPECTED = ["Game Number", "Game Name", "Game Close Date", "Ticket Price", "Prize Level", "Total Prizes in Level", "Prizes Claimed"]


def fetch_prizes(limit: int = 10000, timeout: int = 30):
    if limit <= 0: return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        text = response.read().decode("utf-8-sig", "replace")
    lines = text.splitlines()
    if len(lines) < 2: raise ValueError("scratch-ticket CSV is empty")
    title = next(csv.reader([lines[0]]))[0]
    match = re.fullmatch(r"Scratch-Off Prizes as of (.+)", title)
    if not match: raise ValueError("scratch-ticket CSV is missing its as-of date")
    reader = csv.DictReader(io.StringIO("\n".join(lines[1:])))
    if reader.fieldnames != EXPECTED: raise ValueError("scratch-ticket CSV headers changed")
    for row in list(reader)[:limit]:
        game = row["Game Number"].strip(); prize = row["Prize Level"].strip()
        if not game or not prize: raise ValueError("scratch-ticket row is missing game or prize level")
        for field in ("Ticket Price", "Total Prizes in Level", "Prizes Claimed"):
            value = row[field].replace(",", "").strip()
            if value.isdigit(): row[field] = int(value)
        row.update({"source": SOURCE, "fetched_at": fetched_at, "id": f"{game}:{prize}", "publisher_updated_at": match.group(1)})
        yield row


def main():
    parser = argparse.ArgumentParser(); parser.add_argument("--limit", type=int, default=10000); parser.add_argument("--timeout", type=int, default=30); args = parser.parse_args()
    for record in fetch_prizes(args.limit, args.timeout): print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__": main()
