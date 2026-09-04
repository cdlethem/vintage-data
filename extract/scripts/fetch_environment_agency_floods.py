#!/usr/bin/env python3
"""Environment Agency — active flood warnings for England.

Warnings are explicit transient objects with severity, message, raised time, and state
change timestamps. The official keyless endpoint was verified live 2026-09-04 after the
archive's earlier timeout. Empty output is a valid no-warning state. Data is licensed
under OGL 3.0; poll every 15 minutes during flood events and less often otherwise.

Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

SOURCE = "environment_agency_flood_warnings"
URL = "https://environment.data.gov.uk/flood-monitoring/id/floods"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_warnings(limit: int = 1000, timeout: int = 60):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("items")
    if not isinstance(rows, list):
        raise TypeError("flood response is missing items")
    for row in rows[:limit]:
        warning_id = row.get("@id") or row.get("floodAreaID")
        if not warning_id:
            raise ValueError("flood warning is missing @id and floodAreaID")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": warning_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_warnings(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
