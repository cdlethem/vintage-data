#!/usr/bin/env python3
"""Environment Agency — active flood warnings for England.

Warnings are explicit transient objects with severity, message, raised time, and state
change timestamps. Request JSON with an identifying User-Agent and language preference;
An empty items list is a valid no-warning state, but a nonpositive limit must
not skip the request and appear successful. Retry HTTP 503 once after a short
delay; persistent HTTP errors fail the task with a status-only diagnostic.
Data is licensed under OGL 3.0; poll every 15 minutes during flood events and less
often otherwise.

Stdlib only.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "environment_agency_flood_warnings"
URL = "https://environment.data.gov.uk/flood-monitoring/id/floods"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
REQUEST_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
}
SERVICE_RETRY_DELAY_SECONDS = 3


def fetch_warnings(limit: int = 1000, timeout: int = 60):
    if limit <= 0:
        raise ValueError("limit must be positive")
    request = urllib.request.Request(URL, headers=REQUEST_HEADERS)
    fetched_at = datetime.now(timezone.utc).isoformat()
    for attempt in range(2):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                document = json.load(response)
            break
        except urllib.error.HTTPError as error:
            if error.code != 503 or attempt == 1:
                raise
            error.close()
            time.sleep(SERVICE_RETRY_DELAY_SECONDS)
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
    try:
        for record in fetch_warnings(args.limit, args.timeout):
            print(json.dumps(record, ensure_ascii=False))
    except urllib.error.HTTPError as error:
        # Avoid echoing response reasons or URLs that might contain credentials.
        print(f"Environment Agency flood warnings: HTTP {error.code}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
