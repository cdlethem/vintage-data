#!/usr/bin/env python3
"""DriveBC Open511 — active road events.

Closures, incidents, construction, restrictions, and extreme-condition impacts form a
public road-state event stream. Verified live 2026-09-04 against British Columbia's
official Open511 implementation. Events carry stable namespaced IDs, schedules,
geography, affected roads, severity, and update timestamps. Follow the Open Government
Licence–Canada and poll every 10–15 minutes.

Stdlib only.
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "drivebc_open511_events"
URL = "https://api.open511.gov.bc.ca/events"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_events(limit: int = 1000, timeout: int = 60):
    if limit <= 0:
        return
    query = urllib.parse.urlencode({"format": "json", "limit": min(limit, 1000)})
    request = urllib.request.Request(
        f"{URL}?{query}", headers={"User-Agent": USER_AGENT}
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("events")
    if not isinstance(rows, list):
        raise TypeError("Open511 response is missing events")
    for row in rows[:limit]:
        event_id = row.get("id")
        if not event_id:
            raise ValueError("Open511 event is missing id")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": event_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_events(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
