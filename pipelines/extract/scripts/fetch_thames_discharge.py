#!/usr/bin/env python3
"""Thames Water — storm-overflow discharge status.

Five-minute status snapshots support discharge-duration, catchment-synchrony, and
sensor-staleness analysis. Verified live 2026-09-03 against the official v2 open-data
endpoint, which returned 573 monitored locations. Publisher timestamps can freeze or be
corrected; retain them separately from ``fetched_at``. Thames Water's open-data terms
apply.

Stdlib only.
"""

import argparse
import json
import urllib.request
from datetime import datetime, timezone

SOURCE = "thames_water_discharge"
URL = "https://api.thameswater.co.uk/opendata/v2/discharge/status"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_status(limit: int = 1000, timeout: int = 60):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("items")
    if not isinstance(rows, list):
        raise TypeError("Thames Water response is missing items")
    for row in rows[:limit]:
        location_id = row.get("uniqueId")
        if not location_id:
            raise ValueError("discharge location is missing uniqueId")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": location_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_status(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
