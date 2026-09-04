#!/usr/bin/env python3
"""Ireland EPA — active bathing-water alerts.

Alert snapshots expose restriction duration, escalation, and eventual resolution.
Verified live 2026-09-03 against the official keyless API, which returned 13 active
incidents. IDs are publisher incident IDs; disappearance must be interpreted only after
a successful complete run. Follow Ireland EPA open-data terms and poll every 15–60
minutes during bathing season.

Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

SOURCE = "ireland_bathing_water_alerts"
URL = "https://data.epa.ie/bw/api/v1/alerts"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_alerts(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("list")
    if not isinstance(rows, list):
        raise TypeError("EPA response is missing list")
    for row in rows[:limit]:
        incident_id = row.get("incident_id")
        if incident_id is None:
            raise ValueError("bathing alert is missing incident_id")
        record = dict(row)
        record.update(
            {"source": SOURCE, "fetched_at": fetched_at, "id": str(incident_id)}
        )
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_alerts(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
