#!/usr/bin/env python3
"""AMSAT — crowd-reported amateur-satellite operating status.

Recent reception reports provide a human-observed complement to machine telemetry.
Verified live 2026-09-03 through AMSAT's official keyless v1 API. Reports include a
stable ID, satellite, report time, canonical status, grid square, and amateur callsign.
Callsigns are public attribution identifiers; do not repurpose them for profiling. Poll
every 15 minutes.

Stdlib only.
"""

import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "amsat_status_reports"
URL = "https://www.amsat.org/status/api/v1/reports.php"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_reports(hours: int = 24, limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    query = urllib.parse.urlencode({"hours": hours, "limit": min(limit, 1000)})
    request = urllib.request.Request(
        f"{URL}?{query}", headers={"User-Agent": USER_AGENT}
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("data")
    if not isinstance(rows, list):
        raise TypeError("AMSAT response is missing data")
    for row in rows[:limit]:
        report_id = row.get("id")
        if report_id is None:
            raise ValueError("AMSAT report is missing id")
        record = dict(row)
        record.update(
            {"source": SOURCE, "fetched_at": fetched_at, "id": str(report_id)}
        )
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_reports(args.hours, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
