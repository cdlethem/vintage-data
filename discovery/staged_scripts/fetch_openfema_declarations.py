#!/usr/bin/env python3
"""OpenFEMA — newest disaster declaration summary revisions.

Declarations expand through county and program amendments after first publication.
Verified live 2026-09-03 against the v2 DisasterDeclarationsSummaries endpoint; the
formerly documented v1 path returns 404. Declaration, incident, filing, and
``lastRefresh`` clocks differ, and the publisher supplies a record hash for detecting
amendments. US federal public data. OpenFEMA advertises a 20-minute refresh interval.

Stdlib only.
"""
import argparse
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "openfema_disaster_declarations"
URL = "https://www.fema.gov/api/open/v2/DisasterDeclarationsSummaries"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_declarations(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    query = urllib.parse.urlencode({
        "$top": min(limit, 1000),
        "$orderby": "lastRefresh desc",
    })
    request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    rows = document.get("DisasterDeclarationsSummaries")
    if not isinstance(rows, list):
        raise ValueError("OpenFEMA response is missing DisasterDeclarationsSummaries")
    for row in rows[:limit]:
        record_id = row.get("id")
        if not record_id:
            raise ValueError("declaration summary is missing id")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": record_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_declarations(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
