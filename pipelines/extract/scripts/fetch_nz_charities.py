#!/usr/bin/env python3
"""New Zealand Charities Register — newest organization registrations.

Daily snapshots capture registrations, deregistrations, and changing organization
state. Verified live 2026-09-03 against the official OData service. The publisher's
working endpoint is HTTP, wraps records under the legacy ``d.results`` envelope, and
may encode dates as ``/Date(...)`` strings. Data is CC BY 3.0 NZ. Published contact
information must not be harvested for marketing; poll once daily.

Stdlib only.
"""
import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "nz_charities_recent_registrations"
URL = "http://www.odata.charities.govt.nz/Organisations"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_charities(limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    query = urllib.parse.urlencode({
        "$top": min(limit, 1000),
        "$orderby": "DateRegistered desc",
        "$format": "json",
    })
    request = urllib.request.Request(
        f"{URL}?{query}",
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    envelope = document.get("d")
    rows = envelope.get("results") if isinstance(envelope, dict) else envelope
    if not isinstance(rows, list):
        raise ValueError("Charities OData response is missing d.results")
    for row in rows[:limit]:
        registration_number = row.get("CharityRegistrationNumber")
        if not registration_number:
            raise ValueError("charity is missing CharityRegistrationNumber")
        record = dict(row)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": str(registration_number),
        })
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_charities(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
