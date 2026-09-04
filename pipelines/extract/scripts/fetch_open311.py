#!/usr/bin/env python3
"""Open311 GeoReport v2 — public service-request lifecycle snapshots.

Requests expose requested/updated times, status, service type, location, and public
resolution notes. Austin's official implementation was verified live without a key on
2026-09-03; anonymous reads are limited to ten requests per minute. This client uses at
most one request per page and never submits or mutates requests. Additional cities must
be verified before adding them to the endpoint map.

Stdlib only.
"""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

ENDPOINTS = {"austin": "https://311.austintexas.gov/open311/v2/requests.json"}
SOURCE = "open311_service_requests"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_requests(city: str = "austin", limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    emitted = 0
    page = 1
    while emitted < limit:
        size = min(100, limit - emitted)
        query = urllib.parse.urlencode({"page": page, "per_page": size})
        request = urllib.request.Request(
            f"{ENDPOINTS[city]}?{query}", headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            rows = json.load(response)
        if not isinstance(rows, list):
            raise TypeError("Open311 response is not a list")
        for row in rows:
            request_id = row.get("service_request_id")
            if not request_id:
                raise ValueError("Open311 request is missing service_request_id")
            record = dict(row)
            record.update(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": f"{city}|{request_id}",
                    "city": city,
                }
            )
            yield record
            emitted += 1
        if len(rows) < size:
            break
        page += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--city", choices=tuple(ENDPOINTS), default="austin")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_requests(args.city, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
