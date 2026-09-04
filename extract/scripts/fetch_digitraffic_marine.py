#!/usr/bin/env python3
"""Finland Digitraffic — aids-to-navigation fault snapshots.

Entry/fix timestamps and repeated open-state snapshots support repair-time and fault
survival analysis. Verified live 2026-09-03 at ``/api/aton/v1/faults``. The GeoJSON
collection includes both open and fixed faults. The endpoint returns HTTP 406 unless
the client advertises gzip support. Data is CC BY 4.0 and refreshes every ten minutes;
polling faster is wasteful.

Stdlib only.
"""
import argparse
import gzip
import json
import os
import urllib.request
from datetime import datetime, timezone

SOURCE = "digitraffic_marine_aton_faults"
URL = "https://meri.digitraffic.fi/api/aton/v1/faults"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(timeout: int):
    request = urllib.request.Request(
        URL,
        headers={
            "Accept-Encoding": "gzip",
            "Digitraffic-User": USER_AGENT,
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        if response.headers.get("Content-Encoding", "").lower() == "gzip":
            body = gzip.decompress(body)
    data = json.loads(body)
    if data.get("type") != "FeatureCollection" or not isinstance(data.get("features"), list):
        raise ValueError("Digitraffic response is not a GeoJSON FeatureCollection")
    return data


def fetch_faults(limit: int = 100, timeout: int = 30):
    fetched_at = datetime.now(timezone.utc).isoformat()
    document = _get(timeout)
    for feature in document["features"][:limit]:
        fault_id = (feature.get("properties") or {}).get("id")
        if fault_id is None:
            raise ValueError("fault feature is missing properties.id")
        record = dict(feature)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": str(fault_id),
            "publisher_updated_at": document.get("lastUpdated"),
        })
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_faults(max(0, args.limit), args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
