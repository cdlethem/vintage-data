#!/usr/bin/env python3
"""SNCF — station elevator and escalator operating state.

Equipment-level snapshots expose accessibility outages and repair duration. Verified
live 2026-09-03 through SNCF's OpenDataSoft API: 2,668 records with stable station and
equipment IDs and ``OK``/``KO``/unknown state. The publisher refreshes this dataset
hourly. This client pages in batches of 100 and retains the source fields unchanged.
Follow SNCF open-data reuse terms.

Stdlib only.
"""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "sncf_station_equipment"
URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/etat-fonctionnement-elevatique-gare/records"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_equipment(limit: int = 3000, timeout: int = 30):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    offset = 0
    while offset < limit:
        size = min(100, limit - offset)
        query = urllib.parse.urlencode({"limit": size, "offset": offset})
        request = urllib.request.Request(
            f"{URL}?{query}", headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        rows = document.get("results")
        if not isinstance(rows, list):
            raise TypeError("SNCF response is missing results")
        for row in rows:
            equipment_id = row.get("id")
            if not equipment_id:
                raise ValueError("SNCF equipment is missing id")
            record = dict(row)
            record.update(
                {"source": SOURCE, "fetched_at": fetched_at, "id": equipment_id}
            )
            yield record
        offset += len(rows)
        if not rows or offset >= document.get("total_count", offset):
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=3000)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_equipment(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
