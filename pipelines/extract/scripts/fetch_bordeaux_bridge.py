#!/usr/bin/env python3
"""Bordeaux — current Chaban-Delmas bridge state.

The publisher refreshes this one-record OpenDataSoft dataset about every 2.5 minutes.
Successive snapshots preserve opening duration and sensor transitions. Verified live
2026-09-04. Bordeaux Métropole open-data terms apply.

Stdlib only.
"""

import argparse
import json
import urllib.request
from datetime import datetime, timezone

SOURCE = "bordeaux_chaban_delmas_bridge"
URL = "https://datahub.bordeaux-metropole.fr/api/explore/v2.1/catalog/datasets/ci_passa_p/records?limit=20"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_state(timeout: int = 30):
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("results")
    if not isinstance(rows, list) or not rows:
        raise ValueError("Bordeaux response contains no bridge state")
    for row in rows:
        bridge_id = row.get("ident") or row.get("gid")
        if not bridge_id or "etat" not in row:
            raise ValueError("bridge row is missing ident or etat")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": str(bridge_id)})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_state(args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
