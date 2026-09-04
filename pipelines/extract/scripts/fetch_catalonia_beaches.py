#!/usr/bin/env python3
"""Catalonia — current beach flags and conditions.

Hourly summer updates expose bathing flags, jellyfish, water clarity, sea state, and
weather by beach. Verified live 2026-09-03 through the official Socrata resource; rows
were current through 2026-08-31. The default resource order begins with 2019 records,
so this client explicitly sorts ``estat_data`` descending. Follow Catalonia open-data
terms.

Stdlib only.
"""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "catalonia_beach_status"
URL = "https://analisi.transparenciacatalunya.cat/resource/4baz-cjv2.json"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_beaches(limit: int = 1000, timeout: int = 30):
    if limit <= 0:
        return
    query = urllib.parse.urlencode(
        {"$limit": min(limit, 50000), "$order": "estat_data DESC"}
    )
    request = urllib.request.Request(
        f"{URL}?{query}", headers={"User-Agent": USER_AGENT}
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        rows = json.load(response)
    if not isinstance(rows, list):
        raise TypeError("Catalonia Socrata response is not a list")
    for row in rows[:limit]:
        beach_id = row.get("codiplatja")
        if not beach_id:
            raise ValueError("beach status is missing codiplatja")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": beach_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_beaches(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
