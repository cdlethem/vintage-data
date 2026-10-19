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
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "sncf_station_equipment"
URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/etat-fonctionnement-elevatique-gare/records"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
# SNCF's OpenDataSoft edge intermittently resets connections and times out
# TLS handshakes (observed 2026-10-19: SSL handshake timeout in task log).
# Transient transport errors and 429/5xx retry with 1s/2s backoff so a
# brief flap does not lose a whole run.
MAX_ATTEMPTS = 3
RETRY_BASE_DELAY = 1.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def _get(url: str, timeout: int):
    """Fetch one page, retrying transient failures with a finite bound.

    Non-transient errors (4xx) and exhausted attempts raise the final
    failure unchanged.
    """
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            reason = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            reason = str(exc.reason if isinstance(exc, urllib.error.URLError) else exc)
        delay = RETRY_BASE_DELAY * (2 ** attempt)
        print(f"sncf_station_equipment: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)


def fetch_equipment(limit: int = 3000, timeout: int = 30):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    offset = 0
    while offset < limit:
        size = min(100, limit - offset)
        query = urllib.parse.urlencode({"limit": size, "offset": offset})
        document = _get(f"{URL}?{query}", timeout)
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
