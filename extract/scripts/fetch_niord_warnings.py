#!/usr/bin/env python3
"""Danish Maritime Authority Niord — every published navigational warning and notice.

The public collection returns the complete active set (237 messages when verified live
2026-09-04) with stable UUIDs, create/update times, series, type, status, areas, charts,
publish and follow-up dates, localized text, references, and geometry. Successive
snapshots recover warning lifetime, revisions, and publish-to-cancel transitions.

The public API exposes published messages only and ignores status/limit filters, so this
takes the whole collection in one request. Danish Maritime Authority terms apply. The
production host is used deliberately; the alpha sibling contains junk data.

Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

SOURCE = "niord_navigational_warnings"
URL = "https://niord.dma.dk/rest/public/v1/messages"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_warnings(main_type=None, timeout: int = 120):
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        rows = json.load(response)
    if not isinstance(rows, list) or not rows:
        raise ValueError("Niord response is not a non-empty list")
    for row in rows:
        warning_id = row.get("id")
        if not warning_id or not row.get("status"):
            raise ValueError("Niord message is missing id or status")
        if main_type and row.get("mainType") != main_type:
            continue
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": warning_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-type", choices=("NM", "NW"), help="omit to emit both notices and warnings")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    for record in fetch_warnings(args.main_type, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
