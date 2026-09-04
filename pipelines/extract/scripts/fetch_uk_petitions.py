#!/usr/bin/env python3
"""UK Parliament — open e-petition counters and lifecycle state.

Signature growth, threshold crossings, government responses, and debate scheduling form
a near-live civic diffusion series. Verified live 2026-09-03 against the official
paginated JSON feed. Records retain the API's JSON:API attributes and links; petition
IDs are stable across snapshots. Poll hourly, or every 15 minutes for a deliberately
selected hot set. Follow Parliament's reuse terms.

Stdlib only.
"""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "uk_parliament_petitions"
URL = "https://petition.parliament.uk/petitions.json"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_petitions(state: str = "open", limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    next_url = f"{URL}?{urllib.parse.urlencode({'state': state})}"
    emitted = 0
    while next_url and emitted < limit:
        request = urllib.request.Request(next_url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        rows = document.get("data")
        if not isinstance(rows, list):
            raise TypeError("petition response is missing data")
        for row in rows:
            petition_id = row.get("id")
            if petition_id is None:
                raise ValueError("petition is missing id")
            record = dict(row)
            record.update(
                {"source": SOURCE, "fetched_at": fetched_at, "id": str(petition_id)}
            )
            yield record
            emitted += 1
            if emitted >= limit:
                return
        next_url = (document.get("links") or {}).get("next")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--state", choices=("open", "closed", "rejected"), default="open"
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_petitions(args.state, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
