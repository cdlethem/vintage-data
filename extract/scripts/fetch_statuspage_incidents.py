#!/usr/bin/env python3
"""Atlassian Statuspage — public incident histories across providers.

A shared schema exposes incident state, affected components, and timestamped updates.
Verified live 2026-09-03 against GitHub, OpenAI, and Dropbox public pages. Statuspage's
public API is keyless and documented as not rate-limited, but five-minute polling is a
reasonable floor. Incident IDs are namespaced by Statuspage page ID to prevent
cross-provider collisions.

Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

PAGES = {
    "github": "https://www.githubstatus.com",
    "openai": "https://status.openai.com",
    "dropbox": "https://status.dropbox.com",
}
SOURCE = "statuspage_incidents"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_incidents(page: str = "github", limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    request = urllib.request.Request(
        f"{PAGES[page]}/api/v2/incidents.json",
        headers={"User-Agent": USER_AGENT},
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    page_info = document.get("page") or {}
    rows = document.get("incidents")
    if not page_info.get("id") or not isinstance(rows, list):
        raise ValueError("Statuspage response is missing page or incidents")
    for row in rows[:limit]:
        incident_id = row.get("id")
        if not incident_id:
            raise ValueError("Statuspage incident is missing id")
        record = dict(row)
        record.update(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{page_info['id']}|{incident_id}",
                "page": page_info,
            }
        )
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--page", choices=tuple(PAGES), default="github")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_incidents(args.page, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
