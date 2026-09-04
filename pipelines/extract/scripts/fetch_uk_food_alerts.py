#!/usr/bin/env python3
"""UK Food Standards Agency — product recalls and food alerts.

Structured alerts carry products, hazards, allergens, businesses, status, and revision
timestamps. Verified live 2026-09-04 against the official JSON API. Its default order
starts in 2018, so this client explicitly sorts by descending ``modified`` time. OGL
3.0 applies. Poll hourly or daily and retain revisions to already-seen alert IDs.

Stdlib only.
"""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "uk_food_alerts"
URL = "https://data.food.gov.uk/food-alerts/id.json"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_alerts(limit: int = 100, since: str | None = None, timeout: int = 30):
    if limit <= 0:
        return
    params = {"_limit": min(limit, 1000), "_sort": "-modified"}
    if since:
        params["since"] = since
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{URL}?{query}", headers={"User-Agent": USER_AGENT}
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("items")
    if not isinstance(rows, list):
        raise TypeError("food-alert response is missing items")
    for row in rows[:limit]:
        alert_id = row.get("notation") or row.get("@id")
        if not alert_id:
            raise ValueError("food alert is missing notation and @id")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": alert_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--since", help="publisher-supported lower timestamp bound")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_alerts(args.limit, args.since, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
