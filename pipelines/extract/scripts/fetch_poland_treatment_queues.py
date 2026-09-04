#!/usr/bin/env python3
"""Poland NFZ — specialist-treatment queues and first available dates.

Daily snapshots measure appointment-frontier movement, wait inflation, and provider
staleness. Verified live 2026-09-03 through the official keyless queue API. Queries
require case, province, and benefit filters; defaults reproduce the vetted Mazowieckie
outpatient-clinic search. Records retain the API's accessibility and provider fields.
Follow NFZ reuse terms.

Stdlib only.
"""

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "poland_nfz_treatment_queues"
URL = "https://apinfz.nfz.gov.pl/app-itl-api-pcus/queues"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_queues(
    case: int = 1,
    province: str = "07",
    benefit: str = "poradnia",
    limit: int = 100,
    timeout: int = 30,
):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    emitted = 0
    page = 1
    while emitted < limit:
        page_size = min(25, limit - emitted)
        query = urllib.parse.urlencode(
            {
                "case": case,
                "province": province,
                "benefit": benefit,
                "page": page,
                "limit": page_size,
                "format": "json",
            }
        )
        request = urllib.request.Request(
            f"{URL}?{query}", headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        rows = document.get("data")
        if not isinstance(rows, list):
            raise TypeError("NFZ response is missing data")
        published = (document.get("meta") or {}).get("date-published")
        for row in rows:
            queue_id = row.get("id")
            if not queue_id:
                raise ValueError("NFZ queue is missing id")
            record = dict(row)
            record.update(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": queue_id,
                    "publisher_updated_at": published,
                }
            )
            yield record
            emitted += 1
        if len(rows) < page_size:
            break
        page += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", type=int, default=1)
    parser.add_argument("--province", default="07")
    parser.add_argument("--benefit", default="poradnia")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_queues(
        args.case, args.province, args.benefit, args.limit, args.timeout
    ):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
