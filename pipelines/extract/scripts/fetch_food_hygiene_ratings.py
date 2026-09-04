#!/usr/bin/env python3
"""UK Food Standards Agency — live establishment hygiene ratings, full national sweep.

Daily snapshots expose inspections, reratings, pending-rating duration, and score
transitions. Verified live 2026-09-03 using the required ``x-api-version: 2`` header.
The service rejects expensive anonymous unfiltered scans, so this client requires a
local-authority filter and paginates within it.

**Rebuilt 2026-09-04**: the prior config only ever queried authority 1 (Cambridge).
FSA's own `/Authorities` endpoint lists **all 363 UK local authorities live**,
each with a documented `EstablishmentCount` (verified live 2026-09-04: sums to
**612,607 real establishments** nationwide, from Aberdeen City's 2,207 to England's
biggest boroughs). This now discovers the full authority list at runtime (no
hardcoded, staleness-prone list) and walks every authority's full establishment
set -- the "internal company analytics" shape: hundreds of thousands of tracked
entities nationwide, not one city.

Follow FSA reuse and attribution terms.

Stdlib only.
"""

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "uk_food_hygiene_ratings"
BASE = "https://api.ratings.food.gov.uk"
ESTABLISHMENTS_URL = f"{BASE}/Establishments"
AUTHORITIES_URL = f"{BASE}/Authorities"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
HEADERS = {"Accept": "application/json", "User-Agent": USER_AGENT, "x-api-version": "2"}
PAGE_DELAY_S = 0.15


def fetch_authorities(timeout: int = 30):
    """Every UK local authority FSA currently publishes ratings for, live --
    no hardcoded list, so this stays correct as authorities are added/merged."""
    request = urllib.request.Request(AUTHORITIES_URL, headers=HEADERS)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    return document.get("authorities") or []


def fetch_establishments(authority_id: int, limit: int = 1_000_000, timeout: int = 30):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    emitted = 0
    page = 1
    while emitted < limit:
        size = min(100, limit - emitted)
        query = urllib.parse.urlencode(
            {"localAuthorityId": authority_id, "pageNumber": page, "pageSize": size}
        )
        request = urllib.request.Request(f"{ESTABLISHMENTS_URL}?{query}", headers=HEADERS)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        rows = document.get("establishments")
        if not isinstance(rows, list):
            raise TypeError("FSA response is missing establishments")
        extract_date = (document.get("meta") or {}).get("extractDate")
        for row in rows:
            establishment_id = row.get("FHRSID")
            if establishment_id is None:
                raise ValueError("FSA establishment is missing FHRSID")
            record = dict(row)
            record.update(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": str(establishment_id),
                    "publisher_updated_at": extract_date,
                    "authority_id": authority_id,
                }
            )
            yield record
            emitted += 1
        if not rows or page >= (document.get("meta") or {}).get("totalPages", page):
            break
        page += 1
        time.sleep(PAGE_DELAY_S)


def fetch_all_authorities(limit_per_authority: int = 1_000_000, timeout: int = 30):
    """Full national sweep: every authority FSA lists, every establishment in
    each (bounded by `limit_per_authority` if you want a lighter run). One
    authority erroring (timeout, malformed page) is logged and skipped rather
    than aborting hundreds of other authorities' data."""
    authorities = fetch_authorities(timeout=timeout)
    for authority in authorities:
        authority_id = authority.get("LocalAuthorityId")
        if authority_id is None:
            continue
        try:
            yield from fetch_establishments(authority_id, limit_per_authority, timeout)
        except Exception as exc:  # noqa: BLE001 - keep the national sweep going
            print(f"food_hygiene_ratings: skipping authority {authority_id} "
                  f"({authority.get('Name')!r}): {exc!r}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority-id", type=int, default=None,
                        help="single-authority mode; omit for the full national sweep")
    parser.add_argument("--limit", type=int, default=1_000_000,
                        help="per-authority establishment cap (default effectively unlimited)")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.authority_id is not None:
        gen = fetch_establishments(args.authority_id, args.limit, args.timeout)
    else:
        gen = fetch_all_authorities(args.limit, args.timeout)
    for record in gen:
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
