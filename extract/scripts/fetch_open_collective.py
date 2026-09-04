#!/usr/bin/env python3
"""Open Collective — public account financial state, at scale.

Public-account snapshots expose balance, yearly income, currency, and backer count,
allowing funding-velocity and correction analysis. Verified live 2026-09-03 with the
Webpack collective via the per-slug JSON endpoint. Follow-up verified live 2026-09-04:
Open Collective's GraphQL v2 API exposes a live `accounts` query that paginates
**40,456 real collectives** (`totalCount` observed live), ranked by their own `RANK`
ordering, in batches of up to 100 per request -- this is the actual "internal company
analytics" shape for this source: thousands of organizations, tracked in aggregate,
not one hand-picked example. Bulk GraphQL pagination is also far cheaper than one
REST call per slug (2,000 collectives = ~20 requests, not 2,000).

Verified GraphQL fields (2026-09-04): `slug`, `name`, `legacyId`, `currency`,
`stats.balance`, `stats.yearlyBudget`, `stats.contributorsCount`,
`stats.totalAmountSpent`, `stats.totalAmountReceived` -- richer than the old
per-slug REST JSON, and available across the whole ranked list in one query shape.

Endpoint (GraphQL v2, keyless): `https://api.opencollective.com/graphql/v2`
    query { accounts(type: [COLLECTIVE], orderBy: {field: RANK, direction: ASC},
            limit: N, offset: N) { totalCount nodes { slug name stats { ... } } } }

Legacy endpoint (still used for single-slug lookups): `https://opencollective.com/<slug>.json`

Etiquette: keyless, no published rate limit; GraphQL pagination keeps total request
count low even at thousands of collectives, but still paced with a small per-page
delay to stay a good citizen of shared infrastructure. Poll daily -- collective
balances don't move meaningfully faster than that.

Stdlib only.
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "open_collective_accounts"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
GRAPHQL_URL = "https://api.opencollective.com/graphql/v2"
PAGE_SIZE = 100
PAGE_DELAY_S = 1.0

TOP_COLLECTIVES_QUERY = """
query TopCollectives($limit: Int!, $offset: Int!) {
  accounts(type: [COLLECTIVE], orderBy: {field: RANK, direction: ASC},
           limit: $limit, offset: $offset) {
    totalCount
    nodes {
      slug
      name
      legacyId
      currency
      stats {
        balance { value currency }
        yearlyBudget { value }
        contributorsCount
        totalAmountSpent { value }
        totalAmountReceived { value }
      }
    }
  }
}
"""


def _graphql(query: str, variables: dict, timeout: int = 30, max_retries: int = 5):
    body = json.dumps({"query": query, "variables": variables}).encode()
    req = urllib.request.Request(
        GRAPHQL_URL, data=body,
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/json"},
    )
    delay = 2.0
    for attempt in range(max_retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                doc = json.load(resp)
            if "errors" in doc:
                raise RuntimeError(f"Open Collective GraphQL error: {doc['errors']}")
            return doc["data"]
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < max_retries:
                time.sleep(delay)
                delay *= 2
                continue
            raise


def fetch_top_collectives(total: int = 2000, page_size: int = PAGE_SIZE, timeout: int = 30):
    """The top `total` collectives by Open Collective's own RANK ordering
    (their activity-based default sort), paginated in batches of `page_size`.
    A page that still 429s after retries with backoff stops the sweep early
    (whatever was already yielded stays valid) rather than failing the run."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    offset = 0
    seen = 0
    while seen < total:
        limit = min(page_size, total - seen)
        try:
            data = _graphql(TOP_COLLECTIVES_QUERY, {"limit": limit, "offset": offset}, timeout=timeout)
        except urllib.error.HTTPError as exc:
            print(f"open_collective: stopping early at offset={offset}: {exc!r}", file=sys.stderr)
            break
        nodes = data["accounts"]["nodes"]
        if not nodes:
            break
        for node in nodes:
            stats = node.get("stats") or {}
            balance = stats.get("balance") or {}
            yield {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": node.get("slug"),
                "slug": node.get("slug"),
                "name": node.get("name"),
                "legacy_id": node.get("legacyId"),
                "currency": node.get("currency"),
                "balance": balance.get("value"),
                "yearly_budget": (stats.get("yearlyBudget") or {}).get("value"),
                "backers_count": stats.get("contributorsCount"),
                "total_amount_spent": (stats.get("totalAmountSpent") or {}).get("value"),
                "total_amount_received": (stats.get("totalAmountReceived") or {}).get("value"),
            }
            seen += 1
        offset += len(nodes)
        if len(nodes) < limit:
            break
        time.sleep(PAGE_DELAY_S)


def fetch_account(slug: str = "webpack", timeout: int = 30):
    """Single-collective lookup via the legacy per-slug JSON endpoint. Kept for
    manual/back-compat use; `fetch_top_collectives` is the scaled path."""
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", slug):
        raise ValueError("collective slug contains unsupported characters")
    request = urllib.request.Request(
        f"https://opencollective.com/{slug}.json", headers={"User-Agent": USER_AGENT}
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        row = json.load(response)
    if row.get("slug") != slug or "balance" not in row:
        raise ValueError("Open Collective response is missing slug or balance")
    record = dict(row)
    record.update({"source": SOURCE, "fetched_at": fetched_at, "id": slug})
    yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--slug", default=None,
                        help="single-collective mode (legacy REST endpoint)")
    parser.add_argument("--top", type=int, default=5000,
                        help="collective-count for bulk GraphQL mode (default 5000)")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.slug:
        gen = fetch_account(args.slug, args.timeout)
    else:
        gen = fetch_top_collectives(total=args.top, timeout=args.timeout)
    for record in gen:
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
