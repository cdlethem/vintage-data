#!/usr/bin/env python3
"""USAspending.gov — keyless API over every US federal contract and grant award.

Lead #28: money plus free-text award descriptions plus recipient plus date — "what is
the government buying this week" as a strong, underused text+numeric series.

**Verified live 2026-09-03**:
  * `api.usaspending.gov/api/v2/awards/last_updated/` — **200, `{"last_updated":
    "09/03/2026"}`** — matches the probe date exactly, confirming the dataset itself
    refreshes daily.
  * `api.usaspending.gov/api/v2/search/spending_by_award/` POST sorted by `"Start
    Date" desc` — **200**, but top results had **Start Dates in 2027-2028** — that
    field is period-of-performance start, not when the award was made, so sorting on
    it surfaces long-future contracts, not recent activity.
  * The same endpoint sorted by `"Last Modified Date" desc` — **200, 2,391 bytes,
    5 results**, top `Last Modified Date: 2026-09-01 23:57:34` — **2 days before the
    probe**, the genuinely recent-activity field.

Endpoint: `POST https://api.usaspending.gov/api/v2/search/spending_by_award/`
Content-Type: application/json. Body:
    {"filters": {"time_period": [{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}],
                "award_type_codes": ["A","B","C","D"]},
     "fields": ["Award ID","Recipient Name","Start Date","Last Modified Date",
               "Award Amount","Awarding Agency"],
     "sort": "Last Modified Date", "order": "desc", "limit": N, "page": 1}
`award_type_codes`: A/B/C/D are contracts (BPA, purchase order, delivery order,
definitive contract); other codes cover grants, loans, direct payments.

Quirks that will cost someone an afternoon:
    * **Sorting by "Start Date" does not surface recent activity — it surfaces
      contracts with the latest performance-period start, which can be years in the
      future.** Verified live: `sort: "Start Date"` returned awards starting in
      2027-2028. Use `"Last Modified Date"` for "what changed recently" instead —
      confirmed to return award records updated within the last two days.
    * **Response field names are Title Case with spaces** (`"Award ID"`, `"Recipient
      Name"`, `"Last Modified Date"`), matching the `fields` list you request
      verbatim — not the usual snake_case JSON convention. Request exactly the field
      names you want to consume; there's no separate machine-friendly key.
    * A **GET** to this same endpoint returns **405** — it's POST-only despite being
      a read/search operation, confirmed live.
    * A POST with an **empty body** returns **422** with a validation error, not a
      default/unfiltered result set — `filters` and `time_period` are required.
    * `Last Modified Date` lags real time by roughly a day or two in practice (federal
      award reporting isn't instantaneous) — treat "last_updated" (the dataset-level
      timestamp) as the outer bound on freshness, not a promise of same-day data.

Etiquette: keyless, no published rate limit found. Self-impose 1 req/s; this is
federal spending-transparency infrastructure, not a high-traffic consumer API.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
URL = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
FIELDS = ["Award ID", "Recipient Name", "Start Date", "Last Modified Date",
         "Award Amount", "Awarding Agency"]


def _post(body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(URL, data=data, method="POST",
                                 headers={"User-Agent": USER_AGENT,
                                         "Content-Type": "application/json",
                                         "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_recent(days: int = 30, limit: int = 25):
    """Awards whose record was most recently modified, not most recently starting --
    see docstring for why that distinction matters here."""
    now = datetime.now(timezone.utc)
    since = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    body = {"filters": {"time_period": [{"start_date": since,
                                        "end_date": now.strftime("%Y-%m-%d")}],
                       "award_type_codes": ["A", "B", "C", "D"]},
           "fields": FIELDS, "sort": "Last Modified Date", "order": "desc",
           "limit": limit, "page": 1}
    doc = _post(body)
    fetched_at = now.isoformat()
    for r in doc.get("results") or []:
        yield {
            "source": "usaspending",
            "fetched_at": fetched_at,
            "id": r.get("generated_internal_id") or r.get("internal_id"),
            "award_id": r.get("Award ID"),
            "recipient": r.get("Recipient Name"),
            "start_date": r.get("Start Date"),
            "last_modified_date": r.get("Last Modified Date"),
            "award_amount": r.get("Award Amount"),
            "awarding_agency": r.get("Awarding Agency"),
        }


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    for rec in fetch_recent(days=days, limit=limit):
        print(json.dumps(rec, ensure_ascii=False))
