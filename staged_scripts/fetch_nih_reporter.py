#!/usr/bin/env python3
"""NIH RePORTER — keyless POST API over US biomedical research funding.

Lead #42: money plus long text plus time. Pairs naturally with ClinicalTrials.gov and
CrossRef (both elsewhere in this catalog) to trace funding -> trial -> publication.

**Verified live 2026-09-03.** The local exploration found the search endpoint but
never sorted by recency; a follow-up query with an explicit `sort_field` fixed that:
`api.reporter.nih.gov/v2/projects/search` POST with
`{"criteria":{"fiscal_years":[2026]},"sort_field":"date_added","sort_order":"desc",
"offset":0,"limit":5}` — **200, 44,705 bytes, meta.total: 52,859** FY2026 grants, top
three all `date_added: 2026-08-29T17:03:52` — **five days before the probe.**

Endpoint: `POST https://api.reporter.nih.gov/v2/projects/search`
Content-Type: application/json. Body:
    {"criteria": {"fiscal_years": [2026]}, "sort_field": "date_added",
     "sort_order": "desc", "offset": 0, "limit": N}
`criteria` supports many filters (pi_names, org_names, project_nums, award_amounts,
covid_response, ...) — fiscal_years is the simplest "give me current activity" filter.

Quirks that will cost someone an afternoon:
    * **An unsorted default query does not return newest-first** — the exploration
      run's plain queries with no `sort_field` came back `sort_field: None` in the
      response meta and the same stale-looking first result every time regardless of
      `limit`. You must explicitly set `sort_field: "date_added"` to get a live feed;
      omitting it silently gives you some other (apparently fixed) ordering.
    * `date_added` (when NIH's system ingested the record) is the field to watch for
      "new," not `award_notice_date` or `project_start_date` — those can be months old
      even on a just-added record as NIH backfills or updates award data.
    * `meta.total` is the full match count for your `criteria` filter (tens of
      thousands for a fiscal-year filter), not the number of rows returned — don't
      confuse it with `len(results)`.
    * `organization` and `principal_investigators` are nested objects/lists, not flat
      fields — `organization.org_name`, `principal_investigators[i].full_name`.
    * `abstract_text` and `phr_text` (public health relevance) can be very long free
      text — worth keeping as a genuine text corpus, but don't assume a bounded length.

Etiquette: keyless POST, no key required, no published rate limit found in this
session. Self-impose 1 req/s; this is NIH production infrastructure serving grant
data researchers actually rely on.

Stdlib only.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
URL = "https://api.reporter.nih.gov/v2/projects/search"


def _post(body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(URL, data=data, method="POST",
                                 headers={"User-Agent": USER_AGENT,
                                         "Content-Type": "application/json",
                                         "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_recent(fiscal_year: int | None = None, limit: int = 25):
    """Newest-added grant records. sort_field is required -- an unsorted query does
    not come back newest-first (see docstring)."""
    now = datetime.now(timezone.utc).isoformat()
    fiscal_year = fiscal_year or datetime.now().year
    body = {"criteria": {"fiscal_years": [fiscal_year]},
           "sort_field": "date_added", "sort_order": "desc",
           "offset": 0, "limit": limit}
    doc = _post(body)
    for r in doc.get("results") or []:
        org = r.get("organization") or {}
        pis = r.get("principal_investigators") or []
        yield {
            "source": "nih_reporter",
            "fetched_at": now,
            "id": r.get("appl_id"),
            "project_num": r.get("project_num"),
            "title": r.get("project_title"),
            "date_added": r.get("date_added"),
            "fiscal_year": r.get("fiscal_year"),
            "award_amount": r.get("award_amount"),
            "org_name": org.get("org_name"),
            "org_state": org.get("org_state"),
            "principal_investigators": [p.get("full_name") for p in pis
                                        if isinstance(p, dict) and p.get("full_name")],
            "project_start_date": r.get("project_start_date"),
            "project_end_date": r.get("project_end_date"),
        }


if __name__ == "__main__":
    fy = int(sys.argv[1]) if len(sys.argv) > 1 else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    for rec in fetch_recent(fy, limit=limit):
        print(json.dumps(rec, ensure_ascii=False))
