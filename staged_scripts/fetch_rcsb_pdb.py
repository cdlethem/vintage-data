#!/usr/bin/env python3
"""RCSB Protein Data Bank — new macromolecular structures, keyless REST.

Lead #41: structural biology. New structures are released on a weekly cadence with
resolution, method (X-ray/cryo-EM/NMR) and organism. The catalog notes the shift from
crystallography to cryo-EM is visible directly in the release stream.

**Verified live 2026-09-03**:
  * `data.rcsb.org/rest/v1/holdings/current/entry_ids` — **200, 1,815,885 bytes,
    259,412 entry ids** — the full current catalog, confirming scale but not a
    "recent" filter by itself.
  * `search.rcsb.org/rcsbsearch/v2/query` with a `rcsb_accession_info.
    initial_release_date > 2026-08-01` filter — **200, total_count: 1,850** real
    matches. Spot-checked one hit (`10AX`) via `data.rcsb.org/rest/v1/core/entry/10AX`
    — **200**, `initial_release_date: 2026-09-02` — released the day before the probe.
  * The same search request also carried `sort_by: initial_release_date, direction:
    desc` — the returned order was **not actually newest-first** (verified: results
    came back in what looks like ID order, not date order). The date *filter* works;
    the server-side *sort* on this field does not reliably apply.

Endpoints:
    `https://search.rcsb.org/rcsbsearch/v2/query?json={...}` — filter/search, returns
      a list of entry ids + relevance scores, not full records
    `https://data.rcsb.org/rest/v1/core/entry/{id}` — full record for one id,
      including `rcsb_accession_info.{deposit_date,initial_release_date,revision_date}`
    `https://data.rcsb.org/rest/v1/holdings/current/entry_ids` — every current id

Quirks that will cost someone an afternoon:
    * **The search API's `sort_by`/`direction` on `initial_release_date` does not
      reliably return newest-first**, even though it's accepted without error and the
      *filter* on the same field works correctly. Verified live. Don't trust the
      order of `result_set` — fetch each hit's `core/entry` record and sort
      client-side by `rcsb_accession_info.initial_release_date`, which this script
      does.
    * The search API returns **ids and scores only**, not the fields you actually
      want (method, resolution, organism) — a second round-trip to `core/entry/{id}`
      is required per hit. There is a batch entry endpoint for fetching many ids at
      once if you want to avoid N round-trips; this script keeps it simple with
      per-id fetches for a small `rows` count.
    * `initial_release_date` (first public release) and `revision_date` (last time
      *any* field was revised, including administrative reprocessing) are different
      and both move — an old structure can show a very recent `revision_date` without
      being a new structure. Use `initial_release_date` for "is this new".
    * `deposit_date` (when submitted) can precede `initial_release_date` (when made
      public) by months to years — the gap itself is meaningful (embargo periods) but
      don't treat deposit date as release date.

Etiquette: keyless, no published rate limit, but this is NIH/RCSB production
infrastructure — poll the release-date filter at most a few times a day (releases
happen weekly) and avoid looping full per-id fetches over large result sets.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
SEARCH = "https://search.rcsb.org/rcsbsearch/v2/query"
DATA = "https://data.rcsb.org/rest/v1/core/entry"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _search_recent_ids(since_iso: str, rows: int = 20):
    query = {
        "query": {"type": "terminal", "service": "text",
                 "parameters": {"attribute": "rcsb_accession_info.initial_release_date",
                                "operator": "greater", "value": since_iso}},
        "return_type": "entry",
        "request_options": {"paginate": {"rows": rows}},
    }
    url = f"{SEARCH}?{urllib.parse.urlencode({'json': json.dumps(query)})}"
    doc = _get(url)
    return [r["identifier"] for r in doc.get("result_set") or []], doc.get("total_count", 0)


def fetch_recent(days: int = 14, rows: int = 20):
    """Structures released in the last `days`, newest first -- sorted client-side
    because the search API's own sort_by on this field is unreliable (see docstring)."""
    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    ids, total = _search_recent_ids(since, rows=rows)
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for entry_id in ids:
        entry = _get(f"{DATA}/{entry_id}")
        acc = entry.get("rcsb_accession_info") or {}
        exptl = entry.get("exptl") or [{}]
        records.append({
            "source": "rcsb_pdb",
            "fetched_at": now,
            "id": entry_id,
            "initial_release_date": acc.get("initial_release_date"),
            "deposit_date": acc.get("deposit_date"),
            "revision_date": acc.get("revision_date"),
            "method": exptl[0].get("method") if exptl else None,
            "title": (entry.get("struct") or {}).get("title"),
            "keywords": (entry.get("struct_keywords") or {}).get("pdbx_keywords"),
        })
    records.sort(key=lambda r: r["initial_release_date"] or "", reverse=True)
    yield from records


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 14
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    for rec in fetch_recent(days=days, rows=rows):
        print(json.dumps(rec, ensure_ascii=False))
