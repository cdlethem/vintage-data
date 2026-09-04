#!/usr/bin/env python3
"""Software Heritage — the archive of all public source code, keyless REST API.

Lead #40. Adjacent to the package-registry idea already in this catalog
(`fetch_package_registries.py`) but at repository granularity, not release granularity.
The practical, verified keyless capability is origin search plus per-origin archival
visit history — not a raw "everything just deposited" firehose (Software Heritage's
true ingestion event stream is a Kafka journal, not a simple REST endpoint, and outside
this project's stdlib-only, keyless scope).

**Verified live 2026-09-03**:
  * `archive.softwareheritage.org/api/1/origin/search/linux/?limit=3` — **200, 1,483
    bytes, 3 results** — real GitHub origins matching "linux", each with
    `has_visits`/`nb_visits`/`last_visit_date`.
  * `archive.softwareheritage.org/api/1/origin/https://github.com/torvalds/linux/
    visit/latest/` — **200, 412 bytes** — visit #478, `date: 2026-09-02T03:44:11Z`
    (the day before the probe), `status: "full"`, a real snapshot hash.

Endpoints (base `https://archive.softwareheritage.org/api/1`):
    /origin/search/{query}/?limit=N          find repositories by name/URL fragment
    /origin/{url}/visit/latest/                that origin's most recent archival visit
    /origin/{url}/visits/                       full visit history for one origin

Quirks that will cost someone an afternoon:
    * **This is not a firehose of newly-deposited code** — it's a search-then-lookup
      pattern. A search hit's `has_visits: false, nb_visits: 0` is common (an origin
      Software Heritage knows about but hasn't archived yet) — check `has_visits`
      before assuming `last_visit_date` is populated.
    * **`origin` URL must be embedded directly in the path**, not URL-encoded as a
      query parameter — `/origin/https://github.com/.../visit/latest/` with the full
      URL as a literal path segment, slashes and all. This is unusual REST design;
      most APIs would take it as a query param.
    * `date` on a visit is when Software Heritage crawled it, not when the underlying
      repository was actually pushed to — a repo can have old commits but a recent
      `date` simply because it was recently (re-)archived.
    * `status` on a visit can be `"full"`, `"partial"`, or `"not_found"` — a partial
      or not_found visit is a real, loggable outcome, not an error to retry blindly.
    * `snapshot` (a hash) is `null` until at least one visit has completed — check for
      that before trying to follow the `snapshot_url`.

Etiquette: keyless, no key required for these read endpoints, but Software Heritage
documents anonymous rate limits (roughly 120 requests/hour, checked via response
headers `X-RateLimit-Remaining` at request time) — self-impose well under that, and
check the header if you're polling many origins.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://archive.softwareheritage.org/api/1"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def search_origins(query: str, limit: int = 20):
    """Repositories matching a name/URL fragment, with archival status."""
    now = datetime.now(timezone.utc).isoformat()
    url = f"{BASE}/origin/search/{urllib.parse.quote(query, safe='')}/?limit={limit}"
    for o in _get(url):
        yield {
            "source": "software_heritage_origin",
            "fetched_at": now,
            "id": o.get("url"),
            "url": o.get("url"),
            "visit_types": o.get("visit_types") or [],
            "has_visits": o.get("has_visits"),
            "nb_visits": o.get("nb_visits"),
            "last_visit_date": o.get("last_visit_date"),
        }


def fetch_latest_visit(origin_url: str):
    """One origin's most recent archival visit -- what Software Heritage last
    crawled, and when. has_visits must be true or this 404s."""
    now = datetime.now(timezone.utc).isoformat()
    url = f"{BASE}/origin/{origin_url}/visit/latest/"
    v = _get(url)
    yield {
        "source": "software_heritage_visit",
        "fetched_at": now,
        "id": f"{v.get('origin')}:{v.get('visit')}",
        "origin": v.get("origin"),
        "visit_number": v.get("visit"),
        "visit_date": v.get("date"),
        "status": v.get("status"),
        "visit_type": v.get("type"),
        "snapshot": v.get("snapshot"),
    }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "search"
    if mode == "visit":
        origin = sys.argv[2] if len(sys.argv) > 2 else "https://github.com/torvalds/linux"
        for rec in fetch_latest_visit(origin):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        query = sys.argv[2] if len(sys.argv) > 2 else "linux"
        for rec in search_origins(query):
            print(json.dumps(rec, ensure_ascii=False))
