#!/usr/bin/env python3
"""Zenodo — new research datasets and software releases, keyless, as they're deposited.

Lead #29: complements CrossRef (already in this catalog) by covering the research
*outputs* that aren't papers — datasets, software, presentations, posters — each with
a DOI and abstract, deposited continuously by researchers worldwide.

**Verified live 2026-09-03**:
  * `zenodo.org/api/records?sort=publication-date&size=10` — **400 Bad Request** —
    the catalog's implied sort value doesn't exist; the API's own error body says so
    exactly: `"Invalid sort option 'publication-date'."`.
  * `zenodo.org/api/records?sort=newest&size=10` — **200, 88,194 bytes** — the correct
    parameter value. Top result created **2026-09-03T14:56:48Z**, the same minute as
    the probe: a presentation on ditransitive-construction linguistics, DOI
    `10.5281/zenodo.22283207`, `access_right: "restricted"`.

Endpoint: `https://zenodo.org/api/records?sort=newest&size=N&page=N`
    sort=newest      newest first (NOT "publication-date" — see quirks)
    q=<query>          full-text search, combinable with sort
    communities=<id>    filter to one community collection

Quirks that will cost someone an afternoon:
    * **The sort value is `newest`, not `publication-date`** — a natural guess from
      the field name, and the API rejects it with a clean 400 rather than silently
      falling back to a default order. Verified live; check the error body's
      `errors[].messages` for the exact reason before assuming the endpoint is down.
    * **`access_right: "restricted"` (or `"closed"`) means the record's metadata is
      public but the actual files are not** — verified on a real top-of-feed record.
      Don't assume every DOI-bearing Zenodo record has downloadable content; check
      `access_right` before trying to fetch files.
    * `conceptrecid`/`conceptdoi` identify the *version-independent* work, while `id`/
      `doi` identify this specific version — if you're deduping across our version
      updates to the same deposit, group by `conceptrecid`, not `id`.
    * `metadata.creators[].orcid` is present for some authors and absent for others in
      the same record — don't assume uniform author metadata quality.
    * `metadata.resource_type.type` covers far more than papers/datasets — "poster",
      "presentation", "software", "lesson" all appear; filter on it if you want a
      specific kind of output.

Etiquette: keyless. Zenodo (CERN-operated, EU-funded) documents a rate limit of 60
requests/minute for anonymous use; self-impose well under that for routine polling.

Stdlib only.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://zenodo.org/api/records"

REQUEST_TIMEOUT_SECONDS = 30
MAX_ATTEMPTS = 3
RETRY_DELAYS_SECONDS = (1, 2)


def _is_timeout(error):
    if isinstance(error, urllib.error.HTTPError):
        return False
    return isinstance(error, TimeoutError) or (
        isinstance(error, urllib.error.URLError)
        and isinstance(error.reason, TimeoutError)
    )


def _report_request_failure(params, error, attempt_count):
    """Write bounded failure context without exposing query values or bodies."""
    terminal_error = {"class": type(error).__name__}
    if isinstance(error, urllib.error.URLError):
        terminal_error["reason_class"] = type(error.reason).__name__

    diagnostic = {
        "attempt_count": attempt_count,
        "event": "zenodo_request_failed",
        "request": {
            "endpoint": BASE,
            "query_present": "q" in params,
            "timeout_seconds": REQUEST_TIMEOUT_SECONDS,
        },
        "terminal_error": terminal_error,
    }
    print(json.dumps(diagnostic, sort_keys=True), file=sys.stderr)


def _validate_response(doc):
    if not isinstance(doc, dict):
        raise ValueError("Zenodo response must be an object")
    hits = doc.get("hits")
    if not isinstance(hits, dict) or not isinstance(hits.get("hits"), list):
        raise ValueError("Zenodo response must contain a hits list")
    return doc


def _get(**params):
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    for attempt_count in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                return _validate_response(json.load(resp))
        except Exception as error:
            if _is_timeout(error) and attempt_count < MAX_ATTEMPTS:
                time.sleep(RETRY_DELAYS_SECONDS[attempt_count - 1])
                continue
            _report_request_failure(params, error, attempt_count)
            raise


def fetch_recent(size: int = 25, query: str | None = None):
    """Newest deposits first. sort=newest, not the more-obvious-looking
    'publication-date', which the API rejects with a 400."""
    now = datetime.now(timezone.utc).isoformat()
    params = {"sort": "newest", "size": size}
    if query:
        params["q"] = query
    doc = _get(**params)
    for r in doc.get("hits", {}).get("hits", []):
        meta = r.get("metadata") or {}
        rtype = meta.get("resource_type") or {}
        yield {
            "source": "zenodo",
            "fetched_at": now,
            "id": r.get("id"),
            "doi": r.get("doi"),
            "concept_doi": r.get("conceptdoi"),
            "title": meta.get("title"),
            "publication_date": meta.get("publication_date"),
            "created": r.get("created"),
            "resource_type": rtype.get("type"),
            "access_right": meta.get("access_right"),
            "license": (meta.get("license") or {}).get("id"),
            "creators": [c.get("name") for c in meta.get("creators") or []],
            "keywords": meta.get("keywords") or [],
        }


def main(argv=None):
    args = sys.argv[1:] if argv is None else argv
    size = int(args[0]) if args else 25
    for rec in fetch_recent(size=size):
        print(json.dumps(rec, ensure_ascii=False))


if __name__ == "__main__":
    main()
