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
import argparse
import email.utils
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

REQUEST_TIMEOUT_SECONDS = 60
# Zenodo is intermittently unhealthy: seconds-scale flapping (observed
# 2026-10-02: the 03:17 scheduled run got HTTP 500 while the identical
# request succeeds seconds apart) and sustained multi-minute HTTP 503
# windows (observed 2026-11-26: 503s across the 08:17 run and its
# five-minute task retry, ~6+ minutes), so the delay budget spans
# minutes, not seconds. Retry transient HTTP codes and transport
# failures in a bounded way before the run is allowed to fail.
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})
MAX_TRANSIENT_ATTEMPTS = 10
DEFAULT_RETRY_DELAY_SECONDS = 5
MAX_RETRY_DELAY_SECONDS = 30
MAX_RETRY_CUMULATIVE_DELAY_SECONDS = 480


def next_delay(attempt, headers, now=None):
    """Bounded delay before the next attempt.

    Grows exponentially from DEFAULT to MAX (5, 10, 20, 30, ... seconds),
    stretched to a valid Retry-After header value when the server asks for
    a longer pause; clamped to [DEFAULT, MAX].
    """
    delay = DEFAULT_RETRY_DELAY_SECONDS * (2 ** (attempt - 1))
    value = headers.get("Retry-After") if headers is not None else None
    if isinstance(value, str):
        value = value.strip()
        try:
            if value.isdigit():
                delay = max(delay, int(value))
            else:
                retry_at = email.utils.parsedate_to_datetime(value)
                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)
                delay = max(delay, (retry_at - (now or datetime.now(timezone.utc))).total_seconds())
        except (IndexError, TypeError, ValueError):
            pass
    return max(DEFAULT_RETRY_DELAY_SECONDS, min(MAX_RETRY_DELAY_SECONDS, delay))


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
    """Fetch one records document, retrying transient failures in a bounded way.

    Transient HTTP codes and transport failures (timeouts, DNS blips,
    connection resets) retry with exponential 5-30-second delays (stretched
    to a longer Retry-After value when the server sends one), bounded by a
    480-second cumulative delay budget — well inside the ten-minute task
    timeout. Non-retryable HTTP codes, malformed JSON, and schema mismatches
    fail immediately, never retried.
    """
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    cumulative_delay = 0
    for attempt_count in range(1, MAX_TRANSIENT_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as resp:
                return _validate_response(json.load(resp))
        except urllib.error.HTTPError as error:
            if error.code not in RETRYABLE_HTTP_CODES:
                _report_request_failure(params, error, attempt_count)
                raise
            last_error = error
            delay = next_delay(attempt_count, error.headers)
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = error
            delay = next_delay(attempt_count, None)
        except Exception as error:
            _report_request_failure(params, error, attempt_count)
            raise
        if attempt_count == MAX_TRANSIENT_ATTEMPTS:
            _report_request_failure(params, last_error, attempt_count)
            raise last_error
        if cumulative_delay + delay > MAX_RETRY_CUMULATIVE_DELAY_SECONDS:
            _report_request_failure(params, last_error, attempt_count)
            raise last_error
        print(f"zenodo: transient failure; retrying in {delay:.0f}s "
              f"(attempt {attempt_count} of {MAX_TRANSIENT_ATTEMPTS})", file=sys.stderr)
        time.sleep(delay)
        cumulative_delay += delay


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
    parser = argparse.ArgumentParser(description="Fetch recent Zenodo records as NDJSON.")
    parser.add_argument(
        "size",
        nargs="?",
        type=int,
        default=25,
        help="maximum number of recent records to fetch (default: 25)",
    )
    args = parser.parse_args(argv)
    for rec in fetch_recent(size=args.size):
        print(json.dumps(rec, ensure_ascii=False))


if __name__ == "__main__":
    main()
