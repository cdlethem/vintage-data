#!/usr/bin/env python3
"""DriveBC Open511 — active road events.

Closures, incidents, construction, restrictions, and extreme-condition impacts form a
public road-state event stream. Verified live 2026-09-04 against British Columbia's
official Open511 implementation. Events carry stable namespaced IDs, schedules,
geography, affected roads, severity, and update timestamps. Follow the Open Government
Licence–Canada and poll every 10–15 minutes.

Two live-verified upstream quirks (2026-11-17):
    * The endpoint **flaps with transient HTTP 500s** — one such 500 on
      2026-11-18 00:08 UTC lost an entire scheduled run. The single request is
      therefore retried in a bounded way (see ``_get_document``) before the run
      is allowed to fail.
    * The ``limit`` parameter is **capped at 500 server-side**: ``limit=1000``
      deterministically returns HTTP 500 with body
      ``{"error": "Limit out of acceptable range: 'limit' cannot exceed 500",
      "error_code": 1005}``. Requests are clamped to the cap.

Stdlib only.
"""

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "drivebc_open511_events"
URL = "https://api.open511.gov.bc.ca/events"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

# Upstream hard cap on the ``limit`` parameter (500); larger values are
# rejected with a deterministic HTTP 500, so requests are clamped to it.
MAX_LIMIT = 500

# The events endpoint intermittently flaps with transient HTTP 500s (observed
# live 2026-11-18; a single 500 lost the whole run). Retry transient failures
# in a bounded way so a brief flap does not lose a whole run.
MAX_ATTEMPTS = 4
RETRY_BASE_DELAY = 5.0
MAX_RETRY_DELAY = 30.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def _retry_delay(error: urllib.error.HTTPError, fallback: float) -> float:
    """Return a courteous bounded delay from an HTTP Retry-After header."""
    value = error.headers.get("Retry-After") if error.headers is not None else None
    if value is not None:
        try:
            delay = float(value)
        except (TypeError, ValueError):
            delay = None
        if delay is not None and delay >= 0:
            return min(delay, MAX_RETRY_DELAY)
    return min(fallback, MAX_RETRY_DELAY)


def _get_document(query: str, timeout: int):
    """Fetch one JSON document, with bounded retries on transient failures.

    Transient failures -- 5xx, 429, and connection errors -- retry with
    5s/10s/20s backoff (at most ~35 seconds of sleeping, well inside the
    ten-minute task timeout). A persistent outage still fails the run after
    the bounded attempts with its status surfaced; other HTTP errors (e.g.
    404) fail immediately.
    """
    request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt == MAX_ATTEMPTS - 1:
                raise
            reason = f"HTTP {exc.code}"
            delay = _retry_delay(exc, RETRY_BASE_DELAY * (2 ** attempt))
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            if attempt == MAX_ATTEMPTS - 1:
                raise
            reason = str(exc.reason if isinstance(exc, urllib.error.URLError) else exc)
            delay = min(RETRY_BASE_DELAY * (2 ** attempt), MAX_RETRY_DELAY)
        print(f"drivebc_events: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)


def fetch_events(limit: int = MAX_LIMIT, timeout: int = 60):
    if limit <= 0:
        return
    query = urllib.parse.urlencode({"format": "json", "limit": min(limit, MAX_LIMIT)})
    fetched_at = datetime.now(timezone.utc).isoformat()
    document = _get_document(query, timeout)
    rows = document.get("events")
    if not isinstance(rows, list):
        raise TypeError("Open511 response is missing events")
    for row in rows[:limit]:
        event_id = row.get("id")
        if not event_id:
            raise ValueError("Open511 event is missing id")
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at, "id": event_id})
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=MAX_LIMIT)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_events(args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
