#!/usr/bin/env python3
"""CrossRef — the firehose of newly registered DOIs (keyless, JSON).

Every new journal article, preprint, book chapter, and dataset that gets a
CrossRef DOI shows up here — thousands per hour, with titles, abstracts
(sometimes), subjects, and publishers.

'Current' per run = works created since the previous run, and nothing else.
`from-created-date` has hourly resolution only, so it alone re-serves the
whole current hour on every poll: on an hourly cron roughly half of each run
was already-collected DOIs. The floor is now the stored watermark's hour and
the exact `created` timestamp is applied client-side, with the set of DOIs
registered at exactly that second carried over so a tie can't drop or
duplicate a record. Nothing is capped: the window is bounded by the watermark
itself, so the run walks the cursor to the end of it. State is written only
after every record is printed, so an aborted run re-fetches rather than
skipping (at-least-once).

The first run has no watermark and covers `--hours-back` (default 1) as its
baseline.

Verified live 2026-09-02: server responded (with a 429 to my anonymous
sandbox fetcher, which is their documented polite-pool behavior). REQUIRED
etiquette: include a mailto in your User-Agent — that moves you into the
polite pool and rate limits become generous.

Quirks:
  * Use cursor=* deep paging, never page/offset, for large pulls.
  * The API intermittently answers valid queries with HTTP 500 (observed
    live 2026-10-08: the same page request failed 3 of 6 times). Each page
    is therefore retried a bounded number of times on 429/5xx/connection
    failures with backoff before the run is allowed to fail.
  * created-date captures registrations of old back-catalog too (journal
    transfers). Filter by publication year if you only want new literature.
  * select= keeps payloads small; full records are huge.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

BASE = "https://api.crossref.org/works"
MAILTO = os.environ.get("EXTRACT_CONTACT") or "you@example.com"  # <-- REQUIRED: set a real address (polite pool)
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or f"vintage-data/0.1 (mailto:{MAILTO})"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"

SELECT = "DOI,title,abstract,created,type,publisher,container-title,subject,author"

# api.crossref.org intermittently answers valid queries with HTTP 500 (the
# same valid query failed 3 of 6 times when observed live 2026-10-08). Retry
# transient failures per page in a bounded way so a brief flap does not
# lose a whole run; a persistent outage still fails the run with its status
# surfaced. Other HTTP errors (e.g. 404) are not retried.
MAX_ATTEMPTS = 4
RETRY_BASE_DELAY = 5.0
MAX_RETRY_DELAY = 30.0
RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "crossref.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {"created": "<iso timestamp>", "boundary": [doi, ...]}."""
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed watermark state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


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


def _get_page(query: str) -> dict:
    """Fetch one cursor page of works as its JSON message, with bounded retries.

    Transient failures -- 429, 5xx, and connection errors -- retry with
    5s/10s/20s backoff (at most ~35 seconds of sleeping per page, well
    inside the ten-minute task timeout); other HTTP errors (e.g. 404)
    fail immediately.
    """
    req = urllib.request.Request(f"{BASE}?{query}",
                                 headers={"User-Agent": USER_AGENT})
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp)["message"]
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
        print(f"crossref_new_dois: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)
    raise AssertionError("unreachable")


def fetch_new_works(hours_back: int = 1, rows: int = 1000, watermark: str = "",
                    boundary: frozenset = frozenset()):
    """Yield works registered after ``watermark`` (or in the last ``hours_back`` hours).

    ``watermark`` is an exact ``created.date-time``; the API filter can only be
    given its hour, so the remainder of the hour is discarded client-side.
    ``boundary`` holds the DOIs already emitted at exactly that timestamp.
    """
    if watermark:
        floor = watermark[:13]           # YYYY-MM-DDTHH, the API's finest resolution
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours_back)
        floor = cutoff.strftime("%Y-%m-%dT%H")
    flt = "from-created-date:" + floor
    cursor = "*"
    while True:
        params = urllib.parse.urlencode({
            "filter": flt, "select": SELECT,
            "rows": rows, "cursor": cursor, "mailto": MAILTO,
        })
        msg = _get_page(params)
        items = msg.get("items", [])
        if not items:
            return
        now = datetime.now(timezone.utc).isoformat()
        for it in items:
            record = normalize(it, now)
            created = record.get("created") or ""
            if watermark and created:
                if created < watermark:
                    continue  # inside the filter's hour but before the watermark
                if created == watermark and record["id"] in boundary:
                    continue  # already emitted at this exact second
            yield record
        cursor = msg.get("next-cursor")
        if not cursor:
            return


def normalize(it: dict, now: str) -> dict:
    created = (it.get("created") or {}).get("date-time")
    return {
        "source": "crossref",
        "fetched_at": now,
        "id": it.get("DOI"),
        "created": created,
        "type": it.get("type"),
        "title": (it.get("title") or [None])[0],
        "journal": (it.get("container-title") or [None])[0],
        "publisher": it.get("publisher"),
        "subjects": it.get("subject") or [],
        "n_authors": len(it.get("author") or []),
        "has_abstract": bool(it.get("abstract")),
        "abstract": it.get("abstract"),  # JATS-flavored XML string when present
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("hours_back", nargs="?", type=int, default=1,
                        help="cold-start window; ignored once a watermark exists")
    parser.add_argument("--rows", type=int, default=1000, help="page size (CrossRef max 1000)")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    args = parser.parse_args()

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)
    watermark = state.get("created", "")
    boundary = frozenset(state.get("boundary", []))

    newest = watermark
    emitted_on_newest: set[str] = set(boundary) if watermark else set()
    for record in fetch_new_works(args.hours_back, args.rows, watermark, boundary):
        created = record.get("created") or ""
        if created > newest:
            newest, emitted_on_newest = created, set()
        if created and created == newest:
            emitted_on_newest.add(record["id"])
        print(json.dumps(record, ensure_ascii=False))
    # Only after every record is written: an aborted run re-fetches instead of
    # skipping. An empty pull leaves the watermark untouched (nothing new).
    if not args.no_state and newest:
        save_state(path, {"created": newest, "boundary": sorted(emitted_on_newest)})


if __name__ == "__main__":
    main()
