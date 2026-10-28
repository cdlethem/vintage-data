#!/usr/bin/env python3
"""Wikimedia — every edit to every Wikipedia, as a pollable feed (keyless).

Two ways in, and the choice matters for your architecture:

  A) EventStreams (SSE, push): https://stream.wikimedia.org/v2/stream/recentchange
     A continuous server-sent-events firehose of edits across ALL Wikimedia
     projects. Best fidelity, but it is a long-lived connection, not a
     per-run poll — it belongs in a daemon, not a cron job. Supports
     Last-Event-ID for resume-after-disconnect. Note the public endpoint is
     capped at a limited number of concurrent connections, so be a good
     citizen and open exactly one.

  B) MediaWiki Action API list=recentchanges (JSON, poll)  <-- implemented here
     Per-wiki, paginated, watermarkable via rcend/rccontinue. Fits a
     "grab current data per run" model cleanly.

Docs-verified 2026-09-02 against wikitech.wikimedia.org Event Platform docs;
the Action API endpoint pattern (https://<lang>.wikipedia.org/w/api.php) has
been stable for many years. Smoke-test once from your machine.

Why it's a great corpus: edit comments are natural language, edits carry
byte-deltas (signed magnitude of change), bot vs human is flagged, and edit
spikes on an article routinely PRECEDE mainstream news coverage of an event.
Pair with the pageviews REST API (hourly per-article view counts) for a
demand-side signal alongside this supply-side one.

Reliability: the MediaWiki API edge intermittently 503s under load (observed
live 2026-10-28: windows of several minutes of 503 with `Retry-After: 8` on
en.wikipedia recentchanges). Each page request is therefore retried in a
bounded way on 429/5xx and connection failures (see ``_fetch_page``) before a
run is allowed to fail.

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

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"  # Wikimedia REQUIRES this

RCPROPS = "title|timestamp|ids|sizes|flags|user|comment|tags"

# The MediaWiki API edge intermittently 503s under load (observed live
# 2026-10-28: windows of 503 with `Retry-After: 8` on en.wikipedia
# recentchanges). Retry transient failures in a bounded way so a brief flap
# does not lose a whole run.
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


def _fetch_page(api: str, params: dict) -> dict:
    """Fetch one recentchanges page, retrying transient failures with a finite bound.

    Transient failures -- 429/5xx and connection errors -- retry with
    5s/10s/20s/30s backoff honoring Retry-After (at most ~65 seconds of sleeping,
    well inside the ten-minute task timeout). A persistent outage still fails
    the run after the bounded attempts with its status surfaced; other HTTP
    errors (e.g. 404) fail immediately.
    """
    url = api + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(MAX_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.load(resp)
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
        print(f"wikimedia_changes: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)


def fetch_recent_changes(lang: str = "en", limit: int = 500,
                         rcend: str | None = None, project: str = "wikipedia"):
    """Newest changes first. `rcend` = ISO8601 watermark; stops at that time."""
    api = f"https://{lang}.{project}.org/w/api.php"
    params = {
        "action": "query", "list": "recentchanges", "format": "json",
        "formatversion": "2", "rcprop": RCPROPS,
        "rclimit": min(limit, 500), "rcdir": "older",
    }
    if rcend:
        params["rcend"] = rcend
    cont = None
    while True:
        p = dict(params)
        if cont:
            p["rccontinue"] = cont
        data = _fetch_page(api, p)
        now = datetime.now(timezone.utc).isoformat()
        for rc in data.get("query", {}).get("recentchanges", []):
            old, new = rc.get("oldlen"), rc.get("newlen")
            yield {
                "source": "wikimedia_recentchanges",
                "fetched_at": now,
                "id": rc.get("rcid"),
                "wiki": f"{lang}.{project}",
                "ts": rc.get("timestamp"),
                "type": rc.get("type"),          # edit / new / log / categorize
                "title": rc.get("title"),
                "user": rc.get("user"),
                "bot": bool(rc.get("bot")),
                "minor": bool(rc.get("minor")),
                "comment": rc.get("comment"),    # the text signal
                "bytes_delta": (new - old) if isinstance(old, int)
                               and isinstance(new, int) else None,
                "tags": rc.get("tags") or [],
            }
        cont = data.get("continue", {}).get("rccontinue")
        if not cont:
            break


if __name__ == "__main__":
    lang = sys.argv[1] if len(sys.argv) > 1 else "en"
    for i, rec in enumerate(fetch_recent_changes(lang, limit=50)):
        print(json.dumps(rec, ensure_ascii=False))
        if i >= 49:
            break
