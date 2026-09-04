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

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"  # Wikimedia REQUIRES this

RCPROPS = "title|timestamp|ids|sizes|flags|user|comment|tags"


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
        req = urllib.request.Request(api + "?" + urllib.parse.urlencode(p),
                                     headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=45) as resp:
            data = json.load(resp)
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
