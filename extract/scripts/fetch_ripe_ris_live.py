#!/usr/bin/env python3
"""RIPE RIS Live — real-time BGP routing announcements, keyless HTTP streaming.

Lead #15: internet outages and route hijacks show up here before they hit the news.
Despite the catalog's "over websocket" framing, the practical keyless path is a plain
HTTP streaming connection (chunked transfer, one JSON object per line) — no websocket
client needed, stdlib `urllib` reads it directly.

**Verified live 2026-09-03** — `ris-live.ripe.net/v1/stream/?format=json&client=<id>`
returned **200** and streamed **40,491 real BGP messages** in a single connection read
(the probe tool's 32 MB read cap was hit, not a source limit — the stream never
stops). Message types seen: `UPDATE` (route announcements/withdrawals — carries `path`,
`community`, `announcements`, `withdrawals`), `STATE` (collector session state), and
`KEEPALIVE` (heartbeat, no route data). A real `UPDATE`: peer `2001:7f8:24::120`
(AS4601), AS path `[4601,4601,8298,202365,51559]`, announcing `2a03:2100:145::/48` and
others via next-hop `2001:7f8:24::120`.

Endpoint: `https://ris-live.ripe.net/v1/stream/?format=json&client=<your-id>`
    `client=` is a free-text identifier RIPE asks you to set to something identifying
    your application (not an API key — just good etiquette on a shared firehose).
    Optional filters: `&host=rrc21` (one collector), `&peer=<asn>` (one peer).

Quirks that will cost someone an afternoon:
    * **It's a live HTTP stream, not a request/response call.** The connection stays
      open and RIPE pushes newline-delimited JSON continuously — reading it means
      iterating a socket, not calling `.read()` once and getting a bounded response.
      This script reads and yields line-by-line as they arrive, with a `max_messages`
      cap so a caller isn't stuck consuming forever by accident.
    * **Not every message is route data.** `STATE` and `KEEPALIVE` messages have no
      `announcements`/`withdrawals`/`path` — they're collector-session bookkeeping.
      Filter to `type == "UPDATE"` if you want actual routing events; the other types
      are still worth keeping if you're diagnosing collector uptime, just don't expect
      route fields on them.
    * A single `UPDATE` can carry **both `announcements` and `withdrawals` in the same
      message** — a route can be announced and a different (or the same) prefix
      withdrawn in one BGP update. Handle both fields, don't assume one is always empty.
    * `timestamp` is a Unix epoch float (seconds, with sub-second precision) — not
      milliseconds, not an ISO string.
    * The full RIS message schema is published and versioned at
      `ris-live.ripe.net/schemas/v1/ris_message.schema.json` — worth checking if a
      message shape looks unfamiliar rather than guessing.

Etiquette: keyless, but this is a shared, unbounded, real-time firehose serving
genuine operational data to the internet routing community — set a real `client=`
identifier, and if you only need a slice, use `host=`/`peer=` filters server-side
rather than pulling everything and filtering locally.

Stdlib only.
"""
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://ris-live.ripe.net/v1/stream/"
CLIENT_ID = "vintage-data"

# ris-live's front-end intermittently 502s (observed live 2026-11-18: one
# gateway flap failed the whole run). Retry transient connect failures in a
# bounded way so a brief flap does not lose a whole run.
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


def _open_stream(req):
    """Open the streaming connection, retrying transient failures in a
    bounded way -- 429/5xx and transport errors with 5s/10s/20s backoff
    (at most ~35 seconds of sleeping, well inside the ten-minute task
    timeout). A persistent outage still fails with its status surfaced;
    other HTTP errors (e.g. 400/403) fail immediately."""
    for attempt in range(MAX_ATTEMPTS):
        try:
            return urllib.request.urlopen(req, timeout=60)
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
        print(f"ripe_ris_live: transient failure ({reason}); retrying in {delay:.0f}s "
              f"(attempt {attempt + 1} of {MAX_ATTEMPTS - 1})", file=sys.stderr)
        time.sleep(delay)


def stream_messages(max_messages: int = 100, host: str | None = None,
                    peer: str | None = None, updates_only: bool = True):
    """Yield normalized BGP messages from the live stream. Blocks on the open
    connection -- this generator does not return until max_messages is reached or
    the connection ends; there is no bounded response to wait for otherwise.
    The initial connection is retried in a bounded way on transient gateway
    failures (see _open_stream); a mid-stream drop ends the run early."""
    params = {"format": "json", "client": CLIENT_ID}
    if host:
        params["host"] = host
    if peer:
        params["peer"] = peer
    url = f"{BASE}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    n = 0
    with _open_stream(req) as resp:
        for raw_line in resp:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            d = msg.get("data") or {}
            if updates_only and d.get("type") != "UPDATE":
                continue
            now = datetime.now(timezone.utc).isoformat()
            yield {
                "source": "ripe_ris_live",
                "fetched_at": now,
                "id": d.get("id"),
                "type": d.get("type"),
                "bgp_timestamp": d.get("timestamp"),
                "host": d.get("host"),
                "peer": d.get("peer"),
                "peer_asn": d.get("peer_asn"),
                "path": d.get("path"),
                "origin": d.get("origin"),
                "announcements": d.get("announcements"),
                "withdrawals": d.get("withdrawals"),
            }
            n += 1
            if n >= max_messages:
                return


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    for rec in stream_messages(max_messages=limit):
        print(json.dumps(rec, ensure_ascii=False))
