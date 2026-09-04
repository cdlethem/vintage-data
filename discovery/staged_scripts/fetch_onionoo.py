#!/usr/bin/env python3
"""Tor Onionoo — relay and bridge network status, keyless, updated hourly.

Lead #27: network-health time series with a real geopolitical dimension. Every public
Tor relay's status, bandwidth, and (for non-bridge relays) country and autonomous
system, published by the Tor Project itself.

**Verified live 2026-09-03**:
  * `onionoo.torproject.org/summary?limit=10&type=relay&running=true` — **200,
    1,234 bytes** — compact per-relay records (`n`/`f`/`a`/`r`), `relays_published:
    2026-09-03 13:00:00` (about 2 hours before the probe — Onionoo publishes hourly).
  * `onionoo.torproject.org/details?limit=3&type=relay&running=true&fields=nickname,
    fingerprint,country,as_name,observed_bandwidth,first_seen,last_seen` — **200, 853
    bytes** — full per-relay detail via a field allowlist, including a live count
    `relays_truncated: 9609` confirming the network's real scale.

Endpoints (base `https://onionoo.torproject.org`):
    /summary     compact: nickname(n), fingerprint(f), addresses(a), running(r)
    /details      full record; use `fields=` to keep it small (recommended — the
                  unfiltered details response is large across ~9,600+ relays)
    /bandwidth    historical bandwidth graphs per relay
    /weights      consensus weight (how much traffic a relay is chosen to carry)
Parameters: `type=relay|bridge`, `running=true`, `country=xx`, `limit=N`,
`fields=comma,separated,list` (details/bandwidth/weights only).

Quirks that will cost someone an afternoon:
    * **`summary` uses cryptic one-letter keys** (`n` nickname, `f` fingerprint, `a`
      addresses, `r` running) — not obvious field names, and undocumented in the
      response itself; you have to know the mapping going in.
    * **`country` and `as_name` are absent for some relays**, not merely null —
      verified live: of 3 sampled relays, one had no `as_name` key at all (GeoIP/ASN
      lookup can fail for a given address). Use `.get()`, never direct indexing.
    * `addresses` (`a` in summary) is a list that can mix IPv4 and bracketed IPv6
      (`"[2620:7:6003::141]"`) in the same relay — don't assume the first entry is
      IPv4 or that there's exactly one address.
    * The whole dataset is large: unfiltered `details` across the full relay set is
      several megabytes. Always pass `fields=` to keep responses small unless you
      genuinely need every field.
    * Publication is **hourly**, not real-time (`relays_published` timestamps land on
      the hour) — polling more often than hourly returns the identical snapshot.

Etiquette: keyless, run by the Tor Project as a public service. Respect the hourly
publish cadence — polling faster wastes bandwidth on a project that needs it directed
at actual Tor traffic, not scrapers.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://onionoo.torproject.org"


def _get(path, **params):
    url = f"{BASE}/{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_summary(kind: str = "relay", running_only: bool = True, limit: int = 100):
    """Compact status: nickname, fingerprint, addresses, running. Cheapest call."""
    now = datetime.now(timezone.utc).isoformat()
    params = {"type": kind, "limit": limit}
    if running_only:
        params["running"] = "true"
    doc = _get("summary", **params)
    published = doc.get("relays_published") if kind == "relay" else doc.get("bridges_published")
    key = "relays" if kind == "relay" else "bridges"
    for r in doc.get(key) or []:
        yield {
            "source": f"tor_onionoo_{kind}",
            "fetched_at": now,
            "id": r.get("f") or r.get("h"),  # relay fingerprint or bridge hashed id
            "nickname": r.get("n"),
            "addresses": r.get("a") or [],
            "running": r.get("r"),
            "published": published,
        }


def fetch_details(kind: str = "relay", running_only: bool = True, limit: int = 100):
    """Full detail with country/AS/bandwidth, filtered to a small field set."""
    now = datetime.now(timezone.utc).isoformat()
    fields = "nickname,fingerprint,country,as_name,observed_bandwidth,first_seen,last_seen"
    params = {"type": kind, "limit": limit, "fields": fields}
    if running_only:
        params["running"] = "true"
    doc = _get("details", **params)
    key = "relays" if kind == "relay" else "bridges"
    for r in doc.get(key) or []:
        yield {
            "source": f"tor_onionoo_{kind}_detail",
            "fetched_at": now,
            "id": r.get("fingerprint"),
            "nickname": r.get("nickname"),
            "country": r.get("country"),  # absent for some relays, not just null
            "as_name": r.get("as_name"),  # absent for some relays too
            "observed_bandwidth": r.get("observed_bandwidth"),
            "first_seen": r.get("first_seen"),
            "last_seen": r.get("last_seen"),
        }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "summary"
    fn = fetch_details if mode == "details" else fetch_summary
    for rec in fn():
        print(json.dumps(rec, ensure_ascii=False))
