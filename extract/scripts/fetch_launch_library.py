#!/usr/bin/env python3
"""Launch Library 2 (The Space Devs) — upcoming and past rocket launches, keyless.

Lead #52. The delightful property, as the catalog puts it: **scheduled launch times
slip constantly**. Polling this repeatedly and diffing `net` (No Earlier Than — the
current best-estimate launch time) across snapshots turns a launch schedule into a
forecast-revision dataset, the same analytical shape as JPL close-approach data (#7)
or UK Carbon Intensity (#14) already in this catalog: you get to score how far off an
early estimate was from what actually happened.

**Verified live 2026-09-03** — `ll.thespacedevs.com/2.3.0/launches/upcoming/?limit=10`
returned **200, count: 359** upcoming launches, first: GSLV Mk II carrying
GISAT-1A/EOS-05 for ISRO, status "Go for Launch", `net: 2026-09-03T21:25:00Z`.

**`?mode=list` is 10x smaller for the same records** — verified live: 10 full-detail
records cost 107,375 bytes; the same 10 in list mode cost 11,421 bytes. List mode
drops `launch_service_provider`, `pad`, and `mission` to `null`; fetch one launch's
detail URL (`/launches/{id}/`) if you need those for a specific launch you care about.

Endpoints (base `https://ll.thespacedevs.com/2.3.0`):
    /launches/upcoming/?limit=N&mode=list      scheduled launches, soonest first
    /launches/previous/?limit=N&mode=list       completed launches, most recent first
    /launches/{id}/                              full detail for one launch (provider,
                                                  pad, mission, agencies — all nested)

Quirks that will cost someone an afternoon:
    * **`net` is a point estimate, not a guarantee** — the accompanying
      `net_precision` field (`Minute`/`Hour`/`Day`/`Quarter`, etc.) tells you how much
      to trust the specific timestamp. A launch with `net_precision: Day` will slip
      within its `window_start`/`window_end` in ways a `Minute`-precision one won't.
    * **Full-detail mode (default, no `mode=` param) is enormous and deeply nested**
      — `pad` alone embeds its parent `location`, which embeds `celestial_body`, which
      embeds global launch statistics. Use `mode=list` unless you specifically need
      that nested detail for a launch already identified as interesting.
    * `status` is an object (`{id, name, abbrev, description}`), not a bare string —
      `abbrev` values like "Go", "TBD", "Success", "Failure" are the ones worth
      tracking as a launch moves through its lifecycle.
    * Free-tier rate limit is real and low: The Space Devs documents **15
      requests/hour** unauthenticated on this version of the API. Cache aggressively;
      polling `upcoming` more than once every few minutes wastes your whole hourly
      budget fast.

Etiquette: keyless but rate-limited (15 req/hour documented for anonymous use) — this
is the tightest limit of any keyless source in this catalog. Self-impose well under it
and cache `count`/`next` so you don't re-page unnecessarily.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
BASE = "https://ll.thespacedevs.com/2.3.0"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def fetch_launches(which: str = "upcoming", limit: int = 25, mode: str = "list"):
    """which is 'upcoming' or 'previous'. mode='list' is ~10x smaller; use 'detail'
    (any value other than 'list') only if you need provider/pad/mission."""
    now = datetime.now(timezone.utc).isoformat()
    params = {"limit": limit}
    if mode == "list":
        params["mode"] = "list"
    url = f"{BASE}/launches/{which}/?{urllib.parse.urlencode(params)}"
    doc = _get(url)
    for r in doc.get("results") or []:
        status = r.get("status") or {}
        yield {
            "source": f"launch_library_{which}",
            "fetched_at": now,
            "id": r["id"],
            "name": r.get("name"),
            "status": status.get("abbrev"),
            "status_name": status.get("name"),
            "net": r.get("net"),
            "net_precision": (r.get("net_precision") or {}).get("name"),
            "window_start": r.get("window_start"),
            "window_end": r.get("window_end"),
            "last_updated": r.get("last_updated"),
        }


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "upcoming"
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    for rec in fetch_launches(which, limit=limit):
        print(json.dumps(rec, ensure_ascii=False))
