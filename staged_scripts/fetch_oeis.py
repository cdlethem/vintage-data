#!/usr/bin/env python3
"""OEIS — the On-Line Encyclopedia of Integer Sequences, keyless search API.

Lead #46: "a citation graph of mathematical ideas that grows daily." New sequences are
accepted continuously, each with a description, formulae, cross-references, and — the
part that makes this a genuine time series — a `created` timestamp and a `keyword:new`
tag OEIS itself applies to recent additions.

**Verified live 2026-09-03**:
  * `oeis.org/search?q=id:A000045&fmt=json` — **200, 120,023 bytes** — Fibonacci,
    sanity-checking the schema against a famous, stable sequence.
  * `oeis.org/search?q=keyword:new&fmt=json&start=0` — **200, 21,683 bytes, 10 results**
    — top hit A399000, `created: 2026-08-23T22:53:32-04:00`, author credited same month.
    `keyword:new` is a genuine "recently added" query, not a static category.

Endpoint: `https://oeis.org/search?q=<query>&fmt=json&start=<offset>`
  q=keyword:new          the newest sequences OEIS has accepted (this is the live feed)
  q=id:A000045           one sequence by A-number
  q=keyword:more -keyword:new    combine terms; supports boolean-ish OEIS query syntax

Quirks:
    * **The response is a bare JSON array, not `{"results": [...]}`.** A first read
      assuming a wrapper object throws `TypeError: list indices must be integers or
      slices, not str` immediately — confirmed live on this exact query.
    * `keyword` is a single comma-joined string (`"nonn,tabl,new"`), not a JSON array —
      split on `,` yourself if you want individual tags.
    * `data` is a comma-joined string of the sequence terms, as text — arbitrarily
      large integers can appear, so do not blindly `int()` and expect it to fit a
      machine word; keep as string unless you specifically need to compute with it.
    * `offset` is itself a comma-joined pair (`"0,2"`): the index of the first term,
      and the index of the first term >1 in absolute value (an OEIS-specific
      convention for spotting the sequence's "interesting" starting point).
    * Pagination is `start=`, in increments of 10 results per page (OEIS's own
      default), not a cursor — safe to compute yourself, unlike Gutendex's `next` URL.

Etiquette: keyless, no key. OEIS is a small nonprofit-run foundation; its own guidance
is to be gentle — this script polls `keyword:new` at most, which is a small query, but
do not hammer it. A few requests a day is what this source actually needs.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
BASE = "https://oeis.org/search"


def _get(query, start=0):
    url = f"{BASE}?{urllib.parse.urlencode({'q': query, 'fmt': 'json', 'start': start})}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        # a query with zero hits returns the bare string "null", not an empty array
        body = resp.read()
        doc = json.loads(body)
        return doc or []


def _norm(r, now):
    return {
        "source": "oeis",
        "fetched_at": now,
        "id": f"A{r['number']:06d}",
        "number": r["number"],
        "name": r.get("name"),
        "data": r.get("data"),
        "keywords": (r.get("keyword") or "").split(","),
        "author": r.get("author"),
        "created": r.get("created"),
        "offset": r.get("offset"),
        "url": f"https://oeis.org/A{r['number']:06d}",
    }


def fetch_new(pages: int = 1):
    """Sequences OEIS itself tags as recently added. This is the live feed."""
    now = datetime.now(timezone.utc).isoformat()
    for page in range(pages):
        rows = _get("keyword:new", start=page * 10)
        if not rows:
            return
        for r in rows:
            yield _norm(r, now)


def fetch_by_id(a_number: str):
    """One sequence, e.g. 'A000045' or '45'."""
    now = datetime.now(timezone.utc).isoformat()
    q = a_number if a_number.upper().startswith("A") else f"A{int(a_number):06d}"
    for r in _get(f"id:{q}"):
        yield _norm(r, now)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "new"
    if mode == "id":
        for rec in fetch_by_id(sys.argv[2] if len(sys.argv) > 2 else "A000045"):
            print(json.dumps(rec, ensure_ascii=False))
    else:
        pages = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        for rec in fetch_new(pages=pages):
            print(json.dumps(rec, ensure_ascii=False))
