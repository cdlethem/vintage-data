#!/usr/bin/env python3
"""Apple/iTunes RSS charts — keyless per-country top songs, albums and podcasts.

Lead #68. The interesting property is stated in the catalog: **the international
comparison is the point** — the same feed shape across 100+ storefronts lets you watch
a track propagate country by country, day by day.

**Verified live 2026-09-03.** The newer `rss.applemarketingtools.com/api/v2/...`
endpoint documented in Apple's own marketing-tools GitHub repo returned **403 Forbidden**
from this sandbox on every attempt, headers included. The older, still-live
`itunes.apple.com/{storefront}/rss/{chart}/limit={n}/json` endpoint worked cleanly:
  * `itunes.apple.com/us/rss/topsongs/limit=10/json` — **200, 22,435 bytes**, 10 entries,
    top track "Choosin' Texas".
  * `itunes.apple.com/jp/rss/topalbums/limit=5/json` — **200, 9,026 bytes** — confirmed
    the same shape works across storefronts and chart types.

Endpoints (base `https://itunes.apple.com/{storefront}/rss/{chart}/limit={n}/json`):
    storefront: two-letter country code (us, jp, gb, de, ...) — 100+ available
    chart: topsongs | topalbums | topmovies | toppodcasts | topaudiobooks | ... (many)
    n:     how many chart entries to return

Quirks that will cost someone an afternoon:
    * This is Atom-flavored JSON, not a clean REST shape. Every field is a nested
      `{"label": ..., "attributes": {...}}` object, even simple ones — `feed.entry[i]
      ["im:name"]["label"]` is the track title, not `entry[i]["im:name"]` directly.
    * `im:price` carries both a human label (`"$0.69"`) and a machine `attributes.amount`
      + `attributes.currency` — use the attributes, not the label, for anything numeric.
    * `category` is chart-genre metadata (e.g. "Country"), not the chart type you
      requested — don't confuse the two.
    * The documented newer endpoint (`rss.applemarketingtools.com`) 403'd from this
      sandbox; if it works from your network prefer it (it returns flatter JSON), but
      build against the older `itunes.apple.com` path as the fallback that is confirmed
      keyless and live.
    * `rights` holds a copyright/label string, often with a `℗` (phonogram) mark —
      decode as UTF-8, don't strip non-ASCII.
    * **`link` is sometimes a single object and sometimes a list of objects** — an
      entry with only a page link sends one dict; an entry that also carries an audio
      preview sends a list of `{attributes: {rel, type, href}}`, one per `rel`
      ("alternate" for the page, "enclosure" for the preview). Index into it naively
      and you get `AttributeError: 'list' object has no attribute 'get'` on roughly
      half of real entries — confirmed live. `_first_link()` below normalizes both
      shapes and picks the `rel="alternate"` page URL.

Etiquette: keyless, no published rate limit on the older endpoint. Charts update at
most daily; polling more than once a day is waste. Identify yourself with a real
User-Agent regardless.

Stdlib only.
"""
import json
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: cdlethem@gmail.com)"
BASE = "https://itunes.apple.com"


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def _first_link(entry, rel="alternate"):
    """`link` is one dict for some entries, a list of dicts for others (one per rel
    type: alternate=page, enclosure=audio preview). Normalize and pick by rel."""
    link = entry.get("link")
    if link is None:
        return None
    candidates = link if isinstance(link, list) else [link]
    for c in candidates:
        if (c.get("attributes") or {}).get("rel") == rel:
            return c["attributes"].get("href")
    # fall back to whatever's first if the wanted rel isn't present
    if candidates:
        return (candidates[0].get("attributes") or {}).get("href")
    return None


def _label(d, path, default=None):
    """Walk a chain of Atom-JSON {"label": ...} wrappers, e.g. ('im:name',)."""
    cur = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur.get("label", default) if isinstance(cur, dict) else cur


def fetch_chart(storefront: str = "us", chart: str = "topsongs", limit: int = 25):
    """One country's chart, ranked. `chart` is one of topsongs/topalbums/toppodcasts/..."""
    url = f"{BASE}/{storefront}/rss/{chart}/limit={limit}/json"
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(url)
    entries = doc.get("feed", {}).get("entry", [])
    for rank, e in enumerate(entries, start=1):
        price = e.get("im:price", {}).get("attributes", {})
        yield {
            "source": "itunes_charts",
            "fetched_at": now,
            "id": (e.get("id") or {}).get("attributes", {}).get("im:id")
                  or (e.get("id") or {}).get("label"),
            "storefront": storefront,
            "chart": chart,
            "rank": rank,
            "name": _label(e, ("im:name",)),
            "artist": _label(e, ("im:artist",)),
            "collection": _label(e, ("im:collection", "im:name")),
            "release_date": _label(e, ("im:releaseDate",)),
            "genre": (e.get("category") or {}).get("attributes", {}).get("label"),
            "price_amount": price.get("amount"),
            "price_currency": price.get("currency"),
            "url": _first_link(e),
        }


if __name__ == "__main__":
    storefront = sys.argv[1] if len(sys.argv) > 1 else "us"
    chart = sys.argv[2] if len(sys.argv) > 2 else "topsongs"
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 25
    for rec in fetch_chart(storefront, chart, limit):
        print(json.dumps(rec, ensure_ascii=False))
