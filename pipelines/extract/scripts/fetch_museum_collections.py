#!/usr/bin/env python3
"""Museum open-access collections — Art Institute of Chicago, Met, Cleveland.

Sector: visual art / cultural heritage. Keyless, and unusually well-made for
free APIs — museums built these deliberately for public reuse, not as an
afterthought.

**Honest framing on cadence:** a museum collection is not a firehose. New
acquisitions and digitisations trickle in. So don't build this as a
change-detection pipeline; build it as one of two things instead:

  1. **A reference dimension.** Rich, multilingual, well-curated metadata —
     artist, culture, place of origin, date range, medium, dimensions — to
     join against faster-moving sources. "What art is trending on Wikipedia
     today" needs a Wikipedia edit stream AND a collection table.
  2. **A slow-change dataset that rewards re-polling anyway.** Records get
     *revised*: attributions change, dates get corrected, `is_public_domain`
     and `is_on_view` flip. Poll monthly on `updated_at` and you capture
     scholarship changing its mind — which is a lovely, unusual dataset.

The `is_on_view` / gallery fields on AIC are the most dynamic part: what's
physically hanging on the wall rotates continuously.

Verification status 2026-09-02: KNOWN. These are long-stable public APIs but
I did not fetch them this session (sandbox host restrictions). Confirm field
names on first run — the three museums use quite different vocabularies, and
the normalizer below papers over that.

Endpoints:
  AIC:       https://api.artic.edu/api/v1/artworks?page=&limit=&fields=
             (supports ?query= search via /artworks/search)
  Met:       https://collectionapi.metmuseum.org/public/collection/v1/objects
             then /objects/{id}  — NOTE: the list endpoint returns ~500k bare
             IDs, so you must hydrate one at a time. Be gentle.
  Cleveland: https://openaccess-api.clevelandart.org/api/artworks/?limit=

Notes:
  * **AIC is the nicest of the three** — real pagination, a `fields` selector,
    an Elasticsearch-backed search endpoint, and a documented rate limit.
    Start here.
  * The Met's design (list-all-IDs, then hydrate individually) means a full
    crawl is ~500k requests. Don't. Use it for targeted lookups or take their
    published CSV dump instead.
  * Licensing varies per object, not per museum. Respect `is_public_domain`
    (AIC) / `isPublicDomain` (Met) before reusing any image.

Stdlib only.
"""
import json
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"

AIC = "https://api.artic.edu/api/v1/artworks"
MET = "https://collectionapi.metmuseum.org/public/collection/v1"
CMA = "https://openaccess-api.clevelandart.org/api/artworks/"

AIC_FIELDS = ",".join([
    "id", "title", "artist_display", "artist_title", "place_of_origin",
    "date_display", "date_start", "date_end", "medium_display",
    "classification_title", "department_title", "is_public_domain",
    "is_on_view", "gallery_title", "updated_at", "image_id", "colorfulness",
])


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.load(resp)


def fetch_aic(page: int = 1, limit: int = 100, pages: int = 1):
    """Art Institute of Chicago. The best-designed of the three."""
    now = datetime.now(timezone.utc).isoformat()
    for p in range(page, page + pages):
        url = AIC + "?" + urllib.parse.urlencode(
            {"page": p, "limit": min(limit, 100), "fields": AIC_FIELDS})
        data = _get(url)
        for a in data.get("data", []):
            yield {
                "source": "museum_aic",
                "fetched_at": now,         # snapshot; attributions get revised
                "id": f"aic:{a.get('id')}",
                "title": a.get("title"),
                "artist": a.get("artist_title") or a.get("artist_display"),
                "origin": a.get("place_of_origin"),
                "date_display": a.get("date_display"),
                "date_start": a.get("date_start"),
                "date_end": a.get("date_end"),
                "medium": a.get("medium_display"),
                "classification": a.get("classification_title"),
                "department": a.get("department_title"),
                "public_domain": a.get("is_public_domain"),
                "on_view": a.get("is_on_view"),      # rotates continuously
                "gallery": a.get("gallery_title"),
                "updated_at": a.get("updated_at"),   # your re-poll watermark
            }
        if not data.get("pagination", {}).get("next_url"):
            break


def fetch_cleveland(limit: int = 100, skip: int = 0):
    """Cleveland Museum of Art open access."""
    now = datetime.now(timezone.utc).isoformat()
    url = CMA + "?" + urllib.parse.urlencode({"limit": limit, "skip": skip})
    for a in _get(url).get("data", []):
        creators = a.get("creators") or []
        yield {
            "source": "museum_cleveland",
            "fetched_at": now,
            "id": f"cma:{a.get('id')}",
            "title": a.get("title"),
            "artist": (creators[0].get("description") if creators else None),
            "origin": a.get("culture")[0] if a.get("culture") else None,
            "date_display": a.get("creation_date"),
            "date_start": a.get("creation_date_earliest"),
            "date_end": a.get("creation_date_latest"),
            "medium": a.get("technique") or a.get("medium"),
            "classification": a.get("type"),
            "department": a.get("department"),
            "public_domain": (a.get("share_license_status") == "CC0"),
            "on_view": bool(a.get("current_location")),
            "gallery": a.get("current_location"),
            "updated_at": a.get("updated_at"),
        }


def fetch_met_object(object_id: int):
    """Single Met object. The Met has no bulk list-with-detail — hydrate one
    at a time, and only for IDs you actually care about."""
    a = _get(f"{MET}/objects/{object_id}")
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "museum_met",
        "fetched_at": now,
        "id": f"met:{a.get('objectID')}",
        "title": a.get("title"),
        "artist": a.get("artistDisplayName"),
        "origin": a.get("culture") or a.get("country"),
        "date_display": a.get("objectDate"),
        "date_start": a.get("objectBeginDate"),
        "date_end": a.get("objectEndDate"),
        "medium": a.get("medium"),
        "classification": a.get("classification"),
        "department": a.get("department"),
        "public_domain": a.get("isPublicDomain"),
        "on_view": bool(a.get("GalleryNumber")),
        "gallery": a.get("GalleryNumber"),
        "updated_at": a.get("metadataDate"),
    }


if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "aic"
    if which == "cleveland":
        gen = fetch_cleveland(limit=20)
    elif which == "met":
        print(json.dumps(fetch_met_object(int(sys.argv[2])), ensure_ascii=False))
        raise SystemExit
    else:
        gen = fetch_aic(limit=20)
    for rec in gen:
        print(json.dumps(rec, ensure_ascii=False))
