#!/usr/bin/env python3
"""STAC / Earth Search — new satellite scenes as they are published (keyless).

Gap-list item: **earth observation.**

**What it is:** a SpatioTemporal Asset Catalog (STAC) index of open satellite
imagery on AWS, run by Element 84. New scenes are catalogued continuously as
satellites downlink and process them, so polling a bounding box gives you a
genuine arrival stream: *"what new imagery exists over my area of interest?"*

**Verified live 2026-09-03** — `earth-search.aws.element84.com/v1/collections`
returned **9 collections**: `sentinel-2-l2a`, `sentinel-2-c1-l2a`,
`sentinel-2-l1c`, `sentinel-2-pre-c1-l2a`, `sentinel-1-grd`, `landsat-c2-l2`,
`cop-dem-glo-30`, `cop-dem-glo-90`, `naip`. Sentinel-2 temporal extent runs
from 2015-06-27 to **open-ended (`null`)**, i.e. still growing.

**The key architectural insight, and why this belongs in a pipeline catalog:**
**treat it as a metadata pipeline, not an imagery pipeline.** Each STAC Item is
a small JSON record — scene id, timestamp, footprint geometry, cloud cover,
platform, and *links* to the actual pixels as Cloud-Optimized GeoTIFFs. You can
build a complete, useful time series **without downloading a single raster**:

  * **Revisit cadence** per location — how often is anywhere actually imaged?
  * **Cloud-cover climatology** from `eo:cloud_cover`, free, per tile, for a
    decade. "How often is my city visible from space?" is answerable in an
    afternoon and is genuinely useful for planning any imagery project.
  * **Latency**: compare scene `datetime` (when it was captured) against when
    it first appears in the catalog — a satellite ground-segment SLA you can
    measure yourself.
  * **Sentinel-1 is radar**, so it sees through cloud. Comparing S1 and S2
    availability over the same footprint quantifies exactly what radar buys you.

Because STAC is an **open standard**, this same client works against Microsoft
Planetary Computer, USGS, and dozens of other catalogs — change the base URL.

Endpoints (base `https://earth-search.aws.element84.com/v1`):
  /collections                    list collections (verified)
  /search                         GET or POST; bbox, datetime, collections, query
  /collections/{id}/items         browse one collection

Quirks that matter:
  * **`context.matched` gives the total hit count** — request `limit=1` first to
    size a query cheaply before paging it.
  * Paginate by following the **`rel="next"` link**, not by computing offsets.
  * `datetime` accepts RFC3339 **intervals**: `2026-09-01/2026-09-03`, and open
    ranges with `..`.
  * Filter cloud with the `query` parameter:
    `{"eo:cloud_cover": {"lt": 20}}`.
  * **`sentinel-1-grd` is `storage:requester_pays: true`** — the *metadata* is
    free but downloading those pixels bills your AWS account. Sentinel-2 COGs
    in us-west-2 are free. Know which you're touching before you write a
    download loop.
  * Licences differ per collection (`naip` and `landsat-c2-l2` are public
    domain; Sentinel is "proprietary" under the Copernicus notice). Check
    before redistributing.

Stdlib only.
"""
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

BASE = "https://earth-search.aws.element84.com/v1"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

# Verified live 2026-09-03
KNOWN_COLLECTIONS = [
    "sentinel-2-l2a", "sentinel-2-c1-l2a", "sentinel-2-l1c",
    "sentinel-2-pre-c1-l2a", "sentinel-1-grd", "landsat-c2-l2",
    "cop-dem-glo-30", "cop-dem-glo-90", "naip",
]
REQUESTER_PAYS = {"sentinel-1-grd"}      # metadata free, pixels billed


def _get(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=90) as resp:
        return json.load(resp)


def list_collections():
    now = datetime.now(timezone.utc).isoformat()
    for c in _get(f"{BASE}/collections").get("collections", []):
        temporal = ((c.get("extent") or {}).get("temporal") or {}).get("interval") or [[None, None]]
        yield {
            "source": "stac_collection",
            "fetched_at": now,
            "id": c.get("id"),
            "title": c.get("title"),
            "license": c.get("license"),
            "start": temporal[0][0],
            "end": temporal[0][1],          # None == still growing
            "requester_pays": c.get("id") in REQUESTER_PAYS,
        }


def count_matching(collection: str, bbox, datetime_range: str) -> int:
    """Cheap pre-flight: how many scenes match? Uses context.matched."""
    q = urllib.parse.urlencode({
        "collections": collection, "bbox": ",".join(map(str, bbox)),
        "datetime": datetime_range, "limit": 1})
    data = _get(f"{BASE}/search?{q}")
    return (data.get("context") or {}).get("matched", 0)


def search_scenes(collection: str = "sentinel-2-l2a", bbox=(-89.6, 42.9, -89.2, 43.2),
                  days_back: int = 14, max_cloud: float | None = None,
                  limit: int = 100, max_pages: int = 5):
    """New scenes over a bbox. Default bbox ~ Madison, WI.

    Emits metadata only — no pixels are downloaded.
    """
    end = date.today()
    start = end - timedelta(days=days_back)
    params = {
        "collections": collection,
        "bbox": ",".join(map(str, bbox)),
        "datetime": f"{start.isoformat()}/{end.isoformat()}",
        "limit": min(limit, 100),
    }
    if max_cloud is not None:
        params["query"] = json.dumps({"eo:cloud_cover": {"lt": max_cloud}})
    url = f"{BASE}/search?" + urllib.parse.urlencode(params)

    for _ in range(max_pages):
        data = _get(url)
        now = datetime.now(timezone.utc).isoformat()
        for feat in data.get("features", []):
            yield _norm(feat, now)
        nxt = next((l.get("href") for l in data.get("links", [])
                    if l.get("rel") == "next"), None)
        if not nxt:
            break
        url = nxt                       # follow rel=next, don't compute offsets


def _norm(feat: dict, now: str) -> dict:
    props = feat.get("properties") or {}
    assets = feat.get("assets") or {}
    captured = props.get("datetime")
    # latency = how long between capture and this observation of the catalog
    latency_h = None
    if captured:
        try:
            ts = datetime.fromisoformat(captured.replace("Z", "+00:00"))
            latency_h = round(
                (datetime.now(timezone.utc) - ts).total_seconds() / 3600, 2)
        except ValueError:
            pass
    return {
        "source": "stac_item",
        "fetched_at": now,
        "id": feat.get("id"),
        "collection": feat.get("collection"),
        "captured": captured,
        "age_hours": latency_h,
        "platform": props.get("platform"),
        "constellation": props.get("constellation"),
        "instruments": props.get("instruments"),
        "cloud_cover": props.get("eo:cloud_cover"),
        "gsd": props.get("gsd"),
        "epsg": props.get("proj:epsg"),
        "grid": props.get("grid:code") or props.get("s2:mgrs_tile"),
        "bbox": feat.get("bbox"),
        # links to pixels — deliberately NOT downloaded here
        "asset_keys": sorted(assets.keys()),
        "visual_href": (assets.get("visual") or {}).get("href"),
        "thumbnail_href": (assets.get("thumbnail") or {}).get("href"),
        "requester_pays": feat.get("collection") in REQUESTER_PAYS,
    }


def revisit_report(collection="sentinel-2-l2a", bbox=(-89.6, 42.9, -89.2, 43.2),
                   days_back=60):
    """How often is this bbox imaged, and how often is it actually clear?"""
    scenes = list(search_scenes(collection, bbox, days_back=days_back))
    clouds = [s["cloud_cover"] for s in scenes if s["cloud_cover"] is not None]
    clear = [c for c in clouds if c < 20]
    dates = sorted({(s["captured"] or "")[:10] for s in scenes if s["captured"]})
    return {
        "collection": collection, "bbox": list(bbox), "days": days_back,
        "scenes": len(scenes), "distinct_days": len(dates),
        "revisit_days": round(days_back / len(dates), 2) if dates else None,
        "mean_cloud": round(sum(clouds) / len(clouds), 1) if clouds else None,
        "pct_usable_lt20": round(100 * len(clear) / len(clouds), 1) if clouds else None,
    }


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "collections"
    if mode == "collections":
        for c in list_collections():
            print(json.dumps(c, ensure_ascii=False))
    elif mode == "revisit":
        print(json.dumps(revisit_report(), indent=2))
    else:
        for s in search_scenes(max_cloud=30):
            print(json.dumps(s, ensure_ascii=False))
