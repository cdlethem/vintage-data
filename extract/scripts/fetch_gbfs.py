#!/usr/bin/env python3
"""GBFS — the General Bikeshare Feed Specification, a protocol client covering
hundreds of bike- and scooter-share systems worldwide with one script.

Lead #18 in DATA_SOURCE_IDEAS.md. Like `fetch_socrata_civic.py` for open-data portals
and `fetch_stac_imagery.py` for satellite catalogs, this is a *protocol* client: every
GBFS-compliant system publishes the same discovery file and the same feed names, so
covering one system means covering all of them — MobilityData's official directory
(github.com/MobilityData/gbfs/blob/master/systems.csv) lists several hundred, including
Madison's own BCycle, named explicitly in the catalog blurb.

**Verified live 2026-09-03** against three independent operators, all responding to the
identical discovery pattern:
  * Divvy (Chicago) — `gbfs.divvybikes.com/gbfs/gbfs.json` -> 12 sub-feeds
  * Madison BCycle — `gbfs.bcycle.com/bcycle_madison/gbfs.json` -> 200, real feed index
  * Citibike (NYC) — `gbfs.citibikenyc.com/gbfs/gbfs.json` -> 200, same shape
`station_status.json` for Divvy returned **749,854 bytes, 2,051 stations**;
`station_information.json` returned **1,120,670 bytes** of matching metadata.

**Why it's a good pipeline subject.** `station_status` refreshes on a ~30-second cycle
per the spec (most operators honor something close to that) and gives you bike/dock
counts per station — clean, high-frequency, strongly seasonal numeric data with an
obvious spatial dimension. `station_information` is the slow-moving join (name, lat/lon,
capacity) you pair it against.

Protocol shape:
    GET {base}/gbfs.json                        -> discovery: language -> feed name -> url
    GET {feeds}/station_information.json         -> static-ish: id, name, lat, lon, capacity
    GET {feeds}/station_status.json              -> the fast one: bikes/docks available now
    GET {feeds}/free_bike_status.json            -> dockless/ebike fleets (not all systems have this)

Quirks that will cost someone an afternoon:
  * **A missing station is not deleted, it's `is_installed: 0` with garbage
    telemetry.** Verified live: a Divvy station read `is_installed: 0, is_renting: 0,
    num_bikes_available: 0, num_docks_available: 0, last_reported: 86400` — that
    `last_reported` is 1970-01-02, a sentinel, not a real report time. Flag
    `is_installed == 0` stations and don't trust their counts or timestamp.
  * **GBFS 1.x ships `is_installed`/`is_renting`/`is_returning` as 0/1 integers; GBFS
    2.x+ ships them as JSON booleans.** This script normalizes both to bool. Check
    `gbfs_versions.json` in the discovery payload if you need to know which you got.
  * The discovery file is nested by **language** (`data.en.feeds`, `data.fr.feeds`,
    ...) even for single-language systems — always index by language first.
  * Not every system publishes every feed (dockless-only systems have no
    `station_information`; classic-dock-only systems have no `free_bike_status`).
    Check the discovery payload rather than assuming feed names.
  * `station_id` in `station_status` and `station_information` is the join key, but
    some operators (Divvy included) also carry a separate `legacy_id` some older
    integrations expect — prefer `station_id`.

Etiquette: keyless, no rate limit published by the spec itself (individual operators
occasionally rate-limit at the CDN level). Poll `station_status` at roughly the cadence
it actually updates (spec suggests <=60s); polling faster than the feed's own TTL
(`data.ttl` in the discovery/status payloads) is pure waste.

Stdlib only.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

# A few systems named or implied in the catalog, to make this runnable out of the box.
# Any GBFS discovery URL works — see MobilityData's systems.csv for hundreds more.
SYSTEMS = {
    "divvy": "https://gbfs.divvybikes.com/gbfs/gbfs.json",
    "madison": "https://gbfs.bcycle.com/bcycle_madison/gbfs.json",
    "citibike": "https://gbfs.citibikenyc.com/gbfs/gbfs.json",
}


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.load(resp)


def discover(gbfs_url: str, lang: str | None = None):
    """{feed_name: feed_url} for one system, from its gbfs.json discovery file."""
    doc = _get(gbfs_url)["data"]
    lang = lang or next(iter(doc))
    return {f["name"]: f["url"] for f in doc[lang]["feeds"]}


def _bool01(v):
    """GBFS 1.x sends 0/1 ints for these flags; 2.x+ sends real booleans."""
    if isinstance(v, bool):
        return v
    return bool(v)


def fetch_station_status(gbfs_url: str, system: str = "gbfs"):
    """The fast-moving feed: bikes/docks available right now, per station."""
    feeds = discover(gbfs_url)
    if "station_status" not in feeds:
        return
    now = datetime.now(timezone.utc).isoformat()
    doc = _get(feeds["station_status"])
    ttl = doc.get("ttl")
    for s in doc["data"]["stations"]:
        installed = _bool01(s.get("is_installed", 1))
        last_reported = s.get("last_reported")
        yield {
            "source": f"gbfs_{system}",
            "fetched_at": now,
            "id": s["station_id"],
            "station_id": s["station_id"],
            "is_installed": installed,
            "is_renting": _bool01(s.get("is_renting", 0)),
            "is_returning": _bool01(s.get("is_returning", 0)),
            # a sentinel like epoch+1day shows up on de-installed stations; flag rather
            # than trust it when the station is not installed
            "last_reported": last_reported if installed else None,
            "last_reported_raw": last_reported,
            "num_bikes_available": s.get("num_bikes_available"),
            "num_docks_available": s.get("num_docks_available"),
            "num_ebikes_available": s.get("num_ebikes_available"),
            "num_bikes_disabled": s.get("num_bikes_disabled"),
            "num_docks_disabled": s.get("num_docks_disabled"),
            "ttl": ttl,
        }


def fetch_station_information(gbfs_url: str, system: str = "gbfs"):
    """The slow-moving join: station identity, location, capacity."""
    feeds = discover(gbfs_url)
    if "station_information" not in feeds:
        return
    now = datetime.now(timezone.utc).isoformat()
    for s in _get(feeds["station_information"])["data"]["stations"]:
        yield {
            "source": f"gbfs_{system}",
            "fetched_at": now,
            "id": s["station_id"],
            "station_id": s["station_id"],
            "name": s.get("name"),
            "lat": s.get("lat"),
            "lon": s.get("lon"),
            "capacity": s.get("capacity"),
            "station_type": s.get("station_type"),
        }


if __name__ == "__main__":
    system = sys.argv[1] if len(sys.argv) > 1 else "divvy"
    url = SYSTEMS.get(system, system)  # allow an arbitrary gbfs.json URL too
    mode = sys.argv[2] if len(sys.argv) > 2 else "status"
    fn = fetch_station_information if mode == "info" else fetch_station_status
    for rec in fn(url, system=system if system in SYSTEMS else "custom"):
        print(json.dumps(rec, ensure_ascii=False))
