#!/usr/bin/env python3
"""Municipal ArcGIS — Calgary and Tempe public-art inventories.

Daily snapshots reveal installations, removals, status changes, and attribution
corrections. Both official FeatureServer layers were verified live 2026-09-03. Their
property names differ, so records retain source GeoJSON and identify the collection
explicitly. Deletions require comparing complete ID sets across snapshots. Municipal
open-data terms apply. Bare execution fetches both layers, with a one-second pause
between hosts; ``--collection`` allows separate Airflow schedules later.

Stdlib only.
"""
import argparse
import json
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "municipal_public_art_arcgis"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
LAYERS = {
    "calgary": (
        "https://services1.arcgis.com/AVP60cs0Q9PEA8rH/ArcGIS/rest/services/"
        "Calgary_Public_Art_Locations/FeatureServer/0"
    ),
    "tempe": (
        "https://services.arcgis.com/lQySeXwbBg53XWDi/arcgis/rest/services/"
        "Tempe_Public_Art_%28for_open_data%29/FeatureServer/0"
    ),
}


def _get_features(url: str, limit: int, timeout: int):
    query = urllib.parse.urlencode({
        "where": "1=1",
        "outFields": "*",
        "f": "geojson",
        "resultRecordCount": min(limit, 2000),
    })
    request = urllib.request.Request(f"{url}/query?{query}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    if document.get("type") != "FeatureCollection" or not isinstance(document.get("features"), list):
        raise ValueError("ArcGIS response is not a GeoJSON FeatureCollection")
    return document["features"]


def fetch_art(collection: str = "all", limit: int = 1000, timeout: int = 30):
    if limit <= 0:
        return
    names = tuple(LAYERS) if collection == "all" else (collection,)
    fetched_at = datetime.now(timezone.utc).isoformat()
    for index, name in enumerate(names):
        if index:
            time.sleep(1)
        url = LAYERS[name]
        for feature in _get_features(url, limit, timeout)[:limit]:
            properties = feature.get("properties") or {}
            object_id = feature.get("id")
            if object_id is None:
                object_id = properties.get("OBJECTID", properties.get("ObjectId"))
            if object_id is None:
                raise ValueError(f"{name} feature is missing its object ID")
            record = dict(feature)
            record.update({
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{name}|{object_id}",
                "collection": name,
                "layer_url": url,
            })
            yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--collection", choices=("all", *LAYERS), default="all")
    parser.add_argument("--limit", type=int, default=1000, help="maximum records per collection")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_art(args.collection, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
