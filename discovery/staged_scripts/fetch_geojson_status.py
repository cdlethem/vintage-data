#!/usr/bin/env python3
"""Official GeoJSON status layers — shelters, beaches, and road alerts.

One verified protocol client covers FEMA open shelters, NSW Beachwatch sites, and North
Dakota DOT alerts. All three were fetched live 2026-09-03 and expose stable feature IDs
plus mutable operational properties. Configure ``--feed`` so each Airflow source can
run independently. An empty FeatureCollection is valid; disappearance is meaningful
only after a successful complete response.

Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

FEEDS = {
    "fema_shelters": (
        "https://gis.fema.gov/arcgis/rest/services/NSS/OpenShelters/MapServer/0/query?where=1%3D1&outFields=*&f=geojson",
        "fema_open_shelters",
        "shelter_id",
    ),
    "nsw_beachwatch": (
        "https://api.beachwatch.nsw.gov.au/public/sites/geojson",
        "nsw_beachwatch",
        "id",
    ),
    "nddot_alerts": (
        "https://travelfiles.dot.nd.gov/geojson_nc/alerts.json",
        "north_dakota_road_alerts",
        None,
    ),
}
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_features(feed: str = "fema_shelters", limit: int = 2000, timeout: int = 60):
    if limit <= 0:
        return
    url, source, property_id = FEEDS[feed]
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("features")
    if document.get("type") != "FeatureCollection" or not isinstance(rows, list):
        raise ValueError("response is not a GeoJSON FeatureCollection")
    for feature in rows[:limit]:
        properties = feature.get("properties") or {}
        feature_id = properties.get(property_id) if property_id else feature.get("id")
        if feature_id is None:
            raise ValueError(f"{feed} feature is missing its configured ID")
        record = dict(feature)
        record.update(
            {"source": source, "fetched_at": fetched_at, "id": str(feature_id)}
        )
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feed", choices=tuple(FEEDS), default="fema_shelters")
    parser.add_argument("--limit", type=int, default=2000)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_features(args.feed, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
