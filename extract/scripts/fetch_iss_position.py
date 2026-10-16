#!/usr/bin/env python3
"""Where the ISS at? — live International Space Station position.

Each run polls the keyless WTIA v1 API once and emits the sub-satellite
point (latitude, longitude, altitude, velocity, visibility, footprint) at
the observation timestamp as a single NDJSON record. The position changes
continuously (~27.6 km/s), so every five-minute poll is a new observation;
poll no faster than every five minutes. The positions are computed from
public-domain NORAD TLE ephemerides and complement the celestrak_socrates
TLE collection.

Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone

SOURCE = "iss_position"
SATELLITE_ID = 25544
URL = f"https://api.wheretheiss.at/v1/satellites/{SATELLITE_ID}"
ATTRIBUTION = (
    "Where the ISS at? (https://wheretheiss.at), computed from "
    "public-domain NORAD TLE ephemerides"
)
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def fetch_position(timeout: int = 30):
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    request = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        row = json.load(response)
    if not isinstance(row, dict):
        raise TypeError("WTIA response is not an object")
    for field in ("latitude", "longitude", "timestamp"):
        if row.get(field) is None:
            raise ValueError(f"WTIA response is missing {field}")
    record = dict(row)
    record.update(
        {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"iss-{row['timestamp']}",
            "norad_id": row.get("id"),
            "attribution": ATTRIBUTION,
        }
    )
    return record


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)
    print(json.dumps(fetch_position(args.timeout), ensure_ascii=False))


if __name__ == "__main__":
    main()
