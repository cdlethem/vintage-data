#!/usr/bin/env python3
"""Finland Digitraffic — live trains calling at one station.

Repeated snapshots expose dwell time, cancellations, delay propagation, and recovery.
Verified live 2026-09-03 against ``/api/v1/live-trains/station/HKI``. The response
contains nested timetable rows; a train is naturally identified by departure date and
train number. Digitraffic requires gzip support and an identifying
``Digitraffic-User`` header. Data is CC BY 4.0. One request per run; do not poll this
station endpoint more often than once per minute.

Stdlib only.
"""
import argparse
import gzip
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE_URL = "https://rata.digitraffic.fi/api/v1/live-trains/station"
SOURCE = "digitraffic_rail_live_trains"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _get(station: str, timeout: int):
    query = urllib.parse.urlencode({
        "minutes_before_departure": 0,
        "minutes_after_departure": 60,
        "minutes_before_arrival": 0,
        "minutes_after_arrival": 60,
    })
    station = urllib.parse.quote(station.upper(), safe="")
    request = urllib.request.Request(
        f"{BASE_URL}/{station}?{query}",
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "Digitraffic-User": USER_AGENT,
            "User-Agent": USER_AGENT,
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
        if response.headers.get("Content-Encoding", "").lower() == "gzip":
            body = gzip.decompress(body)
    data = json.loads(body)
    if not isinstance(data, list):
        raise ValueError("Digitraffic response is not a train list")
    return data


def fetch_trains(station: str = "HKI", limit: int = 100, timeout: int = 30):
    if limit <= 0:
        return
    fetched_at = datetime.now(timezone.utc).isoformat()
    for train in _get(station, timeout)[:limit]:
        departure_date = train.get("departureDate")
        train_number = train.get("trainNumber")
        if departure_date is None or train_number is None:
            raise ValueError("train is missing departureDate or trainNumber")
        record = dict(train)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{departure_date}|{train_number}",
            "requested_station": station.upper(),
        })
        yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--station", default="HKI")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_trains(args.station, max(0, args.limit), args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
