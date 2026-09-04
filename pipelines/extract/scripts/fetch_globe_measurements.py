#!/usr/bin/env python3
"""GLOBE — citizen-science measurements by protocol and measured date.

Observation arrival, revision, and campaign activity become time series. Verified live
2026-09-03 against the public protocol/date search API using ``sky_conditions``. The
protocol and date window are configurable; defaults query the current UTC date to bound
the response. ``--limit`` caps output when supplied; omitted, it emits every measurement.
An explicit ``Accept: application/json`` header currently produces HTTP 406, so this
client relies on content negotiation. Follow the GLOBE data-use policy and poll daily
unless tracking an active campaign.

Stdlib only.
"""
import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "globe_protocol_measurements"
URL = "https://api.globe.gov/search/v1/measurement/protocol/measureddate/"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"


def fetch_measurements(
    protocol: str = "sky_conditions",
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int | None = None,
    timeout: int = 60,
):
    if limit is not None and limit <= 0:
        return
    today = datetime.now(timezone.utc).date().isoformat()
    start_date = start_date or today
    end_date = end_date or today
    query = urllib.parse.urlencode({
        "protocols": protocol,
        "startdate": start_date,
        "enddate": end_date,
        "geojson": "FALSE",
        "sample": "FALSE",
    })
    request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)

    rows = document.get("results")
    if document.get("message") != "success" or not isinstance(rows, list):
        raise ValueError("GLOBE response is missing a successful results collection")
    for row in rows[:limit]:
        measurement_id = row.get("measurementId") or row.get("pid")
        if measurement_id is None:
            raise ValueError("GLOBE measurement is missing measurementId and pid")
        record = dict(row)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": str(measurement_id),
        })
        yield record


def main():
    today = datetime.now(timezone.utc).date().isoformat()
    parser = argparse.ArgumentParser()
    parser.add_argument("--protocol", default="sky_conditions")
    parser.add_argument("--start-date", default=today)
    parser.add_argument("--end-date", default=today)
    parser.add_argument("--limit", type=int,
                        help="cap rows emitted; omit for every measurement")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    for record in fetch_measurements(
        args.protocol,
        args.start_date,
        args.end_date,
        args.limit,
        args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
