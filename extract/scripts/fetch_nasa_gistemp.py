#!/usr/bin/env python3
"""Snapshot NASA GISTEMP v4 monthly land-ocean temperature anomalies as NDJSON.

GISTEMP publishes the Land-Ocean Temperature Index (L-OTI) as three small CSV
tables: global, northern hemisphere, and southern hemisphere. Each table holds
monthly anomalies relative to the 1951-1980 baseline from 1880 to the present;
a new month appears roughly two weeks after the month ends, and published values
do not change between releases of the same dataset version.

Each run makes three bounded requests (one per table) and emits one record per
area-year-month. Months GISS marks as having too little data ("***") are emitted
with a null anomaly so gaps stay visible in the time series. Seasonal and annual
aggregate columns are validated but not emitted; they are derivable from the
monthly values.

Stdlib only.
"""

import argparse
from datetime import datetime, timezone
import json
import os
import re
import sys
from typing import Any, Iterator, Sequence
import urllib.request


SOURCE = "nasa_gistemp"
BASE_URL = "https://data.giss.nasa.gov/gistemp/tabledata_v4/"
SERIES = (
    ("glb", "GLB.Ts+dSST.csv"),
    ("nh", "NH.Ts+dSST.csv"),
    ("sh", "SH.Ts+dSST.csv"),
)
EXPECTED_HEADER = (
    "Year,Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec,J-D,D-N,DJF,MAM,JJA,SON"
)
FIELD_COUNT = 19
MONTH_COUNT = 12
FIRST_YEAR = 1880
MAX_SPAN_YEARS = 200
ANOMALY_BASIS = "1951-1980"
MISSING = "***"
ANOMALY_RE = re.compile(r"^-?(?:\d+\.\d+|\.\d+)$")
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


def _parse_anomaly(field: str, context: str) -> float | None:
    if field == MISSING:
        return None
    if not ANOMALY_RE.match(field):
        raise ValueError(f"GISTEMP {context} has a malformed value {field!r}")
    return float(field)


def parse_series(
    name: str, filename: str, text: str, current_year: int
) -> list[dict[str, Any]]:
    """Validate one GISTEMP CSV table and expand it to area-year-month records."""
    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) < 3:
        raise ValueError(f"GISTEMP {filename} has fewer than three lines")

    title = lines[0]
    if not title.strip():
        raise ValueError(f"GISTEMP {filename} is missing its title line")
    if lines[1] != EXPECTED_HEADER:
        raise ValueError(f"GISTEMP {filename} has an unexpected header line")

    records: list[dict[str, Any]] = []
    seen_value = False
    for index, line in enumerate(lines[2:]):
        year = FIRST_YEAR + index
        context = f"{filename} row {year}"
        fields = line.split(",")
        if len(fields) != FIELD_COUNT:
            raise ValueError(
                f"GISTEMP {context} has {len(fields)} fields, expected {FIELD_COUNT}"
            )
        year_field = fields[0]
        if not year_field.isdigit() or int(year_field) != year:
            raise ValueError(
                f"GISTEMP {context} year field {year_field!r} does not continue "
                f"the consecutive run from {FIRST_YEAR}"
            )
        if year > current_year:
            raise ValueError(f"GISTEMP {filename} contains a future year {year}")

        months = [
            _parse_anomaly(field, f"{context} month {index + 1}")
            for index, field in enumerate(fields[1:1 + MONTH_COUNT])
        ]
        for index, field in enumerate(fields[1 + MONTH_COUNT:]):
            _parse_anomaly(field, f"{context} aggregate column {index + 1}")

        for month, value in enumerate(months, start=1):
            if value is not None:
                seen_value = True
            records.append(
                {
                    "source": SOURCE,
                    "id": f"{name}:{year:04d}-{month:02d}",
                    "series": name,
                    "year": year,
                    "month": month,
                    "anomaly_celsius": value,
                    "anomaly_basis": ANOMALY_BASIS,
                }
            )

    last_year = FIRST_YEAR + len(lines) - 3
    if last_year < current_year - 1:
        raise ValueError(
            f"GISTEMP {filename} ends in {last_year}; expected at least "
            f"{current_year - 1}"
        )
    if last_year - FIRST_YEAR + 1 > MAX_SPAN_YEARS:
        raise ValueError(f"GISTEMP {filename} spans more than {MAX_SPAN_YEARS} years")
    if not seen_value:
        raise ValueError(f"GISTEMP {filename} contains no numeric anomalies")

    return records


def fetch_gistemp(
    timeout: int = DEFAULT_TIMEOUT, *, current_year: int | None = None
) -> Iterator[dict[str, Any]]:
    """Fetch the three GISTEMP L-OTI tables in three bounded requests."""
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")
    if current_year is None:
        current_year = datetime.now(timezone.utc).year
    fetched_at = datetime.now(timezone.utc).isoformat()
    for name, filename in SERIES:
        request = urllib.request.Request(
            BASE_URL + filename,
            headers={
                "Accept": "text/csv",
                "User-Agent": USER_AGENT,
            },
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            text = response.read().decode("utf-8")

        for record in parse_series(name, filename, text, current_year):
            record["fetched_at"] = fetched_at
            yield record


def _run_summary(records: Sequence[dict[str, Any]]) -> None:
    areas: dict[str, dict[str, Any]] = {}
    for record in records:
        stats = areas.setdefault(
            record["series"], {"rows": 0, "missing": 0, "first": None, "last": None}
        )
        stats["rows"] += 1
        stamp = f"{record['year']:04d}-{record['month']:02d}"
        if record["anomaly_celsius"] is None:
            stats["missing"] += 1
        else:
            if stats["first"] is None:
                stats["first"] = stamp
            stats["last"] = stamp
    payload = {"rows": len(records), "areas": areas}
    print(SUMMARY_PREFIX + json.dumps(payload, separators=(",", ":")), file=sys.stderr)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    records = list(fetch_gistemp(timeout=args.timeout))
    for record in records:
        print(json.dumps(record, ensure_ascii=False))
    _run_summary(records)


if __name__ == "__main__":
    main()
