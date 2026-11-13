#!/usr/bin/env python3
"""Snapshot the SPC severe weather database tornado reports as NDJSON.

The Storm Prediction Center publishes its U.S. tornado history (1950-present)
as CSV files linked from https://www.spc.noaa.gov/wcm/. Each run makes two
bounded requests: the data page, to resolve the latest
``1950-YYYY_all_tornadoes.csv`` file, and that file itself. One record is
emitted per CSV row; a row is one tornado report or one state/county segment
of a multi-state tornado.

The SPC ``om`` report number is unique only within a year, and a tornado
crossing a state line repeats once per state, so the record ID is the
SHA-256 of the row's canonical values. Source values are preserved verbatim
as source strings: ``date``/``time`` are the touchdown in the zone ``tz``
names, ``mag`` is the EF rating (``-9`` unrated), ``inj``/``fat`` are people,
``loss``/``closs`` are U.S. dollars, ``slat``/``slon``/``elat``/``elon`` are
decimal degrees, ``len`` is path length in miles, ``wid`` is path width in
feet, and ``edat``/``etime`` are empty on older rows.

Stdlib only.
"""

import argparse
import csv
import hashlib
import io
import json
import math
import os
import re
import sys
from datetime import date, datetime, time, timezone
from typing import Any, Iterator, Sequence
import urllib.request


SOURCE = "spc_tornado_reports"
INDEX_URL = "https://www.spc.noaa.gov/wcm/"
DATA_BASE = "https://www.spc.noaa.gov/wcm/data/"
ALL_FILE_RE = re.compile(r"1950-(\d{4})_all_tornadoes\.csv")
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 300
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

# The columns SPC publishes in the all-tornadoes files since 1950. ``edat``
# and ``etime`` were added in the 2026 release and are empty on older rows.
EXPECTED_COLUMNS = (
    "om", "yr", "mo", "dy", "date", "time", "tz", "st", "stf", "stn",
    "mag", "inj", "fat", "loss", "closs", "slat", "slon", "elat", "elon",
    "len", "wid", "ns", "sn", "sg", "f1", "f2", "f3", "f4", "fc",
    "edat", "etime",
)
INT_COLUMNS = (
    "yr", "mo", "dy", "tz", "stf", "stn", "mag", "inj", "fat",
    "ns", "sn", "sg", "f1", "f2", "f3", "f4", "fc",
)
FLOAT_COLUMNS = ("loss", "closs", "slat", "slon", "elat", "elon", "len", "wid")

DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
TIME_RE = re.compile(r"\d{2}:\d{2}:\d{2}")


def _require(column: str, value: Any, row_no: int) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"SPC tornado row {row_no} has a missing or empty {column!r} value"
        )
    return value


def _parse_int(column: str, value: str, row_no: int) -> None:
    try:
        int(value)
    except ValueError:
        raise ValueError(
            f"SPC tornado row {row_no} has a non-integer {column!r}: {value!r}"
        ) from None


def _parse_float(column: str, value: str, row_no: int) -> float:
    try:
        number = float(value)
    except ValueError:
        raise ValueError(
            f"SPC tornado row {row_no} has a non-numeric {column!r}: {value!r}"
        ) from None
    if not math.isfinite(number):
        raise ValueError(
            f"SPC tornado row {row_no} has a non-finite {column!r}: {value!r}"
        )
    return number


def _parse_date(column: str, value: str, row_no: int) -> None:
    if not DATE_RE.fullmatch(value):
        raise ValueError(
            f"SPC tornado row {row_no} has a malformed {column!r}: {value!r}"
        )
    try:
        date.fromisoformat(value)
    except ValueError:
        raise ValueError(
            f"SPC tornado row {row_no} has an impossible {column!r}: {value!r}"
        ) from None


def _parse_time(column: str, value: str, row_no: int) -> None:
    if not TIME_RE.fullmatch(value):
        raise ValueError(
            f"SPC tornado row {row_no} has a malformed {column!r}: {value!r}"
        )
    try:
        time.fromisoformat(value)
    except ValueError:
        raise ValueError(
            f"SPC tornado row {row_no} has an impossible {column!r}: {value!r}"
        ) from None


def row_id(columns: Sequence[str], row: dict[str, Any]) -> str:
    """Stable ID: SHA-256 of the row's canonical column=value values."""
    canonical = "\n".join(f"{column}={row[column]}" for column in columns)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_row(row: dict[str, Any], columns: Sequence[str],
                  row_no: int, fetched_at: str) -> dict[str, Any]:
    """Validate one CSV row without changing its source values."""
    for column in EXPECTED_COLUMNS:
        value = row.get(column)
        if column in ("edat", "etime"):
            if value is None:
                raise ValueError(f"SPC tornado row {row_no} is missing {column!r}")
            if value:
                if column == "edat":
                    _parse_date(column, value, row_no)
                else:
                    _parse_time(column, value, row_no)
            continue
        _require(column, value, row_no)

    for column in INT_COLUMNS:
        _parse_int(column, row[column], row_no)
    if not -9 <= int(row["mag"]) <= 5:
        raise ValueError(
            f"SPC tornado row {row_no} has an impossible EF rating: {row['mag']!r}"
        )
    if not 1 <= int(row["mo"]) <= 12:
        raise ValueError(
            f"SPC tornado row {row_no} has an impossible month: {row['mo']!r}"
        )
    if not 1 <= int(row["dy"]) <= 31:
        raise ValueError(
            f"SPC tornado row {row_no} has an impossible day: {row['dy']!r}"
        )
    _parse_date("date", row["date"], row_no)
    _parse_time("time", row["time"], row_no)
    state = row["st"]
    if len(state) != 2 or not state.isalpha():
        raise ValueError(
            f"SPC tornado row {row_no} has an invalid state: {state!r}"
        )

    for column in FLOAT_COLUMNS:
        number = _parse_float(column, row[column], row_no)
        if column in ("slat", "elat") and not -90 <= number <= 90:
            raise ValueError(
                f"SPC tornado row {row_no} has an impossible "
                f"{column!r}: {row[column]!r}"
            )
        if column in ("slon", "elon") and not -180 <= number <= 180:
            raise ValueError(
                f"SPC tornado row {row_no} has an impossible "
                f"{column!r}: {row[column]!r}"
            )
        if column in ("loss", "closs", "len", "wid") and number < 0:
            raise ValueError(
                f"SPC tornado row {row_no} has a negative "
                f"{column!r}: {row[column]!r}"
            )

    return {
        "source": SOURCE,
        "id": row_id(columns, row),
        "fetched_at": fetched_at,
        **{column: row[column] for column in columns},
    }


def _latest_all_file_url(timeout: int) -> str:
    """Return the URL of the newest 1950-YYYY_all_tornadoes.csv on the page."""
    request = urllib.request.Request(
        INDEX_URL, headers={"Accept": "text/html", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        page = response.read().decode("utf-8", errors="replace")
    years = [int(year) for year in ALL_FILE_RE.findall(page)]
    if not years:
        raise ValueError("SPC data page lists no 1950-YYYY_all_tornadoes.csv file")
    return f"{DATA_BASE}1950-{max(years):04d}_all_tornadoes.csv"


def _get_csv(csv_url: str, timeout: int) -> str:
    request = urllib.request.Request(
        csv_url, headers={"Accept": "text/csv", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8-sig")


def fetch_spc_tornado_reports(timeout: int = DEFAULT_TIMEOUT) -> Iterator[dict[str, Any]]:
    """Resolve the newest all-tornadoes file and yield one record per row."""
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, int)
        or not 1 <= timeout <= MAX_TIMEOUT
    ):
        raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT} seconds")

    csv_url = _latest_all_file_url(timeout)
    text = _get_csv(csv_url, timeout)
    reader = csv.DictReader(io.StringIO(text, newline=""))
    if not reader.fieldnames:
        raise ValueError("SPC tornado CSV is empty or has no header row")
    columns = tuple(reader.fieldnames)
    missing = [column for column in EXPECTED_COLUMNS if column not in columns]
    if missing:
        raise ValueError(f"SPC tornado CSV is missing columns: {missing}")

    fetched_at = datetime.now(timezone.utc).isoformat()
    for row_no, row in enumerate(reader, start=2):
        if None in row or None in row.values():
            raise ValueError(
                f"SPC tornado row {row_no} has the wrong number of values"
            )
        yield normalize_row(row, columns, row_no, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    records = 0
    for record in fetch_spc_tornado_reports(timeout=args.timeout):
        print(json.dumps(record, ensure_ascii=False))
        records += 1
    print(
        f"{SOURCE}: {records} tornado rows from the latest all-tornadoes file",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
