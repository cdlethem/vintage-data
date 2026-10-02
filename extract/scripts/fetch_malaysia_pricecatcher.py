#!/usr/bin/env python3
"""Malaysia PriceCatcher — daily retail shelf-price observations.

gaps. Verified live 2026-09-03 against the official current-month CSV. The object can
contain millions of rows and changes in place throughout the month. This client streams
the response, emitting every row by default or retaining only the newest requested
number of rows. Item and premise lookup tables are separate. Data is CC BY 4.0; fetch
once daily.

The publisher uploads each new month's file shortly after the Malaysia month rollover;
until it exists the current-month URL 404s. A run without an explicit --month then falls
back to the previous month's completed file instead of failing, so the daily series
stays continuous across month boundaries. An explicit --month (e.g. backfills) keeps
failing on a 404. Verified 2026-10-01: pricecatcher_2026-10.csv was not published while
pricecatcher_2026-09.csv served fine.

Stdlib only.
"""
import argparse
import csv
import io
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime, timedelta, timezone

BASE_URL = "https://storage.data.gov.my/pricecatcher"
MALAYSIA_TIME = timezone(timedelta(hours=8))
SOURCE = "malaysia_pricecatcher"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"


def _previous_month(month: str) -> str:
    """The YYYY-MM month immediately before `month`, wrapping January to December."""
    year, number = (int(part) for part in month.split("-"))
    if number == 1:
        return f"{year - 1}-12"
    return f"{year}-{number - 1:02d}"


def _stream_month(month: str, limit: int | None, timeout: int):
    request = urllib.request.Request(
        f"{BASE_URL}/pricecatcher_{month}.csv",
        headers={"User-Agent": USER_AGENT},
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        modified = response.headers.get("Last-Modified")
        reader = csv.DictReader(io.TextIOWrapper(response, encoding="utf-8-sig", newline=""))
        expected = {"date", "premise_code", "item_code", "price"}
        if not reader.fieldnames or not expected.issubset(reader.fieldnames):
            raise ValueError(f"PriceCatcher CSV is missing columns: {sorted(expected)}")
        if limit is None:
            for row in reader:
                record = dict(row)
                record.update({
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": f"{row['date']}|{row['premise_code']}|{row['item_code']}",
                    "publisher_updated_at": modified,
                })
                yield record
            return
        rows = deque(reader, maxlen=limit)

    for row in rows:
        record = dict(row)
        record.update({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": f"{row['date']}|{row['premise_code']}|{row['item_code']}",
            "publisher_updated_at": modified,
        })
        yield record


def fetch_prices(month: str | None = None, limit: int | None = None, timeout: int = 120):
    if limit is not None and limit <= 0:
        return
    month_was_default = month is None
    month = month or datetime.now(MALAYSIA_TIME).strftime("%Y-%m")
    if not re.fullmatch(r"\d{4}-(?:0[1-9]|1[0-2])", month):
        raise ValueError("month must use YYYY-MM")

    try:
        yield from _stream_month(month, limit, timeout)
        return
    except urllib.error.HTTPError as error:
        if not month_was_default or error.code != 404:
            raise
        previous = _previous_month(month)
        print(
            f"pricecatcher_{month}.csv not published yet; "
            f"falling back to pricecatcher_{previous}.csv",
            file=sys.stderr,
        )

    yield from _stream_month(previous, limit, timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", help="YYYY-MM; defaults to the current month in Malaysia")
    parser.add_argument("--limit", type=int, default=None,
                        help="omit to emit the complete file")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    for record in fetch_prices(args.month, args.limit, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
