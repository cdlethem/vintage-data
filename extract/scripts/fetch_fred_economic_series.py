#!/usr/bin/env python3
"""Fetch an explicit basket of US macro time series from FRED's keyless CSV feed.

Each series is one small GET to ``fredgraph.csv``. The feed intermittently
ignores the ``cosd``/``coed`` range parameters and returns full history, so the
client filters observations to a trailing lookback window and drops
out-of-window rows. Missing values arrive as ``.`` or an empty field and are
skipped with a stderr count. Every emitted observation is one NDJSON record
with a stable ``<series>:<date>`` ID, so the run is idempotent and revisions
are re-covered by the lookback window.

Stdlib only.
"""

import argparse
import json
import math
import os
import re
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator, Sequence
from urllib.parse import urlencode

SOURCE = "fred_economic_series"
URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
LOOKBACK_DAYS = 180

SERIES = (
    ("UNRATE", "Unemployment Rate"),
    ("CPIAUCSL", "Consumer Price Index for All Urban Consumers: All Items in U.S. City Average"),
    ("GDP", "Gross Domestic Product"),
    ("T10Y2Y", "10-Year Treasury Constant Maturity Minus 2-Year Treasury Constant Maturity"),
    ("DFF", "Federal Funds Effective Rate"),
    ("DTWEXBGS", "US Dollar Trade-Weighted Exchange Rate Index, Broad"),
)
SERIES_IDS = tuple(series_id for series_id, _ in SERIES)
SERIES_NAMES = dict(SERIES)

DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
MISSING_VALUES = frozenset(("", "."))


def parse_series_csv(
    series_id: str,
    text: str,
    *,
    window_start: date,
    window_end: date,
    fetched_at: str,
) -> tuple[list[dict[str, Any]], int]:
    """Validate one fredgraph.csv body and emit its in-window observations.

    Returns the emitted records and the count of missing-value rows skipped.
    Raises RuntimeError on schema drift, future-dated rows, non-numeric
    values, or a series with no observations inside the window.
    """
    lines = [line for line in text.splitlines() if line != ""]
    if not lines or lines[0] != f"observation_date,{series_id}":
        raise RuntimeError(f"FRED series {series_id} returned an unexpected CSV header")

    records: list[dict[str, Any]] = []
    missing = 0
    for line in lines[1:]:
        raw_date, separator, raw_value = line.partition(",")
        if separator == "" or "," in raw_value:
            raise RuntimeError(
                f"FRED series {series_id} has a malformed row: {line!r}"
            )
        if DATE_RE.fullmatch(raw_date) is None:
            raise RuntimeError(
                f"FRED series {series_id} has an invalid date: {raw_date!r}"
            )
        try:
            observation_date = date.fromisoformat(raw_date)
        except ValueError as error:
            raise RuntimeError(
                f"FRED series {series_id} has an invalid date: {raw_date!r}"
            ) from error
        if observation_date > window_end:
            raise RuntimeError(
                f"FRED series {series_id} has a future-dated observation {raw_date}"
            )
        if observation_date < window_start:
            # The endpoint ignored the range parameters; drop the history.
            continue
        if raw_value in MISSING_VALUES:
            missing += 1
            continue
        try:
            value = float(raw_value)
        except ValueError as error:
            raise RuntimeError(
                f"FRED series {series_id} has a non-numeric value: {raw_value!r}"
            ) from error
        if not math.isfinite(value):
            raise RuntimeError(
                f"FRED series {series_id} has a non-finite value: {raw_value!r}"
            )
        records.append(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": f"{series_id}:{raw_date}",
                "series_id": series_id,
                "series_name": SERIES_NAMES[series_id],
                "date": raw_date,
                "value": value,
            }
        )
    if not records:
        raise RuntimeError(
            f"FRED series {series_id} has no observations in the window "
            f"{window_start}..{window_end}"
        )
    return records, missing


def fetch_series(
    series_id: str, window_start: date, window_end: date, *, timeout: int
) -> str:
    """Fetch one series' CSV body; ``cosd``/``coed`` are best-effort only."""
    query = urlencode(
        {
            "id": series_id,
            "cosd": window_start.isoformat(),
            "coed": window_end.isoformat(),
        }
    )
    request = urllib.request.Request(
        f"{URL}?{query}",
        headers={"User-Agent": USER_AGENT, "Accept": "text/csv"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


def fetch_fred_economic_series(
    *, window_start: date, window_end: date, timeout: int = 30, series_ids: Sequence[str] | None = None
) -> Iterator[dict[str, Any]]:
    """Fetch the series basket and yield normalized observations in order."""
    requested = tuple(series_ids) if series_ids is not None else SERIES_IDS
    unknown = [series_id for series_id in requested if series_id not in SERIES_NAMES]
    if unknown:
        raise ValueError(f"FRED series not allowlisted: {', '.join(unknown)}")
    fetched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for series_id in requested:
        text = fetch_series(series_id, window_start, window_end, timeout=timeout)
        records, missing = parse_series_csv(
            series_id,
            text,
            window_start=window_start,
            window_end=window_end,
            fetched_at=fetched_at,
        )
        if missing:
            print(
                f"{SOURCE}: {series_id}: skipped {missing} missing-value rows",
                file=sys.stderr,
            )
        yield from records


def main(argv: Sequence[str] | None = None) -> None:
    today = datetime.now(timezone.utc).date()
    parser = argparse.ArgumentParser(
        description=(
            "Fetch the allowlisted FRED macro series as NDJSON; observations "
            "older than the trailing lookback window are dropped."
        )
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=LOOKBACK_DAYS,
        help=f"trailing window in days, inclusive (default {LOOKBACK_DAYS})",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    args = parser.parse_args(argv)
    if args.lookback_days <= 0 or args.lookback_days > 366:
        parser.error("lookback must be between 1 and 366 days")
    if args.timeout <= 0:
        parser.error("timeout must be a positive integer")

    window_start = today - timedelta(days=args.lookback_days)
    for record in fetch_fred_economic_series(
        window_start=window_start,
        window_end=today,
        timeout=args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
