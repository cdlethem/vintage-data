#!/usr/bin/env python3
"""ONS — bounded recent-period snapshots of explicit UK time series.

Fetches a curated basket of ONS time series (default: the CPI All-Items
index, monthly rate and annual rate) from the keyless ONS data API and
emits the most recent ``--periods`` observations per series as NDJSON.
ONS publishes most major series on a fixed release schedule (CPI is
monthly, mid-month) and revises recent periods at each release, so a
bounded daily snapshot captures both releases and revisions.

Each response carries publisher metadata (title, unit, next release,
per-observation update dates) that is retained verbatim alongside
``fetched_at``. ONS content is published under the Open Government
Licence v3.0.

Stdlib only.
"""

import argparse
import errno
import json
import math
import os
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

API = "https://api.ons.gov.uk/v1/data"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

MAX_ATTEMPTS = 3
RETRY_DELAYS = (1, 2)
DEFAULT_PERIODS = 36
MAX_PERIODS = 120
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

# Frequency buckets in an ONS series document, most granular first. A
# series carries its observations in exactly one of these lists.
FREQUENCY_BUCKETS = ("months", "quarters", "years")
MONTH_ORDER = {
    name: index
    for index, name in enumerate(
        (
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December",
        ),
        start=1,
    )
}


def is_timeout_error(error):
    """Return whether *error* is a real timeout, including urllib wrappers."""
    if isinstance(error, (socket.timeout, TimeoutError)):
        return True
    if isinstance(error, urllib.error.URLError):
        reason = getattr(error, "reason", None)
        if isinstance(reason, (socket.timeout, TimeoutError)):
            return True
        text = str(reason or "").lower()
        return "timed out" in text
    return False


def fetch_document(uri, timeout, max_attempts=MAX_ATTEMPTS):
    """GET one series document, retrying transport timeouts only.

    Returns ``(document, attempts, payload_bytes)``. Malformed payloads and
    non-timeout failures are raised immediately, without burning the
    retry budget.
    """
    url = f"{API}?uri={urllib.parse.quote(uri, safe='')}"
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read()
            document = json.loads(body)
            return document, attempt, len(body)
        except (urllib.error.URLError, OSError, ValueError, TimeoutError) as error:
            retriable = is_timeout_error(error)
            if not retriable or attempt >= max_attempts:
                raise
            time.sleep(RETRY_DELAYS[attempt - 1])
    raise RuntimeError("unreachable")


def _text(value, field, context):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: {field} must be a non-empty string, got {value!r}")
    return value


def _optional_text(value):
    return value if isinstance(value, str) and value.strip() else None


def period_key(bucket, row):
    """Chronological sort key for one observation in *bucket*."""
    try:
        year = int(row["year"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(f"observation year is not an integer: {row!r}") from error
    if bucket == "months":
        month = row.get("month")
        if month not in MONTH_ORDER:
            raise ValueError(f"unrecognized month {month!r} in {row!r}")
        return (year, MONTH_ORDER[month])
    if bucket == "quarters":
        quarter = str(row.get("quarter") or "").strip()
        if not quarter.isdigit():
            raise ValueError(f"unrecognized quarter {quarter!r} in {row!r}")
        return (year, int(quarter))
    return (year,)


def parse_series(document, uri, source, fetched_at, periods):
    """Validate one ONS series document and return its most recent records."""
    if not isinstance(document, dict):
        raise ValueError("ONS response is not a JSON object")
    if document.get("type") != "timeseries":
        raise ValueError(f"ONS response for {uri} is not a timeseries: {document.get('type')!r}")
    if document.get("uri") != uri:
        raise ValueError(f"ONS response uri {document.get('uri')!r} does not match request {uri!r}")
    description = document.get("description")
    if not isinstance(description, dict):
        raise ValueError(f"ONS response for {uri} is missing description")
    title = _text(description.get("title"), "title", uri)

    path_parts = [part for part in uri.split("/") if part]
    if len(path_parts) < 3 or path_parts[-3] != "timeseries":
        raise ValueError(f"ONS series uri {uri!r} is not of the form /.../timeseries/<dataset>/<series>")
    dataset_id, series_id = path_parts[-2], path_parts[-1]

    bucket = None
    for candidate in FREQUENCY_BUCKETS:
        rows = document.get(candidate)
        if isinstance(rows, list) and rows:
            bucket, rows = candidate, rows
            break
    if bucket is None:
        raise ValueError(f"ONS series {uri} carries no observations in years/quarters/months")

    ordered = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"ONS observation is not an object: {row!r}")
        label = _text(row.get("date"), "date", uri)
        value_text = row.get("value")
        if value_text is None or (isinstance(value_text, str) and not value_text.strip()):
            continue  # ONS leaves value empty where an observation is missing
        try:
            value = float(str(value_text).strip())
        except ValueError as error:
            raise ValueError(f"ONS series {uri} period {label!r} has non-numeric value {value_text!r}") from error
        if not math.isfinite(value):
            raise ValueError(f"ONS series {uri} period {label!r} has non-finite value {value_text!r}")
        ordered.append((period_key(bucket, row), row, label, value))
    if not ordered:
        raise ValueError(f"ONS series {uri} has no usable observations")
    ordered.sort(key=lambda item: item[0])

    metadata = {
        "uri": uri,
        "dataset": dataset_id,
        "series_id": series_id,
        "title": title,
        "unit": _optional_text(description.get("unit")),
        "pre_unit": _optional_text(description.get("preUnit")),
        "frequency": bucket,
        "next_release": _optional_text(description.get("nextRelease")),
        "release_date": _optional_text(description.get("releaseDate")),
        "publisher": "ONS",
    }

    records = []
    for _, row, label, value in ordered[-periods:]:
        update_date = _optional_text(row.get("updateDate"))
        records.append({
            "source": source,
            "fetched_at": fetched_at,
            "id": f"{dataset_id}:{series_id}:{label}",
            "series": metadata,
            "period": label,
            "year": int(row["year"]),
            "month": _optional_text(row.get("month")),
            "quarter": _optional_text(row.get("quarter")),
            "value": value,
            "update_date": update_date,
        })
    return records


class OnsFetchError(RuntimeError):
    """A run-level failure carrying the request stats gathered so far."""

    def __init__(self, message, stats):
        super().__init__(message)
        self.stats = stats


def fetch_ons_statistics(source, series_uris, periods, timeout):
    """Fetch every series and return ``(records, request_stats)``.

    All-or-nothing: a failure on any series raises :class:`OnsFetchError`
    carrying the request stats gathered up to that point.
    """
    if not series_uris:
        raise ValueError("at least one --series is required")
    if len(set(series_uris)) != len(series_uris):
        raise ValueError("duplicate --series values")
    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    records = []
    stats = {"succeeded": 0, "failed": 0, "retries": 0, "payload_bytes": 0}
    for uri in series_uris:
        try:
            document, attempts, size = fetch_document(uri, timeout)
            records.extend(parse_series(document, uri, source, fetched_at, periods))
        except Exception as error:
            stats["failed"] += 1
            raise OnsFetchError(f"ONS series {uri} failed: {error}", stats) from error
        stats["succeeded"] += 1
        stats["retries"] += attempts - 1
        stats["payload_bytes"] += size
    return records, stats


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", help="source name stamped into every record")
    parser.add_argument("--series", action="append", default=[], metavar="URI",
                        help="ONS timeseries uri, e.g. /economy/inflationandpriceindices/timeseries/d7bt/mm23 (repeatable)")
    parser.add_argument("--periods", type=int, default=DEFAULT_PERIODS,
                        help=f"most recent observations per series (default {DEFAULT_PERIODS}, max {MAX_PERIODS})")
    parser.add_argument("--timeout", type=int, default=60, help="per-request timeout in seconds")
    args = parser.parse_args(argv)

    if not 1 <= args.periods <= MAX_PERIODS:
        parser.error(f"--periods must be between 1 and {MAX_PERIODS}")

    try:
        records, stats = fetch_ons_statistics(args.source, args.series, args.periods, args.timeout)
    except OnsFetchError as error:
        stats = error.stats
        summary = {
            "health": "failed",
            "completeness": "failed",
            "requests": {
                "attempted": len(args.series),
                "succeeded": stats["succeeded"],
                "failed": stats["failed"],
                "retries": stats["retries"],
                "payload_bytes": stats["payload_bytes"],
            },
            "error": str(error)[:512],
        }
        print(f"{SUMMARY_PREFIX}{json.dumps(summary, ensure_ascii=False)}", file=sys.stderr)
        return 1
    except ValueError as error:
        print(f"{SUMMARY_PREFIX}{json.dumps({
            'health': 'failed',
            'completeness': 'failed',
            'error': str(error)[:512],
        }, ensure_ascii=False)}", file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    summary = {
        "health": "healthy",
        "completeness": "complete",
        "requests": {
            "attempted": len(args.series),
            "succeeded": stats["succeeded"],
            "failed": stats["failed"],
            "retries": stats["retries"],
            "payload_bytes": stats["payload_bytes"],
        },
        "records": len(records),
    }
    print(f"{SUMMARY_PREFIX}{json.dumps(summary, ensure_ascii=False)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
