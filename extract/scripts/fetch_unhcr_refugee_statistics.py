#!/usr/bin/env python3
"""Fetch one reporting year from UNHCR's public Refugee Statistics API.

Documented endpoint:
https://api.unhcr.org/population/v1/population/

The endpoint is credential-free and page-based. Requests use a fixed page size for
an entire query; the caller's total record limit is enforced locally. This avoids
changing page boundaries on the final request, which could duplicate or omit rows.

Stdlib only.
"""

import argparse
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "unhcr_refugee_statistics"
URL = "https://api.unhcr.org/population/v1/population/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
FIRST_REPORTING_YEAR = 1951
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 100
DEFAULT_LIMIT = 1000
MAX_RECORD_LIMIT = 10_000
MAX_REQUESTS = 100
DEFAULT_TIMEOUT_SECONDS = 60
MAX_TIMEOUT_SECONDS = 120
DEFAULT_REQUEST_INTERVAL_SECONDS = 0.25
MAX_REQUEST_INTERVAL_SECONDS = 10.0

POPULATION_FIELDS = (
    "refugees",
    "asylum_seekers",
    "returned_refugees",
    "idps",
    "returned_idps",
    "stateless",
    "ooc",
    "oip",
    "hst",
)
COUNTRY_FIELDS = (
    "coo_id",
    "coo_name",
    "coo",
    "coo_iso",
    "coa_id",
    "coa_name",
    "coa",
    "coa_iso",
)


def current_reporting_year() -> int:
    return datetime.now(timezone.utc).year


def _validate_integer(name: str, value: Any, minimum: int, maximum: int) -> None:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or not minimum <= value <= maximum
    ):
        raise ValueError(f"{name} must be between {minimum} and {maximum}")


def _validate_identity(row: dict[str, Any], field: str) -> Any:
    if field not in row:
        raise ValueError(f"UNHCR population item is missing required {field}")
    value = row[field]
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ValueError(f"UNHCR population item has invalid required {field}")
    if isinstance(value, str) and not value.strip():
        raise ValueError(f"UNHCR population item has empty required {field}")
    return value


def normalize_observation(row: Any, fetched_at: str) -> dict[str, Any]:
    """Validate identity fields and normalize one source population row."""
    if not isinstance(row, dict):
        raise ValueError("UNHCR population item is not an object")

    year = _validate_identity(row, "year")
    if not isinstance(year, int):
        raise ValueError("UNHCR population item has invalid required year")
    origin_id = _validate_identity(row, "coo_id")
    asylum_id = _validate_identity(row, "coa_id")

    record = {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{year}:{origin_id}:{asylum_id}",
        "year": year,
    }
    for field in COUNTRY_FIELDS:
        record[field] = row.get(field)
    for field in POPULATION_FIELDS:
        record[field] = row.get(field)
    return record


def _parse_page(document: Any) -> tuple[list[Any], int]:
    if not isinstance(document, dict):
        raise ValueError("UNHCR response is not an object")
    items = document.get("items")
    if not isinstance(items, list):
        raise ValueError("UNHCR response has invalid items")
    max_pages = document.get("maxPages")
    if (
        not isinstance(max_pages, int)
        or isinstance(max_pages, bool)
        or max_pages < 0
    ):
        raise ValueError("UNHCR response has invalid maxPages")
    return items, max_pages


def fetch_population(
    year: int,
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    limit: int = DEFAULT_LIMIT,
    timeout: int = DEFAULT_TIMEOUT_SECONDS,
    request_interval: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
) -> Iterator[dict[str, Any]]:
    """Yield at most ``limit`` population rows for one reporting year."""
    _validate_integer("year", year, FIRST_REPORTING_YEAR, current_reporting_year())
    _validate_integer("page_size", page_size, 1, MAX_PAGE_SIZE)
    _validate_integer("limit", limit, 1, MAX_RECORD_LIMIT)
    _validate_integer("timeout", timeout, 1, MAX_TIMEOUT_SECONDS)
    if (
        isinstance(request_interval, bool)
        or not isinstance(request_interval, (int, float))
        or not 0 <= request_interval <= MAX_REQUEST_INTERVAL_SECONDS
    ):
        raise ValueError(
            "request_interval must be between 0 and "
            f"{MAX_REQUEST_INTERVAL_SECONDS:g} seconds"
        )

    request_page_size = min(page_size, limit)
    request_count = min(
        MAX_REQUESTS,
        math.ceil(limit / request_page_size),
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    emitted = 0

    for page in range(1, request_count + 1):
        query = urllib.parse.urlencode(
            {
                "yearFrom": year,
                "yearTo": year,
                "page": page,
                "limit": request_page_size,
            }
        )
        request = urllib.request.Request(
            f"{URL}?{query}",
            headers={
                "Accept": "application/json",
                "User-Agent": USER_AGENT,
            },
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        items, max_pages = _parse_page(document)

        remaining = limit - emitted
        for row in items[:remaining]:
            yield normalize_observation(row, fetched_at)
            emitted += 1

        if emitted >= limit or not items or page >= max_pages:
            return
        if page < request_count:
            time.sleep(request_interval)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("year", nargs="?", type=int, default=current_reporting_year())
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    args = parser.parse_args(argv)

    for record in fetch_population(
        args.year,
        page_size=args.page_size,
        limit=args.limit,
        timeout=args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
