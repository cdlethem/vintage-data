#!/usr/bin/env python3
"""Fetch annual UNHCR population statistics as NDJSON.

Uses UNHCR's public Population Statistics API:
https://api.unhcr.org/docs/refugee-statistics.html

The API paginates with ``page``, returns rows in ``items``, and exposes the
last page as ``maxPages``.  This extractor deliberately caps both records and
requests: a scheduled run is a bounded annual snapshot, not an unbounded API
crawl.  Requests are paced at one per second.

Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from typing import Any, Iterator

ENDPOINT = "https://api.unhcr.org/population/v1/population/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
TIMEOUT_SECONDS = 30
CURRENT_YEAR = datetime.now(timezone.utc).year
MAX_PAGE_SIZE = 100
MAX_RECORDS = 5_000
MAX_PAGES = 50
REQUEST_DELAY_SECONDS = 1.0


def _bounded(value: str, *, name: str, minimum: int, maximum: int) -> int:
    number = int(value)
    if not minimum <= number <= maximum:
        raise argparse.ArgumentTypeError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return number


def page_size_arg(value: str) -> int:
    return _bounded(value, name="page size", minimum=1, maximum=MAX_PAGE_SIZE)


def limit_arg(value: str) -> int:
    return _bounded(value, name="limit", minimum=1, maximum=MAX_RECORDS)


def max_pages_arg(value: str) -> int:
    return _bounded(value, name="max pages", minimum=1, maximum=MAX_PAGES)


def observation_id(row: dict[str, Any]) -> str:
    """Return the API's stable origin/asylum/year observation key."""
    values = (row.get(field) for field in ("year", "coo_id", "coa_id"))
    return "unhcr-population:" + ":".join(
        "" if value is None else str(value) for value in values
    )


def normalize(row: dict[str, Any], fetched_at: str) -> dict[str, Any]:
    """Keep the documented population measures, including explicit nulls."""
    return {
        "source": "unhcr_refugee_statistics",
        "fetched_at": fetched_at,
        "id": observation_id(row),
        "year": row.get("year"),
        "country_of_origin_id": row.get("coo_id"),
        "country_of_origin": row.get("coo_name"),
        "country_of_origin_code": row.get("coo"),
        "country_of_origin_iso3": row.get("coo_iso"),
        "country_of_asylum_id": row.get("coa_id"),
        "country_of_asylum": row.get("coa_name"),
        "country_of_asylum_code": row.get("coa"),
        "country_of_asylum_iso3": row.get("coa_iso"),
        "refugees": row.get("refugees"),
        "asylum_seekers": row.get("asylum_seekers"),
        "returned_refugees": row.get("returned_refugees"),
        "idps": row.get("idps"),
        "returned_idps": row.get("returned_idps"),
        "stateless": row.get("stateless"),
        "other_people_of_concern": row.get("ooc"),
        "other_people_in_need_of_international_protection": row.get("oip"),
        "host_community": row.get("hst"),
    }


def _get(url: str) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        data = json.load(response)
    if not isinstance(data, dict):
        raise ValueError("UNHCR population response must be a JSON object")
    return data


def fetch_population(
    year: int | None = None,
    *,
    page_size: int = MAX_PAGE_SIZE,
    limit: int = MAX_RECORDS,
    max_pages: int = MAX_PAGES,
) -> Iterator[dict[str, Any]]:
    """Yield at most ``limit`` normalized rows for one reporting year.

    Invalid API response shapes fail loudly rather than silently publishing a
    partial dataset.  The local limits remain enforced even if callers bypass
    the command-line parser.
    """
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
    if not 1 <= limit <= MAX_RECORDS:
        raise ValueError(f"limit must be between 1 and {MAX_RECORDS}")
    if not 1 <= max_pages <= MAX_PAGES:
        raise ValueError(f"max_pages must be between 1 and {MAX_PAGES}")

    reporting_year = year if year is not None else CURRENT_YEAR
    if reporting_year < 1951 or reporting_year > CURRENT_YEAR:
        raise ValueError("year must be between 1951 and the current reporting year")

    fetched_at = datetime.now(timezone.utc).isoformat()
    emitted = 0
    page = 1
    while page <= max_pages and emitted < limit:
        params = {
            "yearFrom": reporting_year,
            "yearTo": reporting_year,
            "page": page,
            "limit": min(page_size, limit - emitted),
        }
        data = _get(ENDPOINT + "?" + urllib.parse.urlencode(params))
        rows = data.get("items")
        if not isinstance(rows, list):
            raise ValueError("UNHCR population response has no items list")
        if not rows:
            return
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("UNHCR population response contains a non-object item")
            yield normalize(row, fetched_at)
            emitted += 1
            if emitted == limit:
                return

        api_max_pages = data.get("maxPages")
        if api_max_pages is not None:
            if not isinstance(api_max_pages, int) or api_max_pages < 1:
                raise ValueError("UNHCR population response has invalid maxPages")
            if page >= api_max_pages:
                return
        elif len(rows) < params["limit"]:
            return

        page += 1
        if page <= max_pages:
            time.sleep(REQUEST_DELAY_SECONDS)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "year", nargs="?", type=int, default=CURRENT_YEAR,
        help="reporting year; defaults to the current UTC year",
    )
    parser.add_argument(
        "--page-size", type=page_size_arg, default=MAX_PAGE_SIZE,
        help=f"rows per request (1-{MAX_PAGE_SIZE}; default: %(default)s)",
    )
    parser.add_argument(
        "--limit", type=limit_arg, default=MAX_RECORDS,
        help=f"maximum rows per run (1-{MAX_RECORDS}; default: %(default)s)",
    )
    parser.add_argument(
        "--max-pages", type=max_pages_arg, default=MAX_PAGES,
        help=f"maximum requests per run (1-{MAX_PAGES}; default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    for record in fetch_population(
        args.year,
        page_size=args.page_size,
        limit=args.limit,
        max_pages=args.max_pages,
    ):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
