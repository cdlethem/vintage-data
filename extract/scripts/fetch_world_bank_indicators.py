#!/usr/bin/env python3
"""Fetch a bounded, complete year range from the World Bank Indicators API.

The scheduled run requests a small maintained indicator basket for all World Bank
countries and aggregates. Historical extraction is bounded to one year per backfill
unit. Responses are buffered until pagination is proven complete, so an upstream
schema change or configured bound cannot publish a partial result.

Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
from typing import Any, Iterator, Sequence
import urllib.parse
import urllib.request


SOURCE = "world_bank_indicators"
API_URL = "https://api.worldbank.org/v2/country/{country}/indicator/{indicators}"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
FIRST_YEAR = 1960
INDICATORS = (
    "SP.POP.TOTL",
    "NY.GDP.MKTP.CD",
    "SL.UEM.TOTL.ZS",
    "EN.ATM.CO2E.PC",
)
INDICATOR_ALLOWLIST = frozenset(INDICATORS)
DEFAULT_COUNTRY = "all"
DEFAULT_YEARS_BACK = 3
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000
DEFAULT_MAX_PAGES = 10
HARD_MAX_PAGES = 20
DEFAULT_MAX_RECORDS = 10_000
HARD_MAX_RECORDS = 20_000
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 120
INDICATOR_RE = re.compile(r"[A-Z0-9]+(?:\.[A-Z0-9]+)+")
COUNTRY_RE = re.compile(r"(?:all|[A-Za-z0-9]{2,3})(?:;(?:[A-Za-z0-9]{2,3}))*")
YEAR_RE = re.compile(r"[0-9]{4}")


class WorldBankError(RuntimeError):
    """The World Bank response cannot satisfy the extractor contract."""


class IncompleteCoverageError(WorldBankError):
    """The configured bounds cannot cover the complete response."""


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def validate_year_range(start_year: int, end_year: int) -> None:
    current_year = datetime.now(timezone.utc).year
    _bounded_int(start_year, "start_year", FIRST_YEAR, current_year)
    _bounded_int(end_year, "end_year", FIRST_YEAR, current_year)
    if start_year > end_year:
        raise ValueError("start_year must not be after end_year")


def validate_indicators(indicators: Sequence[str]) -> tuple[str, ...]:
    if isinstance(indicators, (str, bytes)):
        raise ValueError("indicators must be a sequence of indicator IDs")
    requested = tuple(indicators)
    if not requested:
        raise ValueError("at least one indicator is required")
    if len(set(requested)) != len(requested):
        raise ValueError("indicators must not contain duplicates")
    for indicator in requested:
        if not isinstance(indicator, str) or INDICATOR_RE.fullmatch(indicator) is None:
            raise ValueError(f"invalid indicator ID {indicator!r}")
        if indicator not in INDICATOR_ALLOWLIST:
            raise ValueError(f"indicator {indicator!r} is not allowlisted")
    return requested


def _metadata_int(metadata: dict[str, Any], field: str, minimum: int) -> int:
    value = metadata.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise WorldBankError(
            f"World Bank response metadata.{field} must be an integer of at least {minimum}"
        )
    return value


def _parse_page(
    document: Any, requested_page: int
) -> tuple[dict[str, Any], list[Any]]:
    """Validate one World Bank ``[metadata, data]`` response envelope.

    The API represents a genuine zero-result query as ``pages: 0, total: 0`` and
    may encode its data member as either ``null`` or ``[]``. That narrow shape is
    accepted without weakening validation for non-empty pagination.
    """
    if not isinstance(document, list) or len(document) != 2:
        raise WorldBankError("World Bank response must be a two-item [metadata, data] list")
    metadata, data = document
    if not isinstance(metadata, dict):
        raise WorldBankError("World Bank response metadata must be an object")

    page = _metadata_int(metadata, "page", 1)
    pages = _metadata_int(metadata, "pages", 0)
    per_page = _metadata_int(metadata, "per_page", 1)
    total = _metadata_int(metadata, "total", 0)
    if page != requested_page:
        raise WorldBankError(
            f"World Bank response page {page} does not match requested page {requested_page}"
        )
    expected_pages = (total + per_page - 1) // per_page
    if pages != expected_pages:
        raise WorldBankError(
            f"World Bank response metadata.pages is {pages}, expected {expected_pages} "
            f"for total={total} and per_page={per_page}"
        )
    if pages == 0 and total != 0:
        raise WorldBankError("World Bank response has zero pages with a nonzero total")
    if pages and page > pages:
        raise WorldBankError("World Bank response page exceeds metadata.pages")

    if data is None:
        if total == 0 and pages == 0:
            return dict(metadata), []
        raise WorldBankError("World Bank response data may be null only for a zero-result response")
    if not isinstance(data, list):
        raise WorldBankError("World Bank response data must be a list or valid zero-result null")
    if total == 0 and data:
        raise WorldBankError("World Bank zero-total response contains observations")
    if len(data) > per_page:
        raise WorldBankError("World Bank response data exceeds metadata.per_page")
    return dict(metadata), data


def _required_object(row: dict[str, Any], field: str, context: str) -> dict[str, Any]:
    value = row.get(field)
    if not isinstance(value, dict):
        raise WorldBankError(f"World Bank {context} has invalid {field} metadata")
    return value


def _required_string(value: Any, field: str, context: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise WorldBankError(f"World Bank {context} has invalid {field}")
    return value


def normalize_observation(
    row: Any,
    fetched_at: str,
    metadata: dict[str, Any],
    requested_indicators: Sequence[str],
    start_year: int,
    end_year: int,
) -> dict[str, Any]:
    """Validate and normalize one World Bank indicator observation."""
    if not isinstance(row, dict):
        raise WorldBankError("World Bank response contains a non-object observation")

    indicator = _required_object(row, "indicator", "observation")
    indicator_id = _required_string(indicator.get("id"), "indicator.id", "observation")
    indicator_name = _required_string(indicator.get("value"), "indicator.value", "observation")
    if indicator_id not in requested_indicators:
        raise WorldBankError(f"World Bank response contains unrequested indicator {indicator_id!r}")

    country = _required_object(row, "country", f"indicator {indicator_id}")
    country_id = _required_string(country.get("id"), "country.id", f"indicator {indicator_id}")
    country_name = _required_string(
        country.get("value"), "country.value", f"indicator {indicator_id}"
    )
    country_iso3 = _required_string(
        row.get("countryiso3code"),
        "countryiso3code",
        f"indicator {indicator_id}",
        allow_empty=True,
    )
    year_text = _required_string(row.get("date"), "date", f"indicator {indicator_id}")
    if YEAR_RE.fullmatch(year_text) is None:
        raise WorldBankError(f"World Bank indicator {indicator_id} has invalid date")
    year = int(year_text)
    if not start_year <= year <= end_year:
        raise WorldBankError(
            f"World Bank indicator {indicator_id} date {year} is outside requested range"
        )

    value = row.get("value")
    if value is not None and (
        isinstance(value, bool) or not isinstance(value, (int, float))
    ):
        raise WorldBankError(f"World Bank indicator {indicator_id} has invalid value")
    unit = _required_string(
        row.get("unit"), "unit", f"indicator {indicator_id}", allow_empty=True
    )
    obs_status = _required_string(
        row.get("obs_status"),
        "obs_status",
        f"indicator {indicator_id}",
        allow_empty=True,
    )
    decimal = row.get("decimal")
    if isinstance(decimal, bool) or not isinstance(decimal, int) or decimal < 0:
        raise WorldBankError(f"World Bank indicator {indicator_id} has invalid decimal")

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{indicator_id}:{country_id}:{year_text}",
        "indicator_id": indicator_id,
        "indicator_name": indicator_name,
        "country_id": country_id,
        "country_name": country_name,
        "country_iso3_code": country_iso3,
        "year": year,
        "value": value,
        "unit": unit,
        "obs_status": obs_status,
        "decimal": decimal,
        "metadata": dict(metadata),
    }


def fetch_world_bank_indicators(
    start_year: int,
    end_year: int,
    indicators: Sequence[str] = INDICATORS,
    country: str = DEFAULT_COUNTRY,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_records: int = DEFAULT_MAX_RECORDS,
    timeout: int = DEFAULT_TIMEOUT,
) -> Iterator[dict[str, Any]]:
    """Fetch a complete bounded World Bank result set or raise before yielding."""
    validate_year_range(start_year, end_year)
    requested_indicators = validate_indicators(indicators)
    if not isinstance(country, str) or COUNTRY_RE.fullmatch(country) is None:
        raise ValueError("country must be 'all' or semicolon-separated 2-3 character IDs")
    _bounded_int(page_size, "page_size", 1, MAX_PAGE_SIZE)
    _bounded_int(max_pages, "max_pages", 1, HARD_MAX_PAGES)
    _bounded_int(max_records, "max_records", 1, HARD_MAX_RECORDS)
    _bounded_int(timeout, "timeout", 1, MAX_TIMEOUT)

    path = API_URL.format(
        country=urllib.parse.quote(country, safe=";"),
        indicators=urllib.parse.quote(";".join(requested_indicators), safe=".;"),
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    expected_total: int | None = None
    expected_pages: int | None = None
    expected_per_page: int | None = None
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for requested_page in range(1, max_pages + 1):
        query = urllib.parse.urlencode(
            {
                "format": "json",
                "date": f"{start_year}:{end_year}",
                "page": requested_page,
                "per_page": page_size,
                "source": 2,
            }
        )
        request = urllib.request.Request(
            f"{path}?{query}",
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        metadata, data = _parse_page(document, requested_page)

        total = metadata["total"]
        pages = metadata["pages"]
        per_page = metadata["per_page"]
        if per_page != page_size:
            raise WorldBankError(
                f"World Bank response per_page {per_page} does not match requested {page_size}"
            )
        if expected_total is None:
            expected_total = total
            expected_pages = pages
            expected_per_page = per_page
            if total > max_records:
                raise IncompleteCoverageError(
                    f"World Bank reports {total} observations, above max_records={max_records}"
                )
            if pages > max_pages:
                raise IncompleteCoverageError(
                    f"World Bank reports {pages} pages, above max_pages={max_pages}"
                )
        elif (total, pages, per_page) != (
            expected_total,
            expected_pages,
            expected_per_page,
        ):
            raise WorldBankError("World Bank pagination metadata changed during extraction")

        for row in data:
            record = normalize_observation(
                row,
                fetched_at,
                metadata,
                requested_indicators,
                start_year,
                end_year,
            )
            if record["id"] in seen_ids:
                raise WorldBankError(
                    f"World Bank response contains duplicate observation {record['id']}"
                )
            seen_ids.add(record["id"])
            records.append(record)
            if len(records) > max_records or len(records) > expected_total:
                raise WorldBankError("World Bank response contains more observations than declared")

        if requested_page == expected_pages or expected_pages == 0:
            if len(records) != expected_total:
                raise IncompleteCoverageError(
                    f"World Bank pagination ended with {len(records)} observations; "
                    f"metadata.total declared {expected_total}"
                )
            yield from records
            return
        if not data:
            raise IncompleteCoverageError(
                f"World Bank page {requested_page} was empty before pagination completed"
            )

    raise IncompleteCoverageError(
        f"World Bank pagination exceeded max_pages={max_pages} after {len(records)} observations"
    )


def main(argv: Sequence[str] | None = None) -> None:
    current_year = datetime.now(timezone.utc).year
    parser = argparse.ArgumentParser(
        description="Fetch a bounded, complete World Bank indicator year range."
    )
    parser.add_argument("--start-year", type=int, default=current_year - DEFAULT_YEARS_BACK)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument(
        "--indicator",
        action="append",
        dest="indicators",
        help="allowlisted indicator ID; repeat to replace the default basket",
    )
    parser.add_argument("--country", default=DEFAULT_COUNTRY)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)

    try:
        records = fetch_world_bank_indicators(
            args.start_year,
            args.end_year,
            indicators=args.indicators or INDICATORS,
            country=args.country,
            page_size=args.page_size,
            max_pages=args.max_pages,
            max_records=args.max_records,
            timeout=args.timeout,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
