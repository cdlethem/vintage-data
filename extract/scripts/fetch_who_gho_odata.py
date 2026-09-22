#!/usr/bin/env python3
"""Fetch a bounded WHO Global Health Observatory OData observation set.

WHO's OData v4 service returns observations in a ``value`` array and advertises
continuation with ``@odata.nextLink``.  This client follows that server-provided
link, but only after proving that it still addresses the selected indicator and
retains the exact geography/period filter and page size.  All pages are fetched,
validated, normalized, and serialized before stdout is touched, so the extract
runner cannot publish an apparently successful partial result.

The admitted initial scope is deliberately narrow: indicator WHOSIS_000001,
country USA, and annual periods 2020 through 2022.  CLI selections are explicit
and validated against those bounds rather than silently broadening the source.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Sequence

SOURCE = "who_gho_odata"
API_ROOT = "https://ghoapi.azureedge.net/api"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
ALLOWED_INDICATORS = frozenset({"WHOSIS_000001"})
ALLOWED_GEOGRAPHIES = frozenset({("COUNTRY", "USA")})
MIN_YEAR = 2020
MAX_YEAR = 2022
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 1.0
MAX_RETRY_DELAY = 30.0
DEFAULT_MAX_PAGES = 10
HARD_MAX_PAGES = 25
DEFAULT_MAX_RECORDS = 1000
HARD_MAX_RECORDS = 5000
RETRYABLE_HTTP_CODES = frozenset({408, 425, 429, 500, 502, 503, 504})


class WhoGhoError(RuntimeError):
    """The WHO response cannot satisfy the complete-extract contract."""


class IncompleteCoverageError(WhoGhoError):
    """Configured page or record budgets cannot cover the response."""


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _bounded_number(value: Any, name: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a number between {minimum:g} and {maximum:g}")
    number = float(value)
    if not minimum <= number <= maximum:
        raise ValueError(f"{name} must be a number between {minimum:g} and {maximum:g}")
    return number


def validate_scope(
    indicators: Sequence[str],
    geography_type: str,
    geographies: Sequence[str],
    start_year: int,
    end_year: int,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if isinstance(indicators, (str, bytes)) or not indicators:
        raise ValueError("at least one indicator selection is required")
    if isinstance(geographies, (str, bytes)) or not geographies:
        raise ValueError("at least one geography selection is required")
    selected_indicators = tuple(indicators)
    selected_geographies = tuple(geographies)
    if len(set(selected_indicators)) != len(selected_indicators):
        raise ValueError("indicator selections must not contain duplicates")
    if len(set(selected_geographies)) != len(selected_geographies):
        raise ValueError("geography selections must not contain duplicates")
    unknown_indicators = sorted(set(selected_indicators) - ALLOWED_INDICATORS)
    if unknown_indicators:
        raise ValueError(f"indicators outside the admitted scope: {unknown_indicators}")
    unknown_geographies = sorted(
        geography for geography in selected_geographies
        if (geography_type, geography) not in ALLOWED_GEOGRAPHIES
    )
    if unknown_geographies:
        raise ValueError(
            f"geographies outside the admitted {geography_type!r} scope: {unknown_geographies}"
        )
    _bounded_int(start_year, "start_year", MIN_YEAR, MAX_YEAR)
    _bounded_int(end_year, "end_year", MIN_YEAR, MAX_YEAR)
    if start_year > end_year:
        raise ValueError("start_year must not be after end_year")
    return selected_indicators, selected_geographies


def _odata_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _filter_for(
    indicator: str,
    geography_type: str,
    geography: str,
    start_year: int,
    end_year: int,
) -> str:
    return " and ".join(
        (
            f"IndicatorCode eq {_odata_quote(indicator)}",
            f"SpatialDimType eq {_odata_quote(geography_type)}",
            f"SpatialDim eq {_odata_quote(geography)}",
            "TimeDimType eq 'YEAR'",
            f"TimeDim ge {start_year}",
            f"TimeDim le {end_year}",
        )
    )


def _initial_url(indicator: str, filter_expression: str, page_size: int) -> str:
    query = urllib.parse.urlencode(
        {
            "$filter": filter_expression,
            "$orderby": "TimeDim,SpatialDim,Id",
            "$top": page_size,
            "$count": "true",
        }
    )
    return f"{API_ROOT}/{urllib.parse.quote(indicator, safe='')}?{query}"


def _validate_page_url(
    candidate: str,
    previous_url: str,
    indicator: str,
    filter_expression: str,
    page_size: int,
) -> str:
    if not isinstance(candidate, str) or not candidate:
        raise WhoGhoError("WHO @odata.nextLink must be a nonempty string")
    url = urllib.parse.urljoin(previous_url, candidate)
    parsed = urllib.parse.urlsplit(url)
    root = urllib.parse.urlsplit(API_ROOT)
    expected_path = f"{root.path}/{urllib.parse.quote(indicator, safe='')}"
    if parsed.scheme != root.scheme or parsed.netloc != root.netloc or parsed.path != expected_path:
        raise WhoGhoError("WHO @odata.nextLink changed the selected endpoint")
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True, strict_parsing=True)
    if query.get("$filter") != [filter_expression]:
        raise WhoGhoError("WHO @odata.nextLink dropped or changed the requested filter")
    if query.get("$top") != [str(page_size)]:
        raise WhoGhoError("WHO @odata.nextLink dropped or changed the requested page size")
    if "$skip" not in query and "$skiptoken" not in query:
        raise WhoGhoError("WHO @odata.nextLink has no pagination cursor")
    return url


def _reject_nonfinite_json_number(value: str) -> Any:
    raise WhoGhoError(f"WHO returned non-finite JSON number {value}")


def _request_json(
    url: str,
    *,
    timeout: int,
    retries: int,
    retry_delay: float,
) -> Any:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                try:
                    return json.load(response, parse_constant=_reject_nonfinite_json_number)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise WhoGhoError("WHO returned malformed JSON") from error
        except urllib.error.HTTPError as error:
            retryable = error.code in RETRYABLE_HTTP_CODES and attempt < retries
            error.close()
            if not retryable:
                raise
        except urllib.error.URLError:
            if attempt == retries:
                raise
        time.sleep(min(retry_delay * (2**attempt), MAX_RETRY_DELAY))
    raise AssertionError("retry loop exited unexpectedly")


def _parse_envelope(document: Any) -> tuple[list[Any], str | None, int | None]:
    if not isinstance(document, dict):
        raise WhoGhoError("WHO response envelope must be an object")
    rows = document.get("value")
    if not isinstance(rows, list):
        raise WhoGhoError("WHO response envelope value must be a list")
    next_link = document.get("@odata.nextLink")
    if next_link is not None and (not isinstance(next_link, str) or not next_link):
        raise WhoGhoError("WHO @odata.nextLink must be null, absent, or a nonempty string")
    count = document.get("@odata.count")
    if count is not None and (isinstance(count, bool) or not isinstance(count, int) or count < 0):
        raise WhoGhoError("WHO @odata.count must be a nonnegative integer")
    return rows, next_link, count


def _required_string(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value:
        raise WhoGhoError(f"WHO observation has invalid {field}")
    return value


def _nullable_string(row: dict[str, Any], field: str) -> str | None:
    value = row.get(field)
    if value is not None and not isinstance(value, str):
        raise WhoGhoError(f"WHO observation has invalid {field}")
    return value


def _nullable_number(row: dict[str, Any], field: str) -> int | float | None:
    value = row.get(field)
    if value is not None and (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise WhoGhoError(f"WHO observation has invalid non-finite or non-numeric {field}")
    return value


def _dimension(row: dict[str, Any], type_field: str, code_field: str) -> dict[str, str | None]:
    return {
        "type": _nullable_string(row, type_field),
        "code": _nullable_string(row, code_field),
    }


def normalize_observation(
    row: Any,
    *,
    fetched_at: str,
    source_url: str,
    indicator: str,
    geography_type: str,
    geography: str,
    start_year: int,
    end_year: int,
) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise WhoGhoError("WHO response contains a non-object observation")
    indicator_code = _required_string(row, "IndicatorCode")
    spatial_type = _required_string(row, "SpatialDimType")
    spatial_code = _required_string(row, "SpatialDim")
    period_type = _required_string(row, "TimeDimType")
    period = row.get("TimeDim")
    if indicator_code != indicator:
        raise WhoGhoError(f"WHO response contains unrequested indicator {indicator_code!r}")
    if (spatial_type, spatial_code) != (geography_type, geography):
        raise WhoGhoError(
            f"WHO response contains unrequested geography {(spatial_type, spatial_code)!r}"
        )
    if period_type != "YEAR" or isinstance(period, bool) or not isinstance(period, int):
        raise WhoGhoError("WHO observation has an invalid annual period")
    if not start_year <= period <= end_year:
        raise WhoGhoError(f"WHO response period {period} is outside the requested range")

    dimensions = {
        "dim1": _dimension(row, "Dim1Type", "Dim1"),
        "dim2": _dimension(row, "Dim2Type", "Dim2"),
        "dim3": _dimension(row, "Dim3Type", "Dim3"),
        "data_source": _dimension(row, "DataSourceDimType", "DataSourceDim"),
    }
    identity_fields = {
        "indicator_code": indicator_code,
        "geography_type": spatial_type,
        "geography_code": spatial_code,
        "period_type": period_type,
        "period": period,
        "dimensions": dimensions,
    }
    identity = hashlib.sha256(
        json.dumps(identity_fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    upstream_id = row.get("Id")
    if upstream_id is not None and (isinstance(upstream_id, bool) or not isinstance(upstream_id, (str, int))):
        raise WhoGhoError("WHO observation has invalid Id")

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": identity,
        "observation_identity": identity,
        "upstream_id": None if upstream_id is None else str(upstream_id),
        "indicator_code": indicator_code,
        "geography_type": spatial_type,
        "geography_code": spatial_code,
        "parent_geography_code": _nullable_string(row, "ParentLocationCode"),
        "parent_geography_name": _nullable_string(row, "ParentLocation"),
        "period_type": period_type,
        "period": period,
        "numeric_value": _nullable_number(row, "NumericValue"),
        "display_value": _nullable_string(row, "Value"),
        "low": _nullable_number(row, "Low"),
        "high": _nullable_number(row, "High"),
        "comments": _nullable_string(row, "Comments"),
        "date": _nullable_string(row, "Date"),
        "time_dimension_value": _nullable_string(row, "TimeDimensionValue"),
        "time_dimension_begin": _nullable_string(row, "TimeDimensionBegin"),
        "time_dimension_end": _nullable_string(row, "TimeDimensionEnd"),
        "dimensions": dimensions,
        "source_url": source_url,
    }


def fetch_who_gho_odata(
    *,
    indicators: Sequence[str],
    geography_type: str,
    geographies: Sequence[str],
    start_year: int,
    end_year: int,
    page_size: int = DEFAULT_PAGE_SIZE,
    timeout: int = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_records: int = DEFAULT_MAX_RECORDS,
) -> list[dict[str, Any]]:
    """Return a complete bounded result, or raise before exposing any records."""
    selected_indicators, selected_geographies = validate_scope(
        indicators, geography_type, geographies, start_year, end_year
    )
    _bounded_int(page_size, "page_size", 1, MAX_PAGE_SIZE)
    _bounded_int(timeout, "timeout", 1, MAX_TIMEOUT)
    _bounded_int(retries, "retries", 0, MAX_RETRIES)
    _bounded_number(retry_delay, "retry_delay", 0, MAX_RETRY_DELAY)
    _bounded_int(max_pages, "max_pages", 1, HARD_MAX_PAGES)
    _bounded_int(max_records, "max_records", 1, HARD_MAX_RECORDS)

    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen_identities: set[str] = set()
    pages_fetched = 0

    for indicator in selected_indicators:
        for geography in selected_geographies:
            filter_expression = _filter_for(
                indicator, geography_type, geography, start_year, end_year
            )
            source_url = _initial_url(indicator, filter_expression, page_size)
            url: str | None = source_url
            visited_urls: set[str] = set()
            expected_count: int | None = None
            selection_records = 0

            while url is not None:
                if pages_fetched >= max_pages:
                    raise IncompleteCoverageError(
                        f"WHO pagination requires more than max_pages={max_pages}"
                    )
                if url in visited_urls:
                    raise IncompleteCoverageError("WHO pagination did not advance")
                visited_urls.add(url)
                document = _request_json(
                    url, timeout=timeout, retries=retries, retry_delay=float(retry_delay)
                )
                pages_fetched += 1
                rows, next_link, count = _parse_envelope(document)
                if count is not None:
                    if expected_count is None:
                        expected_count = count
                        if len(records) + count > max_records:
                            raise IncompleteCoverageError(
                                f"WHO reports more than max_records={max_records} observations"
                            )
                    elif count != expected_count:
                        raise WhoGhoError("WHO @odata.count changed during pagination")
                page_start = selection_records
                for row in rows:
                    record = normalize_observation(
                        row,
                        fetched_at=fetched_at,
                        source_url=source_url,
                        indicator=indicator,
                        geography_type=geography_type,
                        geography=geography,
                        start_year=start_year,
                        end_year=end_year,
                    )
                    identity = record["id"]
                    if identity in seen_identities:
                        raise WhoGhoError(
                            f"WHO response contains duplicate observation identity {identity}"
                        )
                    seen_identities.add(identity)
                    records.append(record)
                    selection_records += 1
                    if len(records) > max_records:
                        raise IncompleteCoverageError(
                            f"WHO response exceeded max_records={max_records}"
                        )
                if next_link is None:
                    if expected_count is not None and selection_records != expected_count:
                        raise IncompleteCoverageError(
                            f"WHO pagination ended with {selection_records} observations; "
                            f"@odata.count declared {expected_count}"
                        )
                    url = None
                else:
                    if selection_records == page_start:
                        raise IncompleteCoverageError(
                            "WHO pagination returned an empty intermediate page"
                        )
                    url = _validate_page_url(
                        next_link,
                        url,
                        indicator,
                        filter_expression,
                        page_size,
                    )
    return records


def _serialized_lines(records: Sequence[dict[str, Any]]) -> list[str]:
    return [
        json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        for record in records
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch a complete bounded WHO GHO OData observation set."
    )
    parser.add_argument("--indicator", action="append", dest="indicators", required=True)
    parser.add_argument("--geography-type", required=True)
    parser.add_argument("--geography", action="append", dest="geographies", required=True)
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=float, default=DEFAULT_RETRY_DELAY)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help="validate a nonempty live response without publishing records",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        records = fetch_who_gho_odata(
            indicators=args.indicators,
            geography_type=args.geography_type,
            geographies=args.geographies,
            start_year=args.start_year,
            end_year=args.end_year,
            page_size=args.page_size,
            timeout=args.timeout,
            retries=args.retries,
            retry_delay=args.retry_delay,
            max_pages=args.max_pages,
            max_records=args.max_records,
        )
        if args.smoke_test:
            if not records:
                raise WhoGhoError("WHO smoke test returned no normalized observations")
            print(
                json.dumps(
                    {
                        "source": SOURCE,
                        "status": "ok",
                        "records": len(records),
                        "indicators": sorted({record["indicator_code"] for record in records}),
                        "geographies": sorted({record["geography_code"] for record in records}),
                        "periods": sorted({record["period"] for record in records}),
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            return 0
        lines = _serialized_lines(records)
        if lines:
            sys.stdout.write("\n".join(lines) + "\n")
        sys.stdout.flush()
        return 0
    except (ValueError, WhoGhoError, urllib.error.URLError) as error:
        parser.error(str(error))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
