#!/usr/bin/env python3
"""Fetch a bounded basket of World Bank development indicators as NDJSON.

Every request names its indicators, countries, inclusive year range, page size,
and maximum page count. The extractor follows every advertised page within
those bounds and rejects truncated or internally inconsistent responses.

Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, NamedTuple, Sequence

SOURCE = "world_bank_indicators"
API_URL = "https://api.worldbank.org/v2"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

INDICATORS = (
    "SP.POP.TOTL",       # population, total
    "NY.GDP.MKTP.CD",    # GDP, current US dollars
    "NY.GDP.PCAP.CD",    # GDP per capita, current US dollars
    "SP.DYN.LE00.IN",    # life expectancy at birth
    "SL.UEM.TOTL.ZS",    # modeled ILO unemployment rate
)
COUNTRIES = (
    "USA",
    "CAN",
    "MEX",
    "BRA",
    "GBR",
    "FRA",
    "DEU",
    "CHN",
    "IND",
    "JPN",
    "NGA",
    "ZAF",
)
INDICATOR_ALLOWLIST = frozenset(INDICATORS)
COUNTRY_ALLOWLIST = frozenset(COUNTRIES)
MAX_INDICATORS = 10
MAX_COUNTRIES = 25
MAX_YEAR_SPAN = 10
MAX_PAGE_SIZE = 1_000
MAX_PAGE_BOUND = 100
_CODE_RE = re.compile(r"[A-Z0-9.]+")
_YEAR_RE = re.compile(r"\d{4}")


class Page(NamedTuple):
    """Validated pagination metadata and the page's raw observations."""

    metadata: dict[str, Any]
    observations: list[Any]
    page: int
    pages: int
    per_page: int
    total: int


def _validate_codes(
    values: Sequence[str], *, name: str, allowlist: frozenset[str], maximum: int
) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        raise ValueError(f"{name} must be a sequence")
    requested = tuple(values)
    if not requested:
        raise ValueError(f"at least one {name} is required")
    if len(requested) > maximum:
        raise ValueError(f"at most {maximum} {name} may be requested")
    if len(set(requested)) != len(requested):
        raise ValueError(f"{name} must not contain duplicates")
    malformed = [value for value in requested if not isinstance(value, str) or _CODE_RE.fullmatch(value) is None]
    if malformed:
        raise ValueError(f"invalid {name}: {malformed!r}")
    unsupported = [value for value in requested if value not in allowlist]
    if unsupported:
        raise ValueError(f"{name} are not allowlisted: {', '.join(unsupported)}")
    return requested


def validate_request(
    indicators: Sequence[str],
    countries: Sequence[str],
    start_year: int,
    end_year: int,
    page_size: int,
    max_pages: int,
    timeout: int,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Validate all dimensions and hard request limits before making HTTP calls."""
    requested_indicators = _validate_codes(
        indicators,
        name="indicators",
        allowlist=INDICATOR_ALLOWLIST,
        maximum=MAX_INDICATORS,
    )
    requested_countries = _validate_codes(
        countries,
        name="countries",
        allowlist=COUNTRY_ALLOWLIST,
        maximum=MAX_COUNTRIES,
    )
    for name, value in (("start_year", start_year), ("end_year", end_year)):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer calendar year")
    if start_year < 1960 or end_year > 9999:
        raise ValueError("years must be four-digit calendar years from 1960 onward")
    if start_year > end_year:
        raise ValueError("start_year must not exceed end_year")
    if end_year - start_year + 1 > MAX_YEAR_SPAN:
        raise ValueError(f"requests may span at most {MAX_YEAR_SPAN} inclusive years")
    for name, value, maximum in (
        ("page_size", page_size, MAX_PAGE_SIZE),
        ("max_pages", max_pages, MAX_PAGE_BOUND),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
            raise ValueError(f"{name} must be an integer from 1 to {maximum}")
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer")
    return requested_indicators, requested_countries


def build_url(
    indicator: str,
    countries: Sequence[str],
    start_year: int,
    end_year: int,
    page: int,
    page_size: int,
) -> str:
    """Build one explicitly bounded World Bank API page URL."""
    country_path = urllib.parse.quote(";".join(countries), safe=";")
    indicator_path = urllib.parse.quote(indicator, safe=".")
    query = urllib.parse.urlencode(
        {
            "format": "json",
            "date": f"{start_year}:{end_year}",
            "page": page,
            "per_page": page_size,
        }
    )
    return f"{API_URL}/country/{country_path}/indicator/{indicator_path}?{query}"


def _metadata_integer(metadata: dict[str, Any], name: str) -> int:
    value = metadata.get(name)
    if isinstance(value, bool):
        raise RuntimeError(f"World Bank response metadata {name} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise RuntimeError(f"World Bank response metadata {name} must be an integer")


def parse_response(document: Any, expected_page: int, max_pages: int) -> Page:
    """Validate one World Bank two-element page response."""
    if not isinstance(document, list) or len(document) != 2:
        raise RuntimeError("World Bank response must be a two-element list")
    metadata, observations = document
    if not isinstance(metadata, dict):
        raise RuntimeError("World Bank response metadata must be an object")
    if not isinstance(observations, list):
        raise RuntimeError("World Bank response observations must be a list")

    page = _metadata_integer(metadata, "page")
    pages = _metadata_integer(metadata, "pages")
    per_page = _metadata_integer(metadata, "per_page")
    total = _metadata_integer(metadata, "total")
    if page != expected_page:
        raise RuntimeError(
            f"World Bank response returned page {page}, expected {expected_page}"
        )
    if pages < 1 or page > pages:
        raise RuntimeError("World Bank response has invalid page bounds")
    if pages > max_pages:
        raise RuntimeError(
            f"World Bank response requires {pages} pages, exceeding max_pages={max_pages}"
        )
    if per_page < 1 or per_page > MAX_PAGE_SIZE:
        raise RuntimeError("World Bank response has invalid per_page")
    if total < 0:
        raise RuntimeError("World Bank response has invalid total")
    if len(observations) > per_page:
        raise RuntimeError("World Bank response contains more observations than per_page")
    return Page(metadata, observations, page, pages, per_page, total)


def normalize_observation(
    observation: Any,
    *,
    expected_indicator: str,
    requested_countries: frozenset[str],
    start_year: int,
    end_year: int,
    fetched_at: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    """Validate one observation and add the standard stable envelope."""
    if not isinstance(observation, dict):
        raise RuntimeError("World Bank response contains a non-object observation")
    indicator = observation.get("indicator")
    country = observation.get("country")
    if not isinstance(indicator, dict) or not isinstance(country, dict):
        raise RuntimeError("World Bank observation indicator and country must be objects")

    indicator_id = indicator.get("id")
    indicator_name = indicator.get("value")
    country_id = country.get("id")
    country_name = country.get("value")
    country_iso3 = observation.get("countryiso3code")
    year = observation.get("date")
    unit = observation.get("unit")
    status = observation.get("obs_status")
    decimal = observation.get("decimal")
    value = observation.get("value")

    if indicator_id != expected_indicator:
        raise RuntimeError(f"World Bank observation has unexpected indicator {indicator_id!r}")
    if not isinstance(indicator_name, str) or not indicator_name:
        raise RuntimeError("World Bank observation has invalid indicator name")
    if not isinstance(country_id, str) or not country_id:
        raise RuntimeError("World Bank observation has invalid country id")
    if not isinstance(country_name, str) or not country_name:
        raise RuntimeError("World Bank observation has invalid country name")
    if not isinstance(country_iso3, str) or country_iso3 not in requested_countries:
        raise RuntimeError(f"World Bank observation has unexpected country {country_iso3!r}")
    if not isinstance(year, str) or _YEAR_RE.fullmatch(year) is None:
        raise RuntimeError("World Bank observation has invalid date")
    if not start_year <= int(year) <= end_year:
        raise RuntimeError(f"World Bank observation date {year} is outside request bounds")
    if value is not None and (isinstance(value, bool) or not isinstance(value, (int, float, str))):
        raise RuntimeError("World Bank observation has invalid value")
    if not isinstance(unit, str):
        raise RuntimeError("World Bank observation has invalid unit")
    if not isinstance(status, str):
        raise RuntimeError("World Bank observation has invalid status")
    if isinstance(decimal, bool) or not isinstance(decimal, int):
        raise RuntimeError("World Bank observation has invalid decimal metadata")

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{indicator_id}:{country_iso3}:{year}",
        "indicator_id": indicator_id,
        "indicator_name": indicator_name,
        "country_id": country_id,
        "country_iso3": country_iso3,
        "country_name": country_name,
        "date": year,
        "value": value,
        "unit": unit,
        "status": status,
        "decimal": decimal,
        "raw_metadata": metadata,
        "raw": observation,
    }


def fetch_world_bank_indicators(
    *,
    indicators: Sequence[str] = INDICATORS,
    countries: Sequence[str] = COUNTRIES,
    start_year: int,
    end_year: int,
    page_size: int = 100,
    max_pages: int = 10,
    timeout: int = 30,
) -> Iterator[dict[str, Any]]:
    """Fetch all pages for each requested indicator within hard bounds."""
    requested_indicators, requested_countries = validate_request(
        indicators, countries, start_year, end_year, page_size, max_pages, timeout
    )
    country_set = frozenset(requested_countries)
    fetched_at = datetime.now(timezone.utc).isoformat()

    for indicator in requested_indicators:
        expected_pages: int | None = None
        expected_total: int | None = None
        observed_total = 0
        seen_ids: set[str] = set()
        page_number = 1
        while expected_pages is None or page_number <= expected_pages:
            request = urllib.request.Request(
                build_url(
                    indicator,
                    requested_countries,
                    start_year,
                    end_year,
                    page_number,
                    page_size,
                ),
                headers={"Accept": "application/json", "User-Agent": USER_AGENT},
            )
            with urllib.request.urlopen(request, timeout=timeout) as response:
                page = parse_response(json.load(response), page_number, max_pages)

            if page.per_page != page_size:
                raise RuntimeError(
                    f"World Bank response per_page {page.per_page} does not match requested {page_size}"
                )
            if expected_pages is None:
                expected_pages = page.pages
                expected_total = page.total
            elif page.pages != expected_pages or page.total != expected_total:
                raise RuntimeError("World Bank pagination metadata changed between pages")

            for observation in page.observations:
                record = normalize_observation(
                    observation,
                    expected_indicator=indicator,
                    requested_countries=country_set,
                    start_year=start_year,
                    end_year=end_year,
                    fetched_at=fetched_at,
                    metadata=page.metadata,
                )
                if record["id"] in seen_ids:
                    raise RuntimeError(
                        f"World Bank response contains duplicate observation {record['id']}"
                    )
                seen_ids.add(record["id"])
                observed_total += 1
                yield record
            page_number += 1

        if observed_total != expected_total:
            raise RuntimeError(
                f"World Bank response advertised {expected_total} observations but returned {observed_total}"
            )


def _comma_separated(value: str) -> tuple[str, ...]:
    values = tuple(item.strip() for item in value.split(",") if item.strip())
    if not values:
        raise argparse.ArgumentTypeError("value must contain at least one code")
    return values


def main(argv: Sequence[str] | None = None) -> None:
    current_year = datetime.now(timezone.utc).year
    parser = argparse.ArgumentParser(
        description="Fetch an explicitly bounded World Bank indicator basket."
    )
    parser.add_argument("--indicators", type=_comma_separated, default=INDICATORS)
    parser.add_argument("--countries", type=_comma_separated, default=COUNTRIES)
    parser.add_argument("--start-year", type=int, default=current_year - 1)
    parser.add_argument("--end-year", type=int, default=current_year)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)

    try:
        validate_request(
            args.indicators,
            args.countries,
            args.start_year,
            args.end_year,
            args.page_size,
            args.max_pages,
            args.timeout,
        )
    except ValueError as error:
        parser.error(str(error))

    for record in fetch_world_bank_indicators(
        indicators=args.indicators,
        countries=args.countries,
        start_year=args.start_year,
        end_year=args.end_year,
        page_size=args.page_size,
        max_pages=args.max_pages,
        timeout=args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
