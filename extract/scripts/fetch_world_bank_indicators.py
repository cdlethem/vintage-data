#!/usr/bin/env python3
"""Fetch a bounded set of annual World Bank indicator observations as NDJSON.

The World Bank API republishes historical observations when values are revised.
Records therefore use indicator, country, and period as their stable identity; a
later fetch updates the same record rather than creating a new observation.
Only the Python standard library is used.
"""
import argparse
import hashlib
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Dict, Iterable, Iterator, List, Optional

BASE_URL = "https://api.worldbank.org/v2"
SOURCE = "world_bank_indicators"
DEFAULT_INDICATORS = ("NY.GDP.MKTP.CD", "SP.POP.TOTL", "EN.ATM.CO2E.KT")
DEFAULT_COUNTRIES = ("USA", "GBR", "DEU", "IND")
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 20
MAX_PAGE_SIZE = 1000
MAX_PAGES = 1000
MAX_SELECTORS = 100
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1"


def _get_json(url: str) -> object:
    request = urllib.request.Request(
        url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def _selector_values(values: Optional[Iterable[str]], defaults: Iterable[str]) -> List[str]:
    """Accept repeated and comma-separated CLI selectors without empty codes."""
    selected: List[str] = []
    for value in values or defaults:
        selected.extend(part.strip().upper() for part in value.split(",") if part.strip())
    if not selected:
        raise ValueError("at least one selector is required")
    if len(selected) > MAX_SELECTORS:
        raise ValueError("at most %d selectors are allowed" % MAX_SELECTORS)
    return selected


def build_observation_url(
    indicators: Iterable[str], countries: Iterable[str], start_date: str, end_date: str,
    page_size: int, page: int,
) -> str:
    """Build one World Bank API request, with all variable query parts encoded."""
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required")
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise ValueError("page_size must be between 1 and %d" % MAX_PAGE_SIZE)
    if page < 1:
        raise ValueError("page must be at least 1")

    indicator_path = ";".join(indicators)
    country_path = ";".join(countries)
    if not indicator_path or not country_path:
        raise ValueError("at least one indicator and country are required")
    path = "/country/%s/indicator/%s" % (
        urllib.parse.quote(country_path, safe=";"),
        urllib.parse.quote(indicator_path, safe=";."),
    )
    query = urllib.parse.urlencode(
        {"format": "json", "date": "%s:%s" % (start_date, end_date),
         "per_page": page_size, "page": page}
    )
    return BASE_URL + path + "?" + query


def _page_payload(payload: object, requested_page: int) -> tuple[Dict[str, object], List[Dict[str, object]]]:
    """Validate the documented two-element World Bank JSON response."""
    if not isinstance(payload, list) or len(payload) != 2:
        raise ValueError("World Bank response must be a two-element JSON array")
    metadata, observations = payload
    if not isinstance(metadata, dict):
        raise ValueError("World Bank response metadata must be an object")
    if observations is None:
        observations = []
    if not isinstance(observations, list) or not all(isinstance(row, dict) for row in observations):
        raise ValueError("World Bank response observations must be an array of objects")
    try:
        response_page = int(metadata["page"])
        pages = int(metadata["pages"])
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("World Bank response metadata must include numeric page and pages") from error
    if response_page != requested_page or pages < requested_page:
        raise ValueError("World Bank response pagination metadata is inconsistent")
    return metadata, observations


def _stable_id(indicator: str, country: str, period: str) -> str:
    """Identity excludes values because the API can revise published observations."""
    identity = "\x1f".join((SOURCE, indicator, country, period)).encode("utf-8")
    return hashlib.sha256(identity).hexdigest()


def normalize_observation(observation: Dict[str, object], fetched_at: str) -> Dict[str, object]:
    """Convert one API observation while retaining its interpretation metadata."""
    indicator_metadata = observation.get("indicator")
    country_metadata = observation.get("country")
    if not isinstance(indicator_metadata, dict) or not isinstance(country_metadata, dict):
        raise ValueError("World Bank observation must include indicator and country metadata")

    indicator = indicator_metadata.get("id")
    country_code = country_metadata.get("id")
    iso3 = observation.get("countryiso3code")
    period = observation.get("date")
    if not all(isinstance(value, str) and value for value in (indicator, country_code, period)):
        raise ValueError("World Bank observation is missing indicator, country, or period")
    identity_country = iso3 if isinstance(iso3, str) and iso3 else country_code
    if not isinstance(identity_country, str):
        raise ValueError("World Bank observation has an invalid country code")

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": _stable_id(indicator, identity_country, period),
        "indicator": indicator,
        "indicator_name": indicator_metadata.get("value"),
        "indicator_metadata": indicator_metadata,
        "country": country_metadata.get("value"),
        "country_code": country_code,
        "iso3": iso3,
        "period": period,
        "value": observation.get("value"),
        "status": observation.get("obs_status"),
        "unit": observation.get("unit"),
        "decimal": observation.get("decimal"),
        "footnote": observation.get("footnote"),
    }


def fetch_observations(
    indicators: Iterable[str], countries: Iterable[str], start_date: str, end_date: str,
    page_size: int = DEFAULT_PAGE_SIZE, max_pages: int = DEFAULT_MAX_PAGES,
    get_json: Callable[[str], object] = _get_json,
    fetched_at: Optional[str] = None,
) -> Iterator[Dict[str, object]]:
    """Fetch every API page or fail before emitting a silently truncated series."""
    if not 1 <= max_pages <= MAX_PAGES:
        raise ValueError("max_pages must be between 1 and %d" % MAX_PAGES)
    indicators = _selector_values(indicators, ())
    countries = _selector_values(countries, ())
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()

    def page_url(page: int) -> str:
        return build_observation_url(indicators, countries, start_date, end_date, page_size, page)

    metadata, observations = _page_payload(get_json(page_url(1)), 1)
    pages = int(metadata["pages"])
    if pages > max_pages:
        raise ValueError("World Bank response requires %d pages; configured maximum is %d" % (pages, max_pages))
    for observation in observations:
        yield normalize_observation(observation, fetched_at)
    for page in range(2, pages + 1):
        later_metadata, observations = _page_payload(get_json(page_url(page)), page)
        if int(later_metadata["pages"]) != pages:
            raise ValueError("World Bank response page count changed during pagination")
        for observation in observations:
            yield normalize_observation(observation, fetched_at)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--indicator", "--indicators", dest="indicators", action="append", nargs="+",
                        help="World Bank indicator code(s), repeatable or comma-separated")
    parser.add_argument("--country", "--countries", dest="countries", action="append", nargs="+",
                        help="World Bank country code(s), repeatable or comma-separated")
    parser.add_argument("--start-date", required=True, help="inclusive World Bank period, for example 2020")
    parser.add_argument("--end-date", required=True, help="inclusive World Bank period, for example 2026")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    args = parser.parse_args(argv)
    # argparse's append+nargs creates a list of lists; flatten before validation.
    indicator_groups = args.indicators if args.indicators else [DEFAULT_INDICATORS]
    country_groups = args.countries if args.countries else [DEFAULT_COUNTRIES]
    args.indicators = _selector_values(
        (item for group in indicator_groups for item in group), ()
    )
    args.countries = _selector_values(
        (item for group in country_groups for item in group), ()
    )
    if not 1 <= args.page_size <= MAX_PAGE_SIZE:
        parser.error("--page-size must be between 1 and %d" % MAX_PAGE_SIZE)
    if not 1 <= args.max_pages <= MAX_PAGES:
        parser.error("--max-pages must be between 1 and %d" % MAX_PAGES)
    return args


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)
    for record in fetch_observations(
        args.indicators, args.countries, args.start_date, args.end_date,
        page_size=args.page_size, max_pages=args.max_pages,
    ):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
