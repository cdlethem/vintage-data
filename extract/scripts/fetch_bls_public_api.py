#!/usr/bin/env python3
"""Fetch an explicit basket of labor time series from the keyless BLS API.

The public API accepts at most ten inclusive calendar years without registration. Each
BLS observation is emitted as one NDJSON record with a stable series/year/period ID.
Footnotes are normalized from the API's ``[{"code": ..., "text": ...}]`` wire format.

Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

SOURCE = "bls_public_api"
URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
MAX_ANONYMOUS_YEARS = 10
SERIES_IDS = (
    "LNS14000000",  # unemployment rate
    "LNS11300000",  # labor-force participation rate
    "LNS12300000",  # employment-population ratio
    "CES0000000001",  # all employees, total nonfarm
    "CES0500000003",  # average hourly earnings, total private
)
SERIES_ALLOWLIST = frozenset(SERIES_IDS)
YEAR_RE = re.compile(r"\d{4}")
PERIOD_RE = re.compile(r"[A-Z]\d{2}")


def validate_year_range(start_year: int, end_year: int) -> None:
    """Validate the anonymous API's inclusive calendar-year bound."""
    if isinstance(start_year, bool) or not isinstance(start_year, int):
        raise ValueError("start_year must be an integer calendar year")
    if isinstance(end_year, bool) or not isinstance(end_year, int):
        raise ValueError("end_year must be an integer calendar year")
    if start_year < 1900 or end_year > 9999:
        raise ValueError("years must be four-digit calendar years from 1900 onward")
    if start_year > end_year:
        raise ValueError("start_year must not exceed end_year")
    if end_year - start_year + 1 > MAX_ANONYMOUS_YEARS:
        raise ValueError(
            f"anonymous BLS requests may span at most {MAX_ANONYMOUS_YEARS} "
            "inclusive calendar years"
        )


def validate_series_ids(series_ids: Sequence[str]) -> tuple[str, ...]:
    """Return a validated, duplicate-free subset of the maintained allowlist."""
    if isinstance(series_ids, (str, bytes)):
        raise ValueError("series_ids must be a sequence of series IDs")
    requested = tuple(series_ids)
    if not requested:
        raise ValueError("at least one BLS series ID is required")
    if len(set(requested)) != len(requested):
        raise ValueError("series_ids must not contain duplicates")
    unsupported = [series_id for series_id in requested if series_id not in SERIES_ALLOWLIST]
    if unsupported:
        raise ValueError(f"series IDs are not allowlisted: {', '.join(map(str, unsupported))}")
    return requested


def parse_footnotes(value: Any, context: str = "observation") -> list[dict[str, str]]:
    """Normalize BLS footnote objects while retaining their code and text verbatim."""
    if not isinstance(value, list):
        raise RuntimeError(f"{context} footnotes must be a list")
    normalized = []
    for index, footnote in enumerate(value):
        if not isinstance(footnote, dict):
            raise RuntimeError(f"{context} footnote {index} must be an object")
        code = footnote.get("code")
        text = footnote.get("text")
        # BLS represents an observation with no footnote as one empty object.
        if code is None and text is None:
            continue
        if not isinstance(code, str) or not isinstance(text, str):
            raise RuntimeError(
                f"{context} footnote {index} must contain string code and text"
            )
        normalized.append({"code": code, "text": text})
    return normalized


def normalize_observation(
    series_id: str, observation: Any, fetched_at: str
) -> dict[str, Any]:
    """Validate and normalize one observation from a BLS series response."""
    if not isinstance(observation, dict):
        raise RuntimeError(f"BLS series {series_id} contains a non-object observation")

    year = observation.get("year")
    period = observation.get("period")
    period_name = observation.get("periodName")
    value = observation.get("value")
    if not isinstance(year, str) or YEAR_RE.fullmatch(year) is None:
        raise RuntimeError(f"BLS series {series_id} observation has invalid year")
    if not isinstance(period, str) or PERIOD_RE.fullmatch(period) is None:
        raise RuntimeError(f"BLS series {series_id} observation has invalid period")
    if not isinstance(period_name, str) or not period_name:
        raise RuntimeError(f"BLS series {series_id} observation has invalid periodName")
    if not isinstance(value, str) or not value:
        raise RuntimeError(f"BLS series {series_id} observation has invalid value")

    latest_wire = observation.get("latest", "false")
    if latest_wire not in ("true", "false"):
        raise RuntimeError(f"BLS series {series_id} observation has invalid latest status")
    context = f"BLS series {series_id} {year} {period}"
    footnotes = parse_footnotes(observation.get("footnotes"), context)

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": f"{series_id}:{year}:{period}",
        "series_id": series_id,
        "year": int(year),
        "period": period,
        "period_name": period_name,
        "value": value,
        "latest": latest_wire == "true",
        "footnotes": footnotes,
    }


def parse_response(
    document: Any, requested_series: Sequence[str], fetched_at: str
) -> Iterator[dict[str, Any]]:
    """Validate a complete BLS response and emit its normalized observations."""
    if not isinstance(document, dict):
        raise RuntimeError("BLS response must be an object")
    status = document.get("status")
    messages = document.get("message")
    if status != "REQUEST_SUCCEEDED":
        detail = messages if isinstance(messages, list) else []
        raise RuntimeError(f"BLS request failed with status {status!r}: {detail!r}")
    if not isinstance(messages, list) or not all(isinstance(item, str) for item in messages):
        raise RuntimeError("BLS response message must be a list of strings")
    results = document.get("Results")
    if not isinstance(results, dict):
        raise RuntimeError("BLS response is missing object Results")
    series = results.get("series")
    if not isinstance(series, list):
        raise RuntimeError("BLS response Results.series must be a list")

    expected = set(requested_series)
    seen = set()
    normalized_series = []
    for index, item in enumerate(series):
        if not isinstance(item, dict):
            raise RuntimeError(f"BLS response series {index} must be an object")
        series_id = item.get("seriesID")
        if not isinstance(series_id, str) or series_id not in expected:
            raise RuntimeError(f"BLS response contains unexpected seriesID {series_id!r}")
        if series_id in seen:
            raise RuntimeError(f"BLS response contains duplicate seriesID {series_id}")
        data = item.get("data")
        if not isinstance(data, list):
            raise RuntimeError(f"BLS series {series_id} data must be a list")
        seen.add(series_id)
        normalized_series.append((series_id, data))

    missing = expected - seen
    if missing:
        raise RuntimeError(f"BLS response is missing requested series: {', '.join(sorted(missing))}")

    for series_id, data in normalized_series:
        for observation in data:
            yield normalize_observation(series_id, observation, fetched_at)


def fetch_bls_public_api(
    start_year: int,
    end_year: int,
    series_ids: Sequence[str] = SERIES_IDS,
    timeout: int = 30,
) -> Iterator[dict[str, Any]]:
    """POST one bounded, keyless request and emit normalized BLS observations."""
    validate_year_range(start_year, end_year)
    requested_series = validate_series_ids(series_ids)
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer")

    body = json.dumps(
        {
            "seriesid": list(requested_series),
            "startyear": str(start_year),
            "endyear": str(end_year),
        },
        separators=(",", ":"),
    ).encode("utf-8")
    request = urllib.request.Request(
        URL,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    yield from parse_response(document, requested_series, fetched_at)


def main(argv: Sequence[str] | None = None) -> None:
    current_year = datetime.now(timezone.utc).year
    parser = argparse.ArgumentParser(
        description=(
            "Fetch allowlisted BLS labor series without a registration key; "
            "date bounds are inclusive and may span at most 10 calendar years."
        )
    )
    parser.add_argument(
        "--start-year",
        type=int,
        help="first calendar year, inclusive (defaults to --end-year)",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        help=f"last calendar year, inclusive (defaults to current UTC year, {current_year})",
    )
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout in seconds")
    args = parser.parse_args(argv)

    end_year = args.end_year if args.end_year is not None else current_year
    start_year = args.start_year if args.start_year is not None else end_year
    try:
        validate_year_range(start_year, end_year)
        if args.timeout <= 0:
            raise ValueError("timeout must be a positive integer")
    except (TypeError, ValueError) as error:
        parser.error(str(error))

    for record in fetch_bls_public_api(start_year, end_year, timeout=args.timeout):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
