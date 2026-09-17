#!/usr/bin/env python3
"""Fetch selected U.S. Bureau of Labor Statistics monthly indicators as NDJSON.

The BLS public time-series API returns a bounded history for each requested series.
Only the explicit allowlist below is collected so this daily feed remains small and
predictable. Stdlib only.
"""

import argparse
import json
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

SOURCE = "bls_public_api"
API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
MAX_YEARS = 20

# Monthly national household-survey indicators. BLS publishes these without an API key.
SERIES_IDS = (
    "LNS14000000",  # Unemployment rate
    "LNS12000000",  # Civilian labor force level
    "LNS11000000",  # Civilian employment level
    "LNS13000000",  # Unemployment level
    "LNS11300000",  # Employment-population ratio
    "LNS12300000",  # Labor force participation rate
)


def natural_id(series_id: str, year: str, period: str) -> str:
    """Return the stable identity of one published BLS observation."""
    return f"{series_id}:{year}:{period}"


def _require_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"BLS observation has invalid {field}")
    return value


def _latest(value: Any) -> bool:
    if value is None:
        return False
    if value == "true" or value is True:
        return True
    if value == "false" or value is False:
        return False
    raise ValueError("BLS observation has invalid latest status")


def normalize_footnotes(value: Any) -> list[dict[str, str]]:
    """Normalize BLS's empty-object footnote placeholders to an empty list."""
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("BLS observation has invalid footnotes")

    footnotes = []
    for footnote in value:
        if not isinstance(footnote, Mapping):
            raise ValueError("BLS observation has invalid footnote")
        code = footnote.get("footnoteCode")
        text = footnote.get("footnoteText")
        if code is None and text is None:
            continue
        if not isinstance(code, str) or not isinstance(text, str):
            raise ValueError("BLS observation has invalid footnote")
        footnotes.append({"code": code, "text": text})
    return footnotes


def normalize_response(
    payload: Any,
    fetched_at: str,
    series_ids: Sequence[str] = SERIES_IDS,
) -> list[dict[str, Any]]:
    """Validate a BLS response and return contract-compliant observation records."""
    if not isinstance(payload, Mapping):
        raise ValueError("BLS API response must be an object")

    status = payload.get("status")
    if not isinstance(status, str):
        raise ValueError("BLS API response has invalid status")
    if status != "REQUEST_SUCCEEDED":
        messages = payload.get("message", [])
        detail = "; ".join(str(message) for message in messages) if isinstance(messages, list) else ""
        raise ValueError(f"BLS API request failed: {status}{': ' + detail if detail else ''}")

    series = payload.get("Results", {}).get("series") if isinstance(payload.get("Results"), Mapping) else None
    if not isinstance(series, list):
        raise ValueError("BLS API response is missing Results.series")

    expected = tuple(series_ids)
    if not expected or len(set(expected)) != len(expected):
        raise ValueError("BLS series allowlist must contain unique series IDs")

    supplied: dict[str, Mapping[str, Any]] = {}
    for item in series:
        if not isinstance(item, Mapping):
            raise ValueError("BLS API response has invalid series entry")
        series_id = item.get("seriesID")
        if not isinstance(series_id, str) or series_id not in expected:
            raise ValueError("BLS API response has unexpected series ID")
        if series_id in supplied:
            raise ValueError(f"BLS API response has duplicate series ID: {series_id}")
        if not isinstance(item.get("data"), list):
            raise ValueError(f"BLS series {series_id} has invalid data")
        supplied[series_id] = item

    missing = set(expected) - set(supplied)
    if missing:
        raise ValueError(f"BLS API response is missing series: {', '.join(sorted(missing))}")

    records = []
    for series_id in expected:
        for observation in supplied[series_id]["data"]:
            if not isinstance(observation, Mapping):
                raise ValueError(f"BLS series {series_id} has invalid observation")
            year = _require_string(observation.get("year"), "year")
            if not (len(year) == 4 and year.isdigit()):
                raise ValueError("BLS observation has invalid year")
            period = _require_string(observation.get("period"), "period")
            period_name = _require_string(observation.get("periodName"), "periodName")
            value = observation.get("value")
            if isinstance(value, bool) or not isinstance(value, (str, int, float)):
                raise ValueError("BLS observation has invalid value")
            records.append(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": natural_id(series_id, year, period),
                    "series_id": series_id,
                    "year": year,
                    "period": period,
                    "period_name": period_name,
                    "value": str(value),
                    "latest": _latest(observation.get("latest")),
                    "footnotes": normalize_footnotes(observation.get("footnotes")),
                }
            )
    return records


def build_request(series_ids: Sequence[str], start_year: int, end_year: int) -> urllib.request.Request:
    """Build one bounded BLS API request for the configured series."""
    if not series_ids:
        raise ValueError("at least one BLS series ID is required")
    if start_year > end_year or end_year - start_year + 1 > MAX_YEARS:
        raise ValueError(f"BLS requests must cover between 1 and {MAX_YEARS} years")
    body = json.dumps(
        {"seriesid": list(series_ids), "startyear": str(start_year), "endyear": str(end_year)},
        separators=(",", ":"),
    ).encode("utf-8")
    return urllib.request.Request(
        API_URL,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )


def fetch_bls(years: int = 2, end_year: int | None = None, timeout: int = 30) -> Iterable[dict[str, Any]]:
    """Fetch the configured, bounded history of BLS series."""
    if not 1 <= years <= MAX_YEARS:
        raise ValueError(f"years must be between 1 and {MAX_YEARS}")
    current_year = datetime.now(timezone.utc).year if end_year is None else end_year
    if not isinstance(current_year, int):
        raise ValueError("end_year must be an integer")
    request = build_request(SERIES_IDS, current_year - years + 1, current_year)
    fetched_at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(request, timeout=timeout) as response:
        try:
            payload = json.loads(response.read().decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ValueError("BLS API returned invalid JSON") from error
    yield from normalize_response(payload, fetched_at)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=int, default=2, help="bounded history to request (1-20 years)")
    parser.add_argument("--end-year", type=int, help="last calendar year to request; defaults to the current UTC year")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    for record in fetch_bls(args.years, args.end_year, args.timeout):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
