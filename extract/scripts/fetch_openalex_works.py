#!/usr/bin/env python3
"""Fetch a bounded rolling window of climate-adaptation works from OpenAlex.

The extractor walks only the filtered Works result set. It buffers at most 1,000
normalized records so incomplete cursor walks fail without emitting partial NDJSON.
OpenAlex publication dates describe the work, not when OpenAlex first indexed it.

Stdlib only.
"""

import argparse
import json
import os
import re
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

SOURCE = "openalex_works"
URL = "https://api.openalex.org/works"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
PER_PAGE = 100
DEFAULT_DAYS_BACK = 2
MAX_DAYS_BACK = 7
DEFAULT_MAX_PAGES = 10
HARD_MAX_PAGES = 10
DEFAULT_MAX_RECORDS = 1000
HARD_MAX_RECORDS = 1000
TOPICAL_FILTER = 'default.search:"climate change adaptation"'
SELECT_FIELDS = (
    "id",
    "doi",
    "display_name",
    "publication_year",
    "publication_date",
    "type",
    "language",
    "cited_by_count",
    "is_retracted",
    "primary_location",
    "open_access",
    "authorships",
    "topics",
    "updated_date",
)
WORK_ID_RE = re.compile(r"(?:https://openalex\.org/)?(W[1-9][0-9]*)")
DATE_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")


class OpenAlexError(RuntimeError):
    """The OpenAlex response cannot satisfy the extractor contract."""


class IncompleteCoverageError(OpenAlexError):
    """The configured bounds cannot cover every work reported by OpenAlex."""


def _positive_bounded_int(value: Any, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def publication_window(days_back: int, today: date | None = None) -> tuple[str, str]:
    """Return inclusive rolling publication-date bounds in UTC."""
    _positive_bounded_int(days_back, "days_back", MAX_DAYS_BACK)
    end = today if today is not None else datetime.now(timezone.utc).date()
    if not isinstance(end, date):
        raise ValueError("today must be a date")
    start = end - timedelta(days=days_back - 1)
    return start.isoformat(), end.isoformat()


def _optional(value: Any, expected: type | tuple[type, ...], field: str) -> Any:
    if value is not None and (isinstance(value, bool) and expected is int or not isinstance(value, expected)):
        raise OpenAlexError(f"OpenAlex work has invalid {field}")
    return value


def normalize_work(work: Any, fetched_at: str) -> dict[str, Any]:
    """Validate selected fields and create one stable-ID extraction record."""
    if not isinstance(work, dict):
        raise OpenAlexError("OpenAlex results contains a non-object work")

    wire_id = work.get("id")
    if not isinstance(wire_id, str):
        raise OpenAlexError("OpenAlex work has invalid id")
    match = WORK_ID_RE.fullmatch(wire_id)
    if match is None:
        raise OpenAlexError(f"OpenAlex work has invalid id {wire_id!r}")
    work_id = match.group(1)

    title = _optional(work.get("display_name"), str, "display_name")
    if title == "":
        raise OpenAlexError(f"OpenAlex work {work_id} has empty display_name")
    publication_year = _optional(work.get("publication_year"), int, "publication_year")
    publication_date = _optional(work.get("publication_date"), str, "publication_date")
    if publication_date is not None and DATE_RE.fullmatch(publication_date) is None:
        raise OpenAlexError(f"OpenAlex work {work_id} has invalid publication_date")
    cited_by_count = _optional(work.get("cited_by_count"), int, "cited_by_count")
    if cited_by_count is not None and cited_by_count < 0:
        raise OpenAlexError(f"OpenAlex work {work_id} has invalid cited_by_count")
    is_retracted = work.get("is_retracted")
    if is_retracted is not None and not isinstance(is_retracted, bool):
        raise OpenAlexError(f"OpenAlex work {work_id} has invalid is_retracted")

    _optional(work.get("doi"), str, "doi")
    _optional(work.get("type"), str, "type")
    _optional(work.get("language"), str, "language")
    _optional(work.get("primary_location"), dict, "primary_location")
    _optional(work.get("open_access"), dict, "open_access")
    _optional(work.get("authorships"), list, "authorships")
    _optional(work.get("topics"), list, "topics")
    _optional(work.get("updated_date"), str, "updated_date")

    return {
        "source": SOURCE,
        "id": work_id,
        "fetched_at": fetched_at,
        "openalex_url": f"https://openalex.org/{work_id}",
        "doi": work.get("doi"),
        "title": title,
        "publication_year": publication_year,
        "publication_date": publication_date,
        "type": work.get("type"),
        "language": work.get("language"),
        "cited_by_count": cited_by_count,
        "is_retracted": is_retracted,
        "primary_location": work.get("primary_location"),
        "open_access": work.get("open_access"),
        "authorships": work.get("authorships"),
        "topics": work.get("topics"),
        "updated_date": work.get("updated_date"),
    }


def _parse_page(document: Any) -> tuple[int, str | None, list[Any]]:
    if not isinstance(document, dict):
        raise OpenAlexError("OpenAlex response must be an object")
    if "error" in document:
        detail = document.get("message") or document.get("error")
        raise OpenAlexError(f"OpenAlex API error: {detail}")

    meta = document.get("meta")
    if not isinstance(meta, dict):
        raise OpenAlexError("OpenAlex response is missing object meta")
    count = meta.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise OpenAlexError("OpenAlex response meta.count must be a non-negative integer")
    if "next_cursor" not in meta:
        raise OpenAlexError("OpenAlex response meta is missing next_cursor")
    next_cursor = meta["next_cursor"]
    if next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor):
        raise OpenAlexError("OpenAlex response meta.next_cursor must be a string or null")
    results = document.get("results")
    if not isinstance(results, list):
        raise OpenAlexError("OpenAlex response results must be a list")
    return count, next_cursor, results


def fetch_recent_works(
    *,
    days_back: int = DEFAULT_DAYS_BACK,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_records: int = DEFAULT_MAX_RECORDS,
    timeout: int = 30,
    today: date | None = None,
) -> Iterator[dict[str, Any]]:
    """Fetch the complete bounded filtered result set or raise without yielding."""
    _positive_bounded_int(days_back, "days_back", MAX_DAYS_BACK)
    _positive_bounded_int(max_pages, "max_pages", HARD_MAX_PAGES)
    _positive_bounded_int(max_records, "max_records", HARD_MAX_RECORDS)
    if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer")

    start_date, end_date = publication_window(days_back, today)
    publication_filter = (
        f"from_publication_date:{start_date},"
        f"to_publication_date:{end_date},{TOPICAL_FILTER}"
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    api_key = os.environ.get("OPENALEX_API_KEY", "").strip()
    cursor = "*"
    requested_cursors: set[str] = set()
    expected_count: int | None = None
    seen_ids: set[str] = set()
    records: list[dict[str, Any]] = []

    for page_number in range(1, max_pages + 1):
        if cursor in requested_cursors:
            raise OpenAlexError(f"OpenAlex repeated cursor {cursor!r}")
        requested_cursors.add(cursor)
        params = {
            "filter": publication_filter,
            "select": ",".join(SELECT_FIELDS),
            "per-page": str(PER_PAGE),
            "cursor": cursor,
        }
        if api_key:
            params["api_key"] = api_key
        request = urllib.request.Request(
            f"{URL}?{urllib.parse.urlencode(params)}",
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        count, next_cursor, results = _parse_page(document)

        if expected_count is None:
            expected_count = count
            if expected_count > max_records:
                raise IncompleteCoverageError(
                    f"OpenAlex reports {expected_count} works, above max_records={max_records}"
                )
            minimum_pages = (expected_count + PER_PAGE - 1) // PER_PAGE
            if minimum_pages > max_pages:
                raise IncompleteCoverageError(
                    f"OpenAlex reports {expected_count} works, requiring at least "
                    f"{minimum_pages} pages above max_pages={max_pages}"
                )
        elif count != expected_count:
            raise OpenAlexError(
                f"OpenAlex meta.count changed from {expected_count} to {count} during pagination"
            )

        for work in results:
            record = normalize_work(work, fetched_at)
            if record["id"] in seen_ids:
                continue
            seen_ids.add(record["id"])
            records.append(record)
            if len(records) > max_records:
                raise IncompleteCoverageError(
                    f"OpenAlex unique work count exceeded max_records={max_records}"
                )

        if len(records) > expected_count:
            raise OpenAlexError(
                f"OpenAlex returned {len(records)} unique works but meta.count is {expected_count}"
            )

        terminal = next_cursor is None or not results or len(records) == expected_count
        if terminal:
            if len(records) != expected_count:
                raise IncompleteCoverageError(
                    f"OpenAlex pagination ended with {len(records)} unique works; "
                    f"meta.count declared {expected_count}"
                )
            yield from records
            return

        if page_number == max_pages:
            raise IncompleteCoverageError(
                f"OpenAlex pagination exceeded max_pages={max_pages} after "
                f"{len(records)} of {expected_count} unique works"
            )
        cursor = next_cursor

    raise IncompleteCoverageError("OpenAlex pagination ended without complete coverage")


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a bounded recent climate-adaptation Works window from OpenAlex."
    )
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)

    try:
        records = fetch_recent_works(
            days_back=args.days_back,
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
