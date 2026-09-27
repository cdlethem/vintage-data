#!/usr/bin/env python3
"""Fetch a bounded ROR v2 organization result set as NDJSON.

The scheduled mode requests a small inclusive UTC window over ROR's
``admin.last_modified.date`` field. ROR otherwise defaults searches to active
records, so scheduled requests explicitly include active, inactive, and
withdrawn statuses. Query and filter modes are available for bounded targeted
lookups; they are intentionally incompatible with each other and with the
scheduled date-window option.

All pages are buffered and validated before stdout is written. If ROR reports a
result set outside the configured page or record ceilings, the run fails with a
visible truncation diagnostic rather than publishing partial data. This client
never requests an unfiltered full-registry export. Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
import re
import sys
import time
from typing import Any, Callable, Sequence
import urllib.error
import urllib.parse
import urllib.request

SOURCE = "ror_organizations"
API_URL = "https://api.ror.org/v2/organizations"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

PAGE_SIZE = 20  # ROR v2's documented fixed page size.
DEFAULT_DAYS_BACK = 2
MAX_DAYS_BACK = 7
DEFAULT_MAX_PAGES = 5
HARD_MAX_PAGES = 20
DEFAULT_MAX_RECORDS = 100
HARD_MAX_RECORDS = PAGE_SIZE * HARD_MAX_PAGES
DEFAULT_MAX_REQUESTS = 15
HARD_MAX_REQUESTS = 80
DEFAULT_TIMEOUT = 20.0
MAX_TIMEOUT = 60.0
DEFAULT_RETRIES = 2
MAX_RETRIES = 3
DEFAULT_RETRY_BUDGET = 120.0
MAX_RETRY_BUDGET = 300.0
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_RATE_LIMIT_DELAY = 60.0
MAX_QUERY_LENGTH = 300
MAX_FILTER_LENGTH = 600
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
ALL_STATUSES_FILTER = "status:active,status:inactive,status:withdrawn"
ROR_ID_RE = re.compile(r"https://ror\.org/[0-9a-z]{9}")


class RorError(RuntimeError):
    """A ROR request or response cannot satisfy the extractor contract."""


class TruncationError(RorError):
    """The configured ceilings would produce an incomplete result set."""


class RetryBudgetError(RorError):
    """A request, retry delay, or attempt would exceed a local run ceiling."""


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _positive_number(value: Any, name: str, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= maximum
    ):
        raise ValueError(f"{name} must be a positive number no greater than {maximum:g}")
    return float(value)


def _bounded_text(value: Any, name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    text = value.strip()
    if len(text) > maximum:
        raise ValueError(f"{name} must be no longer than {maximum} characters")
    if any(ord(character) < 32 or ord(character) == 127 for character in text):
        raise ValueError(f"{name} must not contain control characters")
    return text


def last_modified_window(days_back: int, today: date | None = None) -> tuple[str, str]:
    """Return inclusive UTC date bounds, including today and an overlap day."""
    _bounded_int(days_back, "days_back", 1, MAX_DAYS_BACK)
    end = datetime.now(timezone.utc).date() if today is None else today
    if isinstance(end, datetime) or not isinstance(end, date):
        raise ValueError("today must be a date")
    start = end - timedelta(days=days_back - 1)
    return start.isoformat(), end.isoformat()


def request_parameters(
    *,
    page: int,
    days_back: int | None = None,
    query: str | None = None,
    filter_expression: str | None = None,
    today: date | None = None,
) -> dict[str, str | int]:
    """Build one bounded request in exactly one documented ROR search mode."""
    _bounded_int(page, "page", 1, HARD_MAX_PAGES)
    selected = int(query is not None) + int(filter_expression is not None)
    if selected > 1:
        raise ValueError("query and filter are incompatible request modes")
    if selected and days_back is not None:
        raise ValueError("days_back cannot be combined with query or filter mode")

    parameters: dict[str, str | int] = {"page": page}
    if query is not None:
        parameters["query"] = _bounded_text(query, "query", MAX_QUERY_LENGTH)
    elif filter_expression is not None:
        parameters["filter"] = _bounded_text(
            filter_expression, "filter", MAX_FILTER_LENGTH
        )
    else:
        effective_days = DEFAULT_DAYS_BACK if days_back is None else days_back
        start, end = last_modified_window(effective_days, today)
        parameters["query.advanced"] = (
            f"admin.last_modified.date:[{start} TO {end}]"
        )
        # The ROR API's ordinary search default is active-only. Repeating the
        # status filter is ROR's documented OR syntax for values of one field.
        parameters["filter"] = ALL_STATUSES_FILTER
    return parameters


def normalize_organization(organization: Any, fetched_at: str) -> dict[str, Any]:
    """Validate the stable ROR ID and retain every field returned by ROR v2."""
    if not isinstance(organization, dict):
        raise RorError("ROR items contains a non-object organization")
    ror_id = organization.get("id")
    if not isinstance(ror_id, str) or ROR_ID_RE.fullmatch(ror_id) is None:
        raise RorError("ROR organization has invalid id")
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": ror_id,
        "raw": dict(organization),
    }


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    value = headers.get(name)
    return str(value).strip() if value is not None else None


def _retry_delay(error: urllib.error.HTTPError, now: datetime) -> float | None:
    value = _header(error.headers, "Retry-After")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at.astimezone(timezone.utc) - now).total_seconds())


class RorClient:
    """Small keyless JSON client with run-wide request and retry-time ceilings."""

    def __init__(
        self,
        *,
        timeout: float,
        retries: int,
        retry_budget: float,
        max_requests: int,
        opener: Callable[..., Any] | None = None,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.max_requests = max_requests
        self.opener = opener or urllib.request.urlopen
        self.clock = clock or time.monotonic
        self.sleeper = sleeper or time.sleep
        self.deadline = self.clock() + retry_budget
        self.requests_attempted = 0

    def _remaining(self) -> float:
        return self.deadline - self.clock()

    def get_page(self, parameters: dict[str, str | int]) -> Any:
        url = API_URL + "?" + urllib.parse.urlencode(parameters)
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "identity",
                "User-Agent": USER_AGENT,
            },
        )

        for attempt in range(self.retries + 1):
            remaining = self._remaining()
            if remaining <= 0:
                raise RetryBudgetError("ROR retry budget exhausted before request")
            if self.requests_attempted >= self.max_requests:
                raise RetryBudgetError(
                    f"ROR request ceiling max_requests={self.max_requests} exhausted"
                )
            self.requests_attempted += 1
            try:
                with self.opener(request, timeout=min(self.timeout, remaining)) as response:
                    body = response.read(MAX_RESPONSE_BYTES + 1)
            except urllib.error.HTTPError as exc:
                try:
                    if exc.code not in RETRYABLE_STATUS:
                        raise RorError(f"ROR request failed with HTTP {exc.code}") from exc
                    failure: BaseException = exc
                    delay = _retry_delay(exc, datetime.now(timezone.utc))
                finally:
                    exc.close()
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                failure = exc
                delay = None
            else:
                if len(body) > MAX_RESPONSE_BYTES:
                    raise RorError("ROR response exceeded the byte limit")
                try:
                    return json.loads(body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise RorError("ROR returned malformed JSON") from exc

            if attempt == self.retries:
                raise RorError(
                    f"ROR request failed after {self.retries + 1} attempts: {failure}"
                ) from failure
            if delay is None:
                delay = min(float(2**attempt), MAX_RATE_LIMIT_DELAY)
            elif delay > MAX_RATE_LIMIT_DELAY:
                raise RetryBudgetError(
                    f"ROR rate-limit delay {delay:g}s exceeds the local "
                    f"{MAX_RATE_LIMIT_DELAY:g}s cap"
                ) from failure
            if delay >= self._remaining():
                raise RetryBudgetError(
                    "ROR retry delay exceeds the remaining retry budget"
                ) from failure
            if delay:
                self.sleeper(delay)

        raise AssertionError("unreachable")


def _parse_page(document: Any, page: int) -> tuple[int, list[Any]]:
    if not isinstance(document, dict):
        raise RorError("ROR response must be an object")
    total = document.get("number_of_results")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise RorError("ROR number_of_results must be a non-negative integer")
    items = document.get("items")
    if not isinstance(items, list):
        raise RorError("ROR items must be a list")
    if len(items) > PAGE_SIZE:
        raise RorError(
            f"ROR page {page} returned {len(items)} items above page size {PAGE_SIZE}"
        )
    if total == 0 and items:
        raise RorError("ROR zero-result response contains organizations")
    if total > 0 and not items:
        raise TruncationError(
            f"ROR pagination ended on page {page} before {total} reported records; "
            "refusing truncated output"
        )
    return total, items


def fetch_organizations(
    *,
    days_back: int | None = None,
    query: str | None = None,
    filter_expression: str | None = None,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_budget: float = DEFAULT_RETRY_BUDGET,
    today: date | None = None,
    client: RorClient | None = None,
) -> list[dict[str, Any]]:
    """Return one complete bounded result set, or fail before emitting records."""
    _bounded_int(max_pages, "max_pages", 1, HARD_MAX_PAGES)
    _bounded_int(max_records, "max_records", 1, HARD_MAX_RECORDS)
    _bounded_int(max_requests, "max_requests", 1, HARD_MAX_REQUESTS)
    _bounded_int(retries, "retries", 0, MAX_RETRIES)
    timeout = _positive_number(timeout, "timeout", MAX_TIMEOUT)
    retry_budget = _positive_number(
        retry_budget, "retry_budget", MAX_RETRY_BUDGET
    )
    effective_today = today
    if query is None and filter_expression is None and effective_today is None:
        effective_today = datetime.now(timezone.utc).date()
    # Validate mode and date bounds before constructing a client or touching HTTP.
    request_parameters(
        page=1,
        days_back=days_back,
        query=query,
        filter_expression=filter_expression,
        today=effective_today,
    )

    http = client or RorClient(
        timeout=timeout,
        retries=retries,
        retry_budget=retry_budget,
        max_requests=max_requests,
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    expected_total: int | None = None
    item_count = 0
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()

    for page in range(1, max_pages + 1):
        document = http.get_page(
            request_parameters(
                page=page,
                days_back=days_back,
                query=query,
                filter_expression=filter_expression,
                today=effective_today,
            )
        )
        total, items = _parse_page(document, page)
        if expected_total is None:
            expected_total = total
            if total > max_records:
                raise TruncationError(
                    f"ROR reports {total} records above max_records={max_records}; "
                    "refusing truncated output"
                )
            required_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
            if required_pages > max_pages:
                raise TruncationError(
                    f"ROR reports {total} records requiring {required_pages} pages "
                    f"above max_pages={max_pages}; refusing truncated output"
                )
        elif total != expected_total:
            raise RorError(
                f"ROR number_of_results changed from {expected_total} to {total} "
                "during pagination"
            )

        for organization in items:
            item_count += 1
            if item_count > expected_total:
                raise RorError(
                    f"ROR returned more items than number_of_results={expected_total}"
                )
            record = normalize_organization(organization, fetched_at)
            if record["id"] in seen_ids:
                continue
            seen_ids.add(record["id"])
            records.append(record)

        if item_count == expected_total:
            if len(records) != expected_total:
                raise TruncationError(
                    f"ROR pagination returned {len(records)} unique organizations for "
                    f"{expected_total} reported records; refusing truncated output"
                )
            return records
        if len(items) < PAGE_SIZE:
            raise TruncationError(
                f"ROR page {page} ended after {item_count} of {expected_total} "
                "reported records; refusing truncated output"
            )

    raise TruncationError(
        f"ROR pagination reached max_pages={max_pages} after {item_count} of "
        f"{expected_total} reported records; refusing truncated output"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--query", help="bounded targeted ROR query")
    mode.add_argument("--filter", dest="filter_expression", help="bounded ROR filter")
    parser.add_argument(
        "--days-back",
        type=int,
        default=None,
        help=f"inclusive UTC last-modified window (default: {DEFAULT_DAYS_BACK})",
    )
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument("--max-requests", type=int, default=DEFAULT_MAX_REQUESTS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-budget", type=float, default=DEFAULT_RETRY_BUDGET)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        records = fetch_organizations(
            days_back=args.days_back,
            query=args.query,
            filter_expression=args.filter_expression,
            max_pages=args.max_pages,
            max_records=args.max_records,
            max_requests=args.max_requests,
            timeout=args.timeout,
            retries=args.retries,
            retry_budget=args.retry_budget,
        )
    except ValueError as exc:
        parser.error(str(exc))
    except RorError as exc:
        print(f"{SOURCE}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
