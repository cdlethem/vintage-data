#!/usr/bin/env python3
"""Fetch a bounded, complete PBDB occurrence modification window as NDJSON.

The Paleobiology Database occurrence-list endpoint is offset-paged.  Every run fixes
both ends of one UTC modification window before making a request and includes a
48-hour overlap for late indexing and boundary changes.  Stable occurrence identifiers
make that overlap safe for downstream upserts.  Pages are buffered until counts,
ordering, uniqueness, and configured bounds prove the result complete, so a failed run
never emits a knowingly partial window.

Only the public, keyless JSON API is used.  Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import re
import sys
from typing import Any, Sequence
import urllib.error
import urllib.parse
import urllib.request


SOURCE = "paleobiology_database"
API_URL = "https://paleobiodb.org/data1.2/occs/list.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SHOW_FIELDS = "ident,class,coords,loc,paleoloc,strat,env,ref,crmod"
OVERLAP_HOURS = 48
DEFAULT_LOOKBACK_HOURS = 24
MAX_LOOKBACK_HOURS = 31 * 24
DEFAULT_PAGE_SIZE = 500
MAX_PAGE_SIZE = 5000
DEFAULT_MAX_RECORDS = 10_000
HARD_MAX_RECORDS = 100_000
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 120
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
DIAGNOSTIC_BODY_BYTES = 4096
DIAGNOSTIC_CHARS = 400
_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_SECRET_RE = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|token|password|passwd|secret|authorization)"
    r"(\s*[:=]\s*)([^\s,;&]+)"
)
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[^\s,;]+")


class ExtractorError(RuntimeError):
    """A categorized failure safe for a bounded command-line diagnostic."""

    def __init__(self, category: str, detail: str, *, status: int | None = None) -> None:
        super().__init__(detail)
        self.category = category
        self.detail = detail
        self.status = status


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ExtractorError(
            "request_validation",
            f"{name} must be an integer between {minimum} and {maximum}",
        )
    return value


def _sanitize_diagnostic(value: Any) -> str:
    """Remove URLs, credential-shaped values, controls, and excess diagnostic text."""
    text = str(value)
    text = _URL_RE.sub("[redacted-url]", text)
    text = _BEARER_RE.sub("Bearer [redacted]", text)
    text = _SECRET_RE.sub(lambda match: match.group(1) + match.group(2) + "[redacted]", text)
    text = " ".join(text.split())
    if len(text) > DIAGNOSTIC_CHARS:
        text = text[: DIAGNOSTIC_CHARS - 3] + "..."
    return text


def _message_excerpt(value: Any) -> str:
    if isinstance(value, str):
        return value[:1000]
    if isinstance(value, list):
        parts = []
        for item in value[:3]:
            parts.append(item[:300] if isinstance(item, str) else f"<{type(item).__name__}>")
        return "; ".join(parts)
    if isinstance(value, dict):
        return "object keys: " + ", ".join(str(key)[:80] for key in list(value)[:10])
    return f"<{type(value).__name__}>"


def _format_window_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def modification_window(run_start: datetime, lookback_hours: int) -> tuple[datetime, datetime]:
    _bounded_int(lookback_hours, "lookback_hours", 1, MAX_LOOKBACK_HOURS)
    if not isinstance(run_start, datetime) or run_start.tzinfo is None:
        raise ExtractorError("request_validation", "run_start must be timezone-aware")
    end = run_start.astimezone(timezone.utc).replace(microsecond=0)
    start = end - timedelta(hours=lookback_hours + OVERLAP_HOURS)
    return start, end


def _request_url(
    api_url: str,
    start: datetime,
    end: datetime,
    page_size: int,
    offset: int,
) -> str:
    query = urllib.parse.urlencode(
        {
            "all_records": 1,
            "modified_after": _format_window_time(start),
            "modified_before": _format_window_time(end),
            "vocab": "pbdb",
            "show": SHOW_FIELDS,
            "order": "occurrence_no",
            "limit": page_size,
            "offset": offset,
        }
    )
    return f"{api_url}?{query}"


def _read_json(url: str, timeout: int, opener: Any) -> Any:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with opener(request, timeout=timeout) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as error:
        body = error.read(DIAGNOSTIC_BODY_BYTES + 1)
        detail = body.decode("utf-8", errors="replace")
        raise ExtractorError("http_status", detail or error.reason, status=error.code) from error
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        raise ExtractorError("transport", str(error.reason if isinstance(error, urllib.error.URLError) else error)) from error

    if len(body) > MAX_RESPONSE_BYTES:
        raise ExtractorError(
            "safety_cap", f"response body exceeded {MAX_RESPONSE_BYTES} bytes"
        )
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        excerpt = body[:DIAGNOSTIC_BODY_BYTES].decode("utf-8", errors="replace")
        raise ExtractorError("malformed_json", excerpt) from error


def _nonnegative_int(document: dict[str, Any], field: str) -> int:
    value = document.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ExtractorError(
            "malformed_document", f"{field} must be a non-negative integer"
        )
    return value


def _parse_page(document: Any) -> tuple[list[Any], int, int]:
    if not isinstance(document, dict):
        raise ExtractorError("malformed_document", "response must be a JSON object")

    for field, category in (("errors", "api_error"), ("warnings", "api_warning")):
        if field in document:
            messages = document[field]
            if messages not in (None, [], ""):
                raise ExtractorError(category, _message_excerpt(messages))

    records = document.get("records")
    if not isinstance(records, list):
        raise ExtractorError("malformed_document", "records must be a list")
    found = _nonnegative_int(document, "records_found")
    returned = _nonnegative_int(document, "records_returned")
    if returned != len(records):
        raise ExtractorError(
            "row_count",
            f"records_returned={returned} but page contains {len(records)} records",
        )
    return records, found, returned


def _occurrence_number(row: Any) -> int:
    if not isinstance(row, dict):
        raise ExtractorError("malformed_document", "occurrence record must be an object")
    value = row.get("occurrence_no")
    if isinstance(value, bool):
        value = None
    if isinstance(value, int) and value > 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdigit() and value == str(int(value)) and int(value) > 0:
        return int(value)
    raise ExtractorError(
        "missing_identifier", "occurrence record has no positive canonical occurrence_no"
    )


def fetch_occurrences(
    *,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_records: int = DEFAULT_MAX_RECORDS,
    timeout: int = DEFAULT_TIMEOUT,
    run_start: datetime | None = None,
    api_url: str = API_URL,
    opener: Any | None = None,
) -> list[dict[str, Any]]:
    """Return one complete, validated modification window or raise without emitting."""
    _bounded_int(page_size, "page_size", 1, MAX_PAGE_SIZE)
    _bounded_int(max_records, "max_records", 1, HARD_MAX_RECORDS)
    _bounded_int(timeout, "timeout", 1, MAX_TIMEOUT)
    if not isinstance(api_url, str) or not api_url:
        raise ExtractorError("request_validation", "api_url must be a non-empty string")

    fixed_start = datetime.now(timezone.utc) if run_start is None else run_start
    window_start, window_end = modification_window(fixed_start, lookback_hours)
    fetched_at = window_end.isoformat()
    lineage = {
        "modified_after": _format_window_time(window_start),
        "modified_before": _format_window_time(window_end),
        "lookback_hours": lookback_hours,
        "overlap_hours": OVERLAP_HOURS,
        "vocab": "pbdb",
        "order": "occurrence_no",
    }
    open_request = urllib.request.urlopen if opener is None else opener
    output: list[dict[str, Any]] = []
    seen: set[int] = set()
    expected_found: int | None = None
    offset = 0
    previous_occurrence_no = 0

    while True:
        document = _read_json(
            _request_url(api_url, window_start, window_end, page_size, offset),
            timeout,
            open_request,
        )
        rows, found, returned = _parse_page(document)

        if expected_found is None:
            expected_found = found
            if found > max_records:
                raise ExtractorError(
                    "safety_cap",
                    f"records_found={found} exceeds max_records={max_records}",
                )
        elif found != expected_found:
            raise ExtractorError(
                "pagination",
                f"records_found changed from {expected_found} to {found}",
            )

        if offset + returned > expected_found:
            raise ExtractorError(
                "row_count", "page rows exceed the declared records_found count"
            )

        for row in rows:
            occurrence_no = _occurrence_number(row)
            if occurrence_no in seen:
                raise ExtractorError(
                    "pagination", f"duplicate occurrence_no {occurrence_no}"
                )
            if occurrence_no <= previous_occurrence_no:
                raise ExtractorError(
                    "pagination", "occurrence records are not in occurrence_no order"
                )
            seen.add(occurrence_no)
            previous_occurrence_no = occurrence_no
            output.append(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": str(occurrence_no),
                    "occurrence_no": occurrence_no,
                    "query_window": dict(lineage),
                    "record": dict(row),
                }
            )

        offset += returned
        if offset == expected_found:
            return output
        if returned == 0:
            raise ExtractorError(
                "pagination", "empty page returned before records_found was reached"
            )
        if returned < page_size:
            raise ExtractorError(
                "pagination", "short page returned before records_found was reached"
            )
        if offset >= max_records:
            raise ExtractorError(
                "safety_cap", f"pagination exhausted max_records={max_records}"
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch a bounded PBDB occurrence modification window."
    )
    parser.add_argument("--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--api-url", default=API_URL, help=argparse.SUPPRESS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        records = fetch_occurrences(
            lookback_hours=args.lookback_hours,
            page_size=args.page_size,
            max_records=args.max_records,
            timeout=args.timeout,
            api_url=args.api_url,
        )
        for record in records:
            print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
        return 0
    except ExtractorError as error:
        fields = [f"category={error.category}"]
        if error.status is not None:
            fields.append(f"status={error.status}")
        fields.append(f"diagnostic={_sanitize_diagnostic(error.detail)!r}")
        print(f"{SOURCE}: " + " ".join(fields), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
