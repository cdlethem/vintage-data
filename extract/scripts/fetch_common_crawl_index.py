#!/usr/bin/env python3
"""Fetch bounded metadata records from one Common Crawl index collection.

The extractor discovers the configured collection from Common Crawl's public
``collinfo.json`` document, then queries only that collection for one explicit,
narrow URL pattern. It emits CDX metadata; it never requests WARC files or
archived response bodies. All response bytes, retries, pages, records, and wall
clock duration share run-wide ceilings.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import http.client
import hashlib
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

SOURCE = "common_crawl_index"
OFFICIAL_HOST = "index.commoncrawl.org"
DISCOVERY_URL = f"https://{OFFICIAL_HOST}/collinfo.json"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

DEFAULT_PAGE_SIZE = 1
MAX_PAGE_SIZE = 5
DEFAULT_MAX_PAGES = 2
HARD_MAX_PAGES = 10
DEFAULT_MAX_RECORDS = 500
HARD_MAX_RECORDS = 10_000
DEFAULT_MAX_BYTES = 8 * 1024 * 1024
HARD_MAX_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_DURATION = 120.0
HARD_MAX_DURATION = 300.0
DEFAULT_TIMEOUT = 10.0
MAX_TIMEOUT = 30.0
DEFAULT_RETRIES = 2
MAX_RETRIES = 3
MAX_RETRY_DELAY = 10.0
READ_CHUNK = 64 * 1024
MAX_PATTERN_LENGTH = 2048
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
REQUIRED_FIELDS = ("urlkey", "timestamp", "url", "mime", "status", "digest", "length")
COLLECTION_RE = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}\Z")
LAST_RUN_METADATA: dict[str, Any] = {}


class CommonCrawlError(RuntimeError):
    """The provider request or response could not satisfy the source contract."""


class BudgetExceeded(CommonCrawlError):
    """A run-wide hard ceiling prevented a required operation."""


class StrictRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Allow only same-endpoint redirects on the official HTTPS host."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        original = urllib.parse.urlsplit(req.full_url)
        target = urllib.parse.urlsplit(newurl)
        _validate_official_url(newurl)
        if target.path != original.path or target.query != original.query:
            raise CommonCrawlError("Common Crawl redirect changed the requested endpoint")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_SAFE_OPENER = urllib.request.build_opener(StrictRedirectHandler())


def _open_official(request: urllib.request.Request, timeout: float):
    return _SAFE_OPENER.open(request, timeout=timeout)


def _bounded_int(value: Any, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def _bounded_number(value: Any, name: str, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or not 0 < value <= maximum
    ):
        raise ValueError(f"{name} must be a positive number no greater than {maximum:g}")
    return float(value)


def _bounded_retries(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_RETRIES:
        raise ValueError(f"retries must be an integer between 0 and {MAX_RETRIES}")
    return value


def validate_collection(collection: Any) -> str:
    if not isinstance(collection, str) or COLLECTION_RE.fullmatch(collection) is None:
        raise ValueError("collection must match CC-MAIN-YYYY-NN")
    return collection


def validate_url_pattern(pattern: Any) -> str:
    """Require one fixed HTTP(S) host and either an exact URL or narrow path prefix."""
    if not isinstance(pattern, str) or not pattern or pattern != pattern.strip():
        raise ValueError("url_pattern must be a non-empty string without surrounding whitespace")
    if len(pattern) > MAX_PATTERN_LENGTH:
        raise ValueError(f"url_pattern exceeds {MAX_PATTERN_LENGTH} characters")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in pattern):
        raise ValueError("url_pattern contains control characters")
    try:
        parsed = urllib.parse.urlsplit(pattern)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("url_pattern is not a valid URL") from exc
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("url_pattern must use http or https")
    if not parsed.hostname or parsed.username is not None or parsed.password is not None or port is not None:
        raise ValueError("url_pattern must contain one fixed host without credentials or a port")
    if parsed.fragment:
        raise ValueError("url_pattern must not contain a fragment")
    if "*" in parsed.netloc or "*" in parsed.query:
        raise ValueError("url_pattern must not broaden the host or query")
    wildcard_count = parsed.path.count("*")
    if wildcard_count:
        if wildcard_count != 1 or not parsed.path.endswith("/*"):
            raise ValueError("url_pattern supports only one trailing path wildcard")
        if not parsed.path[:-2].strip("/"):
            raise ValueError("url_pattern wildcard must be below a named path prefix")
    return pattern


def _validate_official_url(url: str) -> urllib.parse.SplitResult:
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise CommonCrawlError("Common Crawl endpoint is invalid") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname != OFFICIAL_HOST
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise CommonCrawlError("Common Crawl endpoint must use the official HTTPS index host")
    return parsed


def _validate_discovered_endpoint(url: Any, collection: str) -> str:
    if not isinstance(url, str):
        raise CommonCrawlError(f"collection {collection} has no valid cdx-api endpoint")
    parsed = _validate_official_url(url)
    if parsed.path != f"/{collection}-index" or parsed.query:
        raise CommonCrawlError(f"collection {collection} advertised an unexpected cdx-api endpoint")
    return url


@dataclass
class RunBudget:
    max_bytes: int
    max_duration: float
    clock: Callable[[], float]

    def __post_init__(self) -> None:
        self.started = self.clock()
        self.deadline = self.started + self.max_duration
        self.bytes_streamed = 0

    def remaining_time(self) -> float:
        return self.deadline - self.clock()

    def require_time(self, context: str) -> float:
        remaining = self.remaining_time()
        if remaining <= 0:
            raise BudgetExceeded(f"total duration ceiling reached during {context}")
        return remaining

    def remaining_bytes(self) -> int:
        return self.max_bytes - self.bytes_streamed


class HttpClient:
    """Official-host-only HTTP client with bounded retries and shared budgets."""

    def __init__(
        self,
        *,
        timeout: float,
        retries: int,
        budget: RunBudget,
        opener: Callable[[urllib.request.Request, float], Any] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.budget = budget
        self.opener = opener or _open_official
        self.sleeper = sleeper or time.sleep
        self.metrics: dict[str, Any] = {
            "attempted": 0,
            "succeeded": 0,
            "failed": 0,
            "retries": 0,
            "payload_bytes": 0,
            "backoff_s": 0.0,
        }

    def counters(self) -> dict[str, Any]:
        return self.metrics

    def _retry_delay(self, error: urllib.error.HTTPError, attempt: int) -> float:
        value = error.headers.get("Retry-After") if error.headers is not None else None
        if value:
            try:
                delay = float(value)
            except ValueError:
                try:
                    instant = parsedate_to_datetime(value)
                except (TypeError, ValueError, OverflowError):
                    delay = float(2**attempt)
                else:
                    if instant.tzinfo is None:
                        instant = instant.replace(tzinfo=timezone.utc)
                    delay = max(0.0, (instant.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())
        else:
            delay = float(2**attempt)
        if not math.isfinite(delay) or delay < 0 or delay > MAX_RETRY_DELAY:
            raise CommonCrawlError(
                f"Common Crawl retry delay exceeds the local {MAX_RETRY_DELAY:g}s cap"
            ) from error
        return delay

    def _read_response(self, response: Any, context: str, allow_truncate: bool) -> tuple[bytes, str | None]:
        headers = getattr(response, "headers", None)
        declared: int | None = None
        if headers is not None and headers.get("Content-Length") is not None:
            try:
                declared = int(headers.get("Content-Length"))
            except (TypeError, ValueError) as exc:
                raise CommonCrawlError(f"{context} response has invalid Content-Length") from exc
            if declared < 0:
                raise CommonCrawlError(f"{context} response has invalid Content-Length")

        remaining = self.budget.remaining_bytes()
        if remaining <= 0:
            if allow_truncate:
                return b"", "max_bytes"
            raise BudgetExceeded(f"byte ceiling reached during {context}")
        if declared is not None and declared > remaining and not allow_truncate:
            raise BudgetExceeded(f"byte ceiling would be exceeded by {context}")

        target = remaining if declared is None else min(remaining, declared)
        body = bytearray()
        while len(body) < target:
            self.budget.require_time(context)
            chunk = response.read(min(READ_CHUNK, target - len(body)))
            if not chunk:
                break
            if not isinstance(chunk, bytes):
                raise CommonCrawlError(f"{context} response returned non-byte content")
            body.extend(chunk)
            self.budget.bytes_streamed += len(chunk)
            self.metrics["payload_bytes"] = self.budget.bytes_streamed
            if self.budget.remaining_time() <= 0:
                if allow_truncate:
                    return bytes(body), "max_duration"
                raise BudgetExceeded(f"total duration ceiling reached during {context}")

        if declared is not None and len(body) < declared:
            if len(body) == remaining and allow_truncate:
                return bytes(body), "max_bytes"
            raise CommonCrawlError(f"{context} response ended before Content-Length")
        if declared is None and len(body) == remaining:
            if allow_truncate:
                return bytes(body), "max_bytes"
            raise BudgetExceeded(f"byte ceiling reached during {context}")
        return bytes(body), None

    def get(self, url: str, *, context: str, allow_truncate: bool = False) -> tuple[bytes, str | None]:
        _validate_official_url(url)
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        for attempt in range(self.retries + 1):
            remaining = self.budget.require_time(context)
            self.metrics["attempted"] += 1
            try:
                with self.opener(request, min(self.timeout, remaining)) as response:
                    final_url = response.geturl() if hasattr(response, "geturl") else url
                    original = urllib.parse.urlsplit(url)
                    final = _validate_official_url(final_url)
                    if final.path != original.path or final.query != original.query:
                        raise CommonCrawlError("Common Crawl response changed the requested endpoint")
                    status = getattr(response, "status", 200)
                    if not isinstance(status, int) or not 200 <= status < 300:
                        raise CommonCrawlError(f"{context} returned HTTP {status!r}")
                    body, truncation = self._read_response(response, context, allow_truncate)
            except urllib.error.HTTPError as exc:
                self.metrics["failed"] += 1
                try:
                    retryable = exc.code in RETRYABLE_STATUS
                    delay = self._retry_delay(exc, attempt) if retryable else 0.0
                finally:
                    exc.close()
                failure: BaseException = exc
                if not retryable:
                    raise CommonCrawlError(f"{context} failed with HTTP {exc.code}") from exc
            except (urllib.error.URLError, TimeoutError, OSError, http.client.HTTPException) as exc:
                self.metrics["failed"] += 1
                delay = float(2**attempt)
                failure = exc
            except CommonCrawlError:
                self.metrics["failed"] += 1
                raise
            else:
                self.metrics["succeeded"] += 1
                return body, truncation

            if attempt == self.retries:
                raise CommonCrawlError(
                    f"{context} failed after {self.retries + 1} attempts: {failure}"
                ) from failure
            remaining = self.budget.require_time(f"retry for {context}")
            if delay > MAX_RETRY_DELAY or delay >= remaining:
                raise BudgetExceeded(f"retry delay cannot fit total duration ceiling during {context}") from failure
            self.metrics["retries"] += 1
            self.metrics["backoff_s"] += delay
            if delay:
                self.sleeper(delay)
        raise AssertionError("unreachable")


def discover_collection(collection: str, client: HttpClient) -> tuple[str, int]:
    body, truncation = client.get(DISCOVERY_URL, context="collection discovery")
    if truncation is not None:
        raise BudgetExceeded(f"collection discovery was truncated by {truncation}")
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CommonCrawlError("collection discovery returned malformed UTF-8 JSON") from exc
    client.budget.require_time("collection discovery parsing")
    if not isinstance(document, list):
        raise CommonCrawlError("collection discovery response must be a JSON array")
    matches: list[str] = []
    for entry in document:
        client.budget.require_time("collection discovery parsing")
        if not isinstance(entry, dict):
            raise CommonCrawlError("collection discovery contains a non-object entry")
        if entry.get("id") == collection:
            matches.append(_validate_discovered_endpoint(entry.get("cdx-api"), collection))
    if len(matches) != 1:
        raise CommonCrawlError(
            f"configured collection {collection} was discovered {len(matches)} times; expected exactly once"
        )
    return matches[0], len(document)

def discover_page_count(
    index_url: str, url_pattern: str, page_size: int, client: HttpClient
) -> int:
    parameters = urllib.parse.urlencode(
        {
            "url": url_pattern,
            "output": "json",
            "pageSize": page_size,
            "showNumPages": "true",
        }
    )
    body, truncation = client.get(
        f"{index_url}?{parameters}", context="index page discovery"
    )
    if truncation is not None:
        raise BudgetExceeded(f"index page discovery was truncated by {truncation}")
    try:
        document = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CommonCrawlError("index page discovery returned malformed UTF-8 JSON") from exc
    client.budget.require_time("index page discovery parsing")
    if not isinstance(document, dict):
        raise CommonCrawlError("index page discovery response must be a JSON object")
    pages = document.get("pages")
    if isinstance(pages, bool) or not isinstance(pages, int) or pages < 0:
        raise CommonCrawlError("index page discovery response has invalid pages")
    return pages


def _required_text(row: dict[str, Any], field: str) -> str:
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(field)
    return value.strip()


def _normalize_record(
    row: dict[str, Any], *, collection: str, pattern: str, index_url: str,
    page: int, fetched_at: str,
) -> dict[str, Any]:
    values = {field: _required_text(row, field) for field in REQUIRED_FIELDS}
    timestamp = values["timestamp"]
    if re.fullmatch(r"[0-9]{14}", timestamp) is None:
        raise ValueError("timestamp")
    try:
        datetime.strptime(timestamp, "%Y%m%d%H%M%S")
    except ValueError as exc:
        raise ValueError("timestamp") from exc
    parsed_url = urllib.parse.urlsplit(values["url"])
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.hostname:
        raise ValueError("url")
    try:
        content_length = int(values["length"])
    except ValueError as exc:
        raise ValueError("length") from exc
    if content_length < 0:
        raise ValueError("length")
    identity = json.dumps(
        {
            "collection": collection,
            "urlkey": values["urlkey"],
            "timestamp": timestamp,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": hashlib.sha256(identity).hexdigest(),
        "collection_id": collection,
        "query_url_pattern": pattern,
        "index_url": index_url,
        "query_page": page,
        "urlkey": values["urlkey"],
        "url": values["url"],
        "crawl_timestamp": timestamp,
        "mime_type": values["mime"],
        "status_code": values["status"],
        "digest": values["digest"],
        "content_length": content_length,
    }


def _decode_page(
    body: bytes, *, complete: bool, budget: RunBudget
) -> tuple[list[dict[str, Any]], int, int, int, bool]:
    """Return valid rows and bounded parse counters for one response body."""
    lines = body.splitlines()
    if body and not complete and not body.endswith((b"\n", b"\r")):
        lines = lines[:-1]
    objects: list[dict[str, Any]] = []
    malformed = 0
    missing = 0
    wire_rows = 0
    for raw_line in lines:
        if budget.remaining_time() <= 0:
            return objects, malformed, missing, wire_rows, True
        if not raw_line.strip():
            continue
        wire_rows += 1
        try:
            row = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            malformed += 1
            continue
        if not isinstance(row, dict):
            malformed += 1
            continue
        if any(field not in row for field in REQUIRED_FIELDS):
            missing += 1
            continue
        objects.append(row)
    return objects, malformed, missing, wire_rows, False


def fetch_common_crawl_index(
    *,
    collection: str,
    url_pattern: str,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    max_records: int = DEFAULT_MAX_RECORDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    max_duration: float = DEFAULT_MAX_DURATION,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    clock: Callable[[], float] | None = None,
    sleeper: Callable[[float], None] | None = None,
    opener: Callable[[urllib.request.Request, float], Any] | None = None,
) -> list[dict[str, Any]]:
    """Return one validated, bounded metadata sample from one discovered collection."""
    LAST_RUN_METADATA.clear()
    LAST_RUN_METADATA["requests"] = {"attempted": 0}
    collection = validate_collection(collection)
    url_pattern = validate_url_pattern(url_pattern)
    page_size = _bounded_int(page_size, "page_size", MAX_PAGE_SIZE)
    max_pages = _bounded_int(max_pages, "max_pages", HARD_MAX_PAGES)
    max_records = _bounded_int(max_records, "max_records", HARD_MAX_RECORDS)
    max_bytes = _bounded_int(max_bytes, "max_bytes", HARD_MAX_BYTES)
    max_duration = _bounded_number(max_duration, "max_duration", HARD_MAX_DURATION)
    timeout = _bounded_number(timeout, "timeout", MAX_TIMEOUT)
    retries = _bounded_retries(retries)

    active_clock = clock or time.monotonic
    budget = RunBudget(max_bytes=max_bytes, max_duration=max_duration, clock=active_clock)
    client = HttpClient(
        timeout=timeout,
        retries=retries,
        budget=budget,
        opener=opener,
        sleeper=sleeper,
    )
    LAST_RUN_METADATA.update(
        {
            "collection_id": collection,
            "url_pattern": url_pattern,
            "pages": 0,
            "records": 0,
            "skipped_records": 0,
            "malformed_lines": 0,
            "missing_field_records": 0,
            "duplicate_records": 0,
            "truncated": False,
            "truncation_reason": None,
            "requests": client.counters(),
            "max_pages": max_pages,
            "max_records": max_records,
            "max_bytes": max_bytes,
            "max_duration_seconds": max_duration,
        }
    )

    index_url, discovery_count = discover_collection(collection, client)
    available_pages = discover_page_count(index_url, url_pattern, page_size, client)
    LAST_RUN_METADATA.update(
        {
            "index_url": index_url,
            "discovered_collections": discovery_count,
            "available_pages": available_pages,
        }
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    pages_to_fetch = min(available_pages, max_pages)
    record_ceiling_reached = False

    for page in range(pages_to_fetch):
        if budget.remaining_time() <= 0:
            LAST_RUN_METADATA.update(truncated=True, truncation_reason="max_duration")
            break
        parameters = urllib.parse.urlencode(
            {
                "url": url_pattern,
                "output": "json",
                "pageSize": page_size,
                "page": page,
            }
        )
        body, truncation = client.get(
            f"{index_url}?{parameters}",
            context=f"index page {page}",
            allow_truncate=True,
        )
        LAST_RUN_METADATA["pages"] += 1
        objects, malformed, missing, _, parse_timed_out = _decode_page(
            body, complete=truncation is None, budget=budget
        )
        LAST_RUN_METADATA["malformed_lines"] += malformed
        LAST_RUN_METADATA["missing_field_records"] += missing
        LAST_RUN_METADATA["skipped_records"] += malformed + missing
        if parse_timed_out:
            LAST_RUN_METADATA.update(truncated=True, truncation_reason="max_duration")

        for row_number, row in enumerate(objects):
            if budget.remaining_time() <= 0:
                LAST_RUN_METADATA.update(truncated=True, truncation_reason="max_duration")
                break
            try:
                record = _normalize_record(
                    row,
                    collection=collection,
                    pattern=url_pattern,
                    index_url=index_url,
                    page=page,
                    fetched_at=fetched_at,
                )
            except (TypeError, ValueError):
                LAST_RUN_METADATA["missing_field_records"] += 1
                LAST_RUN_METADATA["skipped_records"] += 1
                continue
            if record["id"] in seen_ids:
                LAST_RUN_METADATA["duplicate_records"] += 1
                LAST_RUN_METADATA["skipped_records"] += 1
                continue
            seen_ids.add(record["id"])
            records.append(record)
            if len(records) == max_records:
                record_ceiling_reached = True
                if row_number + 1 < len(objects) or page + 1 < available_pages:
                    LAST_RUN_METADATA.update(truncated=True, truncation_reason="max_records")
                break

        if record_ceiling_reached or LAST_RUN_METADATA["truncated"]:
            break
        if truncation is not None:
            LAST_RUN_METADATA.update(truncated=True, truncation_reason=truncation)
            break

    if not LAST_RUN_METADATA["truncated"] and available_pages > max_pages:
        LAST_RUN_METADATA.update(truncated=True, truncation_reason="max_pages")
    LAST_RUN_METADATA.update(
        {
            "records": len(records),
            "bytes_streamed": budget.bytes_streamed,
            "duration_seconds": max(0.0, active_clock() - budget.started),
            "requests": client.counters(),
        }
    )
    return records


def _summary(*, failed: bool = False, error: str | None = None) -> dict[str, Any]:
    skipped = int(LAST_RUN_METADATA.get("skipped_records", 0))
    truncated = bool(LAST_RUN_METADATA.get("truncated", False))
    summary: dict[str, Any] = {
        "health": "failed" if failed else ("degraded" if skipped or truncated else "healthy"),
        "completeness": "failed" if failed else ("partial" if skipped or truncated else "complete"),
        "records": 0 if failed else int(LAST_RUN_METADATA.get("records", 0)),
        "requests": dict(LAST_RUN_METADATA.get("requests", {"attempted": 0})),
        "metrics": {
            key: value
            for key, value in LAST_RUN_METADATA.items()
            if key not in {"records", "requests"}
        },
    }
    if error is not None:
        summary["error"] = error
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--url-pattern", required=True)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS)
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--max-duration", type=float, default=DEFAULT_MAX_DURATION)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        records = fetch_common_crawl_index(
            collection=args.collection,
            url_pattern=args.url_pattern,
            page_size=args.page_size,
            max_pages=args.max_pages,
            max_records=args.max_records,
            max_bytes=args.max_bytes,
            max_duration=args.max_duration,
            timeout=args.timeout,
            retries=args.retries,
        )
    except Exception as exc:  # the CLI converts all provider/config failures to failed evidence
        print(SUMMARY_PREFIX + json.dumps(_summary(failed=True, error=str(exc))), file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    print(SUMMARY_PREFIX + json.dumps(_summary()), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
