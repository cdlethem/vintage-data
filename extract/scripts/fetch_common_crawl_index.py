#!/usr/bin/env python3
"""Fetch bounded Common Crawl index metadata as deterministic NDJSON.

The public ``collinfo.json`` document describes the available crawl indexes.
This extractor makes one request, limits the response body and entry count,
validates the complete document before emitting records, and orders records by
Common Crawl's stable collection ID. It does not query the CDX indexes or fetch
crawl content.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import http.client
import json
import os
import re
import socket
import sys
from typing import Any, Sequence
import urllib.error
import urllib.parse
import urllib.request

SOURCE = "common_crawl_index"
URL = "https://index.commoncrawl.org/collinfo.json"
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
MAX_INDEXES = 1000
MAX_DIAGNOSTIC_CHARS = 500
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
_CREDENTIALS_IN_URL = re.compile(r"(?i)\b(https?://)[^/\s:@]+(?::[^/\s@]*)?@")
_SECRET_VALUE = re.compile(
    r"(?i)\b(authorization|proxy-authorization|api[-_]?key|access[-_]?token|"
    r"token|password|secret)\s*([:=])\s*(?:bearer\s+|basic\s+)?[^\s&]+"
)


class CommonCrawlIndexError(RuntimeError):
    """The metadata request or response could not satisfy the extract contract."""


def _safe_diagnostic(detail: object) -> str:
    text = " ".join(
        "".join(character if character.isprintable() else " " for character in str(detail)).split()
    )
    text = _CREDENTIALS_IN_URL.sub(r"\1[redacted]@", text)
    text = _SECRET_VALUE.sub(r"\1\2[redacted]", text)
    if not text:
        text = type(detail).__name__
    if len(text) > MAX_DIAGNOSTIC_CHARS:
        text = f"{text[: MAX_DIAGNOSTIC_CHARS - 3]}..."
    return text


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        self.exit(2, f"{SOURCE}: argument error: {_safe_diagnostic(message)}\n")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required_text(row: dict[str, Any], field: str, index: int) -> str:
    value = row.get(field)
    if (
        not isinstance(value, str)
        or not value
        or value.strip() != value
        or len(value) > 1000
    ):
        raise CommonCrawlIndexError(
            f"malformed metadata entry {index}: {field} must be a nonempty string"
        )
    return value


def _required_https_url(row: dict[str, Any], field: str, index: int) -> str:
    value = _required_text(row, field, index)
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username is not None:
        raise CommonCrawlIndexError(
            f"malformed metadata entry {index}: {field} must be a public HTTPS URL"
        )
    return value

def _validate_unicode_scalars(value: Any) -> None:
    """Reject strings that cannot be represented as Unicode scalar values."""
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, str):
            if any(
                0xD800 <= ord(character) <= 0xDFFF or ord(character) > 0x10FFFF
                for character in current
            ):
                raise CommonCrawlIndexError(
                    "Common Crawl returned a non-Unicode-scalar string"
                )
        elif isinstance(current, list):
            pending.extend(current)
        elif isinstance(current, dict):
            pending.extend(current.keys())
            pending.extend(current.values())




def parse_response(document: Any, fetched_at: str) -> list[dict[str, Any]]:
    """Validate and deterministically normalize a complete collinfo document."""
    if not isinstance(document, list):
        raise CommonCrawlIndexError("malformed metadata response: expected an array")
    if not document:
        raise CommonCrawlIndexError("malformed metadata response: index list is empty")
    if len(document) > MAX_INDEXES:
        raise CommonCrawlIndexError(
            f"metadata response exceeds the {MAX_INDEXES}-entry safety limit"
        )

    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for index, row in enumerate(document):
        if not isinstance(row, dict):
            raise CommonCrawlIndexError(
                f"malformed metadata entry {index}: expected an object"
            )
        collection_id = _required_text(row, "id", index)
        if collection_id in seen_ids:
            raise CommonCrawlIndexError(
                f"malformed metadata entry {index}: duplicate collection id"
            )
        seen_ids.add(collection_id)
        name = _required_text(row, "name", index)
        timegate = _required_https_url(row, "timegate", index)
        cdx_api = _required_https_url(row, "cdx-api", index)
        cdx_toolkit = row.get("cdx-toolkit")
        if cdx_toolkit is not None:
            cdx_toolkit = _required_https_url(row, "cdx-toolkit", index)

        records.append(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": collection_id,
                "name": name,
                "timegate": timegate,
                "cdx_api": cdx_api,
                "cdx_toolkit": cdx_toolkit,
                "raw": row,
            }
        )

    records.sort(key=lambda record: record["id"])
    return records


def _read_document(response: Any) -> Any:
    status = getattr(response, "status", None)
    if status is None and hasattr(response, "getcode"):
        status = response.getcode()
    if status is not None and not 200 <= status < 300:
        raise CommonCrawlIndexError(f"Common Crawl returned HTTP {status}")

    headers = getattr(response, "headers", None)
    declared_length = headers.get("Content-Length") if headers is not None else None
    if declared_length is not None:
        try:
            expected_bytes = int(declared_length)
        except (TypeError, ValueError) as error:
            raise CommonCrawlIndexError("Common Crawl returned invalid Content-Length") from error
        if expected_bytes < 0:
            raise CommonCrawlIndexError("Common Crawl returned invalid Content-Length")
        if expected_bytes > MAX_RESPONSE_BYTES:
            raise CommonCrawlIndexError(
                f"Common Crawl response exceeds the {MAX_RESPONSE_BYTES}-byte safety limit"
            )
    else:
        expected_bytes = None

    body = response.read(MAX_RESPONSE_BYTES + 1)
    if not isinstance(body, bytes):
        raise CommonCrawlIndexError("Common Crawl returned an invalid response body")
    if len(body) > MAX_RESPONSE_BYTES:
        raise CommonCrawlIndexError(
            f"Common Crawl response exceeds the {MAX_RESPONSE_BYTES}-byte safety limit"
        )
    if expected_bytes is not None and len(body) != expected_bytes:
        raise CommonCrawlIndexError("Common Crawl returned an incomplete response")

    def reject_nonstandard_number(value: str) -> None:
        raise ValueError(f"non-standard JSON number {value}")

    try:
        document = json.loads(body.decode("utf-8"), parse_constant=reject_nonstandard_number)
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise CommonCrawlIndexError("Common Crawl returned malformed JSON") from error

    _validate_unicode_scalars(document)
    return document


def fetch_index_metadata(timeout: int = DEFAULT_TIMEOUT) -> list[dict[str, Any]]:
    """Make one bounded metadata request and return fully validated records."""
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= MAX_TIMEOUT:
        raise ValueError(f"timeout must be an integer from 1 to {MAX_TIMEOUT} seconds")

    request = urllib.request.Request(
        URL,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = _read_document(response)
    except urllib.error.HTTPError as error:
        raise CommonCrawlIndexError(f"Common Crawl returned HTTP {error.code}") from error
    except (socket.timeout, TimeoutError) as error:
        raise CommonCrawlIndexError(f"Common Crawl timed out after {timeout} seconds") from error
    except urllib.error.URLError as error:
        if isinstance(error.reason, (socket.timeout, TimeoutError)):
            raise CommonCrawlIndexError(
                f"Common Crawl timed out after {timeout} seconds"
            ) from error
        raise CommonCrawlIndexError("network error while contacting Common Crawl") from error
    except http.client.IncompleteRead as error:
        raise CommonCrawlIndexError("Common Crawl returned an incomplete response") from error
    except http.client.HTTPException as error:
        raise CommonCrawlIndexError("HTTP protocol error while contacting Common Crawl") from error
    except OSError as error:
        raise CommonCrawlIndexError("network error while contacting Common Crawl") from error

    return parse_response(document, _utc_now())

def _serialize_records(records: list[dict[str, Any]]) -> bytes:
    """Validate and encode the complete NDJSON payload before emission."""
    _validate_unicode_scalars(records)
    try:
        lines = [
            json.dumps(
                record,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            for record in records
        ]
    except (TypeError, ValueError, UnicodeEncodeError, RecursionError) as error:
        raise CommonCrawlIndexError(
            "validated metadata could not be serialized as UTF-8"
        ) from error
    return b"\n".join(lines) + b"\n"




def build_parser() -> argparse.ArgumentParser:
    parser = SafeArgumentParser(
        description="Fetch bounded Common Crawl index metadata as NDJSON."
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds (1-{MAX_TIMEOUT}; default: {DEFAULT_TIMEOUT})",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        records = fetch_index_metadata(timeout=args.timeout)
        payload = _serialize_records(records)
    except (CommonCrawlIndexError, ValueError) as error:
        print(f"{SOURCE}: {_safe_diagnostic(error)}", file=sys.stderr)
        return 1

    stdout_buffer = getattr(sys.stdout, "buffer", None)
    if stdout_buffer is None:
        sys.stdout.write(payload.decode("utf-8"))
    else:
        stdout_buffer.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
