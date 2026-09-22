#!/usr/bin/env python3
"""Fetch a bounded planet snapshot from the NASA Exoplanet Archive TAP service.

The public, synchronous TAP query reads only ``pscomppars`` and requests the
planet name, host name, and radius in Earth radii. The deterministic result is
capped at 10,000 rows, so a successful run is a bounded snapshot and is not
necessarily the complete catalogue.

Each validated planet is emitted as one NDJSON record. Planet names are the
catalogue's planet identity and therefore form stable IDs; retrieval time and
mutable measurements are deliberately excluded from the ID. Stdlib only.
"""

import argparse
import http.client
import json
import math
import os
import socket
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Sequence

SOURCE = "nasa_exoplanet_archive"
URL = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
ROW_LIMIT = 10_000
DEFAULT_TIMEOUT = 60
MAX_TIMEOUT = 120
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
COLUMNS = ("pl_name", "hostname", "pl_rade")
QUERY = (
    f"select top {ROW_LIMIT} pl_name, hostname, pl_rade "
    "from pscomppars order by pl_name asc, hostname asc"
)
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)


class ArchiveError(RuntimeError):
    """A safe, operator-facing failure from the archive request or response."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_request() -> urllib.request.Request:
    """Build the single fixed, public TAP request used by this extractor."""
    query = urllib.parse.urlencode({"query": QUERY, "format": "json"})
    return urllib.request.Request(
        f"{URL}?{query}",
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        method="GET",
    )


def _read_document(timeout: int) -> Any:
    request = build_request()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = getattr(response, "status", None)
            if status is not None and not 200 <= status < 300:
                raise ArchiveError(f"HTTP error {status}")

            headers = getattr(response, "headers", None)
            declared_length = headers.get("Content-Length") if headers is not None else None
            if declared_length is not None:
                try:
                    expected_bytes = int(declared_length)
                except (TypeError, ValueError) as error:
                    raise ArchiveError("invalid HTTP Content-Length") from error
                if expected_bytes < 0:
                    raise ArchiveError("invalid HTTP Content-Length")
                if expected_bytes > MAX_RESPONSE_BYTES:
                    raise ArchiveError(
                        f"response exceeds the {MAX_RESPONSE_BYTES}-byte safety limit"
                    )
            else:
                expected_bytes = None

            body = response.read(MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as error:
        raise ArchiveError(f"HTTP error {error.code}") from error
    except (socket.timeout, TimeoutError) as error:
        raise ArchiveError(f"timeout after {timeout} seconds") from error
    except urllib.error.URLError as error:
        if isinstance(error.reason, (socket.timeout, TimeoutError)):
            raise ArchiveError(f"timeout after {timeout} seconds") from error
        raise ArchiveError("network error while contacting NASA TAP endpoint") from error
    except http.client.IncompleteRead as error:
        raise ArchiveError("incomplete HTTP response") from error
    except OSError as error:
        raise ArchiveError("network error while contacting NASA TAP endpoint") from error

    if not isinstance(body, bytes):
        raise ArchiveError("invalid HTTP response body")
    if len(body) > MAX_RESPONSE_BYTES:
        raise ArchiveError(f"response exceeds the {MAX_RESPONSE_BYTES}-byte safety limit")
    if expected_bytes is not None and len(body) != expected_bytes:
        raise ArchiveError("incomplete HTTP response")

    def reject_nonstandard_number(value: str) -> None:
        raise ValueError(f"non-standard JSON number {value}")

    try:
        return json.loads(
            body.decode("utf-8"),
            parse_constant=reject_nonstandard_number,
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise ArchiveError("non-JSON response from NASA TAP endpoint") from error


def _normalize_row(row: Any, index: int, fetched_at: str) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ArchiveError(f"malformed row {index}: expected an object")
    if set(row) != set(COLUMNS):
        raise ArchiveError(
            f"invalid schema at row {index}: expected exactly {', '.join(COLUMNS)}"
        )

    planet_name = row["pl_name"]
    hostname = row["hostname"]
    radius = row["pl_rade"]
    if (
        not isinstance(planet_name, str)
        or not planet_name
        or planet_name.strip() != planet_name
    ):
        raise ArchiveError(f"malformed row {index}: pl_name must be a nonempty string")
    if not isinstance(hostname, str) or not hostname or hostname.strip() != hostname:
        raise ArchiveError(f"malformed row {index}: hostname must be a nonempty string")
    if radius is not None:
        if isinstance(radius, bool) or not isinstance(radius, (int, float)):
            raise ArchiveError(f"malformed row {index}: pl_rade must be numeric or null")
        if not math.isfinite(radius) or radius < 0:
            raise ArchiveError(
                f"malformed row {index}: pl_rade must be finite and nonnegative"
            )

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": planet_name,
        "pl_name": planet_name,
        "hostname": hostname,
        "pl_rade": radius,
    }


def parse_response(document: Any, fetched_at: str) -> list[dict[str, Any]]:
    """Validate the complete response before making any record available."""
    if not isinstance(document, list):
        raise ArchiveError("invalid schema: TAP JSON response must be an array")
    if not document:
        raise ArchiveError("invalid schema: TAP response unexpectedly contained no planets")
    if len(document) > ROW_LIMIT:
        raise ArchiveError(f"row cap exceeded: received more than {ROW_LIMIT} rows")

    records: list[dict[str, Any]] = []
    identities: set[str] = set()
    for index, row in enumerate(document):
        record = _normalize_row(row, index, fetched_at)
        identity = record["id"]
        if identity in identities:
            raise ArchiveError(f"malformed row {index}: duplicate planet identity")
        identities.add(identity)
        records.append(record)
    return records


def fetch_planets(timeout: int = DEFAULT_TIMEOUT) -> list[dict[str, Any]]:
    """Fetch and fully validate one bounded catalogue snapshot."""
    if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= MAX_TIMEOUT:
        raise ValueError(f"timeout must be an integer from 1 to {MAX_TIMEOUT} seconds")
    document = _read_document(timeout)
    return parse_response(document, _utc_now())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch the deterministic first 10,000 pscomppars planets from the "
            "public NASA Exoplanet Archive TAP service as NDJSON."
        )
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout in seconds (1-{MAX_TIMEOUT}; default: {DEFAULT_TIMEOUT})",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not 1 <= args.timeout <= MAX_TIMEOUT:
        parser.error(f"timeout must be from 1 to {MAX_TIMEOUT} seconds")

    try:
        records = fetch_planets(timeout=args.timeout)
    except ArchiveError as error:
        # Errors are intentionally bounded and contain no request URL or response data.
        diagnostic = " ".join(str(error).split())[:500]
        print(f"{SOURCE}: {diagnostic}", file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
