#!/usr/bin/env python3
"""Fetch bounded genome assembly metadata from the NCBI Datasets v2 API.

Only assembly accessions explicitly supplied on the command line are requested. The
extractor does not enumerate the genome catalog or download sequence packages.

Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable, Sequence

SOURCE = "ncbi_datasets"
OFFICIAL_API_BASE = "https://api.ncbi.nlm.nih.gov/datasets/v2/genome/accession"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
ACCESSION_RE = re.compile(r"GC[AF]_[0-9]{9}\.[0-9]+")
API_KEY_HEADER_SAFE_RE = re.compile(r"[\x20-\x7e]*")
MAX_ACCESSIONS = 20
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
REQUEST_DELAY_SECONDS = 0.25
DEFAULT_TIMEOUT = 30.0
MAX_TIMEOUT = 120.0


class NCBIDatasetsError(RuntimeError):
    """A safe, categorized extraction failure suitable for stderr."""

    def __init__(self, category: str, message: str):
        super().__init__(message)
        self.category = category


def validate_accessions(accessions: Sequence[str]) -> tuple[str, ...]:
    """Validate a bounded, duplicate-free list of versioned assembly accessions."""
    if isinstance(accessions, (str, bytes)):
        raise ValueError("accessions must be a sequence")
    values = tuple(accessions)
    if not values:
        raise ValueError("at least one assembly accession is required")
    if len(values) > MAX_ACCESSIONS:
        raise ValueError(f"at most {MAX_ACCESSIONS} assembly accessions may be requested")
    for accession in values:
        if not isinstance(accession, str) or ACCESSION_RE.fullmatch(accession) is None:
            raise ValueError(
                f"invalid versioned assembly accession {accession!r}; expected GCF_ or GCA_"
            )
    if len(set(values)) != len(values):
        raise ValueError("assembly accessions must not contain duplicates")
    return values


def _require_header_safe_api_key(api_key: str) -> None:
    """Reject an API key that is unsafe to place in an HTTP header value.

    This runs before any request is constructed or opened. The raised message
    never includes the rejected value (or any substring of it) so a malformed
    or injected key can never reach stdout, stderr, or an exception message,
    including via Python's own header-validation ValueError.
    """
    if API_KEY_HEADER_SAFE_RE.fullmatch(api_key) is None:
        raise ValueError(
            "NCBI_API_KEY contains characters that are not valid in an HTTP header value"
        )


def report_url(accession: str) -> str:
    """Return the credential-free official report endpoint for one accession."""
    encoded = urllib.parse.quote(accession, safe="")
    return f"{OFFICIAL_API_BASE}/{encoded}/dataset_report"


def request_url(accession: str) -> str:
    """Return the fixed official request endpoint for one accession."""
    return report_url(accession)


def _origin(url: str) -> tuple[str, str | None, int | None]:
    parsed = urllib.parse.urlsplit(url)
    try:
        port = parsed.port
    except ValueError:
        return parsed.scheme.lower(), parsed.hostname, None
    if port is None:
        port = 443 if parsed.scheme.lower() == "https" else 80
    hostname = parsed.hostname.lower() if parsed.hostname else None
    return parsed.scheme.lower(), hostname, port


OFFICIAL_API_ORIGIN = _origin(OFFICIAL_API_BASE)


class OfficialHTTPSRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Follow redirects only within the official NCBI HTTPS origin."""

    def redirect_request(self, request, response, code, message, headers, new_url):
        redirect_url = urllib.parse.urljoin(request.full_url, new_url)
        parsed = urllib.parse.urlsplit(redirect_url)
        if (
            parsed.username is not None
            or parsed.password is not None
            or _origin(redirect_url) != OFFICIAL_API_ORIGIN
        ):
            raise NCBIDatasetsError(
                "unsafe_redirect", "refused redirect outside the official NCBI endpoint"
            )
        return super().redirect_request(
            request, response, code, message, headers, redirect_url
        )


_OFFICIAL_OPENER = urllib.request.build_opener(OfficialHTTPSRedirectHandler())


def open_official_request(request: urllib.request.Request, *, timeout: float):
    """Open one request with credential-safe redirect handling."""
    return _OFFICIAL_OPENER.open(request, timeout=timeout)


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise NCBIDatasetsError("malformed_report", f"report field {field} must be a string")
    return value


def _optional_object(report: dict[str, Any], field: str) -> dict[str, Any]:
    value = report.get(field)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise NCBIDatasetsError("malformed_report", f"report field {field} must be an object")
    return value


def normalize_report(
    report: Any, requested_accession: str, fetched_at: str
) -> dict[str, Any]:
    """Validate one assembly report and retain the complete wire object."""
    if not isinstance(report, dict):
        raise NCBIDatasetsError("malformed_report", "assembly report must be an object")
    accession = report.get("accession")
    if not isinstance(accession, str) or ACCESSION_RE.fullmatch(accession) is None:
        raise NCBIDatasetsError(
            "malformed_report", "assembly report has an invalid accession"
        )
    if accession != requested_accession:
        raise NCBIDatasetsError(
            "malformed_report",
            f"assembly report accession {accession!r} does not match requested accession",
        )

    organism = _optional_object(report, "organism")
    assembly_info = _optional_object(report, "assembly_info")
    tax_id = organism.get("tax_id")
    if isinstance(tax_id, str) and tax_id.isdigit():
        tax_id = int(tax_id)
    if tax_id is not None and (isinstance(tax_id, bool) or not isinstance(tax_id, int)):
        raise NCBIDatasetsError(
            "malformed_report", "report field organism.tax_id must be an integer"
        )

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": accession,
        "accession": accession,
        "current_accession": _optional_string(
            report.get("current_accession"), "current_accession"
        ),
        "paired_accession": _optional_string(
            report.get("paired_accession"), "paired_accession"
        ),
        "source_database": _optional_string(
            report.get("source_database"), "source_database"
        ),
        "organism_name": _optional_string(
            organism.get("organism_name"), "organism.organism_name"
        ),
        "tax_id": tax_id,
        "assembly_name": _optional_string(
            assembly_info.get("assembly_name"), "assembly_info.assembly_name"
        ),
        "assembly_level": _optional_string(
            assembly_info.get("assembly_level"), "assembly_info.assembly_level"
        ),
        "release_date": _optional_string(
            assembly_info.get("release_date"), "assembly_info.release_date"
        ),
        "source_url": report_url(accession),
        "raw": report,
    }


def parse_response(
    document: Any, requested_accession: str, fetched_at: str
) -> list[dict[str, Any]]:
    """Distinguish a valid empty report list from malformed API responses."""
    if not isinstance(document, dict):
        raise NCBIDatasetsError("malformed_response", "response must be a JSON object")
    reports = document.get("reports")
    if not isinstance(reports, list):
        raise NCBIDatasetsError(
            "malformed_response", "response field reports must be a list"
        )
    if len(reports) > 1:
        raise NCBIDatasetsError(
            "malformed_response", "single-accession response contains multiple reports"
        )

    total_count = document.get("total_count")
    if total_count is not None:
        if (
            isinstance(total_count, bool)
            or not isinstance(total_count, int)
            or total_count < 0
        ):
            raise NCBIDatasetsError(
                "malformed_response", "response field total_count must be a non-negative integer"
            )
        if total_count != len(reports):
            raise NCBIDatasetsError(
                "malformed_response", "response total_count does not match reports"
            )

    return [normalize_report(report, requested_accession, fetched_at) for report in reports]


def _read_document(response: Any, accession: str) -> Any:
    payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise NCBIDatasetsError(
            "response_too_large",
            f"response for {accession} exceeded {MAX_RESPONSE_BYTES} bytes",
        )
    try:
        text = payload.decode("utf-8")
        return json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise NCBIDatasetsError(
            "non_json_response", f"response for {accession} was not valid JSON"
        ) from error


def fetch_genome_reports(
    accessions: Sequence[str],
    *,
    timeout: float = DEFAULT_TIMEOUT,
    api_key: str | None = None,
    opener: Callable[..., Any] | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """Fetch one bounded report response per explicit assembly accession."""
    requested = validate_accessions(accessions)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError("timeout must be a number")
    if not math.isfinite(timeout):
        raise ValueError("timeout must be finite")
    if timeout <= 0 or timeout > MAX_TIMEOUT:
        raise ValueError(f"timeout must be greater than zero and at most {MAX_TIMEOUT:g}")
    if api_key is None:
        raw_api_key = os.environ.get("NCBI_API_KEY", "")
        _require_header_safe_api_key(raw_api_key)
        api_key = raw_api_key.strip() or None
    elif not isinstance(api_key, str):
        raise ValueError("api_key must be a string or None")
    if api_key:
        _require_header_safe_api_key(api_key)
    if opener is None:
        opener = open_official_request

    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    for index, accession in enumerate(requested):
        if index:
            sleeper(REQUEST_DELAY_SECONDS)
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if api_key:
            headers["api-key"] = api_key
        request = urllib.request.Request(request_url(accession), headers=headers)
        failure: tuple[str, str] | None = None
        try:
            with opener(request, timeout=float(timeout)) as response:
                document = _read_document(response, accession)
        except urllib.error.HTTPError:
            failure = (
                "http_error",
                f"request for {accession} returned an HTTP error",
            )
        except NCBIDatasetsError:
            raise
        except ValueError:
            failure = (
                "invalid_request",
                f"request for {accession} could not be constructed",
            )
        except (urllib.error.URLError, TimeoutError, OSError):
            failure = ("transport_error", f"request for {accession} failed")
        if failure is not None:
            # Raise after leaving the handler so the upstream exception is not retained
            # as __context__ and cannot surface through chaining or tracebacks.
            raise NCBIDatasetsError(*failure) from None
        records.extend(parse_response(document, accession, fetched_at))
    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch bounded NCBI Datasets v2 metadata for explicit versioned genome "
            "assembly accessions."
        )
    )
    parser.add_argument(
        "accessions",
        nargs="+",
        help="versioned GCF_ or GCA_ assembly accession (maximum 20)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"per-request timeout in seconds (maximum {MAX_TIMEOUT:g})",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        accessions = validate_accessions(args.accessions)
        records = fetch_genome_reports(accessions, timeout=args.timeout)
    except ValueError as error:
        parser.error(str(error))
    except NCBIDatasetsError as error:
        print(f"{SOURCE}: {error.category}: {error}", file=sys.stderr)
        return 1

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
