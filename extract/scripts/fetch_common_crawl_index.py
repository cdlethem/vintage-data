#!/usr/bin/env python3
"""Fetch bounded domain snapshots from the latest Common Crawl CDX index.

The collection metadata endpoint is authoritative for both crawl selection and the
crawl-specific index URL.  A watchlist run resolves that metadata once, then uses the
same crawl for every domain so records from one run cannot straddle crawl releases.
Output is normalized newline-delimited JSON suitable for automatic RAW schema
inference.

Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
import re
import sys
import urllib.parse
import urllib.request

SOURCE = "common_crawl_index"
COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_TIMEOUT = 30.0
DEFAULT_PAGE_SIZE = 1
DEFAULT_MAX_PAGES = 5
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
CURATED_DOMAINS = (
    "bbc.com",
    "cdc.gov",
    "github.com",
    "noaa.gov",
    "openai.com",
    "un.org",
    "wikipedia.org",
    "worldbank.org",
)

_CRAWL_ID_RE = re.compile(r"^CC-MAIN-\d{4}-\d{2}$")
_TIMESTAMP_RE = re.compile(r"^\d{14}$")


class CommonCrawlError(RuntimeError):
    """The Common Crawl response did not satisfy the extractor contract."""


def _request(url: str) -> urllib.request.Request:
    return urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )


def _read_response(url: str, timeout: float) -> bytes:
    with urllib.request.urlopen(_request(url), timeout=timeout) as response:
        payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise CommonCrawlError(f"response from {url!r} exceeded {MAX_RESPONSE_BYTES} bytes")
    return payload


def _read_json(url: str, timeout: float, *, context: str):
    payload = _read_response(url, timeout)
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CommonCrawlError(f"{context} returned malformed JSON") from exc


def _with_query(url: str, parameters: dict[str, object]) -> str:
    parts = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    query.extend((key, str(value)) for key, value in parameters.items())
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path, urllib.parse.urlencode(query), parts.fragment)
    )


def _validate_positive(name: str, value: float) -> None:
    if isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be positive")


def discover_crawls(
    collinfo_url: str = COLLINFO_URL,
    *,
    timeout: float = DEFAULT_TIMEOUT,
) -> list[dict[str, str]]:
    """Return validated crawl IDs and their crawl-specific CDX endpoints."""
    _validate_positive("timeout", timeout)
    document = _read_json(collinfo_url, float(timeout), context="collection metadata")
    if not isinstance(document, list):
        raise CommonCrawlError("collection metadata must be a JSON list")

    crawls: list[dict[str, str]] = []
    seen: set[str] = set()
    for position, item in enumerate(document):
        if not isinstance(item, dict):
            raise CommonCrawlError(f"collection metadata item {position} must be an object")
        crawl_id = item.get("id")
        index_url = item.get("cdx-api")
        if not isinstance(crawl_id, str) or not _CRAWL_ID_RE.fullmatch(crawl_id):
            raise CommonCrawlError(f"collection metadata item {position} has an invalid id")
        if not isinstance(index_url, str) or not index_url.startswith(("http://", "https://")):
            raise CommonCrawlError(
                f"collection metadata item {position} has an invalid cdx-api URL"
            )
        if crawl_id in seen:
            raise CommonCrawlError(f"collection metadata repeats crawl {crawl_id!r}")
        seen.add(crawl_id)
        crawls.append({"id": crawl_id, "cdx-api": index_url})
    if not crawls:
        raise CommonCrawlError("collection metadata contains no crawls")
    return crawls


def select_crawl(
    crawls: list[dict[str, str]], crawl_id: str | None = None
) -> dict[str, str]:
    """Select an explicit crawl, or the newest crawl by Common Crawl ID."""
    if crawl_id is not None:
        for crawl in crawls:
            if crawl["id"] == crawl_id:
                return crawl
        raise CommonCrawlError(f"crawl {crawl_id!r} is not present in collection metadata")
    return max(crawls, key=lambda crawl: crawl["id"])


def resolve_crawl(
    crawl_id: str | None = None,
    *,
    collinfo_url: str = COLLINFO_URL,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, str]:
    """Resolve a crawl ID to the index URL advertised by collection metadata."""
    return select_crawl(
        discover_crawls(collinfo_url=collinfo_url, timeout=timeout),
        crawl_id=crawl_id,
    )


def _normalize_domain(domain: str) -> str:
    if not isinstance(domain, str):
        raise ValueError("domain must be a string")
    value = domain.strip().lower().rstrip(".")
    if (
        not value
        or "://" in value
        or "/" in value
        or ":" in value
        or value.startswith(".")
        or value.endswith(".")
        or ".." in value
        or any(not (part.replace("-", "").isalnum()) for part in value.split("."))
    ):
        raise ValueError(f"invalid domain {domain!r}")
    return value


def _page_count(
    domain: str,
    index_url: str,
    *,
    page_size: int,
    timeout: float,
) -> int:
    url = _with_query(
        index_url,
        {
            "url": f"{domain}/*",
            "matchType": "domain",
            "output": "json",
            "pageSize": page_size,
            "showNumPages": "true",
        },
    )
    document = _read_json(url, timeout, context=f"page metadata for {domain}")
    if not isinstance(document, dict):
        raise CommonCrawlError(f"page metadata for {domain} must be an object")
    pages = document.get("pages")
    if isinstance(pages, bool) or not isinstance(pages, int) or pages < 0:
        raise CommonCrawlError(f"page metadata for {domain} has an invalid pages value")
    return pages


def _parse_page(payload: bytes, *, domain: str, page: int) -> list[dict]:
    rows: list[dict] = []
    for line_number, raw_line in enumerate(payload.splitlines(), 1):
        if not raw_line.strip():
            continue
        try:
            row = json.loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CommonCrawlError(
                f"index page {page} for {domain} has malformed JSON on line {line_number}"
            ) from exc
        if not isinstance(row, dict):
            raise CommonCrawlError(
                f"index page {page} for {domain} line {line_number} must be an object"
            )
        rows.append(row)
    return rows


def _optional_string(row: dict, key: str):
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, (str, int)) or isinstance(value, bool):
        raise CommonCrawlError(f"index record field {key!r} must be scalar")
    return str(value)


def _optional_integer(row: dict, key: str):
    value = row.get(key)
    if value is None:
        return None
    if isinstance(value, bool):
        raise CommonCrawlError(f"index record field {key!r} must be an integer")
    if isinstance(value, int):
        number = value
    elif isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value):
        number = int(value)
    else:
        raise CommonCrawlError(f"index record field {key!r} must be an integer")
    if number < 0:
        raise CommonCrawlError(f"index record field {key!r} must not be negative")
    return number


def normalize_record(
    row: dict,
    *,
    crawl_id: str,
    domain: str,
    fetched_at: str,
) -> dict:
    """Map one CDX object to a stable, flat, auto-inferable record."""
    if not isinstance(crawl_id, str) or not _CRAWL_ID_RE.fullmatch(crawl_id):
        raise CommonCrawlError("index record has an invalid crawl id")
    url_key = row.get("urlkey")
    timestamp = row.get("timestamp")
    url = row.get("url")
    warc_filename = row.get("filename")
    if not isinstance(url_key, str) or not url_key:
        raise CommonCrawlError("index record has an invalid urlkey")
    if not isinstance(timestamp, str) or not _TIMESTAMP_RE.fullmatch(timestamp):
        raise CommonCrawlError("index record has an invalid timestamp")
    if not isinstance(url, str) or not url:
        raise CommonCrawlError("index record has an invalid url")
    if not isinstance(warc_filename, str) or not warc_filename.strip():
        raise CommonCrawlError("index record has an invalid filename")
    warc_offset = _optional_integer(row, "offset")
    if warc_offset is None:
        raise CommonCrawlError("index record is missing required field 'offset'")
    try:
        captured_at = datetime.strptime(timestamp, "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        ).isoformat()
    except ValueError as exc:
        raise CommonCrawlError("index record has an invalid timestamp") from exc

    capture_id = ":".join(
        (
            crawl_id,
            urllib.parse.quote(url_key, safe=""),
            timestamp,
            urllib.parse.quote(warc_filename, safe=""),
            str(warc_offset),
        )
    )
    return {
        "source": SOURCE,
        "id": capture_id,
        "fetched_at": fetched_at,
        "crawl_id": crawl_id,
        "watched_domain": domain,
        "url_key": url_key,
        "timestamp": timestamp,
        "captured_at": captured_at,
        "url": url,
        "mime_type": _optional_string(row, "mime"),
        "detected_mime_type": _optional_string(row, "mime-detected"),
        "status_code": _optional_string(row, "status"),
        "digest": _optional_string(row, "digest"),
        "content_length": _optional_integer(row, "length"),
        "warc_offset": warc_offset,
        "warc_filename": warc_filename,
        "languages": _optional_string(row, "languages"),
        "encoding": _optional_string(row, "encoding"),
    }


def fetch_domain(
    domain: str,
    crawl: dict[str, str],
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: float = DEFAULT_TIMEOUT,
    fetched_at: str | None = None,
):
    """Yield normalized records from bounded CDX pages for one domain."""
    _validate_positive("page_size", page_size)
    _validate_positive("max_pages", max_pages)
    _validate_positive("timeout", timeout)
    normalized_domain = _normalize_domain(domain)
    try:
        crawl_id = crawl["id"]
        index_url = crawl["cdx-api"]
    except (KeyError, TypeError) as exc:
        raise ValueError("crawl must contain id and cdx-api") from exc
    if not isinstance(crawl_id, str) or not isinstance(index_url, str):
        raise ValueError("crawl id and cdx-api must be strings")

    observed_at = fetched_at or datetime.now(timezone.utc).isoformat()
    pages = min(
        _page_count(
            normalized_domain,
            index_url,
            page_size=int(page_size),
            timeout=float(timeout),
        ),
        int(max_pages),
    )
    for page in range(pages):
        url = _with_query(
            index_url,
            {
                "url": f"{normalized_domain}/*",
                "matchType": "domain",
                "output": "json",
                "pageSize": int(page_size),
                "page": page,
            },
        )
        payload = _read_response(url, float(timeout))
        for row in _parse_page(payload, domain=normalized_domain, page=page):
            yield normalize_record(
                row,
                crawl_id=crawl_id,
                domain=normalized_domain,
                fetched_at=observed_at,
            )


def fetch_watchlist(
    domains=CURATED_DOMAINS,
    *,
    crawl_id: str | None = None,
    collinfo_url: str = COLLINFO_URL,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: float = DEFAULT_TIMEOUT,
    fetched_at: str | None = None,
):
    """Fetch every domain while isolating domain-specific HTTP or data failures.

    Crawl discovery and selection happen exactly once.  Even an explicit ``crawl_id``
    is resolved through ``collinfo_url`` because that metadata supplies its CDX URL.
    """
    _validate_positive("page_size", page_size)
    _validate_positive("max_pages", max_pages)
    _validate_positive("timeout", timeout)
    crawl = resolve_crawl(
        crawl_id=crawl_id,
        collinfo_url=collinfo_url,
        timeout=float(timeout),
    )
    observed_at = fetched_at or datetime.now(timezone.utc).isoformat()
    for domain in domains:
        try:
            yield from fetch_domain(
                domain,
                crawl,
                page_size=int(page_size),
                max_pages=int(max_pages),
                timeout=float(timeout),
                fetched_at=observed_at,
            )
        except Exception as exc:  # noqa: BLE001 - one domain must not abort the watchlist
            print(f"{SOURCE}: skipping {domain!r}: {exc!r}", file=sys.stderr)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("domains", nargs="*", help="domains to fetch; defaults to curated set")
    parser.add_argument("--crawl-id")
    parser.add_argument("--collinfo-url", default=COLLINFO_URL)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    domains = args.domains or CURATED_DOMAINS
    for record in fetch_watchlist(
        domains,
        crawl_id=args.crawl_id,
        collinfo_url=args.collinfo_url,
        page_size=args.page_size,
        max_pages=args.max_pages,
        timeout=args.timeout,
    ):
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
