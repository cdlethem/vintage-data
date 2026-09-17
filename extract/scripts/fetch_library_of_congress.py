#!/usr/bin/env python3
"""Fetch a bounded changing slice of Library of Congress book search results.

The JSON search API uses link-based pagination. This extractor follows only API-provided
HTTPS pagination links on loc.gov, never downloads linked digital content, and buffers
all requested pages before emitting NDJSON. The configured robotics query is a changing
ranked slice, not a complete or consistent copy of the Library of Congress catalog.
Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import math
import os
import sys
import time
from typing import Any, Callable, Sequence
import urllib.error
import urllib.parse
import urllib.request

SOURCE = "library_of_congress"
SEARCH_URL = "https://www.loc.gov/books/"
SAFE_HOSTS = frozenset({"loc.gov", "www.loc.gov"})
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 Library-of-Congress-extractor "
    "(+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_QUERY = "robotics"
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 2
DEFAULT_TIMEOUT = 30.0
MAX_PAGE_SIZE = 1_000
MAX_TOTAL_ITEMS = 100_000
MAX_TIMEOUT = 120.0
MAX_RESPONSE_BYTES = 32 * 1024 * 1024
MIN_REQUEST_INTERVAL = 3.1  # strictly below 20 requests per minute


class LibraryOfCongressError(RuntimeError):
    """The API request or response cannot satisfy the extractor contract."""


def _positive_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
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


def _validate_search_url(url: Any, purpose: str) -> str:
    if not isinstance(url, str) or not url:
        raise LibraryOfCongressError(f"Library of Congress {purpose} URL is missing")
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
    except ValueError as exc:
        raise LibraryOfCongressError(
            f"Library of Congress {purpose} URL is malformed"
        ) from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname not in SAFE_HOSTS
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or parsed.fragment
        or parsed.path not in {"/books", "/books/"}
    ):
        raise LibraryOfCongressError(
            f"Library of Congress {purpose} URL is unsafe; expected HTTPS loc.gov /books search"
        )
    return url


class RequestPacer:
    """Run-wide serial request pacer, including redirects."""

    def __init__(
        self,
        interval: float = MIN_REQUEST_INTERVAL,
        *,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.interval = interval
        self.clock = clock or time.monotonic
        self.sleeper = sleeper or time.sleep
        self.last_request_at: float | None = None

    def wait(self) -> None:
        now = self.clock()
        if self.last_request_at is not None:
            delay = self.interval - (now - self.last_request_at)
            if delay > 0:
                self.sleeper(delay)
                now = self.clock()
        self.last_request_at = now


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects away from the HTTPS loc.gov books search surface."""

    def __init__(self, pacer: RequestPacer) -> None:
        super().__init__()
        self.pacer = pacer

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        _validate_search_url(newurl, "redirect")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected is None:
            return None
        self.pacer.wait()
        return redirected


class LibraryOfCongressClient:
    """Small unauthenticated JSON client with URL and response bounds."""

    def __init__(
        self,
        *,
        timeout: float,
        pacer: RequestPacer | None = None,
        opener: Any | None = None,
    ) -> None:
        self.timeout = timeout
        self.pacer = pacer or RequestPacer()
        self.opener = opener or urllib.request.build_opener(SafeRedirectHandler(self.pacer))

    def get_page(self, url: str) -> dict[str, Any]:
        _validate_search_url(url, "request")
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        self.pacer.wait()
        try:
            with self.opener.open(request, timeout=self.timeout) as response:
                final_url = response.geturl() if hasattr(response, "geturl") else url
                _validate_search_url(final_url, "redirect target")
                content_type = response.headers.get("Content-Type", "")
                if "application/json" not in content_type.lower():
                    raise LibraryOfCongressError(
                        "Library of Congress returned a non-JSON response "
                        f"(Content-Type: {content_type or 'missing'})"
                    )
                body = response.read(MAX_RESPONSE_BYTES + 1)
        except urllib.error.HTTPError as exc:
            try:
                if exc.code == 429:
                    raise LibraryOfCongressError(
                        "Library of Congress throttled the request with HTTP 429"
                    ) from exc
                if 300 <= exc.code < 400:
                    raise LibraryOfCongressError(
                        "Library of Congress redirect loop or excessive redirects"
                    ) from exc
                raise LibraryOfCongressError(
                    f"Library of Congress request failed with HTTP {exc.code}"
                ) from exc
            finally:
                exc.close()
        except LibraryOfCongressError:
            raise
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise LibraryOfCongressError(f"Library of Congress request failed: {exc}") from exc

        if len(body) > MAX_RESPONSE_BYTES:
            raise LibraryOfCongressError("Library of Congress response exceeded the byte limit")
        try:
            document = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise LibraryOfCongressError("Library of Congress returned malformed JSON") from exc
        if not isinstance(document, dict):
            raise LibraryOfCongressError("Library of Congress response must be a JSON object")
        if not isinstance(document.get("results"), list):
            raise LibraryOfCongressError("Library of Congress response is missing a results list")
        pagination = document.get("pagination")
        if pagination is not None and not isinstance(pagination, dict):
            raise LibraryOfCongressError("Library of Congress response has malformed pagination")
        return document


def _list_field(item: dict[str, Any], *names: str) -> list[Any]:
    for name in names:
        value = item.get(name)
        if value is not None:
            return list(value) if isinstance(value, list) else [value]
    return []


def _resource_links(resources: Any) -> list[str]:
    links: list[str] = []
    seen: set[str] = set()

    def visit(value: Any, key: str | None = None) -> None:
        if isinstance(value, dict):
            for child_key, child in value.items():
                visit(child, child_key.lower())
        elif isinstance(value, list):
            for child in value:
                visit(child, key)
        elif (
            isinstance(value, str)
            and urllib.parse.urlsplit(value).scheme in {"http", "https"}
            and value not in seen
        ):
            seen.add(value)
            links.append(value)

    visit(resources)
    return links


def normalize_item(item: Any, fetched_at: str) -> dict[str, Any]:
    """Project stable analytics fields while retaining the complete API item."""
    if not isinstance(item, dict):
        raise LibraryOfCongressError("Library of Congress results contain a non-object item")
    identifier = item.get("id")
    if not isinstance(identifier, str) or not identifier.strip():
        raise LibraryOfCongressError("Library of Congress item has an invalid id")
    identifier = identifier.strip()
    resources = item.get("resources")
    if resources is None:
        resources = []
    elif not isinstance(resources, list):
        resources = [resources]

    canonical_url = item.get("url")
    if canonical_url is not None and not isinstance(canonical_url, str):
        raise LibraryOfCongressError(f"Library of Congress item {identifier} has an invalid url")
    digitized = item.get("digitized")
    if digitized is not None and not isinstance(digitized, bool):
        raise LibraryOfCongressError(
            f"Library of Congress item {identifier} has an invalid digitized status"
        )

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": identifier,
        "title": item.get("title"),
        "date": item.get("date"),
        "dates": _list_field(item, "dates"),
        "contributors": _list_field(item, "contributor", "contributors"),
        "subjects": _list_field(item, "subject", "subjects"),
        "digitized": digitized,
        "online_formats": _list_field(item, "online_format", "online_formats"),
        "mime_types": _list_field(item, "mime_type", "mime_types"),
        "canonical_url": canonical_url,
        "resources": resources,
        "resource_links": _resource_links(resources),
        "raw": dict(item),
    }


def _next_url(document: dict[str, Any]) -> str | None:
    pagination = document.get("pagination") or {}
    value = pagination.get("next")
    if value is None:
        return None
    return _validate_search_url(value, "next-page")


def fetch_items(
    *,
    query: str = DEFAULT_QUERY,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: float = DEFAULT_TIMEOUT,
    client: LibraryOfCongressClient | None = None,
) -> list[dict[str, Any]]:
    """Return one validated, deduplicated, bounded search observation."""
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    page_size = _positive_int(page_size, "page_size")
    max_pages = _positive_int(max_pages, "max_pages")
    if page_size > MAX_PAGE_SIZE:
        raise ValueError(f"page_size must be no greater than {MAX_PAGE_SIZE}")
    if page_size * max_pages > MAX_TOTAL_ITEMS:
        raise ValueError(
            f"page_size * max_pages must be no greater than {MAX_TOTAL_ITEMS} items"
        )
    timeout = _positive_number(timeout, "timeout", MAX_TIMEOUT)

    url = SEARCH_URL + "?" + urllib.parse.urlencode(
        {"fo": "json", "q": query.strip(), "c": page_size}
    )
    http = client or LibraryOfCongressClient(timeout=timeout)
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_urls: set[str] = set()

    for _ in range(max_pages):
        if url in seen_urls:
            raise LibraryOfCongressError("Library of Congress pagination contains a loop")
        seen_urls.add(url)
        document = http.get_page(url)
        results = document["results"]
        if len(results) > page_size:
            raise LibraryOfCongressError(
                f"Library of Congress page returned {len(results)} items above page_size={page_size}"
            )
        for item in results:
            record = normalize_item(item, fetched_at)
            if record["id"] not in seen_ids:
                seen_ids.add(record["id"])
                records.append(record)
        if not results:
            break
        next_url = _next_url(document)
        if next_url is None:
            break
        if next_url in seen_urls:
            raise LibraryOfCongressError("Library of Congress pagination contains a loop")
        url = next_url

    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        records = fetch_items(
            query=args.query,
            page_size=args.page_size,
            max_pages=args.max_pages,
            timeout=args.timeout,
        )
    except (ValueError, LibraryOfCongressError) as exc:
        print(f"library_of_congress: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
