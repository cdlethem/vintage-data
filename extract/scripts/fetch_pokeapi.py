#!/usr/bin/env python3
"""Fetch a bounded page sample of the PokéAPI Pokémon catalog as NDJSON.

Only the public catalog endpoint is requested.  Pokémon detail URLs are validated
and emitted as identities; they are never fetched.  Pagination is serial and
paced by at least one second between requests.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
import sys
import time
from typing import Any, Callable, Sequence
import urllib.error
import urllib.parse
import urllib.request

SOURCE = "pokeapi_pokemon_data"
RESOURCE = "pokemon"
CATALOG_URL = "https://pokeapi.co/api/v2/pokemon/"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_MAX_PAGES = 20
HARD_MAX_PAGES = 20
MAX_PAGE_SIZE = 1000
REQUEST_TIMEOUT = 20.0
REQUEST_INTERVAL = 1.0
MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class PokeAPIError(RuntimeError):
    """A request or response cannot satisfy the extractor contract."""


@dataclass(frozen=True)
class FetchResult:
    records: list[dict[str, Any]]
    truncated: bool


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _catalog_parameters(url: str, context: str) -> tuple[int, int]:
    """Validate a catalog URL and return its ``(limit, offset)`` values."""
    try:
        parsed = urllib.parse.urlsplit(url)
        parameters = urllib.parse.parse_qs(
            parsed.query, keep_blank_values=True, strict_parsing=True
        )
    except (TypeError, ValueError) as exc:
        raise PokeAPIError(f"{context} is not a valid catalog URL") from exc

    if (
        parsed.scheme != "https"
        or parsed.netloc != "pokeapi.co"
        or parsed.path not in {"/api/v2/pokemon", "/api/v2/pokemon/"}
        or parsed.fragment
        or set(parameters) != {"limit", "offset"}
        or any(len(values) != 1 for values in parameters.values())
    ):
        raise PokeAPIError(
            f"{context} must stay on the HTTPS pokeapi.co Pokémon catalog endpoint"
        )

    limit_text = parameters["limit"][0]
    offset_text = parameters["offset"][0]
    try:
        limit = int(limit_text)
        offset = int(offset_text)
    except ValueError as exc:
        raise PokeAPIError(f"{context} has invalid pagination values") from exc
    if (
        str(limit) != limit_text
        or str(offset) != offset_text
        or not 1 <= limit <= MAX_PAGE_SIZE
        or offset < 0
    ):
        raise PokeAPIError(f"{context} has invalid pagination values")
    return limit, offset


def _catalog_url(page_size: int, offset: int) -> str:
    return CATALOG_URL + "?" + urllib.parse.urlencode(
        {"limit": page_size, "offset": offset}
    )


def _canonical_pokemon_url(value: Any) -> tuple[int, str]:
    if not isinstance(value, str):
        raise PokeAPIError("PokéAPI result has invalid url")
    try:
        parsed = urllib.parse.urlsplit(value)
    except ValueError as exc:
        raise PokeAPIError("PokéAPI result has invalid url") from exc
    prefix = "/api/v2/pokemon/"
    if (
        parsed.scheme != "https"
        or parsed.netloc != "pokeapi.co"
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith(prefix)
        or not parsed.path.endswith("/")
    ):
        raise PokeAPIError("PokéAPI result url is not a canonical Pokémon detail URL")
    identifier_text = parsed.path[len(prefix) : -1]
    try:
        identifier = int(identifier_text)
    except ValueError as exc:
        raise PokeAPIError("PokéAPI result url does not contain a positive integer id") from exc
    canonical = f"{CATALOG_URL}{identifier}/"
    if identifier <= 0 or identifier_text != str(identifier) or value != canonical:
        raise PokeAPIError("PokéAPI result url is not canonical for its positive integer id")
    return identifier, canonical


class CatalogRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects that could move a request outside the catalog endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001
        current_parameters = _catalog_parameters(req.full_url, "PokéAPI request")
        if _catalog_parameters(newurl, "PokéAPI redirect") != current_parameters:
            raise PokeAPIError("PokéAPI redirect changed the requested catalog page")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _read_document(opener: Any, url: str, timeout: float) -> Any:
    requested_parameters = _catalog_parameters(url, "PokéAPI request")
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with opener.open(request, timeout=timeout) as response:
            final_url = response.geturl()
            if _catalog_parameters(final_url, "PokéAPI response URL") != requested_parameters:
                raise PokeAPIError("PokéAPI redirect changed the requested catalog page")
            body = response.read(MAX_RESPONSE_BYTES + 1)
    except PokeAPIError:
        raise
    except urllib.error.HTTPError as exc:
        try:
            raise PokeAPIError(f"PokéAPI request failed with HTTP {exc.code}") from exc
        finally:
            exc.close()
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise PokeAPIError(f"PokéAPI request failed: {exc}") from exc

    if len(body) > MAX_RESPONSE_BYTES:
        raise PokeAPIError("PokéAPI response exceeded the byte limit")
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PokeAPIError("PokéAPI returned malformed JSON") from exc


def _parse_page(
    document: Any,
    *,
    fetched_at: str,
    page_size: int,
    current_offset: int,
    seen_ids: set[int],
) -> tuple[list[dict[str, Any]], str | None]:
    if not isinstance(document, dict):
        raise PokeAPIError("PokéAPI response must be a JSON object")
    if "results" not in document or "next" not in document:
        raise PokeAPIError("PokéAPI response must contain results and next")
    results = document["results"]
    next_url = document["next"]
    if not isinstance(results, list):
        raise PokeAPIError("PokéAPI response results must be a list")
    if len(results) > page_size:
        raise PokeAPIError(
            f"PokéAPI response returned {len(results)} results above page size {page_size}"
        )
    if next_url is not None and not isinstance(next_url, str):
        raise PokeAPIError("PokéAPI response next must be a URL or null")
    if not results and next_url is not None:
        raise PokeAPIError("PokéAPI returned an empty page with further pagination")

    page_records: list[dict[str, Any]] = []
    page_ids: set[int] = set()
    for item in results:
        if not isinstance(item, dict):
            raise PokeAPIError("PokéAPI results contains a non-object item")
        name = item.get("name")
        if not isinstance(name, str) or not name.strip():
            raise PokeAPIError("PokéAPI result has invalid name")
        identifier, canonical_url = _canonical_pokemon_url(item.get("url"))
        if identifier in seen_ids or identifier in page_ids:
            raise PokeAPIError(f"PokéAPI response contains duplicate Pokémon id {identifier}")
        page_ids.add(identifier)
        page_records.append(
            {
                "source": SOURCE,
                "fetched_at": fetched_at,
                "id": identifier,
                "name": name.strip(),
                "url": canonical_url,
            }
        )

    if next_url is not None:
        next_limit, next_offset = _catalog_parameters(next_url, "PokéAPI next URL")
        expected_offset = current_offset + page_size
        if next_limit != page_size or next_offset != expected_offset:
            raise PokeAPIError(
                "PokéAPI next URL does not continue the requested catalog pagination"
            )
    return page_records, next_url


def fetch_pokemon(
    resource: str,
    page_size: int,
    *,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: float = REQUEST_TIMEOUT,
    opener: Any | None = None,
    sleeper: Callable[[float], None] = time.sleep,
) -> FetchResult:
    """Fetch and validate up to ``max_pages`` catalog pages without detail requests."""
    if resource != RESOURCE:
        raise ValueError("resource must be 'pokemon'")
    _bounded_int(page_size, "page_size", 1, MAX_PAGE_SIZE)
    _bounded_int(max_pages, "max_pages", 1, HARD_MAX_PAGES)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ValueError("timeout must be a positive number")

    http = opener or urllib.request.build_opener(CatalogRedirectHandler())
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    offset = 0
    url = _catalog_url(page_size, offset)

    for page_number in range(1, max_pages + 1):
        document = _read_document(http, url, float(timeout))
        _, offset = _catalog_parameters(url, "PokéAPI request")
        page_records, next_url = _parse_page(
            document,
            fetched_at=fetched_at,
            page_size=page_size,
            current_offset=offset,
            seen_ids=seen_ids,
        )
        records.extend(page_records)
        seen_ids.update(record["id"] for record in page_records)

        if next_url is None:
            return FetchResult(records, False)
        if page_number == max_pages:
            return FetchResult(records, True)
        sleeper(REQUEST_INTERVAL)
        url = next_url

    raise AssertionError("unreachable")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("resource", choices=(RESOURCE,))
    parser.add_argument("page_size", type=int)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = fetch_pokemon(
            args.resource,
            args.page_size,
            max_pages=args.max_pages,
        )
    except ValueError as exc:
        parser.error(str(exc))
    except PokeAPIError as exc:
        print(f"{SOURCE}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    for record in result.records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    if result.truncated:
        print(
            f"{SOURCE}: truncated after max-pages={args.max_pages}; "
            "PokéAPI reported another catalog page",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
