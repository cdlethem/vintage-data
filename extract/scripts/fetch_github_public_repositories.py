#!/usr/bin/env python3
"""Inventory repositories for one explicitly configured public GitHub organization.

The unauthenticated REST endpoint exposes only repositories visible to the caller. Each
run is therefore an organization-only snapshot, not an inventory of every repository on
GitHub. Pagination is not atomic: repositories may change or move while later pages are
being fetched. ``fetched_at`` identifies the collection, and ``raw_repository`` retains
the complete GitHub object carried by each normalized record.

HTTP and response failures are buffered and produce no NDJSON. An explicit page cap may
produce a useful partial inventory, but it is diagnosed as incomplete on stderr and the
CLI exits nonzero. Scheduled configuration must use a bound large enough to reach the
terminal page rather than silently treating a capped result as complete.

Stdlib only; no token is read or sent.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
import re
import sys
import time
from typing import Any, Callable, Iterator, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request


SOURCE = "github_public_repositories"
API_ORIGIN = "https://api.github.com"
API_VERSION = "2022-11-28"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_ORGANIZATION = "github"
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 20
HARD_MAX_PAGES = 100
DEFAULT_TIMEOUT = 30.0
MAX_TIMEOUT = 120.0
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
MAX_RETRY_DELAY = 60.0
TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
ORGANIZATION_RE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?")


class GitHubInventoryError(RuntimeError):
    """GitHub cannot satisfy the repository inventory contract."""


class GitHubResponseError(GitHubInventoryError):
    """A GitHub response or pagination link is malformed or unsafe."""


class GitHubRateLimitError(GitHubInventoryError):
    """The anonymous GitHub rate limit prevents bounded completion."""


@dataclass(frozen=True)
class Inventory:
    """A buffered inventory and its explicit completeness state."""

    records: tuple[dict[str, Any], ...]
    pages_fetched: int
    complete: bool
    next_url: str | None = None

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter(self.records)


@dataclass(frozen=True)
class JsonPage:
    document: Any
    headers: Mapping[str, str]


def _bounded_int(value: Any, field: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{field} must be an integer between {minimum} and {maximum}")
    return value


def validate_organization(value: Any) -> str:
    if not isinstance(value, str) or ORGANIZATION_RE.fullmatch(value) is None:
        raise ValueError("organization must be a valid GitHub organization login")
    return value


def _body_context(body: bytes) -> str:
    return body[:500].decode("utf-8", errors="replace").replace("\n", " ")


def _header(headers: Mapping[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _response_headers(response: Any) -> dict[str, str]:
    headers = getattr(response, "headers", {})
    return {str(key): str(value) for key, value in headers.items()}


class HttpClient:
    """Bounded GitHub JSON client with transient and rate-limit retries."""

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        *,
        opener: Callable[..., Any] | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or not 1 <= timeout <= MAX_TIMEOUT
        ):
            raise ValueError(f"timeout must be between 1 and {MAX_TIMEOUT:g} seconds")
        _bounded_int(retries, "retries", 0, MAX_RETRIES)
        self.timeout = float(timeout)
        self.retries = retries
        self.opener = urllib.request.urlopen if opener is None else opener
        self.sleeper = sleeper
        self.wall_clock = wall_clock

    def _retry_delay(self, headers: Mapping[str, str], attempt: int) -> float:
        retry_after = _header(headers, "Retry-After")
        delay: float | None = None
        if retry_after:
            try:
                candidate = float(retry_after)
                delay = candidate if math.isfinite(candidate) else None
            except ValueError:
                try:
                    retry_at = parsedate_to_datetime(retry_after)
                    if retry_at.tzinfo is None:
                        retry_at = retry_at.replace(tzinfo=timezone.utc)
                    candidate = retry_at.timestamp() - self.wall_clock()
                    delay = candidate if math.isfinite(candidate) else None
                except (TypeError, ValueError, OverflowError):
                    delay = None
        if delay is None and _header(headers, "X-RateLimit-Remaining") == "0":
            reset = _header(headers, "X-RateLimit-Reset")
            try:
                candidate = float(reset) - self.wall_clock() if reset is not None else None
                delay = candidate if candidate is not None and math.isfinite(candidate) else None
            except ValueError:
                delay = None
        if delay is None:
            delay = float(2**attempt)
        return max(0.0, delay)

    def get_json(self, url: str) -> JsonPage:
        for attempt in range(self.retries + 1):
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/vnd.github+json",
                    "User-Agent": USER_AGENT,
                    "X-GitHub-Api-Version": API_VERSION,
                },
            )
            rate_limited = False
            try:
                with self.opener(request, timeout=self.timeout) as response:
                    final_url = getattr(response, "geturl", lambda: url)()
                    if final_url != url:
                        raise GitHubResponseError(
                            "GitHub request was redirected away from its exact page URL"
                        )
                    body = response.read()
                    headers = _response_headers(response)
            except urllib.error.HTTPError as error:
                body = error.read()
                headers = _response_headers(error)
                error.close()
                rate_limited = error.code == 429 or (
                    error.code == 403
                    and (
                        _header(headers, "X-RateLimit-Remaining") == "0"
                        or _header(headers, "Retry-After") is not None
                        or b"rate limit" in body.lower()
                    )
                )
                if error.code not in TRANSIENT_STATUS and not rate_limited:
                    raise GitHubInventoryError(
                        f"GitHub request failed: status={error.code} body={_body_context(body)!r}"
                    ) from error
                failure: BaseException = error
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                headers = {}
                failure = error
            else:
                try:
                    return JsonPage(json.loads(body), headers)
                except (json.JSONDecodeError, UnicodeDecodeError) as error:
                    raise GitHubResponseError(
                        f"GitHub response is not JSON: body={_body_context(body)!r}"
                    ) from error

            if attempt == self.retries:
                category = "rate limit" if rate_limited else "request"
                error_type = GitHubRateLimitError if rate_limited else GitHubInventoryError
                raise error_type(
                    f"GitHub {category} failed after {self.retries + 1} attempts: {failure}"
                ) from failure
            delay = self._retry_delay(headers, attempt)
            if delay > MAX_RETRY_DELAY:
                error_type = GitHubRateLimitError if rate_limited else GitHubInventoryError
                raise error_type(
                    f"GitHub retry delay {delay:.0f}s exceeds bounded maximum "
                    f"of {MAX_RETRY_DELAY:.0f}s"
                ) from failure
            self.sleeper(delay)
        raise AssertionError("unreachable")
 
 
def _validate_page_url(
    url: Any, organization: str, page_size: int, expected_page: int
) -> str:
    if not isinstance(url, str) or not url:
        raise GitHubResponseError("GitHub next link must contain a URL")
    parsed = urllib.parse.urlsplit(url)
    expected_path = f"/orgs/{urllib.parse.quote(organization, safe='')}/repos"
    try:
        unsafe_destination = (
            parsed.scheme != "https"
            or parsed.hostname != "api.github.com"
            or parsed.port not in (None, 443)
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path != expected_path
            or bool(parsed.fragment)
        )
    except ValueError as error:
        raise GitHubResponseError(
            "GitHub next link leaves the configured organization endpoint"
        ) from error
    if unsafe_destination:
        raise GitHubResponseError("GitHub next link leaves the configured organization endpoint")
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    if set(query) - {"page", "per_page", "type"}:
        raise GitHubResponseError("GitHub next link contains unexpected query parameters")
    if query.get("per_page") != [str(page_size)] or query.get("type") != ["public"]:
        raise GitHubResponseError("GitHub next link changes the repository request scope")
    if query.get("page") != [str(expected_page)]:
        raise GitHubResponseError("GitHub next link does not identify the consecutive page")
    return urllib.parse.urlunsplit(parsed)
 
 
def _next_link(
    header: str | None, organization: str, page_size: int, expected_page: int
) -> str | None:
    if header is None or not header.strip():
        return None
    next_urls: list[str] = []
    for part in header.split(","):
        match = re.fullmatch(r"\s*<([^<>]+)>\s*((?:;\s*[^;=,]+\s*=\s*(?:\"[^\"]*\"|[^;,\s]+))*)\s*", part)
        if match is None:
            raise GitHubResponseError("GitHub Link header is malformed")
        parameters = match.group(2)
        relations: set[str] = set()
        for parameter in parameters.split(";"):
            parameter = parameter.strip()
            if not parameter:
                continue
            name, separator, value = parameter.partition("=")
            if not separator:
                raise GitHubResponseError("GitHub Link header parameter is malformed")
            if name.strip().lower() == "rel":
                relations.update(value.strip().strip('"').split())
        if "next" in relations:
            next_urls.append(match.group(1))
    if len(next_urls) > 1:
        raise GitHubResponseError("GitHub Link header contains multiple next destinations")
    if not next_urls:
        return None
    return _validate_page_url(
        next_urls[0], organization, page_size, expected_page
    )


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GitHubResponseError(f"GitHub repository has invalid {field}")
    return value.strip()


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise GitHubResponseError(f"GitHub repository has invalid {field}")
    return value


def _optional_bool(value: Any, field: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise GitHubResponseError(f"GitHub repository has invalid {field}")
    return value


def _optional_count(value: Any, field: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GitHubResponseError(f"GitHub repository has invalid {field}")
    return value


def _optional_timestamp(value: Any, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise GitHubResponseError(f"GitHub repository has invalid {field}")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        timestamp = datetime.fromisoformat(text)
    except ValueError as error:
        raise GitHubResponseError(f"GitHub repository has invalid {field}") from error
    if timestamp.tzinfo is None:
        raise GitHubResponseError(f"GitHub repository has timezone-free {field}")
    return timestamp.astimezone(timezone.utc).isoformat()


def normalize_repository(row: Any, organization: str, fetched_at: str) -> dict[str, Any]:
    """Validate and normalize one GitHub repository without losing its wire object."""
    if not isinstance(row, dict):
        raise GitHubResponseError("GitHub repository response contains a non-object item")
    repository_id = row.get("id")
    if isinstance(repository_id, bool) or not isinstance(repository_id, int) or repository_id <= 0:
        raise GitHubResponseError("GitHub repository has invalid id")
    owner = row.get("owner")
    owner_login = None
    if owner is not None:
        if not isinstance(owner, dict):
            raise GitHubResponseError("GitHub repository has invalid owner")
        owner_login = _optional_string(owner.get("login"), "owner.login")

    private = _optional_bool(row.get("private"), "private")
    visibility = _optional_string(row.get("visibility"), "visibility")
    if visibility is None and private is not None:
        visibility = "private" if private else "public"

    return {
        "source": SOURCE,
        "id": str(repository_id),
        "fetched_at": fetched_at,
        "repository_id": repository_id,
        "node_id": _optional_string(row.get("node_id"), "node_id"),
        "organization": organization,
        "name": _required_string(row.get("name"), "name"),
        "full_name": _required_string(row.get("full_name"), "full_name"),
        "owner_login": owner_login,
        "private": private,
        "visibility": visibility,
        "fork": _optional_bool(row.get("fork"), "fork"),
        "archived": _optional_bool(row.get("archived"), "archived"),
        "disabled": _optional_bool(row.get("disabled"), "disabled"),
        "language": _optional_string(row.get("language"), "language"),
        "description": _optional_string(row.get("description"), "description"),
        "created_at": _optional_timestamp(row.get("created_at"), "created_at"),
        "updated_at": _optional_timestamp(row.get("updated_at"), "updated_at"),
        "pushed_at": _optional_timestamp(row.get("pushed_at"), "pushed_at"),
        "has_issues": _optional_bool(row.get("has_issues"), "has_issues"),
        "has_projects": _optional_bool(row.get("has_projects"), "has_projects"),
        "has_wiki": _optional_bool(row.get("has_wiki"), "has_wiki"),
        "has_pages": _optional_bool(row.get("has_pages"), "has_pages"),
        "has_discussions": _optional_bool(row.get("has_discussions"), "has_discussions"),
        "open_issues_count": _optional_count(row.get("open_issues_count"), "open_issues_count"),
        "stargazers_count": _optional_count(row.get("stargazers_count"), "stargazers_count"),
        "watchers_count": _optional_count(row.get("watchers_count"), "watchers_count"),
        "forks_count": _optional_count(row.get("forks_count"), "forks_count"),
        "html_url": _optional_string(row.get("html_url"), "html_url"),
        "api_url": _optional_string(row.get("url"), "url"),
        "clone_url": _optional_string(row.get("clone_url"), "clone_url"),
        "git_url": _optional_string(row.get("git_url"), "git_url"),
        "ssh_url": _optional_string(row.get("ssh_url"), "ssh_url"),
        "svn_url": _optional_string(row.get("svn_url"), "svn_url"),
        "homepage": _optional_string(row.get("homepage"), "homepage"),
        "raw_repository": row,
    }


def fetch_github_public_repositories(
    organization: str = DEFAULT_ORGANIZATION,
    *,
    max_pages: int = DEFAULT_MAX_PAGES,
    page_size: int = DEFAULT_PAGE_SIZE,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    client: HttpClient | None = None,
    fetched_at: str | None = None,
) -> Inventory:
    """Fetch terminal Link pagination or return an explicitly partial capped inventory."""
    organization = validate_organization(organization)
    _bounded_int(max_pages, "max_pages", 1, HARD_MAX_PAGES)
    _bounded_int(page_size, "page_size", 1, DEFAULT_PAGE_SIZE)
    if client is None:
        client = HttpClient(timeout, retries)
    elif not isinstance(client, HttpClient):
        raise TypeError("client must be an HttpClient")
    collection_time = fetched_at or datetime.now(timezone.utc).isoformat()
    collection_time = _optional_timestamp(collection_time, "fetched_at")
    assert collection_time is not None

    query = urllib.parse.urlencode({"type": "public", "per_page": page_size, "page": 1})
    url = f"{API_ORIGIN}/orgs/{urllib.parse.quote(organization, safe='')}/repos?{query}"
    records: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    seen_urls: set[str] = set()

    for page_number in range(1, max_pages + 1):
        if url in seen_urls:
            raise GitHubResponseError("GitHub pagination repeated a page URL")
        seen_urls.add(url)
        page = client.get_json(url)
        if not isinstance(page.document, list):
            raise GitHubResponseError("GitHub repositories response must be a JSON list")
        if len(page.document) > page_size:
            raise GitHubResponseError("GitHub repositories response exceeds the requested page size")
        for row in page.document:
            record = normalize_repository(row, organization, collection_time)
            repository_id = record["repository_id"]
            if repository_id in seen_ids:
                continue
            seen_ids.add(repository_id)
            records.append(record)
        next_url = _next_link(
            _header(page.headers, "Link"), organization, page_size, page_number + 1
        )
        if next_url is None:
            return Inventory(tuple(records), page_number, True)
        url = next_url

    return Inventory(tuple(records), max_pages, False, url)


def _diagnostic(inventory: Inventory, organization: str, max_pages: int) -> dict[str, Any]:
    diagnostic: dict[str, Any] = {
        "source": SOURCE,
        "organization": organization,
        "status": "complete" if inventory.complete else "partial",
        "pages_fetched": inventory.pages_fetched,
        "records": len(inventory.records),
    }
    if not inventory.complete:
        diagnostic["reason"] = f"page cap max_pages={max_pages} reached before terminal pagination"
        diagnostic["next_url"] = inventory.next_url
    return diagnostic


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inventory public repositories for one configured GitHub organization."
    )
    parser.add_argument("--org", default=DEFAULT_ORGANIZATION, dest="organization")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    args = parser.parse_args(argv)

    try:
        inventory = fetch_github_public_repositories(
            args.organization,
            max_pages=args.max_pages,
            page_size=args.page_size,
            timeout=args.timeout,
            retries=args.retries,
        )
    except ValueError as error:
        parser.error(str(error))
    except GitHubInventoryError as error:
        print(
            json.dumps(
                {
                    "source": SOURCE,
                    "organization": args.organization,
                    "status": "failed",
                    "error": str(error),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1

    for record in inventory:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    print(
        json.dumps(
            _diagnostic(inventory, args.organization, args.max_pages),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )
    return 0 if inventory.complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
