#!/usr/bin/env python3
"""Fetch a bounded ranked sample of public projects from GitLab.

The GitLab projects endpoint is mutable while page-number pagination is in progress.
Consequently, ordering plus ``per_page`` and ``max_pages`` defines a bounded sample,
not a consistent or exhaustive snapshot. All requested pages are buffered and
validated before any NDJSON is emitted, so a failed later page cannot publish a
successful partial batch. The endpoint is public and this client sends no token.
Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import math
import os
import sys
import time
from typing import Any, Callable, Sequence
import urllib.error
import urllib.parse
import urllib.request

SOURCE = "gitlab_public_projects"
API_URL = "https://gitlab.com/api/v4/projects"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

DEFAULT_PER_PAGE = 50
MAX_PER_PAGE = 100
DEFAULT_MAX_PAGES = 3
HARD_MAX_PAGES = 10
DEFAULT_TIMEOUT = 20.0
MAX_TIMEOUT = 120.0
DEFAULT_RETRIES = 2
MAX_RETRIES = 5
DEFAULT_RETRY_BUDGET = 180.0
MAX_RETRY_BUDGET = 600.0
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_RATE_LIMIT_DELAY = 120.0
ORDER_BY_CHOICES = (
    "id",
    "name",
    "path",
    "created_at",
    "updated_at",
    "last_activity_at",
    "star_count",
)
SORT_CHOICES = ("asc", "desc")
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class GitLabError(RuntimeError):
    """The GitLab request or response cannot satisfy the extractor contract."""


class RetryBudgetError(GitLabError):
    """The finite retry budget cannot accommodate another attempt or delay."""


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


def _required_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GitLabError(f"GitLab project has invalid {field}")
    return value.strip()


def _timestamp(value: Any, field: str) -> str:
    text = _required_string(value, field)
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise GitLabError(f"GitLab project has invalid {field}") from exc
    if parsed.tzinfo is None:
        raise GitLabError(f"GitLab project has timezone-free {field}")
    return parsed.astimezone(timezone.utc).isoformat()


def _nonnegative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GitLabError(f"GitLab project has invalid {field}")
    return value


def normalize_project(project: Any, fetched_at: str) -> dict[str, Any]:
    """Validate and normalize one public GitLab project while retaining its API object."""
    if not isinstance(project, dict):
        raise GitLabError("GitLab projects response contains a non-object project")

    project_id = project.get("id")
    if isinstance(project_id, bool) or not isinstance(project_id, int) or project_id <= 0:
        raise GitLabError("GitLab project has invalid id")
    visibility = _required_string(project.get("visibility"), "visibility")
    if visibility != "public":
        raise GitLabError(f"GitLab project {project_id} is not public")

    namespace = project.get("namespace")
    if not isinstance(namespace, dict):
        raise GitLabError(f"GitLab project {project_id} has invalid namespace")
    namespace_path = _required_string(namespace.get("full_path"), "namespace.full_path")

    topics = project.get("topics")
    if not isinstance(topics, list) or any(
        not isinstance(topic, str) or not topic.strip() for topic in topics
    ):
        raise GitLabError(f"GitLab project {project_id} has invalid topics")
    normalized_topics = [topic.strip() for topic in topics]

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": str(project_id),
        "name": _required_string(project.get("name"), "name"),
        "path": _required_string(project.get("path"), "path"),
        "path_with_namespace": _required_string(
            project.get("path_with_namespace"), "path_with_namespace"
        ),
        "namespace_path": namespace_path,
        "web_url": _required_string(project.get("web_url"), "web_url"),
        "http_url_to_repo": _required_string(
            project.get("http_url_to_repo"), "http_url_to_repo"
        ),
        "ssh_url_to_repo": _required_string(project.get("ssh_url_to_repo"), "ssh_url_to_repo"),
        "visibility": visibility,
        "created_at": _timestamp(project.get("created_at"), "created_at"),
        "star_count": _nonnegative_int(project.get("star_count"), "star_count"),
        "forks_count": _nonnegative_int(project.get("forks_count"), "forks_count"),
        "topics": normalized_topics,
        "last_activity_at": _timestamp(
            project.get("last_activity_at"), "last_activity_at"
        ),
        "raw": dict(project),
    }


def _header(headers: Any, name: str) -> str | None:
    if headers is None:
        return None
    value = headers.get(name)
    return str(value).strip() if value is not None else None


def _rate_limit_delay(error: urllib.error.HTTPError, now: datetime) -> float | None:
    retry_after = _header(error.headers, "Retry-After")
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            try:
                retry_time = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError, OverflowError):
                pass
            else:
                if retry_time.tzinfo is None:
                    retry_time = retry_time.replace(tzinfo=timezone.utc)
                return max(0.0, (retry_time.astimezone(timezone.utc) - now).total_seconds())

    reset = _header(error.headers, "RateLimit-Reset")
    if reset:
        try:
            return max(0.0, float(reset) - now.timestamp())
        except ValueError:
            pass
    return None


class GitLabClient:
    """Small unauthenticated client with bounded attempts and one run-wide time budget."""

    def __init__(
        self,
        *,
        timeout: float,
        retries: int,
        retry_budget: float,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.clock = clock or time.monotonic
        self.sleeper = sleeper or time.sleep
        self.deadline = self.clock() + retry_budget

    def _remaining(self) -> float:
        return self.deadline - self.clock()

    def get_page(self, parameters: dict[str, Any]) -> tuple[list[Any], Any]:
        url = API_URL + "?" + urllib.parse.urlencode(parameters)
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )

        for attempt in range(self.retries + 1):
            remaining = self._remaining()
            if remaining <= 0:
                raise RetryBudgetError("GitLab retry budget exhausted before request")
            try:
                with urllib.request.urlopen(
                    request, timeout=min(self.timeout, remaining)
                ) as response:
                    body = response.read(MAX_RESPONSE_BYTES + 1)
                    headers = response.headers
            except urllib.error.HTTPError as exc:
                try:
                    if exc.code not in RETRYABLE_STATUS:
                        raise GitLabError(f"GitLab request failed with HTTP {exc.code}") from exc
                    failure: BaseException = exc
                    delay = _rate_limit_delay(exc, datetime.now(timezone.utc))
                finally:
                    exc.close()
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                failure = exc
                delay = None
            else:
                if len(body) > MAX_RESPONSE_BYTES:
                    raise GitLabError("GitLab response exceeded the byte limit")
                try:
                    document = json.loads(body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise GitLabError("GitLab returned malformed JSON") from exc
                if not isinstance(document, list):
                    raise GitLabError("GitLab projects response must be a JSON list")
                return document, headers

            if attempt == self.retries:
                raise GitLabError(
                    f"GitLab request failed after {self.retries + 1} attempts: {failure}"
                ) from failure
            if delay is None:
                delay = min(float(2**attempt), MAX_RATE_LIMIT_DELAY)
            elif delay > MAX_RATE_LIMIT_DELAY:
                raise RetryBudgetError(
                    f"GitLab rate-limit delay {delay:g}s exceeds the local "
                    f"{MAX_RATE_LIMIT_DELAY:g}s cap"
                ) from failure
            if delay >= self._remaining():
                raise RetryBudgetError("GitLab retry delay exceeds the remaining retry budget") from failure
            if delay:
                self.sleeper(delay)

        raise AssertionError("unreachable")


def _next_page(headers: Any, requested_page: int) -> int | None:
    value = _header(headers, "X-Next-Page")
    if value is None:
        return requested_page + 1
    if value == "":
        return None
    try:
        next_page = int(value)
    except ValueError as exc:
        raise GitLabError("GitLab X-Next-Page header must be an integer or empty") from exc
    if next_page != requested_page + 1:
        raise GitLabError(
            f"GitLab X-Next-Page {next_page} does not follow requested page {requested_page}"
        )
    return next_page


def fetch_public_projects(
    *,
    per_page: int = DEFAULT_PER_PAGE,
    max_pages: int = DEFAULT_MAX_PAGES,
    order_by: str = "last_activity_at",
    sort: str = "desc",
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_budget: float = DEFAULT_RETRY_BUDGET,
    client: GitLabClient | None = None,
) -> list[dict[str, Any]]:
    """Return a validated bounded ranked sample, deduplicated by stable project ID."""
    _bounded_int(per_page, "per_page", 1, MAX_PER_PAGE)
    _bounded_int(max_pages, "max_pages", 1, HARD_MAX_PAGES)
    _bounded_int(retries, "retries", 0, MAX_RETRIES)
    timeout = _positive_number(timeout, "timeout", MAX_TIMEOUT)
    retry_budget = _positive_number(retry_budget, "retry_budget", MAX_RETRY_BUDGET)
    if order_by not in ORDER_BY_CHOICES:
        raise ValueError(f"order_by must be one of {', '.join(ORDER_BY_CHOICES)}")
    if sort not in SORT_CHOICES:
        raise ValueError(f"sort must be one of {', '.join(SORT_CHOICES)}")

    http = client or GitLabClient(
        timeout=timeout,
        retries=retries,
        retry_budget=retry_budget,
    )
    fetched_at = datetime.now(timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    page = 1

    while page <= max_pages:
        projects, headers = http.get_page(
            {
                "visibility": "public",
                "simple": "false",
                "order_by": order_by,
                "sort": sort,
                "per_page": per_page,
                "page": page,
            }
        )
        if len(projects) > per_page:
            raise GitLabError(
                f"GitLab page {page} returned {len(projects)} projects above per_page={per_page}"
            )
        page_records = [normalize_project(project, fetched_at) for project in projects]
        for record in page_records:
            if record["id"] not in seen_ids:
                seen_ids.add(record["id"])
                records.append(record)

        next_page = _next_page(headers, page)
        if not projects or len(projects) < per_page or next_page is None:
            break
        page = next_page

    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--order-by", choices=ORDER_BY_CHOICES, default="last_activity_at")
    parser.add_argument("--sort", choices=SORT_CHOICES, default="desc")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-budget", type=float, default=DEFAULT_RETRY_BUDGET)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        records = fetch_public_projects(
            per_page=args.per_page,
            max_pages=args.max_pages,
            order_by=args.order_by,
            sort=args.sort,
            timeout=args.timeout,
            retries=args.retries,
            retry_budget=args.retry_budget,
        )
    except ValueError as exc:
        parser.error(str(exc))
    except GitLabError as exc:
        print(f"{SOURCE}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
