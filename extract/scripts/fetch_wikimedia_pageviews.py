#!/usr/bin/env python3
"""Fetch explicitly configured Wikimedia per-article daily pageviews as bounded NDJSON.

Each ``--article LANGUAGE PROJECT TITLE`` is one request covering the most recent
``--days`` *completed* UTC days (default 2: yesterday and the day before). The
per-article endpoint publishes completed days only — it never returns the current
partial day — so a daily poll adds exactly one new day per run while the overlap
catches late revisions of the newest day.

The data is CC0 1.0 (Wikimedia Analytics API) and keyless. Each emitted record is
one (article, day) observation; ``id`` is a stable hash of the normalized request
identity plus the day, so re-fetching an overlapping day reproduces the same id
and downstream marts can keep the latest ``fetched_at`` per id.

The extractor never follows links or HTTP redirects, and it buffers and validates
every configured response before writing stdout so a failed request cannot publish
a partial batch.

Stdlib only.
"""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Sequence
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

SOURCE = "wikimedia_pageviews"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
BASE = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
GRANULARITY = "daily"  # per-article supports daily and monthly; only daily is polled

PROJECTS = frozenset(
    {
        "wikipedia",
        "wiktionary",
        "wikibooks",
        "wikiquote",
        "wikinews",
        "wikiversity",
        "wikivoyage",
    }
)
ACCESS_VALUES = ("all-access", "desktop", "mobile", "tablet")
AGENT_VALUES = ("all-agents", "user", "automated", "spider")

LANGUAGE_RE = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
TIMESTAMP_RE = re.compile(r"^\d{8}00$")
MAX_LANGUAGE_BYTES = 32
MAX_TITLE_BYTES = 512
MAX_ARTICLES = 20
DEFAULT_DAYS = 2
MAX_DAYS = 31
DEFAULT_TIMEOUT = 15.0
MAX_TIMEOUT = 60.0
DEFAULT_RETRIES = 2
MAX_RETRIES = 3
DEFAULT_RETRY_BUDGET = 90.0
MAX_RETRY_BUDGET = 180.0
MAX_RESPONSE_BYTES = 1024 * 1024
MAX_BACKOFF = 8.0
RETRYABLE_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class WikimediaError(RuntimeError):
    """A request or response cannot satisfy the extractor contract."""


class RetryBudgetError(WikimediaError):
    """The finite run-wide request budget cannot accommodate another attempt."""


class RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Turn redirects into HTTP errors rather than following an untrusted target."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        return None


@dataclass(frozen=True)
class ArticleRequest:
    language: str
    project: str
    requested_title: str
    access: str
    agent: str
    identity: str

    @property
    def project_segment(self) -> str:
        return f"{self.language}.{self.project}"

    def url(self, start: str, end: str) -> str:
        title = urllib.parse.quote(self.requested_title, safe="")
        return (
            f"{BASE}/{self.project_segment}/{self.access}/{self.agent}/"
            f"{title}/{GRANULARITY}/{start}/{end}"
        )


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _positive_number(value: Any, name: str, maximum: float) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not 0 < value <= maximum
    ):
        raise ValueError(f"{name} must be a number in (0, {maximum}]")
    return float(value)


def normalize_language(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("language must be a string")
    language = value.strip().lower()
    if not language or len(language.encode("utf-8")) > MAX_LANGUAGE_BYTES:
        raise ValueError(f"language must be 1-{MAX_LANGUAGE_BYTES} bytes")
    if not LANGUAGE_RE.match(language):
        raise ValueError("language must be a lowercase Wikimedia language code")
    return language


def normalize_project(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("project must be a string")
    project = value.strip().lower()
    if project not in PROJECTS:
        raise ValueError(f"project must be one of {sorted(PROJECTS)}")
    return project


def normalize_title(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("title must be a string")
    title = unicodedata.normalize("NFC", value).strip()
    title = re.sub(r"[ _]+", "_", title)
    if not title:
        raise ValueError("title must not be empty")
    if title in {".", ".."}:
        raise ValueError("title must not be a URL dot segment")
    if any(unicodedata.category(character) in {"Cc", "Cs"} for character in title):
        raise ValueError("title must not contain control or surrogate characters")
    if len(title.encode("utf-8")) > MAX_TITLE_BYTES:
        raise ValueError(f"title must be at most {MAX_TITLE_BYTES} UTF-8 bytes")
    return title


def _choice(value: Any, name: str, allowed: tuple[str, ...]) -> str:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{name} must be one of {list(allowed)}")
    return value


def _identity(language: str, project: str, title: str, access: str, agent: str) -> str:
    document = json.dumps(
        {
            "access": access,
            "agent": agent,
            "article": title,
            "granularity": GRANULARITY,
            "language": language,
            "project": project,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(document.encode("utf-8")).hexdigest()


def make_article_request(language: Any, project: Any, title: Any, access: Any, agent: Any) -> ArticleRequest:
    return ArticleRequest(
        language=normalize_language(language),
        project=normalize_project(project),
        requested_title=normalize_title(title),
        access=_choice(access, "access", ACCESS_VALUES),
        agent=_choice(agent, "agent", AGENT_VALUES),
        identity=_identity(
            normalize_language(language),
            normalize_project(project),
            normalize_title(title),
            _choice(access, "access", ACCESS_VALUES),
            _choice(agent, "agent", AGENT_VALUES),
        ),
    )


def _required_string(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if (
        not isinstance(value, str)
        or (not allow_empty and not value.strip())
        or any(ch in value for ch in "\r\n\0")
    ):
        raise WikimediaError(f"pageviews response has invalid {field}")
    return value if allow_empty else value.strip()


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WikimediaError(f"pageviews response has invalid {field}")
    return value


def normalize_view(
    item: Any,
    article: ArticleRequest,
    *,
    fetched_at: str,
    url: str,
    start: str,
    end: str,
) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise WikimediaError("pageviews response item is not an object")
    project = _required_string(item.get("project"), "project")
    if project != article.project_segment:
        raise WikimediaError("pageviews response project does not match the request")
    title = _required_string(item.get("article"), "article")
    if title != article.requested_title:
        raise WikimediaError("pageviews response article does not match the request")
    granularity = _required_string(item.get("granularity"), "granularity")
    if granularity != GRANULARITY:
        raise WikimediaError("pageviews response granularity is not daily")
    access = _required_string(item.get("access"), "access")
    if access != article.access:
        raise WikimediaError("pageviews response access does not match the request")
    agent = _required_string(item.get("agent"), "agent")
    if agent != article.agent:
        raise WikimediaError("pageviews response agent does not match the request")
    timestamp = _required_string(item.get("timestamp"), "timestamp")
    if not TIMESTAMP_RE.match(timestamp):
        raise WikimediaError("pageviews response timestamp is not a daily marker")
    day = timestamp[:8]
    if not start <= day <= end:
        raise WikimediaError("pageviews response day is outside the requested window")
    views = _positive_int(item.get("views"), "views")

    record_id = hashlib.sha256(
        json.dumps(
            {
                "access": access,
                "agent": agent,
                "article": title,
                "granularity": granularity,
                "language": article.language,
                "project": article.project,
                "timestamp": timestamp,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": record_id,
        "language": article.language,
        "project": article.project,
        "article": title,
        "access": access,
        "agent": agent,
        "granularity": granularity,
        "timestamp": timestamp,
        "date": f"{day[0:4]}-{day[4:6]}-{day[6:8]}",
        "views": views,
        "url": url,
    }


class WikimediaClient:
    """Bounded unauthenticated client that rejects redirects and retries finitely."""

    def __init__(
        self,
        *,
        timeout: float,
        retries: int,
        retry_budget: float,
        opener: Callable[..., Any] | None = None,
        clock: Callable[[], float] | None = None,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.clock = clock or time.monotonic
        self.sleeper = sleeper or time.sleep
        self.deadline = self.clock() + retry_budget
        self.opener = opener or urllib.request.build_opener(RejectRedirects()).open

    def _remaining(self) -> float:
        return self.deadline - self.clock()

    def get(self, url: str) -> Any:
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )

        for attempt in range(self.retries + 1):
            remaining = self._remaining()
            if remaining <= 0:
                raise RetryBudgetError("Wikimedia retry budget exhausted before request")
            try:
                with self.opener(request, timeout=min(self.timeout, remaining)) as response:
                    final_url = response.geturl() if hasattr(response, "geturl") else url
                    if final_url != url:
                        raise WikimediaError("Wikimedia HTTP redirect was rejected")
                    body = response.read(MAX_RESPONSE_BYTES + 1)
            except urllib.error.HTTPError as exc:
                try:
                    if 300 <= exc.code < 400:
                        raise WikimediaError("Wikimedia HTTP redirect was rejected") from exc
                    if exc.code not in RETRYABLE_STATUS:
                        raise WikimediaError(f"Wikimedia request failed with HTTP {exc.code}") from exc
                    failure: BaseException = exc
                finally:
                    exc.close()
            except WikimediaError:
                raise
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                failure = exc
            else:
                if len(body) > MAX_RESPONSE_BYTES:
                    raise WikimediaError("Wikimedia response exceeded the byte limit")
                try:
                    return json.loads(body.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                    raise WikimediaError("Wikimedia returned malformed JSON") from exc

            if attempt == self.retries:
                raise WikimediaError(
                    f"Wikimedia request failed after {self.retries + 1} attempts: "
                    f"{type(failure).__name__}"
                ) from failure
            delay = min(float(2**attempt), MAX_BACKOFF)
            if delay >= self._remaining():
                raise RetryBudgetError(
                    "Wikimedia retry delay exceeds the remaining retry budget"
                ) from failure
            if delay:
                self.sleeper(delay)

        raise AssertionError("unreachable")


def fetch_pageviews(
    articles: Sequence[Sequence[str]],
    *,
    access: str = "all-access",
    agent: str = "user",
    days: int = DEFAULT_DAYS,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_budget: float = DEFAULT_RETRY_BUDGET,
    client: WikimediaClient | None = None,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(articles, (list, tuple)) or not articles:
        raise ValueError("at least one --article LANGUAGE PROJECT TITLE is required")
    if len(articles) > MAX_ARTICLES:
        raise ValueError(f"at most {MAX_ARTICLES} article titles may be requested")
    days = _bounded_int(days, "days", 1, MAX_DAYS)
    _bounded_int(retries, "retries", 0, MAX_RETRIES)
    timeout = _positive_number(timeout, "timeout", MAX_TIMEOUT)
    retry_budget = _positive_number(retry_budget, "retry_budget", MAX_RETRY_BUDGET)
    _choice(access, "access", ACCESS_VALUES)
    _choice(agent, "agent", AGENT_VALUES)

    requests: list[ArticleRequest] = []
    seen: set[str] = set()
    for values in articles:
        if not isinstance(values, (list, tuple)) or len(values) != 3:
            raise ValueError("each article must contain language, project, and title")
        article = make_article_request(values[0], values[1], values[2], access, agent)
        if article.identity in seen:
            raise ValueError("duplicate normalized article identity")
        seen.add(article.identity)
        requests.append(article)

    current = (now or (lambda: datetime.now(timezone.utc)))()
    if current.tzinfo is None:
        raise ValueError("now must return a timezone-aware datetime")
    # The per-article endpoint only publishes completed UTC days; the most recent
    # completed day is yesterday, so end the window there.
    end = current.astimezone(timezone.utc).date() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    start_s = start.strftime("%Y%m%d")
    end_s = end.strftime("%Y%m%d")
    fetched_at = current.astimezone(timezone.utc).isoformat()
    http = client or WikimediaClient(
        timeout=timeout,
        retries=retries,
        retry_budget=retry_budget,
    )

    records: list[dict[str, Any]] = []
    for article in requests:
        url = article.url(start_s, end_s)
        document = http.get(url)
        if not isinstance(document, dict):
            raise WikimediaError("pageviews response is not an object")
        items = document.get("items")
        if not isinstance(items, list):
            raise WikimediaError("pageviews response is missing an items list")
        article_records: list[dict[str, Any]] = []
        seen_days: set[str] = set()
        for item in items:
            record = normalize_view(
                item,
                article,
                fetched_at=fetched_at,
                url=url,
                start=start_s,
                end=end_s,
            )
            if record["timestamp"] in seen_days:
                raise WikimediaError("pageviews response repeated a day")
            seen_days.add(record["timestamp"])
            article_records.append(record)
        records.extend(sorted(article_records, key=lambda r: r["timestamp"]))

    return records


def serialize_pageviews(records: Sequence[dict[str, Any]]) -> str:
    """Serialize and UTF-8 validate a complete batch before it reaches stdout."""
    try:
        payload = "".join(
            json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
            for record in records
        )
        payload.encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise WikimediaError("pageviews records could not be serialized as UTF-8") from exc
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--article",
        nargs=3,
        action="append",
        metavar=("LANGUAGE", "PROJECT", "TITLE"),
        required=True,
    )
    parser.add_argument("--access", default="all-access", choices=ACCESS_VALUES)
    parser.add_argument("--agent", default="user", choices=AGENT_VALUES)
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-budget", type=float, default=DEFAULT_RETRY_BUDGET)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        records = fetch_pageviews(
            args.article,
            access=args.access,
            agent=args.agent,
            days=args.days,
            timeout=args.timeout,
            retries=args.retries,
            retry_budget=args.retry_budget,
        )
        payload = serialize_pageviews(records)
    except (ValueError, WikimediaError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    sys.stdout.write(payload)


if __name__ == "__main__":
    main()
