#!/usr/bin/env python3
"""Fetch explicitly configured Wikimedia page summaries as bounded NDJSON.

Each ``--page LANGUAGE PROJECT TITLE`` is one request. The extractor never follows
links or HTTP redirects, and it buffers and validates every configured response
before writing stdout so a failed request cannot publish a partial batch.
Stdlib only.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from dataclasses import dataclass
import json
import math
import os
import re
import sys
import time
from typing import Any, Callable, Sequence
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

SOURCE = "wikimedia_page_summaries"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)

PROJECT_DOMAINS = {
    "wikipedia": "wikipedia.org",
    "wiktionary": "wiktionary.org",
    "wikibooks": "wikibooks.org",
    "wikiquote": "wikiquote.org",
    "wikinews": "wikinews.org",
    "wikiversity": "wikiversity.org",
    "wikivoyage": "wikivoyage.org",
}
LANGUAGE_RE = re.compile(r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$")
MAX_LANGUAGE_BYTES = 32
MAX_TITLE_BYTES = 512
MAX_TITLES = 20
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
class PageRequest:
    language: str
    project: str
    requested_title: str
    host: str
    url: str
    identity: str




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


def normalize_language(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("language must be a string")
    language = value.strip().lower()
    if (
        not LANGUAGE_RE.fullmatch(language)
        or len(language.encode("ascii", errors="ignore")) != len(language)
        or len(language.encode("ascii")) > MAX_LANGUAGE_BYTES
    ):
        raise ValueError("language must be a bounded ASCII Wikimedia language code")
    return language


def normalize_project(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("project must be a string")
    project = value.strip().lower()
    if project not in PROJECT_DOMAINS:
        raise ValueError(f"project must be one of {', '.join(PROJECT_DOMAINS)}")
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


def _identity(language: str, project: str, title: str) -> str:
    document = json.dumps(
        [language, project, title], ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(document).hexdigest()


def make_page_request(language: Any, project: Any, title: Any) -> PageRequest:
    normalized_language = normalize_language(language)
    normalized_project = normalize_project(project)
    normalized_title = normalize_title(title)
    host = f"{normalized_language}.{PROJECT_DOMAINS[normalized_project]}"
    if not _valid_host(host, expected=host):
        raise ValueError("derived Wikimedia host is invalid")
    encoded_title = urllib.parse.quote(normalized_title, safe="")
    url = f"https://{host}/api/rest_v1/page/summary/{encoded_title}"
    return PageRequest(
        language=normalized_language,
        project=normalized_project,
        requested_title=normalized_title,
        host=host,
        url=url,
        identity=_identity(normalized_language, normalized_project, normalized_title),
    )


def _valid_host(host: str | None, *, expected: str) -> bool:
    if not host or host.lower() != expected:
        return False
    try:
        return host == host.encode("idna").decode("ascii")
    except UnicodeError:
        return False


def _validated_https_url(value: Any, field: str, *, host: str) -> str:
    if not isinstance(value, str) or not value:
        raise WikimediaError(f"Wikimedia response has invalid {field}")
    parsed = urllib.parse.urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise WikimediaError(f"Wikimedia response has invalid {field}") from exc
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or port not in (None, 443)
        or not _valid_host(parsed.hostname, expected=host)
        or not parsed.path.startswith("/")
        or parsed.fragment
    ):
        raise WikimediaError(f"Wikimedia response has untrusted {field}")
    return value


def _required_string(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise WikimediaError(f"Wikimedia response has invalid {field}")
    return value if allow_empty else value.strip()


def _optional_string(value: Any, field: str) -> str | None:
    if value is None:
        return None
    return _required_string(value, field, allow_empty=True)


def _positive_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise WikimediaError(f"Wikimedia response has invalid {field}")
    return value


def _identifier(value: Any, field: str) -> str:
    if isinstance(value, bool):
        raise WikimediaError(f"Wikimedia response has invalid {field}")
    if isinstance(value, int) and value > 0:
        return str(value)
    if isinstance(value, str) and value.isascii() and value.isdigit() and int(value) > 0:
        return str(int(value))
    raise WikimediaError(f"Wikimedia response has invalid {field}")


def _timestamp(value: Any, field: str) -> str:
    text = _required_string(value, field)
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError as exc:
        raise WikimediaError(f"Wikimedia response has invalid {field}") from exc
    if parsed.tzinfo is None:
        raise WikimediaError(f"Wikimedia response has timezone-free {field}")
    return parsed.astimezone(timezone.utc).isoformat()

def _image(document: Any, field: str) -> tuple[str | None, int | None, int | None]:
    if document is None:
        return None, None, None
    if not isinstance(document, dict):
        raise WikimediaError(f"Wikimedia response has invalid {field}")
    return (
        _validated_https_url(document.get("source"), f"{field}.source", host="upload.wikimedia.org"),
        _positive_int(document.get("width"), f"{field}.width"),
        _positive_int(document.get("height"), f"{field}.height"),
    )


def normalize_summary(document: Any, page: PageRequest, fetched_at: str) -> dict[str, Any]:
    if not isinstance(document, dict):
        raise WikimediaError("Wikimedia summary response must be a JSON object")
    response_language = document.get("lang")
    if response_language is not None and response_language != page.language:
        raise WikimediaError("Wikimedia response language does not match the request")

    content_urls = document.get("content_urls")
    if not isinstance(content_urls, dict):
        raise WikimediaError("Wikimedia response has invalid content_urls")
    desktop = content_urls.get("desktop")
    mobile = content_urls.get("mobile")
    if not isinstance(desktop, dict) or not isinstance(mobile, dict):
        raise WikimediaError("Wikimedia response has invalid canonical URLs")

    title = _required_string(document.get("title"), "title")
    thumbnail_source, thumbnail_width, thumbnail_height = _image(
        document.get("thumbnail"), "thumbnail"
    )
    original_source, original_width, original_height = _image(
        document.get("originalimage"), "originalimage"
    )
    try:
        normalized_resolved_title = normalize_title(title)
    except ValueError as exc:
        raise WikimediaError("Wikimedia response has invalid title") from exc

    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": page.identity,
        "language": page.language,
        "project": page.project,
        "requested_title": page.requested_title,
        "title": title,
        "is_redirect": normalized_resolved_title != page.requested_title,
        "page_id": _positive_int(document.get("pageid"), "pageid"),
        "revision_id": _identifier(document.get("revision"), "revision"),
        "revision_timestamp": _timestamp(document.get("timestamp"), "timestamp"),
        "summary": _required_string(document.get("extract"), "extract", allow_empty=True),
        "description": _optional_string(document.get("description"), "description"),
        "wikibase_item": _optional_string(document.get("wikibase_item"), "wikibase_item"),
        "page_url": _validated_https_url(desktop.get("page"), "content_urls.desktop.page", host=page.host),
        "mobile_page_url": _validated_https_url(
            mobile.get("page"),
            "content_urls.mobile.page",
            host=f"{page.language}.m.{PROJECT_DOMAINS[page.project]}",
        ),
        "api_url": page.url,
        "thumbnail_source": thumbnail_source,
        "thumbnail_width": thumbnail_width,
        "thumbnail_height": thumbnail_height,
        "original_image_source": original_source,
        "original_image_width": original_width,
        "original_image_height": original_height,
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

    def get(self, page: PageRequest) -> Any:
        try:
            trusted_page = make_page_request(
                page.language, page.project, page.requested_title
            )
        except ValueError as exc:
            raise WikimediaError("refusing an invalid Wikimedia page request") from exc
        if page != trusted_page:
            raise WikimediaError("refusing an untrusted Wikimedia page request")
        request = urllib.request.Request(
            page.url,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )

        for attempt in range(self.retries + 1):
            remaining = self._remaining()
            if remaining <= 0:
                raise RetryBudgetError("Wikimedia retry budget exhausted before request")
            try:
                with self.opener(request, timeout=min(self.timeout, remaining)) as response:
                    final_url = response.geturl() if hasattr(response, "geturl") else page.url
                    if final_url != page.url:
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


def fetch_page_summaries(
    pages: Sequence[Sequence[str]],
    *,
    timeout: float = DEFAULT_TIMEOUT,
    retries: int = DEFAULT_RETRIES,
    retry_budget: float = DEFAULT_RETRY_BUDGET,
    client: WikimediaClient | None = None,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(pages, (list, tuple)) or not pages:
        raise ValueError("at least one --page LANGUAGE PROJECT TITLE is required")
    if len(pages) > MAX_TITLES:
        raise ValueError(f"at most {MAX_TITLES} page titles may be requested")
    _bounded_int(retries, "retries", 0, MAX_RETRIES)
    timeout = _positive_number(timeout, "timeout", MAX_TIMEOUT)
    retry_budget = _positive_number(retry_budget, "retry_budget", MAX_RETRY_BUDGET)

    requests: list[PageRequest] = []
    seen: set[str] = set()
    for values in pages:
        if not isinstance(values, (list, tuple)) or len(values) != 3:
            raise ValueError("each page must contain language, project, and title")
        page = make_page_request(*values)
        if page.identity in seen:
            raise ValueError("duplicate normalized page identity")
        seen.add(page.identity)
        requests.append(page)

    current = (now or (lambda: datetime.now(timezone.utc)))()
    if current.tzinfo is None:
        raise ValueError("now must return a timezone-aware datetime")
    fetched_at = current.astimezone(timezone.utc).isoformat()
    http = client or WikimediaClient(
        timeout=timeout,
        retries=retries,
        retry_budget=retry_budget,
    )
    return [normalize_summary(http.get(page), page, fetched_at) for page in requests]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--page",
        nargs=3,
        action="append",
        metavar=("LANGUAGE", "PROJECT", "TITLE"),
        required=True,
    )
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-budget", type=float, default=DEFAULT_RETRY_BUDGET)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        records = fetch_page_summaries(
            args.page,
            timeout=args.timeout,
            retries=args.retries,
            retry_budget=args.retry_budget,
        )
    except (ValueError, WikimediaError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
