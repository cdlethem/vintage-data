#!/usr/bin/env python3
"""Fetch recent GLEIF LEI record updates as stateful NDJSON.

The first run is intentionally limited to a short lookback. Later runs query the
stored inclusive update watermark and suppress LEIs already seen at that exact
timestamp. Pagination always follows GLEIF's returned ``links.next`` URL. A page
cap leaves that URL in state so the next invocation resumes the same walk.

State is published atomically only after every fetched page has validated and
all output has been written and flushed. Relationship links are retained as
upstream references; this extractor does not resolve an ownership graph.
"""

import argparse
import json
import os
import pathlib
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Callable, TextIO

SOURCE = "gleif_lei_records"
ENDPOINT = "https://api.gleif.org/api/v1/lei-records"
API_HOST = "api.gleif.org"
API_PATH = "/api/v1/lei-records"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
DEFAULT_LOOKBACK_HOURS = 24
DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 10
MAX_PAGE_SIZE = 200
MAX_LOOKBACK_HOURS = 24
STATE_VERSION = 1
MIN_REQUEST_INTERVAL = 1.0
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
LEI_PATTERN = re.compile(r"[A-Z0-9]{20}\Z")


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects before urllib can issue a destination request."""

    def redirect_request(self, request, file_pointer, code, message, headers, new_url):
        return None


def build_http_opener(*handlers: urllib.request.BaseHandler) -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(NoRedirectHandler(), *handlers)


HTTP_OPENER = build_http_opener()


def timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or "T" not in value:
        raise ValueError(f"{field} must be an ISO 8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{field} must be an ISO 8601 timestamp") from error
    if parsed.utcoffset() is None:
        raise ValueError(f"{field} must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def default_state() -> dict:
    return {
        "version": STATE_VERSION,
        "source": SOURCE,
        "watermark": None,
        "boundary_ids": [],
        "continuation_url": None,
    }


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def validate_continuation(url: object) -> str:
    if not isinstance(url, str) or not url:
        raise ValueError("pagination links.next must be a non-empty URL or null")
    try:
        parsed = urllib.parse.urlsplit(url)
        port = parsed.port
        query_keys = {key.lower() for key, _ in urllib.parse.parse_qsl(parsed.query)}
    except ValueError as error:
        raise ValueError("pagination links.next is not a valid URL") from error
    authentication_keys = {
        "access_token", "api-key", "api_key", "apikey", "authorization", "key", "token"
    }
    if (
        parsed.scheme != "https"
        or parsed.hostname != API_HOST
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != API_PATH
        or parsed.fragment
        or query_keys & authentication_keys
    ):
        raise ValueError("pagination links.next must be an unauthenticated GLEIF lei-records URL")
    return url


def validate_state(document: object, path: pathlib.Path) -> dict:
    if not isinstance(document, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if document.get("version") != STATE_VERSION or document.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: unexpected version or source")
    if set(document) != {"version", "source", "watermark", "boundary_ids", "continuation_url"}:
        raise ValueError(f"malformed state in {path}: unexpected fields")

    watermark = document["watermark"]
    if watermark is not None:
        timestamp(watermark, "state watermark")
    boundary = document["boundary_ids"]
    if not isinstance(boundary, list) or any(
        not isinstance(lei, str) or LEI_PATTERN.fullmatch(lei) is None for lei in boundary
    ):
        raise ValueError(f"malformed state in {path}: invalid boundary_ids")
    if len(boundary) != len(set(boundary)):
        raise ValueError(f"malformed state in {path}: duplicate boundary_ids")
    continuation = document["continuation_url"]
    if continuation is not None:
        validate_continuation(continuation)
    return document


def load_state(path: pathlib.Path) -> dict:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default_state()
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read state from {path}: {error}") from error
    return validate_state(document, path)


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish state after stdout has accepted the complete batch."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, prefix=f".{path.name}.", delete=False
        ) as output:
            temporary_name = output.name
            json.dump(state, output, ensure_ascii=False, indent=2, sort_keys=True)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        if temporary_name is not None:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass
        raise


def build_initial_url(since: datetime, page_size: int) -> str:
    parameters = {
        "filter[registration.lastUpdateDate][gte]": timestamp_text(since),
        "sort": "registration.lastUpdateDate",
        "page[size]": str(page_size),
    }
    return f"{ENDPOINT}?{urllib.parse.urlencode(parameters)}"


def request_json(url: str, timeout: int) -> object:
    validate_continuation(url)
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/vnd.api+json", "User-Agent": USER_AGENT},
    )
    try:
        with HTTP_OPENER.open(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        if 300 <= error.code < 400:
            error.close()
            raise ValueError(
                f"GLEIF request rejected HTTP redirect (status {error.code})"
            ) from None
        raise


def relationship_links(value: object, lei: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"LEI {lei} relationships must be an object")
    links_by_relationship = {}
    for name, relationship in value.items():
        if not isinstance(name, str) or not isinstance(relationship, dict):
            raise ValueError(f"LEI {lei} has a malformed relationship")
        links = relationship.get("links")
        if not isinstance(links, dict):
            raise ValueError(f"LEI {lei} relationship {name!r} has malformed links")
        links_by_relationship[name] = links
    return links_by_relationship


def validate_page(document: object, current_url: str) -> tuple[list[tuple[dict, datetime]], str | None, str]:
    if not isinstance(document, dict):
        raise ValueError("GLEIF response must be an object")

    meta = document.get("meta")
    golden_copy = meta.get("goldenCopy") if isinstance(meta, dict) else None
    publish_date = golden_copy.get("publishDate") if isinstance(golden_copy, dict) else None
    timestamp(publish_date, "meta.goldenCopy.publishDate")

    links = document.get("links")
    if not isinstance(links, dict) or "next" not in links:
        raise ValueError("GLEIF response links must contain next")
    next_url = links["next"]
    if next_url is not None:
        next_url = validate_continuation(next_url)
        if next_url == current_url:
            raise ValueError("pagination links.next must not refer to the current page")

    data = document.get("data")
    if not isinstance(data, list):
        raise ValueError("GLEIF response data must be an array")
    if next_url is not None and not data:
        raise ValueError("a non-final GLEIF page must contain records")

    rows = []
    prior_update = None
    page_ids = set()
    for resource in data:
        if not isinstance(resource, dict) or resource.get("type") != "lei-records":
            raise ValueError("every GLEIF resource must have type lei-records")
        lei = resource.get("id")
        if not isinstance(lei, str) or LEI_PATTERN.fullmatch(lei) is None:
            raise ValueError("every GLEIF resource must have a valid LEI id")
        if lei in page_ids:
            raise ValueError(f"duplicate LEI {lei} in one page")
        page_ids.add(lei)

        attributes = resource.get("attributes")
        if not isinstance(attributes, dict) or attributes.get("lei") != lei:
            raise ValueError(f"LEI {lei} attributes.lei must match the resource id")
        registration = attributes.get("registration")
        update_value = registration.get("lastUpdateDate") if isinstance(registration, dict) else None
        update = timestamp(update_value, f"LEI {lei} registration.lastUpdateDate")
        if prior_update is not None and update < prior_update:
            raise ValueError("GLEIF page is not sorted by registration.lastUpdateDate ascending")
        prior_update = update

        retained_links = relationship_links(resource.get("relationships"), lei)
        rows.append(({
            "lei": lei,
            "attributes": attributes,
            "relationship_links": retained_links,
        }, update))
    return rows, next_url, publish_date


def collect(
    state: dict,
    *,
    fetched_at: datetime,
    lookback_hours: int,
    page_size: int,
    max_pages: int,
    timeout: int,
    transport: Callable[[str, int], object],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
) -> tuple[list[dict], dict]:
    if (
        lookback_hours <= 0
        or lookback_hours > MAX_LOOKBACK_HOURS
        or page_size <= 0
        or page_size > MAX_PAGE_SIZE
        or max_pages <= 0
        or timeout <= 0
    ):
        raise ValueError("lookback, page size, max pages, or timeout is outside its allowed bound")
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at = fetched_at.astimezone(timezone.utc)

    watermark_value = state["watermark"]
    watermark = timestamp(watermark_value, "state watermark") if watermark_value else None
    boundary = set(state["boundary_ids"])
    floor = watermark or (fetched_at - timedelta(hours=lookback_hours))
    url = state["continuation_url"] or build_initial_url(floor, page_size)
    validate_continuation(url)

    records = []
    seen_urls = set()
    seen_leis = set()
    golden_copy_published_at = None
    last_update = watermark
    last_request_at = None
    next_url = url

    for _ in range(max_pages):
        url = validate_continuation(next_url)
        if url in seen_urls:
            raise ValueError("GLEIF pagination contains a cycle")
        seen_urls.add(url)

        now = monotonic()
        if last_request_at is not None:
            delay = MIN_REQUEST_INTERVAL - (now - last_request_at)
            if delay > 0:
                sleep(delay)
        last_request_at = monotonic()
        document = transport(url, timeout)
        rows, next_url, page_publish_date = validate_page(document, url)
        if golden_copy_published_at is None:
            golden_copy_published_at = page_publish_date
        elif timestamp(page_publish_date, "meta.goldenCopy.publishDate") != timestamp(
            golden_copy_published_at, "meta.goldenCopy.publishDate"
        ):
            raise ValueError("Golden Copy publication timestamp changed during pagination")

        for row, update in rows:
            lei = row["lei"]
            if lei in seen_leis:
                raise ValueError(f"duplicate LEI {lei} across GLEIF pages")
            seen_leis.add(lei)
            if update < floor or (last_update is not None and update < last_update):
                raise ValueError("GLEIF returned a record older than the requested or completed watermark")

            if last_update is None or update > last_update:
                last_update = update
                boundary = {lei}
                emit = True
            else:
                emit = lei not in boundary
                boundary.add(lei)

            if emit:
                records.append({
                    "source": SOURCE,
                    "fetched_at": timestamp_text(fetched_at),
                    "id": lei,
                    "golden_copy_published_at": page_publish_date,
                    "attributes": row["attributes"],
                    "relationship_links": row["relationship_links"],
                })
        if next_url is None:
            break

    next_state = {
        "version": STATE_VERSION,
        "source": SOURCE,
        "watermark": timestamp_text(last_update) if last_update is not None else None,
        "boundary_ids": sorted(boundary) if last_update is not None else [],
        "continuation_url": next_url,
    }
    return records, next_state


def run(
    *,
    path: pathlib.Path | None,
    output: TextIO,
    lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: int = 30,
    transport: Callable[[str, int], object] = request_json,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    state = default_state() if path is None else load_state(path)
    records, next_state = collect(
        state,
        fetched_at=now(),
        lookback_hours=lookback_hours,
        page_size=page_size,
        max_pages=max_pages,
        timeout=timeout,
        transport=transport,
        monotonic=monotonic,
        sleep=sleep,
    )
    payload = "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records)
    if payload:
        written = output.write(payload)
        if written is not None and written != len(payload):
            raise OSError("short write to NDJSON output")
    output.flush()
    if path is not None:
        save_state(path, next_state)
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="state file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="do not read or update persistent state")
    parser.add_argument("--lookback-hours", type=int, default=DEFAULT_LOOKBACK_HOURS)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--timeout", type=int, default=30)
    arguments = parser.parse_args()
    if arguments.no_state and arguments.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    try:
        count = run(
            path=None if arguments.no_state else state_path(arguments.state_file),
            output=sys.stdout,
            lookback_hours=arguments.lookback_hours,
            page_size=arguments.page_size,
            max_pages=arguments.max_pages,
            timeout=arguments.timeout,
        )
    except ValueError as error:
        parser.error(str(error))
    print(f"Emitted {count} GLEIF LEI records", file=sys.stderr)


if __name__ == "__main__":
    main()
