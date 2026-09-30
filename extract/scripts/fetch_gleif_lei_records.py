#!/usr/bin/env python3
"""Fetch GLEIF LEI record update batches for completed UTC days as NDJSON.

The GLEIF lei-records API's registration.lastUpdateDate filter selects the
records whose registration was last updated on one UTC calendar day (the
daily Golden Copy batch). The current day is still growing, so runs collect
only completed days: from the day after the state's ``last_collected_date``
through yesterday UTC, oldest first. Each day is walked to its terminal page
via cursor pagination (``page[cursor]=*`` followed by ``links.next``)
because page-number pagination is rejected past 10,000 rows and a full daily
batch reaches ~11,000.

The state file (``last_collected_date``) is published atomically only after
every page of every collected day has validated and all output has been
written and flushed, so a failed run never advances the watermark and the
next invocation replays only uncollected days.
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
from datetime import date, datetime, timedelta, timezone
from typing import Callable, TextIO

SOURCE = "gleif_lei_records"
ENDPOINT = "https://api.gleif.org/api/v1/lei-records"
API_HOST = "api.gleif.org"
API_PATH = "/api/v1/lei-records"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
DEFAULT_LOOKBACK_DAYS = 7
MAX_LOOKBACK_DAYS = 31
DEFAULT_PAGE_SIZE = 200
MAX_PAGE_SIZE = 200
MAX_PAGES_PER_DAY = 100
MAX_DAYS_PER_RUN = 31
STATE_VERSION = 1
MIN_REQUEST_INTERVAL = 1.0
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
LEI_PATTERN = re.compile(r"[A-Z0-9]{20}\Z")
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


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


def iso_date(value: object, field: str) -> date:
    if not isinstance(value, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"{field} must be a YYYY-MM-DD date")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"{field} must be a YYYY-MM-DD date") from error


def default_state() -> dict:
    return {
        "version": STATE_VERSION,
        "source": SOURCE,
        "last_collected_date": None,
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
    if set(document) != {"version", "source", "last_collected_date"}:
        raise ValueError(f"malformed state in {path}: unexpected fields")
    last = document["last_collected_date"]
    if last is not None:
        iso_date(last, "state last_collected_date")
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


def build_day_url(day: date, page_size: int) -> str:
    parameters = {
        "filter[registration.lastUpdateDate]>=": day.isoformat(),
        "sort": "registration.lastUpdateDate",
        "page[cursor]": "*",
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


def validate_page(
    document: object, current_url: str, day: date, seen_leis: set
) -> tuple[list[dict], str | None, str]:
    if not isinstance(document, dict):
        raise ValueError("GLEIF response must be an object")

    meta = document.get("meta")
    golden_copy = meta.get("goldenCopy") if isinstance(meta, dict) else None
    publish_date = golden_copy.get("publishDate") if isinstance(golden_copy, dict) else None
    timestamp(publish_date, "meta.goldenCopy.publishDate")

    links = document.get("links")
    if not isinstance(links, dict):
        raise ValueError("GLEIF response links must be an object")
    next_url = links.get("next")
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
    for resource in data:
        if not isinstance(resource, dict) or resource.get("type") != "lei-records":
            raise ValueError("every GLEIF resource must have type lei-records")
        lei = resource.get("id")
        if not isinstance(lei, str) or LEI_PATTERN.fullmatch(lei) is None:
            raise ValueError("every GLEIF resource must have a valid LEI id")
        if lei in seen_leis:
            raise ValueError(f"duplicate LEI {lei} within GLEIF day {day.isoformat()}")
        seen_leis.add(lei)

        attributes = resource.get("attributes")
        if not isinstance(attributes, dict) or attributes.get("lei") != lei:
            raise ValueError(f"LEI {lei} attributes.lei must match the resource id")
        registration = attributes.get("registration")
        update_value = registration.get("lastUpdateDate") if isinstance(registration, dict) else None
        update = timestamp(update_value, f"LEI {lei} registration.lastUpdateDate")
        if update.date() != day:
            raise ValueError(
                f"LEI {lei} registration.lastUpdateDate {update_value} "
                f"is outside collected day {day.isoformat()}"
            )
        if prior_update is not None and update < prior_update:
            raise ValueError("GLEIF page is not sorted by registration.lastUpdateDate ascending")
        prior_update = update

        rows.append({
            "lei": lei,
            "attributes": attributes,
            "relationship_links": relationship_links(resource.get("relationships"), lei),
        })
    return rows, next_url, publish_date


def collect_day(
    day: date,
    *,
    page_size: int,
    max_pages: int,
    timeout: int,
    transport: Callable[[str, int], object],
    monotonic: Callable[[], float],
    sleep: Callable[[float], None],
    counters: dict,
) -> list[dict]:
    url = build_day_url(day, page_size)
    seen_leis: set = set()
    rows: list[dict] = []
    golden_copy_published_at = None
    last_request_at = None
    for page in range(1, max_pages + 1):
        url = validate_continuation(url)
        now = monotonic()
        if last_request_at is not None:
            delay = MIN_REQUEST_INTERVAL - (now - last_request_at)
            if delay > 0:
                sleep(delay)
        last_request_at = monotonic()
        counters["attempted"] += 1
        document = transport(url, timeout)
        counters["succeeded"] += 1
        page_rows, next_url, page_publish_date = validate_page(document, url, day, seen_leis)
        if golden_copy_published_at is None:
            golden_copy_published_at = page_publish_date
        elif timestamp(page_publish_date, "meta.goldenCopy.publishDate") != timestamp(
            golden_copy_published_at, "meta.goldenCopy.publishDate"
        ):
            raise ValueError("Golden Copy publication timestamp changed during pagination")
        rows.extend(page_rows)
        if next_url is None:
            return rows
        url = next_url
    raise ValueError(f"GLEIF day {day.isoformat()} exceeds the {max_pages}-page cap")


def day_range(
    last_collected: date | None, end_date: date, lookback_days: int
) -> tuple[list[date], bool]:
    """Days to collect, oldest first.

    Returns ``(days, truncated)`` where ``truncated`` is true when the
    MAX_DAYS_PER_RUN cap left later days for a subsequent run.
    """
    if last_collected is not None:
        start = last_collected + timedelta(days=1)
    else:
        start = end_date - timedelta(days=lookback_days - 1)
    if start > end_date:
        return [], False
    total = (end_date - start).days + 1
    days = [start + timedelta(days=offset) for offset in range(min(total, MAX_DAYS_PER_RUN))]
    return days, total > len(days)


def run(
    *,
    path: pathlib.Path | None,
    output: TextIO,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    page_size: int = DEFAULT_PAGE_SIZE,
    timeout: int = 30,
    transport: Callable[[str, int], object] = request_json,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    if (
        lookback_days <= 0
        or lookback_days > MAX_LOOKBACK_DAYS
        or page_size <= 0
        or page_size > MAX_PAGE_SIZE
        or timeout <= 0
    ):
        raise ValueError("lookback, page size, or timeout is outside its allowed bound")
    fetched_at = now()
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at = fetched_at.astimezone(timezone.utc)

    state = default_state() if path is None else load_state(path)
    last_raw = state["last_collected_date"]
    last_collected = iso_date(last_raw, "state last_collected_date") if last_raw else None
    end_date = fetched_at.date() - timedelta(days=1)
    days, truncated = day_range(last_collected, end_date, lookback_days)

    records = []
    counters = {"attempted": 0, "succeeded": 0}
    for day in days:
        for row in collect_day(
            day,
            page_size=page_size,
            max_pages=MAX_PAGES_PER_DAY,
            timeout=timeout,
            transport=transport,
            monotonic=monotonic,
            sleep=sleep,
            counters=counters,
        ):
            records.append({
                "source": SOURCE,
                "fetched_at": timestamp_text(fetched_at),
                "id": row["lei"],
                "updated_date": day.isoformat(),
                "attributes": row["attributes"],
                "relationship_links": row["relationship_links"],
            })

    payload = "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records)
    if payload:
        written = output.write(payload)
        if written is not None and written != len(payload):
            raise OSError("short write to NDJSON output")
    output.flush()

    state_change = {}
    if path is not None and days:
        state_change["last_collected_date"] = days[-1].isoformat()
        save_state(path, {**state, "last_collected_date": days[-1].isoformat()})

    summary = {
        "health": "healthy",
        "completeness": "complete",
        "requests": counters,
        "state_change": state_change,
        "metrics": {
            "days_collected": len(days),
            "days": [day.isoformat() for day in days],
            "records": len(records),
            "truncated": truncated,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return len(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="state file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="do not read or update persistent state")
    parser.add_argument(
        "--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
        help=f"initial-run lookback in completed days, 1-{MAX_LOOKBACK_DAYS} (default {DEFAULT_LOOKBACK_DAYS})",
    )
    parser.add_argument(
        "--page-size", type=int, default=DEFAULT_PAGE_SIZE,
        help=f"page size, 1-{MAX_PAGE_SIZE} (default {DEFAULT_PAGE_SIZE})",
    )
    parser.add_argument("--timeout", type=int, default=30)
    arguments = parser.parse_args()
    if arguments.no_state and arguments.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    try:
        count = run(
            path=None if arguments.no_state else state_path(arguments.state_file),
            output=sys.stdout,
            lookback_days=arguments.lookback_days,
            page_size=arguments.page_size,
            timeout=arguments.timeout,
        )
    except ValueError as error:
        parser.error(str(error))
    print(f"Collected {count} GLEIF LEI records", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
