#!/usr/bin/env python3
"""Fetch a deliberately narrow, recent OpenAlex works feed as NDJSON.

The query is restricted to one primary topic and a short rolling publication-date
window. Publication-date polling is not an exact feed of newly indexed records:
OpenAlex can add or amend older works after their publication date. Consecutive
runs intentionally overlap, so consumers must deduplicate on the stable OpenAlex
work ID.

The extractor requests only fields needed for DOI joins and publication,
authorship/institution-stub, open-access-location, topic, citation, reference,
and related-work enrichment. It does not crawl authors, institutions, or the
full works corpus. Pagination is cursor-based at 100 records per request. The
configured page and record ceilings fail the run if the query cannot be fully
covered; no result set is silently truncated.

OpenAlex metadata is available under CC0. Follow OpenAlex's current API budget
and rate-limit guidance. OPENALEX_API_KEY is optional; EXTRACT_USER_AGENT (or
--user-agent) identifies this client.

Stdlib only.
"""
import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Callable

API_URL = "https://api.openalex.org/works"
PAGE_SIZE = 100
DEFAULT_TOPIC_ID = "T13090"
DEFAULT_USER_AGENT = "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SELECT_FIELDS = (
    "id,doi,title,publication_date,publication_year,type,language,authorships,"
    "open_access,best_oa_location,primary_location,locations,topics,primary_topic,"
    "cited_by_count,referenced_works,related_works"
)
TOPIC_ID_RE = re.compile(r"T[1-9][0-9]*\Z")


class OpenAlexError(RuntimeError):
    """The OpenAlex API could not provide a usable complete response."""


class IncompleteCoverageError(OpenAlexError):
    """The configured request budget cannot cover the complete query."""


def rolling_filter(topic_id: str, days_back: int, today: date | None = None) -> str:
    """Return a primary-topic filter for ``days_back`` UTC calendar dates."""
    if not TOPIC_ID_RE.fullmatch(topic_id):
        raise ValueError("topic_id must be an OpenAlex topic ID such as T13090")
    if days_back < 1:
        raise ValueError("days_back must be at least 1")
    end_date = today or datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days_back - 1)
    return (
        f"primary_topic.id:{topic_id},"
        f"from_publication_date:{start_date.isoformat()},"
        f"to_publication_date:{end_date.isoformat()}"
    )


def request_page(
    cursor: str,
    filter_value: str,
    *,
    api_key: str | None,
    user_agent: str,
    opener: Callable | None = None,
) -> dict:
    """Request and validate one OpenAlex cursor page."""
    params = {
        "filter": filter_value,
        "select": SELECT_FIELDS,
        "per-page": str(PAGE_SIZE),
        "cursor": cursor,
    }
    if api_key:
        params["api_key"] = api_key
    request = urllib.request.Request(
        f"{API_URL}?{urllib.parse.urlencode(params)}",
        headers={"User-Agent": user_agent},
    )
    open_request = opener or urllib.request.urlopen
    try:
        with open_request(request, timeout=60) as response:
            payload = json.load(response)
    except (urllib.error.HTTPError, urllib.error.URLError, OSError) as exc:
        raise OpenAlexError(f"OpenAlex request failed: {exc}") from exc
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise OpenAlexError("OpenAlex returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise OpenAlexError("OpenAlex response must be a JSON object")
    return payload


def normalize(work: dict, fetched_at: str) -> dict:
    """Keep the selected enrichment fields and use the OpenAlex work ID as ``id``."""
    if not isinstance(work, dict):
        raise OpenAlexError("OpenAlex result must be an object")
    work_id = work.get("id")
    if not isinstance(work_id, str) or not work_id:
        raise OpenAlexError("OpenAlex result is missing its work ID")
    return {
        "source": "openalex_works",
        "fetched_at": fetched_at,
        "id": work_id,
        "doi": work.get("doi"),
        "title": work.get("title"),
        "publication_date": work.get("publication_date"),
        "publication_year": work.get("publication_year"),
        "type": work.get("type"),
        "language": work.get("language"),
        "authorships": work.get("authorships") or [],
        "open_access": work.get("open_access"),
        "best_oa_location": work.get("best_oa_location"),
        "primary_location": work.get("primary_location"),
        "locations": work.get("locations") or [],
        "topics": work.get("topics") or [],
        "primary_topic": work.get("primary_topic"),
        "cited_by_count": work.get("cited_by_count"),
        "referenced_works": work.get("referenced_works") or [],
        "related_works": work.get("related_works") or [],
    }


def _page_metadata(payload: dict) -> tuple[list, int, str | None]:
    results = payload.get("results")
    metadata = payload.get("meta")
    if not isinstance(results, list) or not isinstance(metadata, dict):
        raise OpenAlexError("OpenAlex response must contain results and meta objects")
    count = metadata.get("count")
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise OpenAlexError("OpenAlex response has an invalid meta.count")
    next_cursor = metadata.get("next_cursor")
    if next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor):
        raise OpenAlexError("OpenAlex response has an invalid meta.next_cursor")
    return results, count, next_cursor


def fetch_works(
    topic_id: str = DEFAULT_TOPIC_ID,
    days_back: int = 7,
    max_pages: int = 10,
    max_records: int = 1000,
    *,
    today: date | None = None,
    fetched_at: str | None = None,
    api_key: str | None = None,
    user_agent: str | None = None,
    opener: Callable | None = None,
):
    """Yield every work in the bounded query or fail with incomplete coverage."""
    if max_pages < 1:
        raise ValueError("max_pages must be at least 1")
    if max_records < 1:
        raise ValueError("max_records must be at least 1")
    if max_records > max_pages * PAGE_SIZE:
        raise ValueError("max_records cannot exceed max_pages * page size")

    filter_value = rolling_filter(topic_id, days_back, today)
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()
    api_key = os.environ.get("OPENALEX_API_KEY") if api_key is None else api_key
    user_agent = user_agent or os.environ.get("EXTRACT_USER_AGENT") or DEFAULT_USER_AGENT
    cursor = "*"
    pages_fetched = 0
    records_emitted = 0
    seen_ids: set[str] = set()
    expected_count: int | None = None

    while True:
        if pages_fetched >= max_pages:
            raise IncompleteCoverageError(
                f"OpenAlex query needs more than configured {max_pages} pages; coverage is incomplete"
            )
        payload = request_page(
            cursor,
            filter_value,
            api_key=api_key,
            user_agent=user_agent,
            opener=opener,
        )
        pages_fetched += 1
        results, count, next_cursor = _page_metadata(payload)
        if expected_count is None:
            expected_count = count
            if count > max_records:
                raise IncompleteCoverageError(
                    f"OpenAlex query has {count} records, above configured {max_records}-record limit"
                )
            if count > max_pages * PAGE_SIZE:
                raise IncompleteCoverageError(
                    f"OpenAlex query has {count} records, above configured {max_pages}-page limit"
                )
        elif count != expected_count:
            raise OpenAlexError("OpenAlex meta.count changed during cursor pagination")

        for work in results:
            record = normalize(work, fetched_at)
            if record["id"] in seen_ids:
                continue
            if records_emitted >= max_records:
                raise IncompleteCoverageError(
                    f"OpenAlex query exceeded configured {max_records}-record limit; coverage is incomplete"
                )
            seen_ids.add(record["id"])
            records_emitted += 1
            yield record

        if next_cursor is None:
            return
        cursor = next_cursor


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic-id", default=DEFAULT_TOPIC_ID)
    parser.add_argument("--days-back", type=int, default=7)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--max-records", type=int, default=1000)
    parser.add_argument("--user-agent", help="override $EXTRACT_USER_AGENT")
    args = parser.parse_args()

    for record in fetch_works(
        topic_id=args.topic_id,
        days_back=args.days_back,
        max_pages=args.max_pages,
        max_records=args.max_records,
        user_agent=args.user_agent,
    ):
        print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
