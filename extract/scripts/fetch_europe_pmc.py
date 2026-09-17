#!/usr/bin/env python3
"""Fetch a bounded Europe PMC publication-metadata feed as JSON lines.

This uses Europe PMC's public REST and annotation APIs only.  It never downloads
article full text.  Cursor state advances only after the corresponding page has
been written, so an interrupted run resumes from the last complete page.
"""
import argparse
import json
import os
import pathlib
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

SEARCH_ENDPOINT = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
ANNOTATIONS_ENDPOINT = (
    "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"
)
ARTICLE_URL = "https://europepmc.org/article/{source}/{identifier}"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
DEFAULT_QUERY = "FIRST_PDATE:[2026-01-01 TO 2026-12-31]"
USER_AGENT = "vintage-data-europe-pmc/1.0"


class FetchError(RuntimeError):
    """A response could not be fetched after its configured retries."""


class ResponseError(ValueError):
    """A successful HTTP response did not have the expected JSON shape."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "europe_pmc.json"


def load_state(path: pathlib.Path) -> dict[str, Any]:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed cursor state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    """Atomically publish a cursor only after its page has been emitted."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split())
    return normalized or None


def yes_no(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.upper() in {"Y", "YES", "TRUE"}:
            return True
        if value.upper() in {"N", "NO", "FALSE"}:
            return False
    return None


def normalize_doi(value: Any) -> str | None:
    value = text(value)
    if not value:
        return None
    lowered = value.lower()
    for prefix in ("https://doi.org/", "http://doi.org/", "doi:"):
        if lowered.startswith(prefix):
            value = value[len(prefix):]
            break
    return value.strip().lower() or None


def normalize_author(author: Any) -> dict[str, str | None] | None:
    if not isinstance(author, dict):
        return None
    full_name = text(author.get("fullName"))
    if not full_name:
        full_name = text(" ".join(
            part for part in (text(author.get("firstName")), text(author.get("lastName"))) if part
        ))
    if not full_name:
        full_name = text(author.get("collectiveName"))
    if not full_name:
        return None
    return {
        "name": full_name,
        "first_name": text(author.get("firstName")),
        "last_name": text(author.get("lastName")),
        "initials": text(author.get("initials")),
        "identifier": text(author.get("authorId")) or text(author.get("orcid")),
    }


def normalize_fulltext_links(value: Any) -> list[dict[str, str | None]]:
    if isinstance(value, dict):
        value = value.get("fullTextUrl")
    links = []
    for link in as_list(value):
        if not isinstance(link, dict):
            continue
        url = text(link.get("url"))
        if url:
            links.append({
                "url": url,
                "site": text(link.get("site")),
                "document_style": text(link.get("documentStyle")),
                "availability": text(link.get("availability")),
            })
    return links


def normalize_annotation(annotation: Any) -> dict[str, Any] | None:
    if not isinstance(annotation, dict):
        return None
    tags = []
    for tag in as_list(annotation.get("tags")):
        if isinstance(tag, dict):
            tags.append({"name": text(tag.get("name")), "uri": text(tag.get("uri"))})
    normalized = {
        "type": text(annotation.get("type")) or text(annotation.get("annotationType")),
        "exact": text(annotation.get("exact")) or text(annotation.get("text")),
        "prefix": text(annotation.get("prefix")),
        "suffix": text(annotation.get("suffix")),
        "tags": tags,
    }
    return normalized if any(value for value in normalized.values()) else None


def annotations_from_payload(payload: Any) -> list[dict[str, Any]]:
    """Accept documented article-wrapped and direct annotation responses."""
    if not isinstance(payload, dict):
        raise ResponseError("annotation response is not a JSON object")
    annotations = payload.get("annotations")
    if annotations is None:
        articles = payload.get("articles")
        if not isinstance(articles, list):
            raise ResponseError("annotation response has no annotations")
        annotations = []
        for article in articles:
            if isinstance(article, dict):
                annotations.extend(as_list(article.get("annotations")))
    return [normalized for item in as_list(annotations)
            if (normalized := normalize_annotation(item)) is not None]


class HttpClient:
    def __init__(self, timeout: float, retries: int, rate_limit_seconds: float,
                 opener: Callable[..., Any] = urllib.request.urlopen,
                 sleeper: Callable[[float], None] = time.sleep):
        self.timeout = timeout
        self.retries = retries
        self.rate_limit_seconds = rate_limit_seconds
        self.opener = opener
        self.sleeper = sleeper
        self.last_request_at: float | None = None

    def get_json(self, endpoint: str, params: dict[str, Any]) -> tuple[dict[str, Any], str]:
        url = endpoint + "?" + urllib.parse.urlencode(params)
        if self.last_request_at is not None and self.rate_limit_seconds:
            remaining = self.rate_limit_seconds - (time.monotonic() - self.last_request_at)
            if remaining > 0:
                self.sleeper(remaining)
        for attempt in range(self.retries + 1):
            try:
                request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
                with self.opener(request, timeout=self.timeout) as response:
                    payload = json.load(response)
                self.last_request_at = time.monotonic()
                if not isinstance(payload, dict):
                    raise ResponseError("response is not a JSON object")
                return payload, url
            except (urllib.error.HTTPError, urllib.error.URLError, OSError) as exc:
                self.last_request_at = time.monotonic()
                if attempt == self.retries:
                    raise FetchError(f"request failed after {attempt + 1} attempts: {url}") from exc
                self.sleeper(2 ** attempt)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise ResponseError(f"malformed JSON response: {url}") from exc


def search_page(client: HttpClient, query: str, cursor: str, page_size: int) -> tuple[list[dict[str, Any]], str | None, str]:
    payload, url = client.get_json(SEARCH_ENDPOINT, {
        "query": query,
        "format": "json",
        "resultType": "core",
        "pageSize": page_size,
        "cursorMark": cursor,
    })
    result_list = payload.get("resultList")
    if not isinstance(result_list, dict):
        raise ResponseError("search response has no resultList")
    results = result_list.get("result", [])
    if not isinstance(results, list) or not all(isinstance(item, dict) for item in results):
        raise ResponseError("search response resultList.result is not a list of objects")
    next_cursor = payload.get("nextCursorMark")
    if next_cursor is not None and not isinstance(next_cursor, str):
        raise ResponseError("search response nextCursorMark is not a string")
    return results, next_cursor, url


def fetch_annotations(client: HttpClient, publication: dict[str, Any]) -> tuple[list[dict[str, Any]], str | None]:
    if not yes_no(publication.get("hasTextMinedTerms")):
        return [], None
    source = text(publication.get("source"))
    identifier = text(publication.get("id"))
    if not source or not identifier:
        raise ResponseError("publication lacks source or id")
    payload, url = client.get_json(ANNOTATIONS_ENDPOINT, {
        "articleIds": f"{source}:{identifier}",
        "format": "JSON",
    })
    return annotations_from_payload(payload), url


def normalize_publication(publication: dict[str, Any], fetched_at: str, search_url: str,
                          annotations: list[dict[str, Any]], annotations_url: str | None) -> dict[str, Any]:
    source = text(publication.get("source"))
    identifier = text(publication.get("id"))
    if not source or not identifier:
        raise ResponseError("publication lacks source or id")
    authors = [author for item in as_list((publication.get("authorList") or {}).get("author")
                                          if isinstance(publication.get("authorList"), dict) else None)
               if (author := normalize_author(item)) is not None]
    source_url = ARTICLE_URL.format(source=urllib.parse.quote(source), identifier=urllib.parse.quote(identifier))
    first_publication_date = text(publication.get("firstPublicationDate"))
    return {
        "source": "europe_pmc",
        "id": f"{source}:{identifier}",
        "fetched_at": fetched_at,
        "source_url": source_url,
        "identifiers": {
            "europe_pmc": f"{source}:{identifier}",
            "pmid": text(publication.get("pmid")),
            "pmcid": text(publication.get("pmcid")).upper() if text(publication.get("pmcid")) else None,
            "doi": normalize_doi(publication.get("doi")),
        },
        "title": text(publication.get("title")),
        "authors": authors,
        "author_string": text(publication.get("authorString")),
        "journal": {
            "title": text(publication.get("journalTitle")),
            "issn": text(publication.get("journalIssn")),
            "volume": text(publication.get("journalVolume")),
            "issue": text(publication.get("issue")),
            "pages": text(publication.get("pageInfo")),
            "publisher": text(publication.get("publisher")),
        },
        "publication": {
            "first_publication_date": first_publication_date,
            "electronic_publication_date": text(publication.get("electronicPublicationDate")),
            "journal_publication_date": text(publication.get("journalPublicationDate")),
            "year": text(publication.get("pubYear")),
        },
        "open_access": yes_no(publication.get("isOpenAccess")),
        "in_pmc": yes_no(publication.get("inPMC")),
        "citations": {
            "cited_by_count": publication.get("citedByCount") if isinstance(publication.get("citedByCount"), int) else None,
            "has_references": yes_no(publication.get("hasReferences")),
        },
        "abstract": text(publication.get("abstractText")),
        "fulltext_links": normalize_fulltext_links(publication.get("fullTextUrlList")),
        "has_text_mined_terms": yes_no(publication.get("hasTextMinedTerms")),
        "text_mined_annotations": annotations,
        "retrieval": {
            "search_url": search_url,
            "annotations_url": annotations_url,
            "publication_source": source,
            "publication_id": identifier,
        },
    }


def cursor_for_run(state: dict[str, Any], query: str) -> str | None:
    if state.get("query") != query:
        return "*"
    if state.get("complete") is True:
        return None
    cursor = state.get("cursor", "*")
    if not isinstance(cursor, str) or not cursor:
        raise ValueError("malformed cursor state: cursor must be a non-empty string")
    return cursor


def report_coverage(complete: bool, pages: int, records: int, cursor: str | None,
                    reason: str | None = None) -> None:
    report = {
        "source": "europe_pmc",
        "event": "coverage",
        "complete": complete,
        "pages_fetched": pages,
        "records_emitted": records,
        "next_cursor": cursor,
    }
    if reason:
        report["partial_reason"] = reason
    print(json.dumps(report, ensure_ascii=False, sort_keys=True), file=os.sys.stderr)


def run(query: str, page_size: int, max_pages: int, max_records: int, state: dict[str, Any],
        save: Callable[[dict[str, Any]], None], client: HttpClient,
        emit: Callable[[dict[str, Any]], None]) -> None:
    cursor = cursor_for_run(state, query)
    fetched_at = utc_now()
    if cursor is None:
        report_coverage(True, 0, 0, state.get("cursor"))
        return

    pages = 0
    records_emitted = 0
    while pages < max_pages and records_emitted < max_records:
        requested_size = min(page_size, max_records - records_emitted)
        publications, next_cursor, search_url = search_page(client, query, cursor, requested_size)
        pages += 1
        if len(publications) > requested_size:
            raise ResponseError("search response exceeded requested page size")
        for publication in publications:
            annotations, annotations_url = fetch_annotations(client, publication)
            emit(normalize_publication(publication, fetched_at, search_url, annotations, annotations_url))
        records_emitted += len(publications)

        complete = not publications or not next_cursor or next_cursor == cursor
        next_state = {
            "query": query,
            "cursor": next_cursor or cursor,
            "complete": complete,
            "updated_at": fetched_at,
        }
        save(next_state)
        if complete:
            report_coverage(True, pages, records_emitted, next_state["cursor"])
            return
        cursor = next_cursor

    reason = "max_pages" if pages >= max_pages else "max_records"
    save({"query": query, "cursor": cursor, "complete": False, "updated_at": fetched_at})
    report_coverage(False, pages, records_emitted, cursor, reason)


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--query", default=DEFAULT_QUERY, help="Europe PMC query; must be publication-date bounded")
    parser.add_argument("--page-size", type=positive_int, default=100, help="records per API page (maximum 1000)")
    parser.add_argument("--max-pages", type=positive_int, default=5, help="maximum pages per run")
    parser.add_argument("--max-records", type=positive_int, default=500, help="maximum records per run")
    parser.add_argument("--state-file", help="cursor state file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=2, help="retries after transport failures")
    parser.add_argument("--rate-limit-seconds", type=float, default=0.25, help="minimum delay between HTTP requests")
    parser.add_argument("--no-state", action="store_true", help="do not read or write cursor state")
    args = parser.parse_args()
    if args.page_size > 1000:
        parser.error("--page-size must not exceed 1000")
    if args.timeout <= 0 or args.retries < 0 or args.rate_limit_seconds < 0:
        parser.error("timeout must be positive; retries and rate limit must not be negative")

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)
    client = HttpClient(args.timeout, args.retries, args.rate_limit_seconds)
    save = (lambda next_state: None) if args.no_state else lambda next_state: save_state(path, next_state)
    run(args.query, args.page_size, args.max_pages, args.max_records, state, save, client,
        lambda record: print(json.dumps(record, ensure_ascii=False, sort_keys=True)))


if __name__ == "__main__":
    main()
