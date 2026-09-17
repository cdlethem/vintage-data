#!/usr/bin/env python3
"""Bounded, keyless Europe PMC publication-metadata feed.

The search API is cursor-paged.  An unfinished bounded walk stores only the cursor
following the last fully fetched and annotated page.  A completed walk stores ``*`` so
the next scheduled run polls the query again; stable ``SOURCE:ID`` identifiers support
downstream deduplication of replayed, corrected, newly indexed, and late-indexed
publications.

Records are accumulated and serialized before stdout is touched.  The candidate cursor
is atomically stored only after every line has been written and stdout has flushed.  The
extract runner stages stdout and discards it on a non-zero exit, so page, annotation,
serialization, and observable output failures before state publication replay from the
prior durable cursor.  There is necessarily a narrow cross-process boundary after this
script stores its cursor but before the runner commits its staged file: a runner crash or
sink commit failure there discards output although state advanced.  That page is not
retried immediately; later replay depends on completing the traversal and the record
still being in the rolling query window.  Delivery therefore is not exactly-once.

Only metadata, abstracts, links, and text-mined annotations are requested.  Full-text
links are emitted when advertised, but full text is never downloaded.  Stdlib only.
"""

import argparse
import json
import os
import pathlib
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Sequence

SOURCE = "europe_pmc"
SEARCH_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
ANNOTATIONS_URL = (
    "https://www.ebi.ac.uk/europepmc/annotations_api/annotationsByArticleIds"
)
ARTICLE_URL = "https://europepmc.org/article"
FULL_TEXT_URL = "https://europepmc.org/articles"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
DEFAULT_QUERY = "FIRST_PDATE:[NOW-14DAY TO NOW]"
STATE_VERSION = 1
MAX_PAGE_SIZE = 1000
MAX_ANNOTATION_IDS_PER_REQUEST = 8


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            state = json.load(handle)
    except FileNotFoundError:
        return {"version": STATE_VERSION, "source": SOURCE, "query": None, "cursor_mark": "*"}
    if not isinstance(state, dict):
        raise ValueError(f"malformed state in {path}: expected an object")
    if state.get("version") != STATE_VERSION or state.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: incompatible version or source")
    query = state.get("query")
    cursor = state.get("cursor_mark")
    if query is not None and (not isinstance(query, str) or not query):
        raise ValueError(f"malformed state in {path}: query must be null or a non-empty string")
    if not isinstance(cursor, str) or not cursor:
        raise ValueError(f"malformed state in {path}: cursor_mark must be a non-empty string")
    return state


def save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    """Atomically publish continuation only after staged stdout has flushed."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(state, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def _body_context(body: bytes) -> str:
    return body[:500].decode("utf-8", errors="replace").replace("\n", " ")


class HttpClient:
    """Paced JSON GET client with bounded transient retries."""

    def __init__(
        self,
        timeout: float,
        retries: int,
        request_interval: float,
        *,
        opener=None,
        clock=time.monotonic,
        sleeper=time.sleep,
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.request_interval = request_interval
        self.opener = urllib.request.urlopen if opener is None else opener
        self.clock = clock
        self.sleeper = sleeper
        self._last_request: float | None = None

    def _pace(self) -> None:
        if self._last_request is not None:
            delay = self.request_interval - (self.clock() - self._last_request)
            if delay > 0:
                self.sleeper(delay)
        self._last_request = self.clock()

    def get_json(self, url: str, endpoint: str) -> Any:
        for attempt in range(self.retries + 1):
            self._pace()
            request = urllib.request.Request(
                url, headers={"Accept": "application/json", "User-Agent": USER_AGENT}
            )
            try:
                with self.opener(request, timeout=self.timeout) as response:
                    body = response.read()
            except urllib.error.HTTPError as error:
                body = error.read()
                if error.code not in {408, 425, 429, 500, 502, 503, 504}:
                    raise RuntimeError(
                        f"Europe PMC {endpoint} request failed: status={error.code} "
                        f"body={_body_context(body)!r}"
                    ) from error
                failure: BaseException = error
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                failure = error
            else:
                try:
                    return json.loads(body)
                except (json.JSONDecodeError, UnicodeDecodeError) as error:
                    raise ValueError(
                        f"Europe PMC {endpoint} response is not JSON: "
                        f"body={_body_context(body)!r}"
                    ) from error
            if attempt == self.retries:
                raise RuntimeError(
                    f"Europe PMC {endpoint} request failed after {self.retries + 1} attempts: "
                    f"{failure}"
                ) from failure
            self.sleeper(min(2**attempt, 30))
        raise AssertionError("unreachable")


def _search_request_url(query: str, page_size: int, cursor_mark: str) -> str:
    return SEARCH_URL + "?" + urllib.parse.urlencode(
        {
            "query": query,
            "format": "json",
            "resultType": "core",
            "pageSize": page_size,
            "cursorMark": cursor_mark,
        }
    )


def _annotations_request_url(article_ids: Sequence[str]) -> str:
    return ANNOTATIONS_URL + "?" + urllib.parse.urlencode(
        {"articleIds": ",".join(article_ids), "format": "JSON"}
    )


def parse_search_response(document: Any) -> tuple[list[dict[str, Any]], str | None, int | None]:
    """Validate the search endpoint's object envelope; arrays are never accepted here."""
    if not isinstance(document, dict):
        raise ValueError("Europe PMC search response must be an object")
    result_list = document.get("resultList")
    if not isinstance(result_list, dict):
        raise ValueError("Europe PMC search response resultList must be an object")
    results = result_list.get("result")
    if not isinstance(results, list):
        raise ValueError("Europe PMC search response resultList.result must be a list")
    for index, result in enumerate(results):
        if not isinstance(result, dict):
            raise ValueError(f"Europe PMC search result {index} must be an object")
    cursor = document.get("nextCursorMark")
    if cursor is not None and (not isinstance(cursor, str) or not cursor):
        raise ValueError("Europe PMC search response nextCursorMark must be a non-empty string")
    hit_count = document.get("hitCount")
    if hit_count is not None and (
        isinstance(hit_count, bool) or not isinstance(hit_count, int) or hit_count < 0
    ):
        raise ValueError("Europe PMC search response hitCount must be a non-negative integer")
    return results, cursor, hit_count


def parse_annotations_response(
    document: Any, expected_article_ids: set[str]
) -> dict[str, list[dict[str, Any]]]:
    """Flatten the annotations endpoint's production top-level article array."""
    if not isinstance(document, list):
        raise ValueError("Europe PMC annotations response must be an array")
    flattened = {article_id: [] for article_id in expected_article_ids}
    for article_index, article in enumerate(document):
        if not isinstance(article, dict):
            raise ValueError(f"Europe PMC annotations article {article_index} must be an object")
        source = article.get("source")
        external_id = article.get("extId")
        annotations = article.get("annotations")
        if not isinstance(source, str) or not source or not isinstance(external_id, str) or not external_id:
            raise ValueError(
                f"Europe PMC annotations article {article_index} must have string source and extId"
            )
        article_id = f"{source.upper()}:{external_id}"
        if article_id not in expected_article_ids:
            raise ValueError(
                f"Europe PMC annotations response contains unexpected article {article_id!r}"
            )
        if not isinstance(annotations, list):
            raise ValueError(
                f"Europe PMC annotations article {article_id} annotations must be a list"
            )
        for annotation_index, annotation in enumerate(annotations):
            if not isinstance(annotation, dict):
                raise ValueError(
                    f"Europe PMC annotations article {article_id} annotation "
                    f"{annotation_index} must be an object"
                )
            flattened[article_id].append(dict(annotation))
    return flattened


def _optional_object(parent: dict[str, Any], key: str, context: str) -> dict[str, Any]:
    value = parent.get(key)
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Europe PMC {context}.{key} must be an object")
    return value


def _nested_list(
    parent: dict[str, Any], container_key: str, list_key: str, context: str
) -> list[Any]:
    container = _optional_object(parent, container_key, context)
    if not container:
        return []
    value = container.get(list_key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"Europe PMC {context}.{container_key}.{list_key} must be a list")
    return value


def _string_list(values: list[Any], context: str) -> list[str]:
    if not all(isinstance(value, str) for value in values):
        raise ValueError(f"Europe PMC {context} must contain only strings")
    return values


def _clean_text(value: Any, context: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Europe PMC {context} must be a string")
    return " ".join(value.split()) or None

def _author_id(value: Any, context: str) -> dict[str, str] | None:
    if value is None:
        return None
    if isinstance(value, str) and value:
        return {"type": "unknown", "value": value}
    if not isinstance(value, dict):
        raise ValueError(f"Europe PMC {context} must be an object")
    identifier_type = value.get("type")
    identifier = value.get("value")
    if not isinstance(identifier_type, str) or not identifier_type:
        raise ValueError(f"Europe PMC {context}.type must be a non-empty string")
    if not isinstance(identifier, str) or not identifier:
        raise ValueError(f"Europe PMC {context}.value must be a non-empty string")
    return {"type": identifier_type, "value": identifier}


def _yes_no(value: Any, context: str) -> bool | None:
    if value is None or isinstance(value, bool):
        return value
    if value == "Y":
        return True
    if value == "N":
        return False
    raise ValueError(f"Europe PMC {context} must be Y, N, boolean, or null")


def normalize_result(
    result: dict[str, Any], fetched_at: str, query: str, cursor_mark: str,
    page: int, requested_size: int
) -> dict[str, Any]:
    source = result.get("source")
    external_id = result.get("id")
    if not isinstance(source, str) or not source or not isinstance(external_id, str) or not external_id:
        raise ValueError("Europe PMC search result must have non-empty string source and id")
    article_id = f"{source.upper()}:{external_id}"

    authors = []
    for index, author in enumerate(_nested_list(result, "authorList", "author", "result")):
        if not isinstance(author, dict):
            raise ValueError(f"Europe PMC result author {index} must be an object")
        affiliations = []
        for affiliation_index, affiliation in enumerate(
            _nested_list(
                author,
                "authorAffiliationDetailsList",
                "authorAffiliation",
                f"author {index}",
            )
        ):
            if not isinstance(affiliation, dict):
                raise ValueError(
                    f"Europe PMC result author {index} affiliation {affiliation_index} "
                    "must be an object"
                )
            text = _clean_text(
                affiliation.get("affiliation"),
                f"author {index} affiliation {affiliation_index}",
            )
            if text is not None:
                affiliations.append(text)
        authors.append(
            {
                "full_name": _clean_text(author.get("fullName"), f"author {index}.fullName"),
                "first_name": _clean_text(author.get("firstName"), f"author {index}.firstName"),
                "last_name": _clean_text(author.get("lastName"), f"author {index}.lastName"),
                "initials": _clean_text(author.get("initials"), f"author {index}.initials"),
                "author_id": _author_id(author.get("authorId"), f"author {index}.authorId"),
                "affiliations": affiliations,
            }
        )

    journal_info = _optional_object(result, "journalInfo", "result")
    journal = _optional_object(journal_info, "journal", "result.journalInfo")
    issue = _optional_object(journal_info, "journalIssue", "result.journalInfo")

    full_text_ids = _string_list(
        _nested_list(result, "fullTextIdList", "fullTextId", "result"),
        "result.fullTextIdList.fullTextId",
    )
    links: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for full_text_id in full_text_ids:
        url = f"{FULL_TEXT_URL}/{urllib.parse.quote(full_text_id, safe='')}"
        links.append({"url": url, "id": full_text_id, "type": "europe_pmc_full_text"})
        seen_urls.add(url)
    for index, advertised in enumerate(
        _nested_list(result, "fullTextUrlList", "fullTextUrl", "result")
    ):
        if not isinstance(advertised, dict) or not isinstance(advertised.get("url"), str):
            raise ValueError(f"Europe PMC result full-text URL {index} must be an object with url")
        if advertised["url"] not in seen_urls:
            links.append(dict(advertised))
            seen_urls.add(advertised["url"])

    publication_types = _string_list(
        _nested_list(result, "pubTypeList", "pubType", "result"),
        "result.pubTypeList.pubType",
    )
    keywords = _string_list(
        _nested_list(result, "keywordList", "keyword", "result"),
        "result.keywordList.keyword",
    )
    mesh_headings = _nested_list(result, "meshHeadingList", "meshHeading", "result")
    grants = _nested_list(result, "grantsList", "grant", "result")
    if not all(isinstance(value, dict) for value in mesh_headings):
        raise ValueError("Europe PMC result mesh headings must be objects")
    if not all(isinstance(value, dict) for value in grants):
        raise ValueError("Europe PMC result grants must be objects")

    search_url = _search_request_url(query, requested_size, cursor_mark)
    source_url = (
        f"{ARTICLE_URL}/{urllib.parse.quote(source.upper(), safe='')}/"
        f"{urllib.parse.quote(external_id, safe='')}"
    )
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": article_id,
        "publication_source": source.upper(),
        "publication_id": external_id,
        "pmid": result.get("pmid"),
        "pmcid": result.get("pmcid"),
        "doi": result.get("doi"),
        "title": _clean_text(result.get("title"), "result.title"),
        "abstract": _clean_text(result.get("abstractText"), "result.abstractText"),
        "author_string": _clean_text(result.get("authorString"), "result.authorString"),
        "authors": authors,
        "journal": {
            "title": journal.get("title"),
            "abbreviation": journal.get("medlineAbbreviation"),
            "issn": journal.get("issn"),
            "essn": journal.get("essn"),
            "volume": journal_info.get("volume", issue.get("volume")),
            "issue": journal_info.get("issue", issue.get("issue")),
            "date_of_publication": journal_info.get("dateOfPublication"),
            "year_of_publication": journal_info.get("yearOfPublication"),
            "month_of_publication": journal_info.get("monthOfPublication"),
            "print_publication_date": journal_info.get("printPublicationDate"),
            "electronic_publication_date": journal_info.get("electronicPublicationDate"),
        },
        "publication_year": result.get("pubYear"),
        "first_publication_date": result.get("firstPublicationDate"),
        "first_index_date": result.get("firstIndexDate"),
        "publication_status": result.get("publicationStatus"),
        "publication_types": publication_types,
        "language": result.get("language"),
        "keywords": keywords,
        "mesh_headings": [dict(value) for value in mesh_headings],
        "grants": [dict(value) for value in grants],
        "cited_by_count": result.get("citedByCount"),
        "is_open_access": _yes_no(result.get("isOpenAccess"), "result.isOpenAccess"),
        "in_epmc": _yes_no(result.get("inEPMC"), "result.inEPMC"),
        "in_pmc": _yes_no(result.get("inPMC"), "result.inPMC"),
        "has_pdf": _yes_no(result.get("hasPDF"), "result.hasPDF"),
        "source_url": source_url,
        "full_text_links": links,
        "annotations": [],
        "retrieval": {
            "query": query,
            "cursor_mark": cursor_mark,
            "page": page,
            "search_url": search_url,
            "annotations_url": None,
        },
    }


def fetch_bounded(
    *,
    query: str,
    cursor_mark: str,
    page_size: int,
    max_pages: int,
    max_records: int,
    fetched_at: str,
    client: HttpClient,
) -> tuple[list[dict[str, Any]], str]:
    """Fetch a bounded traversal and return records plus the uncommitted next cursor."""
    records: list[dict[str, Any]] = []
    cursor = cursor_mark
    seen_cursors = {cursor}
    completed = False

    for page in range(1, max_pages + 1):
        remaining = max_records - len(records)
        if remaining <= 0:
            break
        requested_size = min(page_size, remaining)
        search_url = _search_request_url(query, requested_size, cursor)
        document = client.get_json(search_url, "search")
        results, next_cursor, _hit_count = parse_search_response(document)
        if len(results) > requested_size:
            raise ValueError(
                f"Europe PMC search returned {len(results)} results for pageSize={requested_size}"
            )
        page_records = [
            normalize_result(result, fetched_at, query, cursor, page, requested_size)
            for result in results
        ]
        page_ids = [record["id"] for record in page_records]
        if len(set(page_ids)) != len(page_ids):
            raise ValueError(f"Europe PMC search page {page} contains duplicate publication IDs")

        if page_records:
            for start in range(0, len(page_ids), MAX_ANNOTATION_IDS_PER_REQUEST):
                batch_ids = page_ids[start:start + MAX_ANNOTATION_IDS_PER_REQUEST]
                annotations_url = _annotations_request_url(batch_ids)
                annotations_document = client.get_json(annotations_url, "annotations")
                annotations = parse_annotations_response(annotations_document, set(batch_ids))
                for record in page_records[start:start + len(batch_ids)]:
                    record["annotations"] = annotations[record["id"]]
                    record["retrieval"]["annotations_url"] = annotations_url
        records.extend(page_records)

        if not results or len(results) < requested_size or next_cursor is None or next_cursor == cursor:
            completed = True
            break
        if next_cursor in seen_cursors:
            raise ValueError("Europe PMC search cursor cycle detected")
        cursor = next_cursor
        seen_cursors.add(cursor)

    return records, "*" if completed else cursor


def serialize_records(records: Sequence[dict[str, Any]]) -> list[str]:
    """Finish every potentially failing serialization before writing staged output."""
    return [
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for record in records
    ]


def publish(
    lines: Sequence[str],
    out,
    path: pathlib.Path | None,
    candidate_state: dict[str, Any],
) -> None:
    for line in lines:
        out.write(line)
    out.flush()
    if path is not None:
        save_state(path, candidate_state)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def main(argv: Sequence[str] | None = None, *, out=None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch a bounded Europe PMC metadata and annotations cursor traversal."
    )
    parser.add_argument("--query", default=DEFAULT_QUERY)
    parser.add_argument("--page-size", type=_positive_int, default=100)
    parser.add_argument("--max-pages", type=_positive_int, default=4)
    parser.add_argument("--max-records", type=_positive_int, default=400)
    parser.add_argument("--cursor", help="override the saved starting cursor")
    parser.add_argument("--timeout", type=_positive_float, default=30.0)
    parser.add_argument("--retries", type=_nonnegative_int, default=2)
    parser.add_argument(
        "--request-interval",
        type=_nonnegative_float,
        default=0.5,
        help="minimum seconds between HTTP attempts",
    )
    parser.add_argument("--state-file")
    parser.add_argument("--no-state", action="store_true")
    args = parser.parse_args(argv)
    if not args.query:
        parser.error("--query must not be empty")
    if args.page_size > MAX_PAGE_SIZE:
        parser.error(f"--page-size must not exceed {MAX_PAGE_SIZE}")
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.cursor is not None and not args.cursor:
        parser.error("--cursor must not be empty")

    path = None if args.no_state else state_path(args.state_file)
    state = (
        {"version": STATE_VERSION, "source": SOURCE, "query": None, "cursor_mark": "*"}
        if path is None
        else load_state(path)
    )
    # A changed scheduled query is a different traversal, never a compatible cursor.
    saved_cursor = state["cursor_mark"] if state.get("query") == args.query else "*"
    starting_cursor = args.cursor or saved_cursor
    fetched_at = datetime.now(timezone.utc).isoformat()
    client = HttpClient(args.timeout, args.retries, args.request_interval)
    records, next_cursor = fetch_bounded(
        query=args.query,
        cursor_mark=starting_cursor,
        page_size=args.page_size,
        max_pages=args.max_pages,
        max_records=args.max_records,
        fetched_at=fetched_at,
        client=client,
    )
    lines = serialize_records(records)
    candidate_state = {
        "version": STATE_VERSION,
        "source": SOURCE,
        "query": args.query,
        "cursor_mark": next_cursor,
    }
    publish(lines, sys.stdout if out is None else out, path, candidate_state)


if __name__ == "__main__":
    main()
