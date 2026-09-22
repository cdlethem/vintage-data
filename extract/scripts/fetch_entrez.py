#!/usr/bin/env python3
"""Bounded, keyless PubMed retrieval through NCBI Entrez E-utilities.

ESearch discovers stable PubMed identifiers in bounded pages. Each identifier is then
retrieved separately with EFetch so ``raw_payload`` is the exact UTF-8 XML response for
that PMID, not a reconstructed XML fragment. NCBI request identification is mandatory.
Stdlib only.
"""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Sequence

SOURCE = "entrez"
BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
SEARCH_URL = f"{BASE_URL}/esearch.fcgi"
FETCH_URL = f"{BASE_URL}/efetch.fcgi"
SUPPORTED_DATABASES = frozenset({"pubmed"})
DEFAULT_TOOL = "vintage-data"
DEFAULT_TERM = "climate change"
DEFAULT_RETMAX = 25
DEFAULT_PAGE_SIZE = 10
DEFAULT_MAX_PAGES = 3
HARD_MAX_RECORDS = 100
HARD_MAX_PAGE_SIZE = 100
HARD_MAX_PAGES = 10
TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})


class EntrezError(RuntimeError):
    """The API response cannot satisfy the Entrez record contract."""


def _positive_int(value: Any, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value.strip()


def validate_request(db: str, term: str, email: str, tool: str) -> tuple[str, str, str, str]:
    database = _required_text(db, "db").lower()
    if database not in SUPPORTED_DATABASES:
        supported = ", ".join(sorted(SUPPORTED_DATABASES))
        raise ValueError(f"unsupported Entrez database {database!r}; supported: {supported}")
    return (
        database,
        _required_text(term, "term"),
        _required_text(email, "email"),
        _required_text(tool, "tool"),
    )


def _body_context(body: bytes) -> str:
    return body[:300].decode("utf-8", errors="replace").replace("\n", " ")


class HttpClient:
    """Paced byte-response client with bounded transient retries."""

    def __init__(
        self,
        *,
        timeout: float = 30,
        retries: int = 2,
        request_interval: float = 0.34,
        opener: Callable[..., Any] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
    ) -> None:
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= 5:
            raise ValueError("retries must be an integer between 0 and 5")
        if request_interval < 0:
            raise ValueError("request_interval must be non-negative")
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

    @staticmethod
    def _retry_after(error: urllib.error.HTTPError) -> float | None:
        value = error.headers.get("Retry-After") if error.headers is not None else None
        if not value:
            return None
        try:
            return max(0.0, min(float(value), 30.0))
        except ValueError:
            try:
                delay = parsedate_to_datetime(value).timestamp() - datetime.now(timezone.utc).timestamp()
                return max(0.0, min(delay, 30.0))
            except (TypeError, ValueError, OverflowError):
                return None

    def get(self, url: str, endpoint: str) -> bytes:
        for attempt in range(self.retries + 1):
            self._pace()
            request = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json" if endpoint == "ESearch" else "application/xml",
                    "User-Agent": "vintage-data/0.1 Entrez extractor",
                },
            )
            try:
                with self.opener(request, timeout=self.timeout) as response:
                    return response.read()
            except urllib.error.HTTPError as error:
                body = error.read()
                if error.code not in TRANSIENT_STATUS:
                    raise EntrezError(
                        f"NCBI Entrez {endpoint} failed: status={error.code} "
                        f"body={_body_context(body)!r}"
                    ) from error
                failure: BaseException = error
                retry_after = self._retry_after(error)
            except (urllib.error.URLError, TimeoutError, OSError) as error:
                failure = error
                retry_after = None
            if attempt == self.retries:
                raise EntrezError(
                    f"NCBI Entrez {endpoint} failed after {self.retries + 1} attempts: {failure}"
                ) from failure
            self.sleeper(retry_after if retry_after is not None else min(2**attempt, 30))
        raise AssertionError("unreachable")


def _url(endpoint: str, params: dict[str, str]) -> str:
    return endpoint + "?" + urllib.parse.urlencode(params)


def _parse_search(body: bytes) -> tuple[int, list[str]]:
    try:
        document = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise EntrezError(f"NCBI Entrez ESearch response is not JSON: {_body_context(body)!r}") from error
    if not isinstance(document, dict):
        raise EntrezError("NCBI Entrez ESearch response must be an object")
    if document.get("error"):
        raise EntrezError(f"NCBI Entrez ESearch API error: {document['error']}")
    result = document.get("esearchresult")
    if not isinstance(result, dict):
        raise EntrezError("NCBI Entrez ESearch response is missing esearchresult")
    if result.get("errorlist"):
        raise EntrezError(f"NCBI Entrez ESearch API error: {result['errorlist']}")
    count_value = result.get("count")
    ids = result.get("idlist")
    if not isinstance(count_value, str) or not count_value.isdigit():
        raise EntrezError("NCBI Entrez ESearch count must be a non-negative integer string")
    if not isinstance(ids, list) or not all(isinstance(item, str) and item.isdigit() for item in ids):
        raise EntrezError("NCBI Entrez ESearch idlist must contain only numeric strings")
    return int(count_value), ids


def _text(element: ET.Element | None) -> str | None:
    if element is None:
        return None
    value = " ".join("".join(element.itertext()).split())
    return value or None


def _abstract(container: ET.Element) -> str | None:
    parts = []
    for part in container.findall("./Abstract/AbstractText"):
        value = _text(part)
        if value:
            label = part.attrib.get("Label")
            parts.append(f"{label}: {value}" if label else value)
    return "\n".join(parts) or None


def _authors(container: ET.Element) -> list[dict[str, str | None]]:
    return [
        {
            "last_name": _text(author.find("./LastName")),
            "fore_name": _text(author.find("./ForeName")),
            "initials": _text(author.find("./Initials")),
            "collective_name": _text(author.find("./CollectiveName")),
        }
        for author in container.findall("./AuthorList/Author")
    ]


def _publication_date(pub_date: ET.Element | None) -> str | None:
    if pub_date is None:
        return None
    medline_date = _text(pub_date.find("./MedlineDate"))
    if medline_date is not None:
        return medline_date
    return " ".join(
        value
        for value in (
            _text(pub_date.find("./Year")),
            _text(pub_date.find("./Month")),
            _text(pub_date.find("./Day")),
        )
        if value
    ) or None


def _doi(record: ET.Element, identifier_paths: Sequence[str]) -> str | None:
    for path in identifier_paths:
        for identifier in record.findall(path):
            if identifier.attrib.get("IdType") == "doi":
                return _text(identifier)
    return None


def _parse_pubmed_payload(body: bytes, expected_pmid: str) -> dict[str, Any]:
    try:
        raw_payload = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise EntrezError("NCBI Entrez EFetch response is not UTF-8") from error
    try:
        root = ET.fromstring(body)
    except ET.ParseError as error:
        raise EntrezError(f"NCBI Entrez EFetch response is not XML: {_body_context(body)!r}") from error
    api_error = root.find(".//ERROR")
    if api_error is not None:
        raise EntrezError(f"NCBI Entrez EFetch API error: {_text(api_error) or 'unknown error'}")
    if root.tag != "PubmedArticleSet":
        raise EntrezError(
            f"NCBI Entrez EFetch expected PubmedArticleSet, received {root.tag!r}"
        )
    returned_records = list(root)
    if len(returned_records) != 1:
        raise EntrezError(
            f"NCBI Entrez EFetch expected one PubMed record, received {len(returned_records)}"
        )

    record = returned_records[0]
    if record.tag == "PubmedArticle":
        citation = record.find("./MedlineCitation")
        if citation is None:
            raise EntrezError("NCBI Entrez EFetch PubmedArticle is missing MedlineCitation")
        content = citation.find("./Article")
        if content is None:
            raise EntrezError("NCBI Entrez EFetch PubmedArticle is missing Article")
        pmid = _text(citation.find("./PMID"))
        journal = content.find("./Journal")
        journal_title = _text(journal.find("./Title")) if journal is not None else None
        pub_date = journal.find("./JournalIssue/PubDate") if journal is not None else None
        identifier_paths = ("./PubmedData/ArticleIdList/ArticleId",)
        publication_type_nodes = content.findall("./PublicationTypeList/PublicationType")
    elif record.tag == "PubmedBookArticle":
        content = record.find("./BookDocument")
        if content is None:
            raise EntrezError("NCBI Entrez EFetch PubmedBookArticle is missing BookDocument")
        pmid = _text(content.find("./PMID"))
        book = content.find("./Book")
        journal_title = _text(book.find("./BookTitle")) if book is not None else None
        pub_date = book.find("./PubDate") if book is not None else None
        identifier_paths = (
            "./PubmedBookData/ArticleIdList/ArticleId",
            "./BookDocument/ArticleIdList/ArticleId",
        )
        publication_type_nodes = content.findall("./PublicationType")
    else:
        raise EntrezError(f"NCBI Entrez EFetch unsupported PubMed record variant {record.tag!r}")

    if pmid != expected_pmid:
        raise EntrezError(
            f"NCBI Entrez EFetch PMID mismatch: expected {expected_pmid}, received {pmid!r}"
        )
    return {
        "title": _text(content.find("./ArticleTitle")),
        "abstract": _abstract(content),
        "journal": journal_title,
        "publication_date": _publication_date(pub_date),
        "doi": _doi(record, identifier_paths),
        "authors": _authors(content),
        "publication_types": [
            value for node in publication_type_nodes if (value := _text(node)) is not None
        ],
        "raw_payload": raw_payload,
    }


def fetch_entrez(
    *,
    db: str,
    term: str,
    email: str,
    tool: str = DEFAULT_TOOL,
    retmax: int = DEFAULT_RETMAX,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: float = 30,
    retries: int = 2,
    request_interval: float = 0.34,
    client: HttpClient | None = None,
    now: Callable[[], datetime] | None = None,
) -> list[dict[str, Any]]:
    """Discover and retrieve at most ``retmax`` records, or fail without partial output."""
    db, term, email, tool = validate_request(db, term, email, tool)
    _positive_int(retmax, "retmax", HARD_MAX_RECORDS)
    _positive_int(page_size, "page_size", HARD_MAX_PAGE_SIZE)
    _positive_int(max_pages, "max_pages", HARD_MAX_PAGES)
    http = client or HttpClient(
        timeout=timeout,
        retries=retries,
        request_interval=request_interval,
    )
    fetched_at = (now or (lambda: datetime.now(timezone.utc)))().astimezone(timezone.utc).isoformat()
    common = {"db": db, "tool": tool, "email": email}
    discovered: list[str] = []
    seen: set[str] = set()
    expected_count: int | None = None

    for page in range(max_pages):
        remaining = retmax - len(discovered)
        if remaining <= 0:
            break
        requested = min(page_size, remaining)
        body = http.get(
            _url(
                SEARCH_URL,
                {
                    **common,
                    "term": term,
                    "retmode": "json",
                    "retstart": str(page * page_size),
                    "retmax": str(requested),
                    "sort": "pub date",
                },
            ),
            "ESearch",
        )
        count, identifiers = _parse_search(body)
        if expected_count is None:
            expected_count = count
        elif count != expected_count:
            raise EntrezError(
                f"NCBI Entrez ESearch count changed from {expected_count} to {count} during pagination"
            )
        for identifier in identifiers:
            if identifier not in seen:
                seen.add(identifier)
                discovered.append(identifier)
                if len(discovered) == retmax:
                    break
        if not identifiers or len(discovered) >= min(count, retmax):
            break

    records = []
    for pmid in discovered:
        body = http.get(
            _url(
                FETCH_URL,
                {
                    **common,
                    "id": pmid,
                    "rettype": "abstract",
                    "retmode": "xml",
                },
            ),
            "EFetch",
        )
        normalized = _parse_pubmed_payload(body, pmid)
        records.append(
            {
                "source": SOURCE,
                "id": pmid,
                "fetched_at": fetched_at,
                "db": db,
                "term": term,
                **normalized,
            }
        )
    return records


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch a bounded PubMed query through NCBI Entrez.")
    parser.add_argument("--db", default="pubmed")
    parser.add_argument("--term", default=DEFAULT_TERM)
    parser.add_argument("--email", default=os.environ.get("NCBI_EMAIL", ""))
    parser.add_argument("--tool", default=os.environ.get("NCBI_TOOL", DEFAULT_TOOL))
    parser.add_argument("--retmax", type=int, default=DEFAULT_RETMAX)
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--request-interval", type=float, default=0.34)
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        records = fetch_entrez(
            db=args.db,
            term=args.term,
            email=args.email,
            tool=args.tool,
            retmax=args.retmax,
            page_size=args.page_size,
            max_pages=args.max_pages,
            timeout=args.timeout,
            retries=args.retries,
            request_interval=args.request_interval,
        )
    except ValueError as error:
        parser.error(str(error))
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
