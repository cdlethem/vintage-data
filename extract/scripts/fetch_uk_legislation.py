#!/usr/bin/env python3
"""UK legislation.gov.uk new-legislation feed.

The official "new legislation" listing is a keyless Atom feed at
https://www.legislation.gov.uk/new/data.feed (equivalent to /all/data.feed with
sort=published), excluding items uploaded with the "silent" flag. Entries are
sorted by first publication date/time descending. Each entry carries the stable
legislation id (e.g. uksi/2026/1146), type, year, number, title, preamble,
creation date, optional ISBN, publication timestamp and versioned URLs.
Correction slips are published as standalone PDF entries whose id is the PDF
URL; they are kept as separate records under that id and linked to their
instrument via the legislation_id field.
Welsh instrument titles are XHTML (English/Welsh); the fetcher flattens them to
plain text.

Legislation is published on UK business days (a typical day yields ~20-30 items,
i.e. one 100-item page). This extractor walks the feed from the newest item down
to an inclusive floor: the stored publication watermark on later runs, an explicit
--since for a reconciliation run, or a bounded lookback window on the initial
stateless run (the full historical catalogue is deliberately not ingested).
Records are deduplicated by a canonical content hash keyed on the legislation id,
so a re-served item is silent while a revised item (changed preamble, dates or
links) is re-emitted.

Legislation content is Crown copyright published under the Open Government
Licence v3.0; attribution is required: "Contains public sector information
licensed under the Open Government Licence v3.0 (OGL v3)". Documented fair use
is 3,000 requests per 5 minutes per IP; one run makes a handful of page
requests.

Stdlib only. Verified live 2026-11-01.
"""

import argparse
import email.utils
import hashlib
import json
import math
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ElementTree
from datetime import datetime, timedelta, timezone
from pathlib import Path

SOURCE = "uk_legislation"
FEED_URL = "https://www.legislation.gov.uk/new/data.feed"
PAGE_SIZE = 100
MAX_PAGES = 50
INITIAL_LOOKBACK_DAYS = 7
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
STATE_VERSION = 1

ID_URL_PATTERN = re.compile(r"^https?://www\.legislation\.gov\.uk/id/([^/]+)/(\d+)/([^/]+)$")
PDF_URL_PATTERN = re.compile(r"^https?://www\.legislation\.gov\.uk/([^/]+)/(\d+)/([^/]+)/pdfs/([^/]+)$")
ATOM = "{http://www.w3.org/2005/Atom}"
UKM = "{http://www.legislation.gov.uk/namespaces/metadata}"

TRANSIENT_STATUS = frozenset({408, 425, 429, 500, 502, 503, 504})
MAX_TRANSIENT_ATTEMPTS = 4
DEFAULT_RETRY_DELAY_SECONDS = 1
MAX_RETRY_DELAY_SECONDS = 30
MAX_RETRY_CUMULATIVE_DELAY_SECONDS = 60


def parse_timestamp(value, name):
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO-8601 string")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} is not ISO-8601: {value!r}") from error
    if timestamp.tzinfo is None:
        raise ValueError(f"{name} must include a UTC offset: {value!r}")
    return timestamp.astimezone(timezone.utc)


def timestamp_text(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_hash(record):
    encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def retry_after_delay(headers, now=None):
    value = headers.get("Retry-After") if headers is not None else None
    try:
        delay = int(value)
        if delay < 0:
            raise ValueError
    except (TypeError, ValueError):
        try:
            retry_at = email.utils.parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                raise ValueError
            delay = max(0, math.ceil((retry_at - (now or datetime.now(timezone.utc))).total_seconds()))
        except (TypeError, ValueError, IndexError, OverflowError):
            delay = DEFAULT_RETRY_DELAY_SECONDS
    return min(delay, MAX_RETRY_DELAY_SECONDS)


def http_error_message(url, status, body):
    return f"GET {url} returned HTTP {status}; response body omitted ({len(body)} bytes read)"


def request_feed(url, timeout):
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/atom+xml", "User-Agent": USER_AGENT},
    )
    cumulative_delay = 0
    for attempt in range(1, MAX_TRANSIENT_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                status = response.status
                body = response.read()
            transient = False
            headers = None
        except urllib.error.HTTPError as error:
            if error.code not in TRANSIENT_STATUS:
                try:
                    detail = error.read(1000)
                finally:
                    error.close()
                raise RuntimeError(http_error_message(url, error.code, detail)) from error
            error.close()
            status = error.code
            headers = error.headers
            transient = True
        except (urllib.error.URLError, TimeoutError) as error:
            if attempt == MAX_TRANSIENT_ATTEMPTS:
                raise RuntimeError(
                    f"GET {url} failed after {attempt} attempts: {getattr(error, 'reason', error)}"
                ) from error
            status = None
            headers = None
            transient = True
        if not transient:
            if status != 200:
                raise RuntimeError(http_error_message(url, status, body))
            return body
        if status is not None and attempt == MAX_TRANSIENT_ATTEMPTS:
            raise RuntimeError(f"GET {url} returned HTTP {status} after {attempt} attempts; retry limit exhausted")
        label = "failed" if status is None else f"returned HTTP {status}"
        delay = retry_after_delay(headers)
        if cumulative_delay + delay > MAX_RETRY_CUMULATIVE_DELAY_SECONDS:
            raise RuntimeError(
                f"GET {url} {label}; retry delay budget exhausted after "
                f"{attempt} attempts and {cumulative_delay} seconds"
            )
        time.sleep(delay)
        cumulative_delay += delay
    raise AssertionError("unreachable")


def parse_entry(entry):
    entry_id = (entry.findtext(f"{ATOM}id") or "").strip()
    pdf_match = PDF_URL_PATTERN.match(entry_id)
    if pdf_match:
        # Correction slips are published as PDF documents whose entry id is the
        # PDF URL, not the legislation id; the ukm metadata names the instrument.
        type_code, year, number, pdf_file = pdf_match.groups()
        year_element = entry.find(f"{UKM}Year")
        number_element = entry.find(f"{UKM}Number")
        if (
            year_element is None
            or year_element.get("Value") != year
            or number_element is None
            or number_element.get("Value") != number
        ):
            raise ValueError(f"entry {entry_id!r} ukm metadata does not match its id")
        record_id = f"{type_code}/{year}/{number}/pdfs/{pdf_file}"
        legislation_id = f"{type_code}/{year}/{number}"
    else:
        match = ID_URL_PATTERN.match(entry_id)
        if not match:
            raise ValueError(f"entry id is not a legislation identifier URI: {entry_id!r}")
        type_code, year, number = match.groups()
        record_id = legislation_id = f"{type_code}/{year}/{number}"
    title_element = entry.find(f"{ATOM}title")
    if title_element is None:
        title = ""
    else:
        # WSI entries carry XHTML titles (English/Welsh spans); flatten to text.
        title = " ".join("".join(title_element.itertext()).split())
    if not title:
        raise ValueError(f"entry {record_id} has no title")
    published_raw = entry.findtext(f"{ATOM}published")
    published = parse_timestamp(published_raw, f"entry {record_id} published")
    type_element = entry.find(f"{UKM}DocumentMainType")
    if type_element is None or not (type_element.get("Value") or "").strip():
        raise ValueError(f"entry {record_id} is missing ukm:DocumentMainType")
    type_name = type_element.get("Value").strip()

    url = None
    data_xml_url = None
    for link in entry.findall(f"{ATOM}link"):
        rel = link.get("rel")
        link_type = link.get("type")
        href = link.get("href")
        if not href:
            continue
        if rel is None and url is None:
            url = href
        elif rel == "alternate" and link_type == "application/xml" and data_xml_url is None:
            data_xml_url = href

    summary = entry.findtext(f"{ATOM}summary")
    created_element = entry.find(f"{UKM}CreationDate")
    created = created_element.get("Date") if created_element is not None else None
    isbn_element = entry.find(f"{UKM}ISBN")
    isbn = isbn_element.get("Value") if isbn_element is not None else None

    return {
        "id": record_id,
        "legislation_id": legislation_id,
        "type": type_name,
        "type_code": type_code,
        "year": year,
        "number": number,
        "title": title,
        "summary": summary.strip() if isinstance(summary, str) and summary.strip() else None,
        "created": created.strip() if isinstance(created, str) and created.strip() else None,
        "isbn": isbn.strip() if isinstance(isbn, str) and isbn.strip() else None,
        "published_at": timestamp_text(published),
        "url": url,
        "pdf_url": url if pdf_match else None,
        "data_xml_url": data_xml_url,
    }


def first_page_url():
    return f"{FEED_URL}?sort=published&results-count={PAGE_SIZE}"


def collect(floor, timeout):
    """Yield (record, published) for every feed entry with published >= floor."""
    url = first_page_url()
    pages = 0
    while url is not None:
        pages += 1
        if pages > MAX_PAGES:
            raise RuntimeError(
                f"feed pagination exceeded {MAX_PAGES} pages; the floor may be too old"
            )
        body = request_feed(url, timeout)
        try:
            root = ElementTree.fromstring(body)
        except ElementTree.ParseError as error:
            raise RuntimeError(f"GET {url} returned invalid Atom XML: {error}") from error
        next_url = None
        for link in root.findall(f"{ATOM}link"):
            if link.get("rel") == "next" and link.get("href"):
                next_url = link.get("href")
                break
        entries = root.findall(f"{ATOM}entry")
        for entry in entries:
            record = parse_entry(entry)
            published = parse_timestamp(record["published_at"], f"entry {record['id']} published_at")
            if published < floor:
                return
            yield record, published
        url = next_url


def build_records(rows, known, fetched_at):
    records = []
    next_known = dict(known)
    for record, _published in rows:
        row_hash = canonical_hash(record)
        record_id = record["id"]
        if next_known.get(record_id) != row_hash:
            out = dict(record)
            out.update({"source": SOURCE, "fetched_at": fetched_at})
            records.append(out)
            next_known[record_id] = row_hash
    return records, next_known


def load_state(path):
    if not path.exists():
        return {"version": STATE_VERSION, "source": SOURCE, "watermark": None, "known": {}}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError(f"cannot read state file {path}: {error}") from error
    if (
        not isinstance(state, dict)
        or state.get("version") != STATE_VERSION
        or state.get("source") != SOURCE
        or not isinstance(state.get("known"), dict)
    ):
        raise RuntimeError(f"state file {path} has an unexpected shape")
    watermark = state.get("watermark")
    if watermark is not None:
        parse_timestamp(watermark, "state watermark")
    if not all(isinstance(key, str) and isinstance(value, str) for key, value in state["known"].items()):
        raise RuntimeError(f"state file {path} has invalid known records")
    return state


def write_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            json.dump(state, output, sort_keys=True, separators=(",", ":"))
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def state_path(argument):
    if argument:
        return Path(argument)
    root = os.environ.get("EXTRACT_DATA_ROOT")
    if not root:
        raise RuntimeError("EXTRACT_DATA_ROOT is required unless --state-file or --no-state is used")
    return Path(root) / "state" / f"{SOURCE}.json"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file")
    parser.add_argument("--no-state", action="store_true")
    parser.add_argument("--since", help="inclusive ISO-8601 publication floor for a reconciliation run")
    parser.add_argument("--lookback-days", type=int, default=INITIAL_LOOKBACK_DAYS,
                        help="initial-run lookback window in days (default %(default)s)")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.lookback_days <= 0:
        parser.error("--lookback-days must be positive")
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")

    state_file = None if args.no_state else state_path(args.state_file)
    state = {"version": STATE_VERSION, "source": SOURCE, "watermark": None, "known": {}}
    if state_file is not None:
        state = load_state(state_file)

    if args.since:
        floor = parse_timestamp(args.since, "--since")
    elif state.get("watermark"):
        floor = parse_timestamp(state["watermark"], "state watermark")
    else:
        floor = datetime.now(timezone.utc) - timedelta(days=args.lookback_days)
    print(
        f"Fetching UK legislation published since {timestamp_text(floor)}",
        file=sys.stderr,
    )

    rows = list(collect(floor, args.timeout))
    fetched_at = datetime.now(timezone.utc).isoformat()
    records, next_known = build_records(rows, state["known"], fetched_at)
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    sys.stdout.flush()

    if state_file is not None:
        watermark = state.get("watermark")
        if rows:
            latest = max(published for _record, published in rows)
            watermark = timestamp_text(latest)
        boundary_ids = sorted(
            record["id"]
            for record, published in rows
            if watermark and timestamp_text(published) == watermark
        )
        next_state = {
            "version": STATE_VERSION,
            "source": SOURCE,
            "watermark": watermark,
            "boundary_ids": boundary_ids,
            "known": next_known,
        }
        write_state(state_file, next_state)
    print(
        f"Fetched {len(rows)} legislation entries; emitted {len(records)} records",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
