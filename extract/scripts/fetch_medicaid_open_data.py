#!/usr/bin/env python3
"""Fetch CMS Medicaid Open Data catalog metadata without downloading distributions.

The catalog search is a complete snapshot. Each successful run validates and reconciles
all reported results before emitting records. Stateful runs retain only each dataset's
upstream ``modified`` watermark, so unchanged catalog entries are suppressed while
changed entries are emitted again. The state is replaced atomically only after all
output has been written.

This source is catalog metadata, not Medicaid analytical measures. Python standard
library only.
"""

import argparse
from datetime import datetime, timezone
import json
import os
import pathlib
import sys
import tempfile
from typing import Any, Sequence
import urllib.error
import urllib.parse
import urllib.request
import uuid

SOURCE = "medicaid_open_data"
URL = "https://data.medicaid.gov/api/1/search"
FULLTEXT = "Medicaid"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 100
HARD_MAX_PAGES = 100
MAX_TIMEOUT = 300


class CatalogError(RuntimeError):
    """The catalog cannot satisfy the complete-snapshot contract."""


class IncompleteCatalogError(CatalogError):
    """Pagination ended or hit a bound before the declared total was reconciled."""


def _bounded_positive_int(value: Any, name: str, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
        raise ValueError(f"{name} must be an integer between 1 and {maximum}")
    return value


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def _valid_uuid(value: Any) -> bool:
    if not isinstance(value, str) or not value:
        return False
    try:
        uuid.UUID(value)
    except (ValueError, AttributeError):
        return False
    return True


def _valid_modified(value: Any) -> bool:
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def load_state(path: pathlib.Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            state = json.load(handle)
    except FileNotFoundError:
        return {"version": STATE_VERSION, "source": SOURCE, "watermarks": {}}
    except json.JSONDecodeError as error:
        raise ValueError(f"malformed state in {path}: invalid JSON") from error

    if not isinstance(state, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if state.get("version") != STATE_VERSION or state.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: incompatible version or source")
    watermarks = state.get("watermarks")
    if not isinstance(watermarks, dict) or not all(
        _valid_uuid(dataset_id) and _valid_modified(modified)
        for dataset_id, modified in watermarks.items()
    ):
        raise ValueError(
            f"malformed state in {path}: watermarks must map UUIDs to modified timestamps"
        )
    return state


def save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    """Atomically publish state after complete extraction and successful stdout."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                state,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
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


def _request_page(page: int, page_size: int, timeout: int) -> Any:
    params = {
        "fulltext": FULLTEXT,
        "facets": "0",
        "page-size": str(page_size),
        "page": str(page),
    }
    request = urllib.request.Request(
        f"{URL}?{urllib.parse.urlencode(params)}",
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        raise CatalogError(
            f"Medicaid catalog request failed: status={error.code} "
            f"body={_body_context(error.read())!r}"
        ) from error
    with response:
        status = getattr(response, "status", None)
        if status is None:
            getcode = getattr(response, "getcode", None)
            status = getcode() if getcode is not None else None
        body = response.read()
    if status is not None and not 200 <= status < 300:
        raise CatalogError(
            f"Medicaid catalog request failed: status={status} "
            f"body={_body_context(body)!r}"
        )
    try:
        return json.loads(
            body,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"invalid JSON constant {value}")
            ),
        )
    except (UnicodeDecodeError, ValueError) as error:
        raise CatalogError(
            f"Medicaid catalog response is not valid JSON: body={_body_context(body)!r}"
        ) from error


def _parse_page(document: Any) -> tuple[int, list[Any]]:
    if not isinstance(document, dict):
        raise CatalogError("Medicaid catalog response must be an object")
    if document.get("success") is False or "error" in document:
        raise CatalogError("Medicaid catalog response reports an API error")
    total = document.get("total")
    results = document.get("results")
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise CatalogError("Medicaid catalog response total must be a non-negative integer")
    if not isinstance(results, list):
        raise CatalogError("Medicaid catalog response results must be a list")
    return total, results


def _validate_distribution(value: Any, record_index: int) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise CatalogError(
            f"Medicaid catalog result {record_index} distribution must be a list"
        )
    for distribution_index, distribution in enumerate(value):
        if not isinstance(distribution, dict):
            raise CatalogError(
                "Medicaid catalog result "
                f"{record_index} distribution[{distribution_index}] must be an object"
            )
        for link_name in ("accessURL", "downloadURL"):
            link = distribution.get(link_name)
            if link is not None and (not isinstance(link, str) or not link):
                raise CatalogError(
                    "Medicaid catalog result "
                    f"{record_index} distribution[{distribution_index}].{link_name} "
                    "must be a non-empty string"
                )


def normalize_record(record: Any, fetched_at: str, record_index: int = 0) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise CatalogError(f"Medicaid catalog result {record_index} must be an object")
    identifier = record.get("identifier")
    if not _valid_uuid(identifier):
        raise CatalogError(
            f"Medicaid catalog result {record_index} identifier must be a UUID"
        )
    modified = record.get("modified")
    if not _valid_modified(modified):
        raise CatalogError(
            f"Medicaid catalog result {record_index} modified must be an ISO timestamp"
        )
    _validate_distribution(record.get("distribution"), record_index)

    normalized = dict(record)
    normalized.update({"source": SOURCE, "fetched_at": fetched_at, "id": identifier})
    return normalized


def fetch_catalog(
    *,
    page_size: int = DEFAULT_PAGE_SIZE,
    max_pages: int = DEFAULT_MAX_PAGES,
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """Return a fully validated, total-reconciled catalog snapshot."""
    _bounded_positive_int(page_size, "page_size", MAX_PAGE_SIZE)
    _bounded_positive_int(max_pages, "max_pages", HARD_MAX_PAGES)
    _bounded_positive_int(timeout, "timeout", MAX_TIMEOUT)

    fetched_at = datetime.now(timezone.utc).isoformat()
    expected_total: int | None = None
    records: list[dict[str, Any]] = []
    seen_modified: dict[str, str] = {}

    for page in range(1, max_pages + 1):
        document = _request_page(page, page_size, timeout)
        total, page_results = _parse_page(document)
        if expected_total is None:
            expected_total = total
            if expected_total > page_size * max_pages:
                raise IncompleteCatalogError(
                    f"Medicaid catalog reports {expected_total} results, above bounded "
                    f"capacity {page_size * max_pages}"
                )
        elif total != expected_total:
            raise CatalogError(
                f"Medicaid catalog total changed from {expected_total} to {total} "
                "during pagination"
            )

        new_on_page = 0
        for page_index, raw_record in enumerate(page_results):
            record = normalize_record(raw_record, fetched_at, page_index)
            dataset_id = record["id"]
            previous_modified = seen_modified.get(dataset_id)
            if previous_modified is not None:
                if previous_modified != record["modified"]:
                    raise CatalogError(
                        f"Medicaid catalog UUID {dataset_id} changed modified watermark "
                        "during pagination"
                    )
                continue
            seen_modified[dataset_id] = record["modified"]
            records.append(record)
            new_on_page += 1

        if len(records) > expected_total:
            raise CatalogError(
                f"Medicaid catalog returned {len(records)} unique results but total "
                f"declared {expected_total}"
            )
        if len(records) == expected_total:
            return records
        if not page_results:
            raise IncompleteCatalogError(
                f"Medicaid catalog returned an empty page after {len(records)} of "
                f"{expected_total} unique results"
            )
        if new_on_page == 0:
            raise IncompleteCatalogError(
                f"Medicaid catalog repeated page {page} after {len(records)} of "
                f"{expected_total} unique results"
            )
        if len(page_results) < page_size:
            raise IncompleteCatalogError(
                f"Medicaid catalog pagination ended after {len(records)} of "
                f"{expected_total} unique results"
            )

    raise IncompleteCatalogError(
        f"Medicaid catalog exceeded max_pages={max_pages} after {len(records)} of "
        f"{expected_total} unique results"
    )


def records_changed_since(
    records: Sequence[dict[str, Any]], previous: dict[str, str]
) -> list[dict[str, Any]]:
    return [record for record in records if previous.get(record["id"]) != record["modified"]]


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description="Fetch complete Medicaid Open Data catalog metadata."
    )
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state"
    )
    parser.add_argument(
        "--no-state",
        action="store_true",
        help="emit the complete catalog without reading or writing state",
    )
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")

    try:
        path = None if args.no_state else state_path(args.state_file)
        state = (
            {"version": STATE_VERSION, "source": SOURCE, "watermarks": {}}
            if path is None
            else load_state(path)
        )
        records = fetch_catalog(
            page_size=args.page_size,
            max_pages=args.max_pages,
            timeout=args.timeout,
        )
        current = {record["id"]: record["modified"] for record in records}
        emitted = records if path is None else records_changed_since(records, state["watermarks"])
        for record in emitted:
            print(
                json.dumps(
                    record,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                )
            )
        sys.stdout.flush()
        if path is not None:
            save_state(
                path,
                {"version": STATE_VERSION, "source": SOURCE, "watermarks": current},
            )
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
