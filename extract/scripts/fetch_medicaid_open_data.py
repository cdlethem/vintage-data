#!/usr/bin/env python3
"""Fetch CMS Medicaid Open Data catalog metadata without downloading distributions.

The catalog search is a complete snapshot. Each successful run validates and reconciles
all reported results before emitting records. Stateful runs retain each dataset's
upstream ``modified`` watermark, so unchanged entries are suppressed after (and only
after) the local raw sink has published the exact emitted artifact. A pending checkpoint
contains that artifact's digest; the next run promotes it only after finding the digest
under the source's raw landing directory. Missing evidence always causes safe replay.

This source is catalog metadata, not Medicaid analytical measures. Python standard
library only.
"""

import argparse
import hashlib
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
URL = os.environ.get("MEDICAID_OPEN_DATA_URL") or "https://data.medicaid.gov/api/1/search"
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


def _valid_watermarks(value: Any) -> bool:
    return isinstance(value, dict) and all(
        _valid_uuid(dataset_id) and _valid_modified(modified)
        for dataset_id, modified in value.items()
    )


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
    if not _valid_watermarks(state.get("watermarks")):
        raise ValueError(
            f"malformed state in {path}: watermarks must map UUIDs to modified timestamps"
        )
    pending = state.get("pending")
    if pending is not None:
        valid_digest = (
            isinstance(pending, dict)
            and isinstance(pending.get("artifact_sha256"), str)
            and len(pending["artifact_sha256"]) == 64
            and all(character in "0123456789abcdef" for character in pending["artifact_sha256"])
        )
        if not (
            valid_digest
            and isinstance(pending.get("bytes"), int)
            and not isinstance(pending["bytes"], bool)
            and pending["bytes"] > 0
            and isinstance(pending.get("records"), int)
            and not isinstance(pending["records"], bool)
            and pending["records"] > 0
            and _valid_watermarks(pending.get("watermarks"))
        ):
            raise ValueError(f"malformed state in {path}: invalid pending checkpoint")
    return state


def save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    """Atomically publish committed or pending state."""
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


def raw_source_path() -> pathlib.Path:
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "raw" / f"source={SOURCE}"


def artifact_landed(pending: dict[str, Any]) -> bool:
    """Return whether the local raw sink published the exact pending stdout bytes."""
    source_root = raw_source_path()
    try:
        artifacts = source_root.glob("dt=*/*.ndjson")
        for artifact in artifacts:
            try:
                if artifact.stat().st_size != pending["bytes"]:
                    continue
                digest = hashlib.sha256()
                with artifact.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        digest.update(chunk)
                if digest.hexdigest() == pending["artifact_sha256"]:
                    return True
            except (FileNotFoundError, OSError):
                continue
    except OSError:
        pass
    return False


def reconcile_landed_state(path: pathlib.Path, state: dict[str, Any]) -> dict[str, Any]:
    """Promote a pending snapshot only when its raw artifact is observable."""
    pending = state.get("pending")
    if pending is None or not artifact_landed(pending):
        return state
    committed = {
        "version": STATE_VERSION,
        "source": SOURCE,
        "watermarks": pending["watermarks"],
    }
    save_state(path, committed)
    return committed


def serialize_records(records: Sequence[dict[str, Any]]) -> tuple[list[str], str, int]:
    lines: list[str] = []
    digest = hashlib.sha256()
    byte_count = 0
    for record in records:
        line = json.dumps(
            record,
            ensure_ascii=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        encoded = (line + "\n").encode("ascii")
        lines.append(line)
        digest.update(encoded)
        byte_count += len(encoded)
    return lines, digest.hexdigest(), byte_count


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
        if path is not None:
            state = reconcile_landed_state(path, state)
        records = fetch_catalog(
            page_size=args.page_size,
            max_pages=args.max_pages,
            timeout=args.timeout,
        )
        current = {record["id"]: record["modified"] for record in records}
        emitted = records if path is None else records_changed_since(records, state["watermarks"])
        lines, artifact_sha256, byte_count = serialize_records(emitted)

        if path is not None:
            if lines:
                save_state(
                    path,
                    {
                        "version": STATE_VERSION,
                        "source": SOURCE,
                        "watermarks": state["watermarks"],
                        "pending": {
                            "artifact_sha256": artifact_sha256,
                            "bytes": byte_count,
                            "records": len(lines),
                            "watermarks": current,
                        },
                    },
                )
            else:
                save_state(
                    path,
                    {"version": STATE_VERSION, "source": SOURCE, "watermarks": current},
                )

        for line in lines:
            sys.stdout.write(line + "\n")
        sys.stdout.flush()
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
