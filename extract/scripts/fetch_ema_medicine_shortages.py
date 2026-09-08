#!/usr/bin/env python3
"""European Medicines Agency medicine-shortages snapshot changes.

EMA publishes this complete, machine-readable shortages catalogue twice daily, at 06:00
and 18:00 Amsterdam time.  This extractor retrieves the complete snapshot each run and
compares it to an atomic local baseline, emitting additions, revisions, and removals.
The source-natural identifier is ``shortage_url``.  Schedule no more often than every
12 hours.

EMA permits commercial and non-commercial reproduction and distribution of its website
information when EMA is acknowledged in every copy.  This does not cover third-party
material.  Attribution: "Source: European Medicines Agency (EMA), medicine shortages
catalogue" with the source URL.

Verified against the official endpoint on 2026-09-07.  Python standard library only.
"""

import argparse
import hashlib
import json
import os
import pathlib
import sys
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "ema_medicine_shortages"
URL = "https://www.ema.europa.eu/en/documents/report/shortages-output-json-report_en.json"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
REQUIRED_ROW_FIELDS = frozenset(
    {
        "category",
        "medicine_affected",
        "supply_shortage_status",
        "international_non_proprietary_name_inn_or_common_name",
        "therapeutic_area_mesh",
        "pharmaceutical_forms_affected",
        "strengths_affected",
        "availability_of_alternatives",
        "start_of_shortage_date",
        "expected_resolution_date",
        "expected_resolution",
        "first_published_date",
        "last_updated_date",
        "shortage_url",
    }
)


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        with path.open(encoding="utf-8") as handle:
            state = json.load(handle)
    except FileNotFoundError:
        return {"version": STATE_VERSION, "source": SOURCE, "records": {}}
    if not isinstance(state, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if state.get("version") != STATE_VERSION or state.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: incompatible version or source")
    last_modified = state.get("last_modified")
    if last_modified is not None and not isinstance(last_modified, str):
        raise ValueError(f"malformed state in {path}: last_modified must be a string")
    records = state.get("records")
    if not isinstance(records, dict) or not all(
        isinstance(record_id, str) and isinstance(row, dict)
        for record_id, row in records.items()
    ):
        raise ValueError(f"malformed state in {path}: records must map ids to objects")
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish a baseline only after stdout has accepted every record."""
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


def response_context(body: bytes) -> str:
    return body[:500].decode("utf-8", errors="replace").replace("\n", " ")


def fetch_snapshot(timeout: int, last_modified: str | None) -> tuple[dict, str | None, str] | None:
    headers = {"User-Agent": USER_AGENT}
    if last_modified:
        headers["If-Modified-Since"] = last_modified
    request = urllib.request.Request(URL, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=timeout)
    except urllib.error.HTTPError as error:
        if error.code == 304:
            return None
        raise RuntimeError(
            f"EMA shortages request failed: endpoint={URL} status={error.code} "
            f"body={response_context(error.read())!r}"
        ) from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"EMA shortages request failed: endpoint={URL} error={error.reason}") from error
    with response:
        body = response.read()
        last_modified = response.headers.get("Last-Modified")
    try:
        document = json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(
            f"EMA shortages response is not JSON: endpoint={URL} body={response_context(body)!r}"
        ) from error
    if not isinstance(document, dict):
        raise ValueError(f"EMA shortages response is not an object: endpoint={URL}")
    return document, last_modified, hashlib.sha256(body).hexdigest()


def validated_rows(document: dict) -> tuple[str, dict[str, dict]]:
    meta = document.get("meta")
    rows = document.get("data")
    if not isinstance(meta, dict) or not isinstance(meta.get("timestamp"), str):
        raise ValueError(f"EMA shortages response has invalid meta.timestamp: endpoint={URL}")
    if not isinstance(meta.get("total_records"), int) or not isinstance(rows, list):
        raise ValueError(f"EMA shortages response has invalid meta.total_records or data: endpoint={URL}")
    if meta["total_records"] != len(rows):
        raise ValueError(
            f"EMA shortages response count mismatch: endpoint={URL} "
            f"meta.total_records={meta['total_records']} data={len(rows)}"
        )
    indexed: dict[str, dict] = {}
    for number, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError(f"EMA shortages data[{number}] is not an object: endpoint={URL}")
        missing = REQUIRED_ROW_FIELDS - row.keys()
        if missing:
            raise ValueError(
                f"EMA shortages data[{number}] is missing fields {sorted(missing)}: endpoint={URL}"
            )
        if not all(isinstance(row[field], str) for field in REQUIRED_ROW_FIELDS):
            raise ValueError(f"EMA shortages data[{number}] has non-string documented fields: endpoint={URL}")
        record_id = row["shortage_url"]
        if not record_id:
            raise ValueError(f"EMA shortages data[{number}] has empty shortage_url: endpoint={URL}")
        if record_id in indexed:
            raise ValueError(f"EMA shortages has duplicate shortage_url {record_id!r}: endpoint={URL}")
        indexed[record_id] = row
    return meta["timestamp"], indexed


def canonical(row: dict) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def changed_records(current: dict[str, dict], previous: dict[str, dict], fetched_at: str,
                    publisher_timestamp: str, snapshot_sha256: str):
    metadata = {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "publisher_timestamp": publisher_timestamp,
        "snapshot_sha256": snapshot_sha256,
    }
    for record_id in sorted(current):
        row = current[record_id]
        old = previous.get(record_id)
        if old is not None and canonical(old) == canonical(row):
            continue
        record = dict(row)
        record.update(metadata)
        record["id"] = record_id
        record["change"] = "new" if old is None else "updated"
        yield record
    for record_id in sorted(previous.keys() - current.keys()):
        record = dict(previous[record_id])
        record.update(metadata)
        record["id"] = record_id
        record["change"] = "removed"
        yield record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", help="baseline file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="emit the complete current snapshot without reading or writing state")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    print(
        "Source: European Medicines Agency (EMA), medicine shortages catalogue; "
        "reproduction requires EMA acknowledgement. " + URL,
        file=sys.stderr,
    )
    path = None if args.no_state else state_path(args.state_file)
    previous_state = {"records": {}} if path is None else load_state(path)
    result = fetch_snapshot(args.timeout, None if path is None else previous_state.get("last_modified"))
    if result is None:
        return
    document, last_modified, snapshot_sha256 = result
    publisher_timestamp, current = validated_rows(document)
    fetched_at = datetime.now(timezone.utc).isoformat()
    previous = previous_state["records"]

    if path is None:
        records = changed_records(current, {}, fetched_at, publisher_timestamp, snapshot_sha256)
    else:
        records = changed_records(current, previous, fetched_at, publisher_timestamp, snapshot_sha256)
    for record in records:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    sys.stdout.flush()

    if path is not None:
        save_state(
            path,
            {
                "version": STATE_VERSION,
                "source": SOURCE,
                "last_modified": last_modified,
                "publisher_timestamp": publisher_timestamp,
                "snapshot_sha256": snapshot_sha256,
                "records": current,
            },
        )


if __name__ == "__main__":
    main()
