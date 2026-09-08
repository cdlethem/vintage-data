#!/usr/bin/env python3
"""UK Parliament OData BusinessItem laying activity.

The official OData service is publicly available and documented as throttled at 100
requests per second per IP. This extractor uses one sequential request at a time and
collects every BusinessItem with a LayingDate, preserving new rows and revisions.
BusinessItem LocalId is the Parliament-assigned stable identifier; LayingDate is the
upstream event-time watermark. The Open Parliament Licence v3.0 requires attribution:
"Contains Parliamentary information licensed under the Open Parliament Licence v3.0."
It excludes personal data and third-party rights; downstream users must comply.

State holds an exact timestamp watermark, its boundary ids, and representations needed
to emit later revisions. On an initial stateful run the complete dated collection is
walked. Later runs restart at the inclusive watermark, so items sharing the boundary
cannot be skipped. A complete weekly reconciliation detects revisions to older items;
use --since for an explicit historical reconciliation window.

Stdlib only. Verified live 2026-09-06.
"""

import argparse
import hashlib
import json
import os
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

SOURCE = "uk_parliament_procedural_activity"
ENDPOINT = "https://api.parliament.uk/odata/BusinessItem"
PAGE_SIZE = 100
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
STATE_VERSION = 1
RECONCILIATION_INTERVAL_DAYS = 7
SELECT = "LocalId,LayingDate,WithdrawalDate,BusinessItemDate"
EXPECTED_FIELDS = frozenset(SELECT.split(","))


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


def canonical_hash(row):
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def request_json(url, timeout):
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as error:
        body = error.read(1000).decode("utf-8", "replace")
        raise RuntimeError(f"GET {url} returned HTTP {error.code}: {body!r}") from error
    except urllib.error.URLError as error:
        raise RuntimeError(f"GET {url} failed: {error.reason}") from error
    if status != 200:
        raise RuntimeError(f"GET {url} returned HTTP {status}: {body[:1000]!r}")
    try:
        document = json.loads(body)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"GET {url} returned invalid JSON: {body[:1000]!r}") from error
    if not isinstance(document, dict):
        raise RuntimeError(f"GET {url} returned {type(document).__name__}, expected object")
    return document


def load_state(path):
    if not path.exists():
        return {"version": STATE_VERSION, "source": SOURCE, "known": {}}
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
    reconciled_at = state.get("last_full_reconciliation")
    if reconciled_at is not None:
        parse_timestamp(reconciled_at, "state last_full_reconciliation")
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


def endpoint_url(since, skip):
    parameters = {"$select": SELECT, "$orderby": "LayingDate asc,LocalId asc", "$top": str(PAGE_SIZE), "$skip": str(skip)}
    if since is None:
        parameters["$filter"] = "LayingDate ne null"
    else:
        parameters["$filter"] = f"LayingDate ge {timestamp_text(since)}"
    return f"{ENDPOINT}?{urllib.parse.urlencode(parameters)}"


def collect(since, timeout):
    rows = []
    skip = 0
    while True:
        document = request_json(endpoint_url(since, skip), timeout)
        page = document.get("value")
        if not isinstance(page, list):
            raise RuntimeError("BusinessItem response is missing list value")
        if not page:
            return rows
        for row in page:
            if not isinstance(row, dict):
                raise RuntimeError("BusinessItem value contains a non-object row")
            local_id = row.get("LocalId")
            laying_date = row.get("LayingDate")
            if not isinstance(local_id, str) or not local_id:
                raise RuntimeError("BusinessItem row is missing LocalId")
            parse_timestamp(laying_date, f"BusinessItem {local_id} LayingDate")
            if set(row) != EXPECTED_FIELDS:
                raise RuntimeError(f"BusinessItem {local_id} has unexpected selected fields")
            if not isinstance(row.get("BusinessItemDate"), list):
                raise RuntimeError(f"BusinessItem {local_id} BusinessItemDate is not a list")
            rows.append(row)
        if len(page) < PAGE_SIZE:
            return rows
        skip += len(page)


def build_records(rows, known, fetched_at):
    records = []
    next_known = dict(known)
    for row in rows:
        row_hash = canonical_hash(row)
        local_id = row["LocalId"]
        if next_known.get(local_id) != row_hash:
            record = dict(row)
            record.update({"source": SOURCE, "fetched_at": fetched_at, "id": local_id})
            records.append(record)
            next_known[local_id] = row_hash
    return records, next_known


def next_watermark(rows, previous):
    if not rows:
        return previous
    latest = max(parse_timestamp(row["LayingDate"], "LayingDate") for row in rows)
    return timestamp_text(latest)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file")
    parser.add_argument("--no-state", action="store_true")
    parser.add_argument("--since", help="inclusive ISO-8601 LayingDate for a reconciliation run")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")

    state_file = None if args.no_state else state_path(args.state_file)
    state = {"version": STATE_VERSION, "source": SOURCE, "known": {}} if state_file is None else load_state(state_file)
    requested_since = parse_timestamp(args.since, "--since") if args.since else None
    full_reconciliation = requested_since is None and (
        state.get("last_full_reconciliation") is None
        or datetime.now(timezone.utc)
        - parse_timestamp(state["last_full_reconciliation"], "state last_full_reconciliation")
        >= timedelta(days=RECONCILIATION_INTERVAL_DAYS)
    )
    since = None if full_reconciliation else requested_since
    if since is None and not full_reconciliation and state.get("watermark"):
        since = parse_timestamp(state["watermark"], "state watermark")
    print(
        f"Fetching UK Parliament BusinessItems since {timestamp_text(since) if since else 'start'}",
        file=sys.stderr,
    )
    rows = collect(since, args.timeout)
    fetched_at = datetime.now(timezone.utc).isoformat()
    records, next_known = build_records(rows, state["known"], fetched_at)
    for record in records:
        print(json.dumps(record, ensure_ascii=False, separators=(",", ":")))
    sys.stdout.flush()

    if state_file is not None:
        watermark = next_watermark(rows, state.get("watermark"))
        boundary_ids = sorted(row["LocalId"] for row in rows if watermark and timestamp_text(parse_timestamp(row["LayingDate"], "LayingDate")) == watermark)
        next_state = {
            "version": STATE_VERSION,
            "source": SOURCE,
            "watermark": watermark,
            "boundary_ids": boundary_ids,
            "known": next_known,
            "last_full_reconciliation": (
                fetched_at if full_reconciliation else state.get("last_full_reconciliation")
            ),
        }
        write_state(state_file, next_state)
    print(f"Fetched {len(rows)} BusinessItems; emitted {len(records)} records", file=sys.stderr)


if __name__ == "__main__":
    main()
