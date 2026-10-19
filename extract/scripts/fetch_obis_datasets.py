#!/usr/bin/env python3
"""Fetch the OBIS (Ocean Biodiversity Information System) public dataset catalog as NDJSON.

OBIS (IOC-UNESCO) is the global archive of marine biodiversity data. The v3
API /dataset endpoint lists every public dataset with its ingest timestamp,
record counts, institutes, spatial extent and intellectual rights. The API
offers no cursor pagination for /dataset, so each run snapshots the whole
catalog in a single request (verified 2026-10-19: 6941 datasets, ~34 MB,
~75 s) and emits only datasets that are new or changed since the persisted
state. The first run emits the full baseline; later runs emit just the diff.
The server's `total` field is not reliable (returned 6937 against 6941
results), so the emitted snapshot size is authoritative and a mismatch only
produces a stderr warning.
"""

import argparse
import hashlib
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
from typing import Callable, TextIO

SOURCE = "obis_datasets"
ENDPOINT = "https://api.obis.org/v3/dataset"
API_HOST = "api.obis.org"
API_PATH = "/v3/dataset"
# One-request snapshot: the catalog is ~6.9k datasets (2026-10-19); the cap
# leaves ~3k of headroom before the server starts refusing the page size.
SNAPSHOT_SIZE = 10000
DEFAULT_TIMEOUT = 180
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Reject redirects before urllib can issue a destination request."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def build_http_opener(*handlers: urllib.request.BaseHandler) -> urllib.request.OpenerDirector:
    return urllib.request.build_opener(NoRedirectHandler(), *handlers)


HTTP_OPENER = build_http_opener()


def timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def build_url() -> str:
    return f"{ENDPOINT}?{urllib.parse.urlencode({'size': SNAPSHOT_SIZE})}"


def request_json(url: str, timeout: int) -> object:
    parsed = urllib.parse.urlsplit(url)
    port = parsed.port
    if (
        parsed.scheme != "https"
        or parsed.hostname != API_HOST
        or port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != API_PATH
        or parsed.fragment
    ):
        raise ValueError(f"unexpected OBIS URL: {url}")
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with HTTP_OPENER.open(request, timeout=timeout) as response:
            if response.status != 200:
                raise ValueError(f"OBIS API returned HTTP {response.status}")
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise ValueError(f"OBIS API request failed: HTTP {error.code}") from error
    except urllib.error.URLError as error:
        raise ValueError(f"OBIS API request failed: {error.reason}") from error
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("OBIS API returned a non-JSON body") from error


def default_state() -> dict:
    return {
        "version": STATE_VERSION,
        "source": SOURCE,
        "datasets": {},
    }


def state_path(explicit: str | None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def validate_state(document: object, path: pathlib.Path) -> dict:
    if not isinstance(document, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if document.get("version") != STATE_VERSION or document.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: unexpected version or source")
    if set(document) != {"version", "source", "datasets"}:
        raise ValueError(f"malformed state in {path}: unexpected fields")
    datasets = document["datasets"]
    if not isinstance(datasets, dict):
        raise ValueError(f"malformed state in {path}: datasets must be an object")
    for dataset_id, fingerprint in datasets.items():
        if not isinstance(dataset_id, str) or not isinstance(fingerprint, str):
            raise ValueError(f"malformed state in {path}: dataset map entries must be strings")
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


def names_field(record: dict, field: str) -> list:
    values = record.get(field)
    if not isinstance(values, list):
        return []
    return [item["name"] for item in values if isinstance(item, dict) and isinstance(item.get("name"), str)]


def as_list(value: object) -> list:
    return value if isinstance(value, list) else []


def project(record: dict) -> dict:
    """Slim, stable projection of a dataset record (excludes per-year download
    telemetry, which changes every day and is usage data, not catalog data)."""
    dataset_id = record.get("id")
    if not isinstance(dataset_id, str) or not dataset_id:
        raise ValueError("OBIS dataset record is missing its id")
    feed = record.get("feed")
    statistics = record.get("statistics")
    return {
        "id": dataset_id,
        "title": record.get("title"),
        "core": record.get("core"),
        "created": record.get("created"),
        "updated": record.get("updated"),
        "published": record.get("published"),
        "url": record.get("url"),
        "records": record.get("records"),
        "statistics": statistics if isinstance(statistics, dict) else {},
        "intellectualrights": record.get("intellectualrights"),
        "extent": record.get("extent"),
        "institutes": names_field(record, "institutes"),
        "nodes": names_field(record, "nodes"),
        "keywords": [
            item["keyword"]
            for item in as_list(record.get("keywords"))
            if isinstance(item, dict) and isinstance(item.get("keyword"), str)
        ],
        "tags": record.get("tags") if isinstance(record.get("tags"), list) else [],
        "feed_url": feed.get("url") if isinstance(feed, dict) else None,
        "citation": record.get("citation"),
    }


def fingerprint(projected: dict) -> str:
    canonical = json.dumps(projected, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_snapshot(document: object) -> tuple[list[dict], object]:
    if not isinstance(document, dict) or not isinstance(document.get("results"), list):
        raise ValueError("OBIS snapshot is not a result list")
    records = document["results"]
    if not records:
        raise ValueError("OBIS snapshot is empty; refusing to overwrite state")
    for record in records:
        if not isinstance(record, dict) or not isinstance(record.get("id"), str) or not record["id"]:
            raise ValueError("OBIS snapshot contains a record without a string id")
    seen: set[str] = set()
    for record in records:
        if record["id"] in seen:
            raise ValueError(f"OBIS snapshot contains duplicate dataset id {record['id']}")
        seen.add(record["id"])
    return records, document.get("total")


def run(
    *,
    path: pathlib.Path | None,
    output: TextIO,
    timeout: int = DEFAULT_TIMEOUT,
    transport: Callable[[str, int], object] = request_json,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    fetched_at = now()
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at = fetched_at.astimezone(timezone.utc)

    url = build_url()
    document = transport(url, timeout)
    records, total = validate_snapshot(document)
    total_mismatch = isinstance(total, int) and total != len(records)
    if total_mismatch:
        print(
            f"OBIS snapshot reports total={total} but returned {len(records)} records; "
            "treating the returned snapshot as authoritative",
            file=sys.stderr,
        )

    state = default_state() if path is None else load_state(path)
    baseline = not state["datasets"]

    projected = [project(record) for record in records]
    fingerprints = {row["id"]: fingerprint(row) for row in projected}
    changed = [row for row in projected if state["datasets"].get(row["id"]) != fingerprints[row["id"]]]

    payload = "".join(
        json.dumps({"source": SOURCE, "fetched_at": timestamp_text(fetched_at), **row}, ensure_ascii=False, sort_keys=True) + "\n"
        for row in changed
    )
    if payload:
        written = output.write(payload)
        if written is not None and written != len(payload):
            raise OSError("short write to NDJSON output")
    output.flush()

    if path is not None:
        save_state(path, {"version": STATE_VERSION, "source": SOURCE, "datasets": fingerprints})

    summary = {
        "health": "healthy",
        "completeness": "complete",
        "requests": {"attempted": 1, "succeeded": 1},
        "state_change": {"baseline": baseline, "emitted": len(changed)},
        "metrics": {
            "records_in_snapshot": len(projected),
            "emitted": len(changed),
            "state_records": len(fingerprints),
            "total_field_mismatch": total_mismatch,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return len(changed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="state file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="do not read or update persistent state")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    arguments = parser.parse_args()
    if arguments.no_state and arguments.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    try:
        count = run(
            path=None if arguments.no_state else state_path(arguments.state_file),
            output=sys.stdout,
            timeout=arguments.timeout,
        )
    except ValueError as error:
        parser.error(str(error))
    print(f"Collected {count} OBIS dataset records", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
