#!/usr/bin/env python3
"""Fetch a curated basket of UK national statistics from the keyless ONS beta API.

The UK Office for National Statistics publishes monthly/quarterly official
statistics (labour market, GDP, CPIH inflation, retail sales, weekly deaths)
through the beta dataset API at ``api.beta.ons.gov.uk``.  For each dataset in
the basket this extractor:

1. resolves the latest ``time-series`` version number,
2. fetches the version metadata (the ordered dimension list),
3. downloads the complete historical CSV for that version, and
4. emits one NDJSON record per observation row whose stable id (dataset slug
   plus all dimension values) has not been emitted before.

Because each CSV contains the full history, an atomic local state file tracks
the set of already-emitted row ids per dataset.  Rows that ONS later revises
keep the first-observed value: the raw layer is insert-only, so a revision
with the same id is not re-emitted.  Schedule no more often than daily; the
fastest-changing dataset in the basket (weekly deaths) updates weekly.

Data is licensed under the Open Government Licence v3.0, which permits
redistribution with attribution.  Attribution: "Contains public sector
information from the UK Office for National Statistics, licensed under the
Open Government Licence v3.0."  Python standard library only.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import pathlib
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SOURCE = "ons_statistics"
API_BASE = "https://api.beta.ons.gov.uk/v1"
DOWNLOAD_BASE = "https://download.ons.gov.uk/downloads/datasets"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

DATASETS = (
    "labour-market",
    "gdp-to-four-decimal-places",
    "retail-sales-index",
    "cpih01",
    "weekly-deaths-age-sex",
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
        return {"version": STATE_VERSION, "source": SOURCE, "emitted": {}}
    if not isinstance(state, dict):
        raise ValueError(f"malformed state in {path}: expected object")
    if state.get("version") != STATE_VERSION or state.get("source") != SOURCE:
        raise ValueError(f"malformed state in {path}: incompatible version or source")
    emitted = state.get("emitted")
    if not isinstance(emitted, dict) or not all(
        isinstance(slug, str)
        and isinstance(ids, list)
        and all(isinstance(row_id, str) for row_id in ids)
        for slug, ids in emitted.items()
    ):
        raise ValueError(f"malformed state in {path}: emitted must map dataset slugs to id lists")
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish state only after stdout has accepted every record."""
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


def http_get(url: str, timeout: int, retries: int, metrics: dict) -> bytes:
    """GET one URL, retrying transient network errors and 5xx responses."""
    metrics["attempted"] += 1
    delay = 2.0
    last_error = "unknown"
    for attempt in range(retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            body = response_context(error.read())
            if not 500 <= error.code < 600:
                raise RuntimeError(
                    f"ONS request failed: endpoint={url} status={error.code} body={body!r}"
                ) from error
            last_error = f"status={error.code} body={body!r}"
        except urllib.error.URLError as error:
            last_error = str(error.reason)
        except TimeoutError as error:
            last_error = str(error)
        if attempt < retries:
            time.sleep(delay)
            delay *= 2
    raise RuntimeError(
        f"ONS request failed after {retries + 1} attempts: endpoint={url} error={last_error}"
    )


def latest_version(slug: str, timeout: int, retries: int, metrics: dict) -> int:
    body = http_get(f"{API_BASE}/datasets/{slug}/editions/time-series", timeout, retries, metrics)
    document = json.loads(body)
    if isinstance(document, dict) and isinstance(document.get("items"), list):
        items = document["items"]
        edition = items[0] if items else None
    elif isinstance(document, dict):
        edition = document
    else:
        edition = None
    if not isinstance(edition, dict):
        raise ValueError(f"ONS editions response for {slug} has no edition object")
    latest = (edition.get("links") or {}).get("latest_version") or {}
    version_id = latest.get("id")
    if not isinstance(version_id, str) or not version_id.isdigit():
        raise ValueError(f"ONS editions response for {slug} lacks a numeric latest version")
    return int(version_id)


def version_dimensions(slug: str, version: int, timeout: int, retries: int, metrics: dict) -> tuple[str, ...]:
    url = f"{API_BASE}/datasets/{slug}/editions/time-series/versions/{version}"
    body = http_get(url, timeout, retries, metrics)
    document = json.loads(body)
    if not isinstance(document, dict):
        raise ValueError(f"ONS version response for {slug} is not an object")
    state = document.get("state")
    if state is not None and state != "published":
        raise ValueError(f"ONS version {version} for {slug} is not published (state={state!r})")
    dimensions = document.get("dimensions")
    if not isinstance(dimensions, list) or not dimensions:
        raise ValueError(f"ONS version response for {slug} has no dimensions")
    dimension_ids: list[str] = []
    for dimension in dimensions:
        if not isinstance(dimension, dict):
            raise ValueError(f"ONS dimension for {slug} is not an object")
        dimension_id = dimension.get("id")
        if not isinstance(dimension_id, str) or not dimension_id:
            raise ValueError(f"ONS dimension for {slug} lacks an id")
        dimension_ids.append(dimension_id)
    return tuple(dimension_ids)


def parse_observations(
    slug: str,
    version: int,
    rows: list[list[str]],
    dimension_ids: tuple[str, ...],
    fetched_at: str,
    emitted: frozenset[str],
):
    """Yield normalized records for CSV rows whose stable id is new."""
    if len(rows) < 2:
        raise ValueError(f"ONS CSV for {slug} has no data rows")
    header = rows[0]
    value_count = len(header) - 2 * len(dimension_ids)
    if value_count <= 0:
        raise ValueError(
            f"ONS CSV for {slug} has {len(header)} columns; expected at least {2 * len(dimension_ids) + 1}"
        )
    offset = value_count
    for position, dimension_id in enumerate(dimension_ids):
        column = header[offset + 2 * position]
        if column != dimension_id:
            raise ValueError(
                f"ONS CSV header for {slug} does not match the API dimension list: "
                f"expected {dimension_id!r} at column {offset + 2 * position}, found {column!r}"
            )
    for row in rows[1:]:
        if not row:
            continue
        if len(row) != len(header):
            raise ValueError(
                f"ONS CSV row for {slug} has {len(row)} columns, expected {len(header)}"
            )
        record_id = "|".join([slug] + [row[offset + 2 * position] for position in range(len(dimension_ids))])
        if record_id in emitted:
            continue
        dimensions = {
            header[offset + 2 * position]: row[offset + 2 * position]
            for position in range(len(dimension_ids))
        }
        labels = {
            header[offset + 2 * position]: row[offset + 2 * position + 1]
            for position in range(len(dimension_ids))
        }
        values = {header[i]: row[i] for i in range(value_count) if row[i] != ""}
        yield {
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": record_id,
            "dataset": slug,
            "version": version,
            "dimensions": dimensions,
            "dimension_labels": labels,
            "values": values,
        }


def fetch_dataset(
    slug: str,
    timeout: int,
    retries: int,
    metrics: dict,
    fetched_at: str,
    emitted: set[str],
) -> dict:
    version = latest_version(slug, timeout, retries, metrics)
    dimension_ids = version_dimensions(slug, version, timeout, retries, metrics)
    url = f"{DOWNLOAD_BASE}/{slug}/editions/time-series/versions/{version}.csv"
    body = http_get(url, timeout, retries, metrics)
    text = body.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))
    total = max(len(rows) - 1, 0)
    records = []
    for record in parse_observations(slug, version, rows, dimension_ids, fetched_at, frozenset(emitted)):
        records.append(record)
        emitted.add(record["id"])
    return {"status": "ok", "version": version, "rows": total, "new": len(records), "records": records}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", help="emitted-id baseline; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true", help="emit every row without reading or writing state")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.retries < 0:
        parser.error("--retries must not be negative")

    print(
        "Source: UK Office for National Statistics, official statistics (CPIH, GDP, "
        "labour market, retail sales, weekly deaths); Open Government Licence v3.0. "
        + API_BASE,
        file=sys.stderr,
    )

    path = None if args.no_state else state_path(args.state_file)
    metrics = {"attempted": 0}
    emitted = 0
    results: dict[str, dict] = {}
    try:
        previous_state = {"emitted": {}} if path is None else load_state(path)
        emitted_by_slug: dict[str, set[str]] = {
            slug: set(ids) for slug, ids in previous_state["emitted"].items()
        }
        fetched_at = datetime.now(timezone.utc).isoformat()

        for slug in DATASETS:
            try:
                result = fetch_dataset(
                    slug,
                    args.timeout,
                    args.retries,
                    metrics,
                    fetched_at,
                    emitted_by_slug.setdefault(slug, set()),
                )
            except Exception as exc:
                result = {"status": "failed", "error": str(exc)}
                print(f"ONS dataset {slug} failed: {exc}", file=sys.stderr)
            results[slug] = {key: value for key, value in result.items() if key != "records"}
            for record in result.get("records", ()):
                print(json.dumps(record, ensure_ascii=False, sort_keys=True))
                emitted += 1
        sys.stdout.flush()
    except Exception as exc:
        summary = {
            "health": "failed",
            "completeness": "failed",
            "records": emitted,
            "datasets": results,
            "requests": {"attempted": metrics["attempted"]},
            "error": str(exc),
        }
        print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
        return 1

    failed = [slug for slug in DATASETS if results[slug]["status"] == "failed"]
    if failed and len(failed) == len(DATASETS):
        health, completeness = "failed", "failed"
    elif failed:
        health, completeness = "degraded", "partial"
    else:
        health, completeness = "healthy", "complete"
    summary = {
        "health": health,
        "completeness": completeness,
        "records": emitted,
        "datasets": results,
        "requests": {"attempted": metrics["attempted"]},
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)

    if path is not None and not (failed and len(failed) == len(DATASETS)):
        save_state(
            path,
            {
                "version": STATE_VERSION,
                "source": SOURCE,
                "emitted": {slug: sorted(ids) for slug, ids in emitted_by_slug.items() if ids},
            },
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
