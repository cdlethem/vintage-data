#!/usr/bin/env python3
"""US FDA enforcement actions (recalls and market withdrawals) via openFDA.

Covers the three keyless openFDA enforcement endpoints — food, drug and
device — which list recalls and voluntary/mandated market withdrawals by
``report_date``. Each run fetches only the rows reported after the
per-endpoint watermark, so output is incremental and bounded.

Verified live 2026-10-21: GET https://api.fda.gov/{food,drug,device}/enforcement.json
answered without a key; a recent weekday carries roughly 20-50 rows per
endpoint and the datasets' ``meta.last_updated`` is at most a week old.

Quirks (important for the ingestion agent):
  * ``report_date`` is an unformatted ``YYYYMMDD`` string.
  * FDA refreshes the datasets in batches, so a run can legitimately return
    zero new rows even when the dataset changed; conversely a batch may
    backfill rows whose report_date is older than the watermark, which are
    not re-emitted (the watermark is monotonic by design).
  * HTTP 400 means the query was rejected (e.g. bad range), not a transient
    fault; it is not retried.
  * An empty date range answers HTTP 404 "No matches found!" — that is a
    legitimate zero-row result, not an error; it is treated as empty.
  * No API key. openFDA allows 240 req/min and 1,000 req/day per IP; a run
    makes at most a few requests per endpoint.

Stdlib only.
"""
from __future__ import annotations

import argparse
import datetime
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Sequence

SOURCE = "fda_enforcement"
API_ROOT = os.environ.get("FDA_API_ROOT") or "https://api.fda.gov"
ENDPOINTS = ("food", "drug", "device")
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
STATE_VERSION = 1
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_TIMEOUT = 30
MAX_ATTEMPTS = 3
RETRY_BACKOFF_SECONDS = 2.0
PAGE_SIZE = 100
MAX_PAGES = 50  # 5,000 rows per endpoint per run; observed volume is ~50/week

# Fields carried from the source records, in a stable order. Anything else
# (endpoint-specific extras) is dropped.
FIELDS = (
    "report_date",
    "event_id",
    "recall_number",
    "recall_initiation_date",
    "recalling_firm",
    "classification",
    "product_description",
    "product_type",
    "product_quantity",
    "distribution_pattern",
    "reason_for_recall",
    "status",
    "termination_date",
    "voluntary_mandated",
    "center_classification_date",
    "code_info",
    "more_code_info",
    "initial_firm_notification",
    "address_1",
    "address_2",
    "city",
    "state",
    "country",
    "postal_code",
)


class EndpointError(RuntimeError):
    """openFDA rejected a request or answered with data that does not parse."""


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    return (
        pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT)
        .expanduser()
        / "state"
        / f"{SOURCE}.json"
    )


def load_watermarks(path: pathlib.Path) -> dict[str, str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if (
        not isinstance(data, dict)
        or data.get("version") != STATE_VERSION
        or data.get("source") != SOURCE
    ):
        raise ValueError(f"unrecognised watermark state in {path}")
    watermarks = data.get("watermarks")
    if not isinstance(watermarks, dict):
        raise ValueError(f"malformed watermarks in {path}")
    for endpoint, value in watermarks.items():
        if not (isinstance(value, str) and _valid_report_date(value)):
            raise ValueError(f"malformed watermark for {endpoint} in {path}")
    return {endpoint: watermarks[endpoint] for endpoint in ENDPOINTS if endpoint in watermarks}


def save_state(path: pathlib.Path, watermarks: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(
        json.dumps({"version": STATE_VERSION, "source": SOURCE, "watermarks": watermarks}) + "\n",
        encoding="utf-8",
    )
    os.replace(staged, path)


def _valid_report_date(value: Any) -> bool:
    if not (isinstance(value, str) and len(value) == 8 and value.isdigit()):
        return False
    try:
        datetime.datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return False
    return True


def _get_json(url: str, timeout: int, counter: dict[str, int]) -> Any:
    """GET a URL as JSON, retrying 5xx/429/network errors only."""
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        counter["requests"] = counter.get("requests", 0) + 1
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            body = ""
            try:
                body = error.read(300).decode("utf-8", "replace")
            except Exception:
                pass
            if error.code == 404:
                # openFDA answers "No matches found!" with HTTP 404 — an empty
                # result set, not an error.
                return {"results": []}
            if error.code in (429, 500, 502, 503, 504) and attempt < MAX_ATTEMPTS:
                last_error = error
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue
            raise EndpointError(f"HTTP {error.code} from {url}: {body[:200]}") from None
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError) as error:
            if attempt < MAX_ATTEMPTS:
                last_error = error
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                continue
            raise EndpointError(f"request failed after {attempt} attempts: {error}") from error
    raise EndpointError(f"request failed after {MAX_ATTEMPTS} attempts: {last_error}")


def _query_endpoint(
    endpoint: str, from_date: str, to_date: str, timeout: int, counter: dict[str, int]
) -> list[dict[str, Any]]:
    """All enforcement rows for `endpoint` with report_date in (from, to]."""
    search = f"report_date:[{from_date} TO {to_date}]"
    rows: list[dict[str, Any]] = []
    for page in range(MAX_PAGES):
        params = {
            "search": search,
            "sort": "report_date:asc",
            "limit": PAGE_SIZE,
            "skip": page * PAGE_SIZE,
        }
        url = f"{API_ROOT}/{endpoint}/enforcement.json?{urllib.parse.urlencode(params)}"
        document = _get_json(url, timeout, counter)
        if not isinstance(document, dict) or not isinstance(document.get("results"), list):
            raise EndpointError(f"unexpected document shape from {url}")
        rows.extend(document["results"])
        if len(document["results"]) < PAGE_SIZE:
            return rows
    raise EndpointError(
        f"{endpoint}: more than {MAX_PAGES * PAGE_SIZE} rows in range "
        f"({from_date}..{to_date}); widen the bound before advancing"
    )

def normalize(row: dict[str, Any], endpoint: str, fetched_at: str) -> dict[str, Any] | None:
    """Map one source row to the canonical record, or None if unusable.

    The id is ``<endpoint>:<recall_number>`` — the recall number is unique
    per product row, while one ``event_id`` can cover many products. Rows
    without a usable recall number fall back to the event id.
    """
    report_date = row.get("report_date")
    event_id = row.get("event_id")
    if not _valid_report_date(report_date) or event_id in (None, ""):
        return None
    recall_number = row.get("recall_number")
    if recall_number in (None, "", "N/A"):
        record_id = f"{endpoint}:event-{event_id}"
    else:
        record_id = f"{endpoint}:{recall_number}"
    record: dict[str, Any] = {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": record_id,
        "endpoint": endpoint,
    }
    for field in FIELDS:
        if field in row:
            record[field] = row[field]
    return record


def plan_ranges(
    watermarks: dict[str, str], today: datetime.date, lookback_days: int
) -> dict[str, tuple[str, str] | None]:
    """Per-endpoint [from_date, to_date] search bounds in YYYYMMDD form.

    Bounds are inclusive on both ends (the openFDA range syntax is); the
    watermark is excluded by starting at watermark + 1 day. ``None`` encodes
    an empty range (watermark already at or ahead of today).
    """
    ranges: dict[str, tuple[str, str] | None] = {}
    to = today.strftime("%Y%m%d")
    for endpoint in ENDPOINTS:
        if endpoint in watermarks:
            wm = datetime.datetime.strptime(watermarks[endpoint], "%Y%m%d").date()
            from_dt = wm + datetime.timedelta(days=1)
        else:
            from_dt = today - datetime.timedelta(days=lookback_days)
        ranges[endpoint] = None if from_dt > today else (from_dt.strftime("%Y%m%d"), to)
    return ranges


def fetch_all(
    watermarks: dict[str, str],
    today: datetime.date,
    lookback_days: int,
    timeout: int,
    counter: dict[str, int],
) -> tuple[list[dict[str, Any]], dict[str, str], int]:
    """Fetch every endpoint; return (records, advanced watermarks, skipped)."""
    ranges = plan_ranges(watermarks, today, lookback_days)
    fetched_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    records: list[dict[str, Any]] = []
    advanced = dict(watermarks)
    skipped = 0
    for endpoint in ENDPOINTS:
        if ranges[endpoint] is None:
            continue
        from_date, to_date = ranges[endpoint]
        for row in _query_endpoint(endpoint, from_date, to_date, timeout, counter):
            record = normalize(row, endpoint, fetched_at)
            if record is None:
                skipped += 1
                continue
            records.append(record)
            if record["report_date"] > advanced.get(endpoint, ""):
                advanced[endpoint] = record["report_date"]
        # Pagination covered (from_date, to_date] completely, so advance to
        # the range's upper bound even when no rows matched the range.
        advanced[endpoint] = max(advanced.get(endpoint, ""), to_date)
    return records, advanced, skipped


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help="initial pull depth when no watermark exists")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.lookback_days <= 0 or args.timeout <= 0:
        parser.error("--lookback-days and --timeout must be positive")

    path = None if args.no_state else state_path(args.state_file)
    watermarks: dict[str, str] = {}
    counter: dict[str, int] = {"requests": 0}
    emitted = 0
    try:
        if path is not None:
            watermarks = load_watermarks(path)
        today = datetime.datetime.now(datetime.timezone.utc).date()
        records, advanced, skipped = fetch_all(watermarks, today, args.lookback_days,
                                               args.timeout, counter)
        if skipped:
            print(f"fda_enforcement: skipped {skipped} row(s) lacking a usable "
                  f"report_date/event_id", file=sys.stderr)
        for record in records:
            print(json.dumps(record, ensure_ascii=False))
            emitted += 1
        if path is not None:
            save_state(path, advanced)
    except Exception as exc:
        print(
            SUMMARY_PREFIX
            + json.dumps({"health": "failed", "completeness": "incomplete",
                          "records": emitted, "requests": counter["requests"],
                          "error": str(exc)}),
            file=sys.stderr,
        )
        return 1
    print(
        SUMMARY_PREFIX
        + json.dumps({"health": "healthy", "completeness": "complete",
                      "records": emitted, "requests": counter["requests"],
                      "skipped": skipped,
                      "coverage": {"watermarks": advanced},
                      "state": "none" if path is None else str(path)}),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
