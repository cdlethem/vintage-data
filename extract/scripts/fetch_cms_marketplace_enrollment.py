#!/usr/bin/env python3
"""Fetch CMS "Health Insurance Exchanges Monthly Effectuated Enrollment" (keyless).

Monthly ACA Marketplace effectuated enrollment by state and exchange platform
(HealthCare.gov FFP/SBE-FP vs state-run SBE), 2016 to present. CMS Data Hub
series ``1829fdb3-a6f9-4036-856a-3af1f7828c8c``; each new release appends the
newest month(s) of rows, so the fetcher emits rows for (state, platform, year,
month) keys it has not seen before and keeps that set as its watermark.

**Verified live 2026-10-18** — the dataset endpoint returned **200** with
**6,588 rows** (2 pages of ``size=5000``; ``/data/stats`` agreed:
``{"found_rows":6588,"total_rows":6588}``). Rows carry exactly five fields:
``State`` (51 states + DC + ``Total``), ``Platform`` (``All``, ``HealthCare.gov``,
``SBE``), ``Year`` (string, 2016..2026), ``Month`` (English month name), and
``Effectuated_Enr`` (numeric string). The newest release covered through
February 2026 — this series publishes with a lag of roughly half a year after
the reference month, so weekly polling comfortably outpaces the real cadence
and an unchanged month simply yields zero rows.

Endpoint: ``https://data.cms.gov/data-api/v1/dataset/1829fdb3-a6f9-4036-856a-3af1f7828c8c/data?size=5000&offset=N``
    Keyless; the series UUID always resolves to the latest release. Page size
    caps at 5,000 rows per the CMS API FAQ. ``/data/stats`` reports the total.

Quirks:
    * The API intermittently answers **503** under load (observed on sibling
      datasets within the same minute as successful responses), so transient
      HTTP errors and network timeouts get bounded exponential backoff.
    * Numeric fields arrive as **strings** (``"9473222"``); they are parsed to
      ints on normalize and a non-numeric value fails the run.
    * CMS revises releases in place; this fetcher only emits keys it has not
      seen, so a revised number for an old month is not re-emitted (RAW is
      insert-only anyway).
    * The ``state`` key set grows slowly (a few hundred bytes per month) and
      is committed atomically only after output succeeds.

US federal government work: public domain; CMS asks for attribution.

Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "cms_marketplace_enrollment"
SERIES_ID = "1829fdb3-a6f9-4036-856a-3af1f7828c8c"
API_URL = f"https://data.cms.gov/data-api/v1/dataset/{SERIES_ID}/data"
PAGE_SIZE = 5000
DEFAULT_MAX_PAGES = 200  # safety cap: 1,000,000 rows
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
TRANSIENT_HTTP = frozenset({429, 500, 502, 503, 504})
MONTHS = frozenset({
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
})


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> set[str]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return set()
    seen = data.get("seen")
    if not isinstance(seen, list) or not all(isinstance(k, str) for k in seen):
        raise ValueError(f"malformed state (missing 'seen' key list) in {path}")
    return set(seen)


def save_state(path: pathlib.Path, seen: set[str]) -> None:
    """Publish the watermark atomically so a crash cannot leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(
        json.dumps({"version": 1, "updated_at": datetime.now(timezone.utc).isoformat(), "seen": sorted(seen)}) + "\n",
        encoding="utf-8",
    )
    os.replace(staged, path)


def _get_json(url: str, timeout: float, retries: int = 3, retry_delay: float = 2.0):
    """GET a URL and decode JSON, retrying transient failures with backoff."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    last: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in TRANSIENT_HTTP or attempt == retries:
                raise
            last = exc
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == retries:
                raise
            last = exc
        time.sleep(retry_delay * (2 ** attempt))
    raise last  # unreachable


def _int_field(row: dict, field: str, name: str):
    value = row.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"row missing numeric {name}: {row!r}")
    try:
        return int(value.strip())
    except ValueError:
        raise ValueError(f"row has non-numeric {name} {value!r}: {row!r}") from None


def normalize(row: dict, fetched_at: str) -> dict:
    if not isinstance(row, dict):
        raise ValueError(f"row is not an object: {row!r}")
    state = row.get("State")
    platform = row.get("Platform")
    month = row.get("Month")
    if not isinstance(state, str) or not state.strip():
        raise ValueError(f"row missing State: {row!r}")
    if not isinstance(platform, str) or not platform.strip():
        raise ValueError(f"row missing Platform: {row!r}")
    if not isinstance(month, str) or month not in MONTHS:
        raise ValueError(f"row has unknown Month {month!r}: {row!r}")
    year = _int_field(row, "Year", "Year")
    enrollment = _int_field(row, "Effectuated_Enr", "Effectuated_Enr")
    if not 1900 <= year <= 2100:
        raise ValueError(f"row has out-of-range Year {year}: {row!r}")
    key = f"{state}|{platform}|{year}|{month}"
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": key,
        "state": state,
        "platform": platform,
        "year": year,
        "month": month,
        "effectuated_enrollment": enrollment,
    }


def fetch_rows(timeout: float, retries: int = 3, retry_delay: float = 2.0,
               max_pages: int = DEFAULT_MAX_PAGES, progress=None):
    """Yield normalized rows for the latest release, paging to exhaustion.

    ``progress`` (optional dict) accumulates ``rows_total`` and the number of
    requests attempted, for the run summary.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()
    offset = 0
    for page_index in range(max_pages):
        url = f"{API_URL}?size={PAGE_SIZE}&offset={offset}"
        document = _get_json(url, timeout, retries=retries, retry_delay=retry_delay)
        if not isinstance(document, list):
            raise ValueError(f"dataset endpoint returned {type(document).__name__}, not a list")
        if progress is not None:
            progress["rows_total"] = progress.get("rows_total", 0) + len(document)
            progress["requests"] = progress.get("requests", 0) + 1
        for row in document:
            yield normalize(row, fetched_at)
        if len(document) < PAGE_SIZE:
            return
        offset += PAGE_SIZE
    raise RuntimeError(f"more than {max_pages * PAGE_SIZE} rows; pagination cap reached")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="emit every row, not just new ones (re-baseline)")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    if args.retries < 0:
        parser.error("--retries must be non-negative")
    if args.retry_delay < 0:
        parser.error("--retry-delay must be non-negative")

    path = state_path(args.state_file)
    seen = set() if args.no_state else load_state(path)
    records = 0
    progress: dict = {}
    try:
        for record in fetch_rows(args.timeout, retries=args.retries,
                                 retry_delay=args.retry_delay, progress=progress):
            if record["id"] in seen:
                continue
            print(json.dumps(record, ensure_ascii=False, sort_keys=True))
            seen.add(record["id"])
            records += 1
        if not args.no_state and records:
            save_state(path, seen)
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps({
            "health": "failed", "completeness": "failed",
            "records": records, "error": str(exc),
        }), file=sys.stderr)
        return 1
    print(SUMMARY_PREFIX + json.dumps({
        "health": "healthy", "completeness": "complete",
        "records": records,
        "coverage": {"rows_total": progress.get("rows_total", 0), "rows_new": records},
        "requests": {"attempted": progress.get("requests", 0)},
    }), file=sys.stderr)
    return 0
if __name__ == "__main__":
    raise SystemExit(main())
