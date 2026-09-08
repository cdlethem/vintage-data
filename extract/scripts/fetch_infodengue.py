#!/usr/bin/env python3
"""InfoDengue municipality alert extractor.

The scheduled invocation is a bounded, most-recent-ten-week national sweep.
Historical backfill is intentionally one epidemiological year per unit.  A
non-zero cap breach is safe: extract_runner discards all staged stdout records
and writes only a failed sidecar.

Stdlib only.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

SOURCE = "infodengue_city_alerts"
URL = "https://info.dengue.mat.br/api/alertcity"
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
REQUEST_DELAY_S = 0.3
MAX_ATTEMPTS = 3
ROLLING_MAX_RECORDS = 75_000
ROLLING_MAX_BYTES = 64 * 2**20
YEAR_MAX_RECORDS = 350_000
YEAR_MAX_BYTES = 320 * 2**20
MAX_FAILURE_SAMPLES = 100
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


class CapExceeded(RuntimeError):
    """The generated stream cannot be safely published."""


@dataclass
class Metrics:
    geocodes_discovered: int = 0
    geocodes_attempted: int = 0
    geocodes_succeeded: int = 0
    geocodes_failed: int = 0
    requests: int = 0
    request_bytes: int = 0
    retries: int = 0
    records: int = 0
    emitted_bytes: int = 0
    deduplicated: int = 0
    min_week: str | None = None
    max_week: str | None = None
    failures: list[dict[str, str]] = field(default_factory=list)

    def failure(self, geocode: str, exc: BaseException) -> None:
        self.geocodes_failed += 1
        if len(self.failures) < MAX_FAILURE_SAMPLES:
            self.failures.append({"geocode": geocode, "error": f"{type(exc).__name__}: {exc}"[:500]})

    def observe_week(self, week: str) -> None:
        if self.min_week is None or week < self.min_week:
            self.min_week = week
        if self.max_week is None or week > self.max_week:
            self.max_week = week


def fetch_municipality_geocodes(timeout: int = 30) -> list[str]:
    """Return every current IBGE municipality geocode, deterministically sorted."""
    request = urllib.request.Request(IBGE_MUNICIPIOS_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        if response.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
    rows = json.loads(raw.decode("utf-8"))
    if not isinstance(rows, list):
        raise ValueError("IBGE municipalities response is not a JSON list")
    return sorted({str(row["id"]) for row in rows if isinstance(row, dict) and row.get("id")})


def _alert_request(geocode: str, disease: str, ey_start: int, ey_end: int) -> urllib.request.Request:
    query = urllib.parse.urlencode({
        "geocode": geocode, "disease": disease, "format": "csv",
        "ew_start": 1, "ew_end": 53, "ey_start": ey_start, "ey_end": ey_end,
    })
    return urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})


def _read_alert_rows(
    geocode: str, disease: str, ey_start: int, ey_end: int, timeout: int, metrics: Metrics | None,
) -> tuple[list[dict[str, str]], int]:
    """Fetch and schema-check one municipality with bounded retries."""
    for attempt in range(MAX_ATTEMPTS):
        try:
            if metrics:
                metrics.requests += 1
            with urllib.request.urlopen(_alert_request(geocode, disease, ey_start, ey_end), timeout=timeout) as response:
                raw = response.read()
            if metrics:
                metrics.request_bytes += len(raw)
            text = raw.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(text))
            expected = {"SE", "municipio_nome"}
            if not reader.fieldnames or not expected.issubset(reader.fieldnames):
                raise ValueError(f"InfoDengue CSV is missing columns: {sorted(expected)}")
            return [dict(row) for row in reader], attempt
        except (urllib.error.URLError, TimeoutError, ValueError, UnicodeDecodeError) as exc:
            if attempt + 1 == MAX_ATTEMPTS:
                raise
            if metrics:
                metrics.retries += 1
            time.sleep(2**attempt)
    raise AssertionError("unreachable")


def fetch_alerts(
    geocode: str = "3304557", disease: str = "dengue", year: int | None = None,
    limit: int | None = 100, timeout: int = 30, ey_start: int | None = None,
    ey_end: int | None = None,
) -> Iterable[dict[str, str]]:
    """Compatibility generator for one municipality without aggregate metrics."""
    if ey_start is None:
        ey_start = year or datetime.now(timezone.utc).year
    if ey_end is None:
        ey_end = year if year is not None else ey_start
    if ey_start > ey_end:
        raise ValueError(f"start year {ey_start} is after end year {ey_end}")
    rows, _ = _read_alert_rows(geocode, disease, ey_start, ey_end, timeout, None)
    fetched_at = datetime.now(timezone.utc).isoformat()
    for index, row in enumerate(rows):
        if limit is not None and limit > 0 and index >= limit:
            break
        record = dict(row)
        record.update({"source": SOURCE, "fetched_at": fetched_at,
                       "id": f"{geocode}|{disease}|{row['SE']}",
                       "geocode": geocode, "disease": disease})
        yield record


def _cap_for(single_year: bool) -> tuple[int, int, str]:
    if single_year:
        return YEAR_MAX_RECORDS, YEAR_MAX_BYTES, "single_year"
    return ROLLING_MAX_RECORDS, ROLLING_MAX_BYTES, "rolling_10_week"


def emit_sweep(
    geocodes: Iterable[str], *, disease: str, ey_start: int, ey_end: int,
    limit: int, timeout: int, out: Any, metrics: Metrics | None = None,
) -> Metrics:
    """Fetch, deduplicate, cap, and write a national sweep to ``out``."""
    if ey_start != ey_end:
        raise ValueError("historical backfill must use exactly one epidemiological year per unit")
    metrics = metrics or Metrics()
    geocodes = list(geocodes)
    metrics.geocodes_discovered = len(geocodes)
    max_records, max_bytes, scope = _cap_for(limit == 0)
    for geocode in geocodes:
        metrics.geocodes_attempted += 1
        try:
            rows, _ = _read_alert_rows(geocode, disease, ey_start, ey_end, timeout, metrics)
            seen: set[tuple[str, str]] = set()
            fetched_at = datetime.now(timezone.utc).isoformat()
            for index, row in enumerate(rows):
                if limit > 0 and index >= limit:
                    break
                week = str(row.get("SE") or "")
                key = (disease, week)
                if not week:
                    raise ValueError("InfoDengue row has empty SE")
                if key in seen:
                    metrics.deduplicated += 1
                    continue
                seen.add(key)
                record = dict(row)
                record.update({"source": SOURCE, "fetched_at": fetched_at,
                               "id": f"{geocode}|{disease}|{week}",
                               "geocode": geocode, "disease": disease})
                encoded = json.dumps(record, ensure_ascii=False) + "\n"
                metrics.records += 1
                metrics.emitted_bytes += len(encoded.encode("utf-8"))
                metrics.observe_week(week)
                if metrics.records > max_records or metrics.emitted_bytes > max_bytes:
                    raise CapExceeded(
                        f"{scope} cap exceeded: records={metrics.records}/{max_records}, "
                        f"bytes={metrics.emitted_bytes}/{max_bytes}"
                    )
                out.write(encoded)
            metrics.geocodes_succeeded += 1
        except CapExceeded:
            raise
        except Exception as exc:  # One municipality must not erase successful peers.
            metrics.failure(geocode, exc)
            print(f"infodengue: skipping geocode {geocode!r}: {exc!r}", file=sys.stderr)
        time.sleep(REQUEST_DELAY_S)
    return metrics


def summary(metrics: Metrics, *, cap_scope: str, failed: bool = False, error: str | None = None) -> dict[str, Any]:
    complete = not failed and metrics.geocodes_failed == 0
    return {
        "health": "failed" if failed else ("degraded" if metrics.geocodes_failed else "healthy"),
        "completeness": "failed" if failed else ("complete" if complete else "partial"),
        "records": metrics.records,
        "bytes": metrics.emitted_bytes,
        "requests": {"attempted": metrics.requests, "bytes": metrics.request_bytes, "retries": metrics.retries},
        "partitions": {"attempted": metrics.geocodes_attempted, "succeeded": metrics.geocodes_succeeded,
                       "failed": metrics.geocodes_failed, "failures": metrics.failures},
        "coverage": {"geocodes_discovered": metrics.geocodes_discovered, "week_min": metrics.min_week,
                     "week_max": metrics.max_week, "cap_scope": cap_scope},
        "metrics": {"deduplicated": metrics.deduplicated},
        **({"error": error} if error else {}),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--geocode", default=None, help="single-municipality mode")
    parser.add_argument("--disease", choices=("dengue", "chikungunya", "zika"), default="dengue")
    parser.add_argument("--year", type=int, help="single epidemiological year")
    parser.add_argument("--start-year", type=int, help="first epidemiological year")
    parser.add_argument("--end-year", type=int, help="last epidemiological year")
    parser.add_argument("--limit", type=int, default=10, help="per-municipality cap; 0 means all rows")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args(argv)
    if args.year is not None and (args.start_year is not None or args.end_year is not None):
        parser.error("--year cannot be combined with --start-year/--end-year")
    if (args.start_year is None) != (args.end_year is None):
        parser.error("--start-year and --end-year must be provided together")
    ey_start = args.start_year if args.start_year is not None else (args.year or datetime.now(timezone.utc).year)
    ey_end = args.end_year if args.end_year is not None else ey_start
    if ey_start > ey_end:
        parser.error("--start-year must not exceed --end-year")
    if not args.geocode and ey_start != ey_end:
        parser.error("national historical backfill requires one year per unit")
    limit = max(0, args.limit)
    cap_scope = "single_year" if limit == 0 else "rolling_10_week"
    metrics = Metrics()
    try:
        geocodes = [args.geocode] if args.geocode else fetch_municipality_geocodes(timeout=args.timeout)
        metrics = emit_sweep(geocodes, disease=args.disease, ey_start=ey_start, ey_end=ey_end,
                             limit=limit, timeout=args.timeout, out=sys.stdout, metrics=metrics)
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps(summary(metrics, cap_scope=cap_scope, failed=True, error=str(exc))), file=sys.stderr)
        print(f"infodengue: fatal: {exc}", file=sys.stderr)
        return 1
    print(SUMMARY_PREFIX + json.dumps(summary(metrics, cap_scope=cap_scope)), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
