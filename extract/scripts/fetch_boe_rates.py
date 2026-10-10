#!/usr/bin/env python3
"""Fetch the Bank of England's own daily interest-rate series as NDJSON.

BoE's statistics database is a legacy CSV endpoint that needs no key or login.
Each run makes exactly one bounded request for a fixed, vetted set of series and
a trailing date window, then emits one record per (series, date) observation
that is newer than the stored watermark. The window plus the watermark means a
run after an outage re-fetches the gap without duplicating what is already
collected (RAW is insert-only; the stable ``<series>:<date>`` id is the
source-natural identity).

Only the Bank's *own* series are collected. They are copyright the Governor and
Company of the Bank of England and reproduced under the UK Open Government
Licence v3.0. The database's daily spot *exchange-rate* series (the ``XUDL*``
codes) are deliberately excluded: the Bank's terms state they are reproduced
under licence from third parties (LSEG) and are not covered by the OGL, so
reproducing them would require separate permission. See
https://www.bankofengland.co.uk/legal ("Bank of England Database").

Series collected (all daily, BoE-owned):
  IUDBEDR  Bank Rate (official Bank Rate, percent)
  IUDSOIA  SONIA rate (Sterling Overnight Index Average, percent)
  IUDZOS2  SONIA Compounded Index (index, base 100)
  IUDZLT2  SONIA total nominal value (million GBP traded)

A series can be blank for a given date (SONIA lags the Bank Rate by a day on
some dates); a blank cell is simply not emitted for that (series, date).

Stdlib only. State is written atomically outside the repository, only after all
stdout records and the run summary are flushed, so an aborted run re-fetches
rather than skipping (at-least-once).
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any

SOURCE = "boe_rates"
BASE_URL = "https://www.bankofengland.co.uk/boeapps/database/_iadb-fromshowcolumns.asp"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
STATE_VERSION = 1
DEFAULT_LOOKBACK_DAYS = 14
MAX_LOOKBACK_DAYS = 60
DEFAULT_TIMEOUT = 60.0
MAX_RESPONSE_BYTES = 4 * 1024 * 1024

# Vetted BoE-owned daily series (OGL v3.0). Codes confirmed live against the
# database's advanced search on 2026-10-10. Do not add XUDL* exchange-rate
# codes here: those are third-party (LSEG) licensed and excluded from the OGL.
SERIES: dict[str, dict[str, str]] = {
    "IUDBEDR": {"name": "Bank Rate", "unit": "percent"},
    "IUDSOIA": {"name": "SONIA rate", "unit": "percent"},
    "IUDZOS2": {"name": "SONIA Compounded Index", "unit": "index"},
    "IUDZLT2": {"name": "SONIA total nominal value", "unit": "million GBP"},
}

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[2]


class BoeResponseError(ValueError):
    """The provider returned something outside the expected CSV contract."""


class SummaryEmissionError(OSError):
    """The reserved run-summary line could not be fully emitted."""


def _is_within(path: pathlib.Path, directory: pathlib.Path) -> bool:
    return path == directory or directory in path.parents


def validate_state_path(value: str | os.PathLike[str]) -> pathlib.Path:
    """Resolve a state path and reject any path that addresses this checkout."""
    resolved = pathlib.Path(value).expanduser().resolve()
    if _is_within(resolved, REPOSITORY_ROOT):
        raise ValueError(f"state file must live outside the repository: {resolved}")
    return resolved


def default_state_path() -> pathlib.Path:
    data_root = pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT") or "~/.local/share/vintage-data/extract").expanduser()
    return data_root / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"version": STATE_VERSION, "watermarks": {}}
    if not isinstance(document, dict):
        raise ValueError(f"state file is not a JSON object: {path}")
    watermarks = document.get("watermarks")
    if not isinstance(watermarks, dict):
        raise ValueError(f"state file is missing a 'watermarks' object: {path}")
    cleaned = {
        code: str(value) for code, value in watermarks.items()
        if isinstance(value, str) and value
    }
    return {"version": STATE_VERSION, "watermarks": cleaned}


def save_state(path: pathlib.Path, state: dict[str, Any]) -> None:
    """Durably replace state without exposing a partially written JSON file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def _utc_iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_url(lookback_days: int, now: datetime) -> tuple[str, date, date]:
    """Build one bounded CSV request covering the trailing ``lookback_days`` window."""
    end = now.astimezone(timezone.utc).date()
    start = end - timedelta(days=lookback_days - 1)
    query = {
        "csv.x": "yes",
        "Datefrom": start.strftime("%d/%b/%Y"),
        "Dateto": "now",
        "SeriesCodes": ",".join(SERIES),
        "CSVF": "TN",
        "UsingCodes": "Y",
    }
    return f"{BASE_URL}?{urllib.parse.urlencode(query)}", start, end


def _parse_observation_date(value: str) -> date:
    """Parse a BoE reporting date such as ``01 Oct 2026``."""
    return datetime.strptime(value.strip(), "%d %b %Y").date()


def parse_csv(
    text: str,
    watermark: dict[str, str],
    fetched_at: str,
    date_from: date,
    date_to: date,
    diagnostics: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Validate one BoE CSV response and return (records, latest-date-per-series).

    ``diagnostics`` accumulates counters (missing values, unparseable values,
    bad dates, out-of-window rows, dropped series) for the run summary.
    """
    lines = [line for line in text.splitlines() if line.strip() != ""]
    if not lines or not lines[0].startswith("DATE,"):
        snippet = " ".join(text[:200].split())
        raise BoeResponseError(
            "response is not the expected tabular CSV (first line must be "
            f"'DATE,<codes>...'); got: {snippet!r}"
        )

    header = next(csv.reader(io.StringIO(lines[0])))
    column: dict[str, int] = {}
    for index, name in enumerate(header):
        if index == 0:
            continue
        if name in SERIES and name not in column:
            column[name] = index
    missing = [code for code in SERIES if code not in column]
    for code in missing:
        diagnostics["dropped_series"].append(
            {"series": code, "reason": "series code absent from response header"}
        )

    records: list[dict[str, Any]] = []
    progress = {code: watermark.get(code, "") for code in column}
    for row in csv.reader(io.StringIO("\n".join(lines[1:]))):
        if not row:
            continue
        raw_date = row[0]
        try:
            observed = _parse_observation_date(raw_date)
        except ValueError:
            diagnostics["bad_dates"] += 1
            continue
        if observed < date_from or observed > date_to:
            # Outside the requested window; never emit, never advance the watermark.
            diagnostics["out_of_window"] += 1
            continue
        iso = observed.isoformat()
        for code, meta in SERIES.items():
            if code not in column:
                continue
            cell = row[column[code]].strip() if column[code] < len(row) else ""
            if cell == "":
                diagnostics["missing_values"] += 1
                continue
            try:
                value = float(cell)
            except ValueError:
                diagnostics["unparseable_values"] += 1
                continue
            if watermark.get(code, "") and iso <= watermark[code]:
                continue
            records.append({
                "source": SOURCE,
                "id": f"{code}:{iso}",
                "fetched_at": fetched_at,
                "series_code": code,
                "series_name": meta["name"],
                "unit": meta["unit"],
                "date": iso,
                "value": value,
            })
            if iso > progress[code]:
                progress[code] = iso
    return records, progress


def _fetch_csv(url: str, timeout: float) -> tuple[str, int]:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "text/csv"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise BoeResponseError(f"response exceeds {MAX_RESPONSE_BYTES} bytes")
    return payload.decode("utf-8", errors="replace"), len(payload)


def run_summary(
    *,
    records: int,
    requests_succeeded: bool,
    series_present: int,
    dropped: list[dict[str, str]],
    window: dict[str, str],
    watermarks: dict[str, str],
    payload_bytes: int,
    diagnostics: dict[str, Any],
    error: str | None = None,
) -> dict[str, Any]:
    series_total = len(SERIES)
    if not requests_succeeded:
        health, completeness = "failed", "failed"
        succeeded, failed = 0, 1
        partitions = {"attempted": series_total, "succeeded": 0, "failed": series_total, "failures": dropped}
    elif dropped:
        health, completeness = "degraded", "partial"
        succeeded, failed = 1, 0
        partitions = {
            "attempted": series_total,
            "succeeded": series_present,
            "failed": len(dropped),
            "failures": dropped,
        }
    else:
        health, completeness = "healthy", "complete"
        succeeded, failed = 1, 0
        partitions = {"attempted": series_total, "succeeded": series_total, "failed": 0, "failures": []}

    summary: dict[str, Any] = {
        "health": health,
        "completeness": completeness,
        "requests": {
            "attempted": 1,
            "succeeded": succeeded,
            "failed": failed,
            "payload_bytes": payload_bytes,
        },
        "partitions": partitions,
        "records": records,
        "window": window,
        "state_change": {"watermarks": watermarks},
        "metrics": {
            "series_present": series_present,
            "missing_values": diagnostics.get("missing_values", 0),
            "unparseable_values": diagnostics.get("unparseable_values", 0),
            "bad_dates": diagnostics.get("bad_dates", 0),
            "out_of_window": diagnostics.get("out_of_window", 0),
        },
    }
    if error is not None:
        summary["error"] = error
    return summary


def _emit_summary(err: Any, summary: dict[str, Any]) -> None:
    try:
        line = SUMMARY_PREFIX + json.dumps(summary, ensure_ascii=False, separators=(",", ":")) + "\n"
        err.write(line)
        err.flush()
    except OSError as exc:
        raise SummaryEmissionError(str(exc)) from exc


def _failure_message(exc: BaseException) -> str:
    message = " ".join(str(exc).split())
    return message[:500]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch BoE daily interest-rate series as NDJSON.")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write watermarks (one-off full pull)")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                        help=f"trailing window to fetch per run (default {DEFAULT_LOOKBACK_DAYS}, max {MAX_LOOKBACK_DAYS})")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT,
                        help=f"request timeout in seconds (default {DEFAULT_TIMEOUT})")
    return parser


def main(argv: list[str] | None = None, *, out: Any = None, err: Any = None) -> int:
    out = sys.stdout if out is None else out
    err = sys.stderr if err is None else err
    args = _parser().parse_args(argv)

    if not 1 <= args.lookback_days <= MAX_LOOKBACK_DAYS:
        err.write(f"lookback-days must be between 1 and {MAX_LOOKBACK_DAYS}\n")
        return 2

    now = datetime.now(timezone.utc)
    fetched_at = _utc_iso(now)
    window_from = window_to = ""
    watermarks: dict[str, str] = {}

    try:
        url, date_from, date_to = build_url(args.lookback_days, now)
        window_from, window_to = date_from.isoformat(), date_to.isoformat()

        if args.no_state:
            state_path = None
        else:
            state_path = validate_state_path(args.state_file or default_state_path())
        state = {"version": STATE_VERSION, "watermarks": {}} if args.no_state else load_state(state_path)
        watermarks = dict(state["watermarks"])

        text, payload_bytes = _fetch_csv(url, args.timeout)

        diagnostics: dict[str, Any] = {
            "dropped_series": [], "missing_values": 0, "unparseable_values": 0,
            "bad_dates": 0, "out_of_window": 0,
        }
        records, progress = parse_csv(text, watermarks, fetched_at, date_from, date_to, diagnostics)

        for record in records:
            out.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        out.flush()

        summary = run_summary(
            records=len(records),
            requests_succeeded=True,
            series_present=len(SERIES) - len(diagnostics["dropped_series"]),
            dropped=diagnostics["dropped_series"],
            window={"from": window_from, "to": window_to},
            watermarks=progress,
            payload_bytes=payload_bytes,
            diagnostics=diagnostics,
        )
        _emit_summary(err, summary)
        if not args.no_state:
            save_state(state_path, {"version": STATE_VERSION, "watermarks": progress})
        return 0
    except Exception as exc:
        error = _failure_message(exc)
        summary = run_summary(
            records=0,
            requests_succeeded=False,
            series_present=0,
            dropped=[{"series": code, "reason": error} for code in SERIES],
            window={"from": window_from, "to": window_to},
            watermarks=watermarks,
            payload_bytes=0,
            diagnostics={},
            error=error,
        )
        try:
            _emit_summary(err, summary)
        except SummaryEmissionError:
            pass
        err.write(f"{SOURCE}: failed: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())