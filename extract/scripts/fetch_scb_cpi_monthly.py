#!/usr/bin/env python3
"""Statistics Sweden (SCB) — monthly Consumer Price Index (CPI), keyless.

Source: SCB Statistical Database, PxWebApi v2
(https://statistikdatabasen.scb.se/api/v2/). Table TAB6596, "Consumer Price
Index (CPI), total 2020=100", metric 00000808 ("CPI, fixed index numbers").
The fixed index numbers are the series used for legal and contractual
index-linking, so they are the headline value.

**Verified live 2026-11-03:**
  * `GET /api/v2/config` — 200; `license` is CC0, `sourceReferences` recommends
    "Source: Statistics Sweden", limit 30 calls per 10 s per IP (this run uses
    two).
  * `GET /api/v2/tables/TAB6596` — 200; `lastPeriod` 2026M09, `updated`
    2026-10-14: the series refreshes monthly (SCB publishes the monthly CPI
    around the 12th-14th), so a daily poll is well below the data change rate.
  * `GET /api/v2/tables/TAB6596/data?...&outputFormat=json-stat2` — 200;
    json-stat 2.0 with a flat `value` array indexed by the `Tid` category
    position.

Quirks that will cost someone an afternoon:
    * **GET `valueCodes[month]=last(3)` returns 400 "Non-existent variable".**
      The query parameters must use the table's internal dimension codes
      (`Tid` for time, `ContentsCode` for the measure), not the display names
      (`month`, `observations`) reported by the table summary.
    * Missing cells arrive as JSON null (the 2020=100 series starts at
      2026M01, so older months in the window are null) or the string "n" —
      skip them and count them in the run summary, not as data.
    * `value` is a flat list aligned to the `dimension.Tid.category.index`
      positions, not a period->value map; map through the index explicitly.

Every run re-emits the whole lookback window (3 months by default): SCB
occasionally revises recent months, and the warehouse is insert-only, so a
consumer taking the latest row per id by `fetched_at` picks up revisions. The
watermark still advances each run and is reported when the published `lastPeriod`
jumps more than the window, so a skipped release is visible in the task log.

Stdlib only.
"""

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
from typing import Callable, TextIO

SOURCE = "scb_cpi_monthly"
BASE = "https://statistikdatabasen.scb.se/api/v2"
TABLE_ID = "TAB6596"
METRIC_CODE = "00000808"  # "CPI, fixed index numbers"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
DEFAULT_LOOKBACK_MONTHS = 3
DEFAULT_INITIAL_LOOKBACK_MONTHS = 12
MAX_LOOKBACK_MONTHS = 24
STATE_VERSION = 1
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
PERIOD = "Tid"
CONTENT = "ContentsCode"


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit)
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {"last_period": "YYYYMmm"}; no file means no watermark yet."""
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        raise ValueError(f"SCB CPI state file {path} has an unexpected version")
    if state.get("source") != SOURCE:
        raise ValueError(f"SCB CPI state file {path} belongs to another source")
    period = state.get("last_period")
    if not isinstance(period, str) or parse_period(period) is None:
        raise ValueError(f"SCB CPI state file {path} has a malformed last_period")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_suffix(path.suffix + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def parse_period(text: object) -> tuple[int, int] | None:
    """'2026M09' -> (2026, 9); anything else -> None."""
    if not isinstance(text, str) or len(text) != 7 or text[4] != "M":
        return None
    try:
        year, month = int(text[:4]), int(text[5:])
    except ValueError:
        return None
    if not 1900 <= year <= 2100 or not 1 <= month <= 12:
        return None
    return year, month


def format_period(year: int, month: int) -> str:
    return f"{year}M{month:02d}"


def shift_period(period: str, months: int) -> str:
    """Move a 'YYYYMmm' period by `months` (negative allowed)."""
    year, month = parse_period(period)
    index = year * 12 + (month - 1) + months
    return format_period(index // 12, index % 12 + 1)


def period_index(period: str) -> int:
    """Absolute month position of a 'YYYYMmm' period, for gap arithmetic."""
    year, month = parse_period(period)
    return year * 12 + (month - 1)


def request_json(url: str, timeout: int) -> object:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as resp:
        return json.load(resp)


def table_info(transport: Callable[[str, int], object], timeout: int) -> dict:
    """Table summary: latest published period plus the table's own metadata."""
    url = f"{BASE}/tables/{TABLE_ID}?lang=en"
    doc = transport(url, timeout)
    if not isinstance(doc, dict):
        raise ValueError("SCB table summary must be an object")
    last_period = parse_period(doc.get("lastPeriod"))
    if last_period is None:
        raise ValueError(f"SCB table summary has no valid lastPeriod: {doc.get('lastPeriod')!r}")
    return {
        "last_period": format_period(*last_period),
        "updated": doc.get("updated"),
        "label": doc.get("label"),
    }


def window_periods(last_period: str, watermark: str | None,
                   lookback: int, initial_lookback: int) -> list[str]:
    """Periods to emit, oldest first, ending at the latest published period."""
    if watermark:
        start = shift_period(last_period, -(lookback - 1))
    else:
        start = shift_period(last_period, -(initial_lookback - 1))
    if start > last_period:
        return []
    periods = []
    period = start
    while period <= last_period:
        periods.append(period)
        period = shift_period(period, 1)
    return periods


def data_document(transport: Callable[[str, int], object], periods: list[str],
                  timeout: int) -> dict:
    """One bounded json-stat2 query for the whole window."""
    params = urllib.parse.urlencode({
        "lang": "en",
        f"valueCodes[{PERIOD}]": ",".join(periods),
        f"valueCodes[{CONTENT}]": METRIC_CODE,
        "outputFormat": "json-stat2",
    })
    doc = transport(f"{BASE}/tables/{TABLE_ID}/data?{params}", timeout)
    if not isinstance(doc, dict) or doc.get("class") != "dataset":
        raise ValueError("SCB data response must be a json-stat2 dataset")
    return doc


def normalize(doc: dict, periods: list[str], info: dict,
              fetched_at: str) -> tuple[list[dict], int]:
    """Map the flat value array onto period codes; skip missing cells."""
    tid = (doc.get("dimension") or {}).get(PERIOD)
    index = tid.get("category", {}).get("index") if isinstance(tid, dict) else None
    if not isinstance(index, dict) or sorted(index) != sorted(periods):
        raise ValueError("SCB response time dimension does not match the requested periods")
    contents = (doc.get("dimension") or {}).get(CONTENT)
    if not isinstance(contents, dict):
        raise ValueError("SCB response is missing the measure dimension")
    metric_label = contents.get("category", {}).get("label", {}).get(METRIC_CODE)
    size = doc.get("size")
    values = doc.get("value")
    if not isinstance(size, list) or size != [1, len(periods)]:
        raise ValueError("SCB response size does not match the requested window")
    # A single-metric json-stat2 response is a flat value array aligned to the
    # Tid category positions, not a row per metric.
    if not isinstance(values, list) or len(values) != len(periods):
        raise ValueError("SCB value array does not match the requested window")

    records = []
    missing = 0
    for position, period in enumerate(sorted(index, key=index.get)):
        value = values[index[period]]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            # SCB marks unpublished cells with JSON null (months before the
            # 2020=100 reference series starts) or the string "n".
            missing += 1
            continue
        records.append({
            "source": SOURCE,
            "id": period,
            "fetched_at": fetched_at,
            "period": period,
            "value": value,
            "metric_code": METRIC_CODE,
            "metric": metric_label,
            "table_id": TABLE_ID,
            "table_label": info.get("label"),
            "table_updated": info.get("updated"),
        })
    return records, missing


def run(
    *,
    path: pathlib.Path | None,
    output: TextIO,
    lookback_months: int = DEFAULT_LOOKBACK_MONTHS,
    initial_lookback_months: int = DEFAULT_INITIAL_LOOKBACK_MONTHS,
    timeout: int = 30,
    transport: Callable[[str, int], object] = request_json,
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> int:
    for name, value in (("lookback_months", lookback_months),
                        ("initial_lookback_months", initial_lookback_months)):
        if not 1 <= value <= MAX_LOOKBACK_MONTHS:
            raise ValueError(f"{name} must be within 1-{MAX_LOOKBACK_MONTHS}")
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    fetched_at = now()
    if fetched_at.utcoffset() is None:
        raise ValueError("fetched_at must include a UTC offset")
    fetched_at_text = fetched_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    watermark = load_state(path).get("last_period") if path is not None else None
    info = table_info(transport, timeout)
    periods = window_periods(info["last_period"], watermark,
                             lookback_months, initial_lookback_months)
    missing = 0
    records = []
    if periods:
        records, missing = normalize(
            data_document(transport, periods, timeout), periods, info, fetched_at_text
        )

    payload = "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
                      for record in records)
    if payload:
        written = output.write(payload)
        if written is not None and written != len(payload):
            raise OSError("short write to NDJSON output")
    output.flush()

    state_change = {}
    if path is not None and records and info["last_period"] != watermark:
        # Only after every record is written: an aborted run re-fetches instead
        # of skipping. An empty pull, or one whose watermark did not advance,
        # leaves the state file untouched.
        state_change["last_period"] = info["last_period"]
        save_state(path, {"version": STATE_VERSION, "source": SOURCE,
                          "last_period": info["last_period"]})

    missed_months = 0
    if watermark:
        behind = period_index(info["last_period"]) - period_index(watermark)
        if behind > lookback_months:
            missed_months = behind - lookback_months
            print(f"SCB CPI: published {info['last_period']} is {behind} months after the "
                  f"watermark {watermark}; {missed_months} month(s) outside the "
                  f"{lookback_months}-month window were not re-collected", file=sys.stderr)

    summary = {
        "health": "healthy",
        "completeness": "complete",
        "requests": 2 if periods else 1,
        "state_change": state_change,
        "metrics": {
            "periods": len(records),
            "first_period": periods[0] if periods else None,
            "last_period": info["last_period"],
            "missing_values": missing,
            "missed_months": missed_months,
        },
    }
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    return len(records)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="do not read or update persistent state")
    parser.add_argument("--lookback-months", type=int, default=DEFAULT_LOOKBACK_MONTHS,
                        help=f"periods per stateful run, 1-{MAX_LOOKBACK_MONTHS} "
                             f"(default {DEFAULT_LOOKBACK_MONTHS})")
    parser.add_argument("--initial-lookback-months", type=int, default=DEFAULT_INITIAL_LOOKBACK_MONTHS,
                        help=f"backfill window for a run without a watermark, 1-{MAX_LOOKBACK_MONTHS} "
                             f"(default {DEFAULT_INITIAL_LOOKBACK_MONTHS})")
    parser.add_argument("--timeout", type=int, default=30)
    arguments = parser.parse_args()
    if arguments.no_state and arguments.state_file:
        parser.error("--no-state and --state-file are mutually exclusive")
    try:
        count = run(
            path=None if arguments.no_state else state_path(arguments.state_file),
            output=sys.stdout,
            lookback_months=arguments.lookback_months,
            initial_lookback_months=arguments.initial_lookback_months,
            timeout=arguments.timeout,
        )
    except ValueError as error:
        parser.error(str(error))
    print(f"Collected {count} SCB CPI monthly observations", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
