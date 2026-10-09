#!/usr/bin/env python3
"""US Treasury daily par yield curve (keyless).

The Treasury appends one row per US business day to the current-year CSV at
~20:30 UTC (4:30 PM ET); earlier rows are stable. The watermark is the ISO date of
the last row landed, so business-day runs emit one row and weekend/holiday runs
emit none (exit 0, empty stdout is valid). ``--no-state`` re-baselines the year;
``--year`` retrieves one bounded historical year without touching state. The
current-year file is ~16 KB.

Stdlib only.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import pathlib
import sys
import urllib.request
from datetime import datetime, timezone

SOURCE = "us_treasury_yield_curve"
BASE_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
STATE_VERSION = 1
# Published tenor columns: CSV header -> canonical field name.
TENOR_COLUMNS = {
    "1 Mo": "mo1",
    "1.5 Month": "mo1_5",
    "2 Mo": "mo2",
    "3 Mo": "mo3",
    "4 Mo": "mo4",
    "6 Mo": "mo6",
    "1 Yr": "yr1",
    "2 Yr": "yr2",
    "3 Yr": "yr3",
    "5 Yr": "yr5",
    "7 Yr": "yr7",
    "10 Yr": "yr10",
    "20 Yr": "yr20",
    "30 Yr": "yr30",
}


def year_url(year: int) -> str:
    return f"{BASE_URL}/{year}/all?type=daily_treasury_yield_curve&_format=csv"


def state_path(explicit: str | None = None) -> pathlib.Path:
    if explicit:
        return pathlib.Path(explicit).expanduser()
    return pathlib.Path(os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"version": STATE_VERSION, "last_date": None}
    except ValueError:
        raise ValueError(f"malformed state in {path}")
    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        raise ValueError(f"malformed state in {path}")
    if state.get("last_date") is not None and not isinstance(state["last_date"], str):
        raise ValueError(f"malformed last_date in {path}")
    return state


def save_state(path: pathlib.Path, state: dict) -> None:
    """Atomically publish the watermark only after stdout has accepted every record."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def download(year: int, timeout: int) -> bytes:
    request = urllib.request.Request(year_url(year), headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def parse_curve(body: bytes, year: int, fetched_at: str) -> list[dict]:
    rows = list(csv.reader(io.StringIO(body.decode("utf-8-sig"))))
    if not rows:
        raise ValueError("yield curve CSV is empty")
    header = rows[0]
    if not header or header[0] != "Date":
        raise ValueError(f"unexpected yield curve CSV header: {header[:3]!r}")
    columns = {name: index for index, name in enumerate(header)}
    missing = [name for name in TENOR_COLUMNS if name not in columns]
    if missing:
        raise ValueError(f"yield curve CSV is missing tenor columns {missing}")
    records = []
    seen = set()
    for row in rows[1:]:
        if not row or not row[0].strip():
            continue
        try:
            day = datetime.strptime(row[0].strip(), "%m/%d/%Y").date()
        except ValueError:
            raise ValueError(f"unparseable yield curve date: {row[0]!r}")
        if day.year != year:
            raise ValueError(f"row {day.isoformat()} is outside the requested year {year}")
        if day.isoformat() in seen:
            raise ValueError(f"duplicate yield curve row for {day.isoformat()}")
        seen.add(day.isoformat())
        record = {"id": day.isoformat(), "date": day.isoformat()}
        for name, key in TENOR_COLUMNS.items():
            value = row[columns[name]].strip()
            record[key] = None if value == "" else float(value)
        record.update({"source": SOURCE, "fetched_at": fetched_at})
        records.append(record)
    records.sort(key=lambda record: record["date"])
    return records


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off full-year pull)")
    parser.add_argument("--year", type=int, help="retrieve one bounded historical calendar year")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    current_year = datetime.now(timezone.utc).year
    year = args.year if args.year is not None else current_year
    fetched_at = datetime.now(timezone.utc).isoformat()
    path = None if args.no_state else state_path(args.state_file)
    records = 0
    try:
        state = {"version": STATE_VERSION, "last_date": None} if path is None else load_state(path)
        last_date_before = state.get("last_date")
        rows = parse_curve(download(year, args.timeout), year, fetched_at)
        # Watermark only governs the scheduled current-year run; explicit
        # historical years are always complete bounded dumps.
        if args.year is None and path is not None and last_date_before is not None:
            rows = [row for row in rows if row["date"] > last_date_before]
        for row in rows:
            print(json.dumps(row, ensure_ascii=False, separators=(",", ":")))
            records += 1
        last_date_after = last_date_before
        if args.year is None and path is not None and rows:
            last_date_after = rows[-1]["date"]
            state["last_date"] = last_date_after
            save_state(path, state)
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps({
            "health": "failed", "completeness": "failed", "records": records, "error": str(exc),
        }, separators=(",", ":")), file=sys.stderr)
        return 1
    print(SUMMARY_PREFIX + json.dumps({
        "health": "healthy", "completeness": "complete", "records": records,
        "coverage": {"year": year, "last_date_before": last_date_before, "last_date_after": last_date_after},
        "requests": {"attempted": 1},
    }, separators=(",", ":")), file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
