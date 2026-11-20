#!/usr/bin/env python3
"""USNO Earth rotation parameters (EOP) — IERS daily finals, keyless, public domain.

The U.S. Naval Observatory's Earth Rotation and Reference Systems Service
publishes the IERS finals for Earth orientation as a fixed-width text file,
gaining one new final row per calendar day: UT1-UTC, polar motion x/y,
length of day, and precession-nutation corrections, each with its
uncertainty.  ``finals.daily`` carries roughly the last 90 days of finals
plus ~90 days of predictions (type P) that are revised daily until the
final supersedes them.

Only final rows (IERS flag I) are collected, gated by an MJD watermark: a
run emits the rows with ``mjd > last_mjd`` and state is written only after
every record has been printed, so an aborted run re-fetches instead of
skipping (at-least-once).  The first run backfills every final row in the
file (~90 records).  Predicted rows are excluded by design: they are
provisional and will be revised.

The data is a U.S. government work (public domain), attributed to
USNO / IERS.  One bounded request per run.  Stdlib only.
"""
import argparse
import json
import os
import pathlib
import re
import sys
import urllib.request
from datetime import datetime, timezone

URL = "https://maia.usno.navy.mil/ser7/finals.daily"
SOURCE = "usno_earth_rotation"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_TIMEOUT = 30
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
STATE_VERSION = 1

# Fixed-width columns per https://maia.usno.navy.mil/ser7/readme.finals
# (0-based slices).  The date field is YY + right-justified month + right-
# justified day, so single-digit months/days carry a pad space ("26 822").
def _num(text: str) -> float | None:
    return float(text) if text.strip() else None


def parse_row(line: str) -> dict:
    """Parse one finals.daily line; raise ValueError on any malformed field."""
    if len(line) < 135:
        raise ValueError(f"line too short ({len(line)} chars): {line!r}")
    row = {
        "yy": line[0:2], "mm": line[2:4], "dd": line[4:6],
        "mjd": line[7:15],
        "pm_flag": line[16],
        "pm_x": line[18:28], "pm_x_err": line[28:37],
        "pm_y": line[37:47], "pm_y_err": line[47:57],
        "ut1_flag": line[57],
        "ut1_utc": line[58:69], "ut1_utc_err": line[69:79],
        "lod": line[79:87], "lod_err": line[87:94],
        "nutation_flag": line[95],
        "dpsi": line[97:107], "dpsi_err": line[107:116],
        "depsilon": line[116:126], "depsilon_err": line[126:135],
        "bull_b_pm_x": line[135:145], "bull_b_pm_y": line[145:155],
        "bull_b_ut1_utc": line[155:166],
        "bull_b_dpsi": line[166:176], "bull_b_depsilon": line[176:186],
    }
    if not re.fullmatch(r"\d{2}", row["yy"]):
        raise ValueError(f"bad year field: {line!r}")
    if not re.fullmatch(r"\d{1,2}", row["mm"].strip()):
        raise ValueError(f"bad month field: {line!r}")
    if not re.fullmatch(r"\d{1,2}", row["dd"].strip()):
        raise ValueError(f"bad day field: {line!r}")
    if not 1 <= int(row["mm"].strip()) <= 12:
        raise ValueError(f"month out of range: {line!r}")
    if not 1 <= int(row["dd"].strip()) <= 31:
        raise ValueError(f"day out of range: {line!r}")
    if not re.fullmatch(r"\d{5}\.\d{2}", row["mjd"]):
        raise ValueError(f"bad MJD field: {line!r}")
    mjd = float(row["mjd"])
    if mjd < 51544.0:
        raise ValueError(f"MJD predates the 2000-base year convention: {line!r}")
    for key in ("pm_flag", "ut1_flag", "nutation_flag"):
        if row[key] not in ("I", "P", " "):
            raise ValueError(f"bad {key} flag: {line!r}")
    for key in ("pm_x", "pm_x_err", "pm_y", "pm_y_err", "ut1_utc", "ut1_utc_err",
                "lod", "lod_err", "dpsi", "dpsi_err", "depsilon", "depsilon_err",
                "bull_b_pm_x", "bull_b_pm_y", "bull_b_ut1_utc", "bull_b_dpsi",
                "bull_b_depsilon"):
        _num(row[key])  # raises ValueError on a non-numeric filled column
    return row


def parse_rows(text: str):
    """Yield parsed rows; skip blank lines, fail on anything malformed."""
    for line in text.splitlines():
        if not line.strip():
            continue
        if not line[0].isdigit():
            raise ValueError(f"unrecognized line: {line!r}")
        yield parse_row(line)


def normalize(row: dict, fetched_at: str) -> dict:
    """One finals row as a raw record, preserving source values and flags."""
    year = 2000 + int(row["yy"])  # readme: add 2000 for MJD >= 51544
    return {
        "source": SOURCE,
        "fetched_at": fetched_at,
        "id": row["mjd"],
        "date": f"{year:04d}-{int(row['mm']):02d}-{int(row['dd']):02d}",
        "mjd": float(row["mjd"]),
        "pm_flag": row["pm_flag"],
        "pm_x": _num(row["pm_x"]),
        "pm_x_err": _num(row["pm_x_err"]),
        "pm_y": _num(row["pm_y"]),
        "pm_y_err": _num(row["pm_y_err"]),
        "ut1_flag": row["ut1_flag"],
        "ut1_utc": _num(row["ut1_utc"]),
        "ut1_utc_err": _num(row["ut1_utc_err"]),
        "lod": _num(row["lod"]),
        "lod_err": _num(row["lod_err"]),
        "nutation_flag": row["nutation_flag"],
        "dpsi": _num(row["dpsi"]),
        "dpsi_err": _num(row["dpsi_err"]),
        "depsilon": _num(row["depsilon"]),
        "depsilon_err": _num(row["depsilon_err"]),
        "bull_b_pm_x": _num(row["bull_b_pm_x"]),
        "bull_b_pm_y": _num(row["bull_b_pm_y"]),
        "bull_b_ut1_utc": _num(row["bull_b_ut1_utc"]),
        "bull_b_dpsi": _num(row["bull_b_dpsi"]),
        "bull_b_depsilon": _num(row["bull_b_depsilon"]),
    }


def fetch_records(text: str, watermark: float, fetched_at: str):
    """Yield normalized final rows newer than ``watermark``."""
    for row in parse_rows(text):
        if row["ut1_flag"] != "I":
            continue  # predictions are provisional; the final supersedes them
        if float(row["mjd"]) <= watermark:
            continue
        yield normalize(row, fetched_at)


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed watermark state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="emit every final row without reading or writing state")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    args = parser.parse_args(argv)
    if args.no_state and args.state_file:
        parser.error("--no-state and --state-file cannot be combined")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    path = None if args.no_state else state_path(args.state_file)
    state = {} if path is None else load_state(path)
    watermark = float(state.get("last_mjd", 0.0))
    fetched_at = datetime.now(timezone.utc).isoformat()

    records = 0
    last = watermark
    try:
        req = urllib.request.Request(URL, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=args.timeout) as resp:
            text = resp.read().decode("utf-8")
        for record in fetch_records(text, watermark, fetched_at):
            print(json.dumps(record, ensure_ascii=False))
            records += 1
            last = max(last, record["mjd"])
    except Exception as exc:
        print(SUMMARY_PREFIX + json.dumps({
            "health": "failed", "completeness": "failed",
            "records": records, "error": str(exc),
        }), file=sys.stderr)
        sys.exit(1)

    # Only after every record is written: an aborted run re-fetches instead of skipping.
    if path is not None and last > watermark:
        save_state(path, {"version": STATE_VERSION, "source": SOURCE, "last_mjd": last})
    print(SUMMARY_PREFIX + json.dumps(
        {"health": "healthy", "completeness": "complete", "records": records},
        separators=(",", ":"),
    ), file=sys.stderr)


if __name__ == "__main__":
    main()
