#!/usr/bin/env python3
"""CDC NNDSS — provisional weekly counts of national notifiable diseases (keyless Socrata/SODA).

Sector: **public health surveillance.** The National Notifiable Diseases
Surveillance System collates case reports submitted by the 50 states, New York
City, the District of Columbia and the U.S. territories and publishes them
weekly as provisional tables. This dataset (``x9gk-5huc`` on data.cdc.gov) is
the machine-readable edition: one row per notifiable condition x reporting
area (120 conditions x 70 areas = ~8,400 rows per MMWR week), carrying the
current week's case count (``m1``), the previous-52-week maximum (``m2``), the
YTD cumulative for the current MMWR year (``m3``) and the YTD total for the
previous MMWR year (``m4``), each with a data-quality flag.

Each MMWR week's batch is the unit of change: CDC publishes tables "as
published each week" and a new batch lands roughly weekly (verified 2026-11-02:
newest batch 2026-W42, dataset rows updated 2026-10-28). A daily poll is
therefore slower than the data changes.

The fetcher checkpoints the newest emitted (year, week) and only fetches weeks
past it: one server-side ``$group`` request finds the latest batch, then one
request per new week. Cold start emits only the latest week — the full history
back to 2014 is deliberately not backfilled. The watermark is committed only
after every row of the new weeks is printed, so an aborted run re-fetches
rather than skipping (at-least-once).

Licence: U.S. federal government work (CDC), public domain. Attribution is
optional; the dataset is credited to the Office of Public Health Data,
Surveillance, and Technology, Centers for Disease Control and Prevention.
Etiquette: anonymous SODA requests share a throttled pool; this source makes
at most two requests in a week with new data and one on quiet days.

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "nndss_weekly"
DOMAIN = "data.cdc.gov"
DATASET = "x9gk-5huc"
# A full MMWR week is 8,400 rows today (120 conditions x 70 areas); 10,000
# leaves headroom for the table to grow. A full page triggers one offset-paged
# follow-up rather than truncation.
PAGE_LIMIT = 10000
REQUEST_TIMEOUT = 60
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "nndss_weekly.json"


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


def soda(params: dict) -> list:
    """One SODA query against the NNDSS weekly view; returns the JSON array."""
    url = f"https://{DOMAIN}/resource/{DATASET}.json"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
        data = json.load(resp)
    if not isinstance(data, list):
        raise ValueError(f"unexpected SODA response for {DATASET}: expected a JSON array")
    return data


def latest_week() -> tuple[int, int]:
    """Newest (year, week) batch in the dataset, via a server-side group-by."""
    rows = soda({
        "$select": "year,week,count(*) as n",
        "$group": "year,week",
        "$order": "year desc, week desc",
        "$limit": "1",
    })
    if not rows:
        raise RuntimeError("SODA returned no year/week batches; dataset is empty")
    return int(rows[0]["year"]), int(rows[0]["week"])


def _count(value):
    """Convert SODA count columns ('1.0') to numbers; keep everything else
    (including suppression markers) verbatim so no source value is lost."""
    if value is None:
        return None
    text = str(value)
    if text.replace(".", "", 1).isdigit():
        number = float(text)
        return int(number) if number.is_integer() else number
    return text


def normalize_row(row: dict, fetched_at: str) -> dict:
    """One NNDSS table row into the extract envelope. ``sort_order`` is the
    source-natural stable row ID (year, week, sequence)."""
    row_id = row.get("sort_order")
    if not row_id:
        raise ValueError(f"row without sort_order cannot be id'd: {row!r}")
    return {
        "source": SOURCE,
        "id": str(row_id),
        "fetched_at": fetched_at,
        "mmwr_year": int(row["year"]),
        "mmwr_week": int(row["week"]),
        "condition": row.get("label"),
        "location": row.get("states"),
        "current_week_cases": _count(row.get("m1")),
        "current_week_flag": row.get("m1_flag"),
        "max_52_weeks_cases": _count(row.get("m2")),
        "max_52_weeks_flag": row.get("m2_flag"),
        "ytd_cases": _count(row.get("m3")),
        "ytd_flag": row.get("m3_flag"),
        "ytd_previous_year_cases": _count(row.get("m4")),
        "ytd_previous_year_flag": row.get("m4_flag"),
    }


def fetch_week(year: int, week: int, fetched_at: str):
    """Yield normalized rows for one MMWR week, offset-paged for safety."""
    offset = 0
    while True:
        rows = soda({
            "$where": f"year='{year}' and week='{week}'",
            "$order": "sort_order",
            "$limit": str(PAGE_LIMIT),
            "$offset": str(offset),
        })
        for row in rows:
            yield normalize_row(row, fetched_at)
        if len(rows) < PAGE_LIMIT:
            return
        offset += PAGE_LIMIT


def run(state_file: str | None = None, no_state: bool = False,
        output=None, now=None) -> int:
    """Fetch weeks past the watermark and print them as NDJSON.

    Returns the number of rows emitted. The watermark is committed only after
    every row is printed, so an interrupted run re-fetches its weeks.
    """
    output = output if output is not None else sys.stdout
    path = state_path(state_file)
    state = {} if no_state else load_state(path)
    watermark = state.get("watermark")
    watermark = tuple(watermark) if watermark is not None else None

    latest = latest_week()
    if watermark is None:
        targets = [latest]
        print(f"{SOURCE}: cold start; emitting only latest week {latest[0]}-W{latest[1]:02d}",
              file=sys.stderr)
    elif latest > watermark:
        targets = [latest]
    elif latest < watermark:
        # Upstream never retracts a published week; an older "latest" means the
        # group-by answer regressed. Emit nothing and keep the watermark.
        print(f"{SOURCE}: latest week {latest[0]}-W{latest[1]:02d} is older than watermark "
              f"{watermark[0]}-W{watermark[1]:02d}; emitting nothing", file=sys.stderr)
        targets = []
    else:
        targets = []

    fetched_at = (now or (lambda: datetime.now(timezone.utc)))().isoformat()
    total = 0
    for year, week in targets:
        count = 0
        for record in fetch_week(year, week, fetched_at):
            output.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
        print(f"{SOURCE}: week {year}-W{week:02d}: {count} rows", file=sys.stderr)
        total += count

    if total and not no_state:
        state["watermark"] = [latest[0], latest[1]]
        save_state(path, state)

    print(SUMMARY_PREFIX + json.dumps({
        "latest_week": list(latest),
        "emitted_weeks": [list(t) for t in targets],
        "rows": total,
    }, separators=(",", ":")), file=sys.stderr)
    return total


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Extract CDC NNDSS provisional weekly notifiable-disease counts.")
    parser.add_argument("--state-file",
                        help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write watermarks (one-off pull of the latest week)")
    args = parser.parse_args(argv)
    run(state_file=args.state_file, no_state=args.no_state)


if __name__ == "__main__":
    main()
