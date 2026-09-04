#!/usr/bin/env python3
"""NOAA SWPC — live space weather (keyless, plain JSON files).

services.swpc.noaa.gov/json/ is a directory of continuously refreshed JSON
files — no API ceremony at all, just GET and parse. The sun does not care
about your sprint schedule: solar wind updates every minute, X-ray flux every
minute, the planetary K-index every 3 hours, and flares/CMEs arrive on their
own dramatic timeline. Aurora nowcast grids update ~every 5 minutes (relevant
at Madison's latitude during strong storms, pleasingly).

High-confidence known endpoints (SWPC has served these paths for years);
not live-fetched in this session, so smoke-test each file once and prune the
FEEDS dict to what you actually want. If a path 404s, browse the parent
directory listing — SWPC occasionally reorganizes.

'Current' per run = latest rows of each feed newer than your stored watermark.
Each file is a trailing window (the 1-minute feeds carry ~2,700 rows, roughly a
day), so every poll re-serves rows already collected. The newest ``ts`` emitted
per feed is checkpointed and later runs emit only rows past it: no duplicates,
no gaps, whatever the cron interval. A feed whose rows carry no usable timestamp
cannot be watermarked and is emitted in full, as before. The watermark is written
only after every record is printed, so an aborted run re-fetches rather than
skipping (at-least-once).

Stdlib only.
"""
import argparse
import json
import os
import pathlib
import urllib.request
from datetime import datetime, timezone

HOST = "https://services.swpc.noaa.gov"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"

# feed name -> (path, time field). Verify paths on first run.
FEEDS = {
    "kp_index":    ("/products/noaa-planetary-k-index.json", 0),   # array-of-arrays, header row
    # verified live: the catalog-plausible /products/solar-wind/plasma-1-day.json
    # 404s; this is the real, currently-served real-time solar wind feed.
    "solar_wind":  ("/json/rtsw/rtsw_wind_1m.json", "time_tag"),   # array of dicts
    "xray_flux":   ("/json/goes/primary/xrays-1-day.json", "time_tag"),
    "flares":      ("/json/solar_probabilities.json", None),       # daily forecast probabilities
}


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the per-feed watermarks live: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "swpc_space_weather.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    if not isinstance(state, dict):
        raise ValueError(f"malformed watermark state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the watermarks atomically so a crash can't leave them half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def _get(path: str):
    req = urllib.request.Request(HOST + path, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.load(resp)


def fetch_feed(name: str, watermark: str = "", progress=None):
    """Yield normalized rows for one feed newer than ``watermark``.

    Handles both SWPC JSON shapes: (a) array-of-arrays with a header row,
    (b) array of dicts. ``progress`` receives the newest ``ts`` seen.
    """
    path, timefield = FEEDS[name]
    data = _get(path)
    now = datetime.now(timezone.utc).isoformat()

    def emit(rec: dict, ts, index: int):
        # A row without a usable timestamp can't be watermarked; pass it through.
        if ts is not None and watermark and str(ts) <= watermark:
            return None
        if ts is not None and progress is not None and str(ts) > progress.get(name, ""):
            progress[name] = str(ts)
        # **rec first: some feeds carry their own "source"/"id"-shaped keys
        # (e.g. rtsw's per-row "source": "ACE"/"DSCOVR") that must not
        # clobber our envelope -- ours always wins by coming last.
        return {**rec, "source": f"swpc_{name}", "fetched_at": now,
                "id": f"{name}:{ts if ts is not None else index}", "ts": ts}

    if isinstance(data, list) and data and isinstance(data[0], list):
        header, rows = data[0], data[1:]
        for i, row in enumerate(rows):
            rec = dict(zip(header, row))
            ts = row[timefield] if isinstance(timefield, int) else None
            record = emit(rec, ts, i)
            if record is not None:
                yield record
    elif isinstance(data, list):
        for i, rec in enumerate(data):
            # not every feed's row carries the declared timefield (e.g. "flares"
            # uses "date" regardless of its own FEEDS timefield of None) -- fall
            # back through both before resorting to a positional id.
            ts = (rec.get(timefield) if isinstance(timefield, str) else None) \
                or rec.get("date") or rec.get("time_tag")
            record = emit(rec, ts, i)
            if record is not None:
                yield record
    else:
        record = emit(dict(data), None, 0)
        if record is not None:
            yield record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("feed", nargs="?", default="kp_index", choices=tuple(FEEDS))
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write watermarks (one-off full pull)")
    args = parser.parse_args()

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)
    watermark = state.get(args.feed, "")
    progress = dict(state)
    for rec in fetch_feed(args.feed, watermark, progress):
        print(json.dumps(rec, ensure_ascii=False))
    # Only after every record is written: an aborted run re-fetches instead of skipping.
    if not args.no_state and progress.get(args.feed, "") > watermark:
        save_state(path, progress)


if __name__ == "__main__":
    main()
