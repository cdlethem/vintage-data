#!/usr/bin/env python3
"""Melbourne — minute-level pedestrian counts for every sensor in the recent window.

The publisher refreshes roughly every 15 minutes and exposes a rolling window only, so
polling builds the durable history. Pages arrive newest-first, so this walks them and
stops as soon as it reaches minutes it has already collected, keeping the publisher's
presence/absence signal (a missing minute is left missing rather than emitted as zero).
Verified live 2026-09-04 through the City of Melbourne OpenDataSoft API; city open-data
terms apply.

``sensing_datetime`` is a natural high-water mark, so the newest one emitted is
checkpointed and the next run stops there instead of re-emitting an arbitrary overlap
window. That makes successive runs fetch only genuinely new minutes: no duplicates and
no gaps, whatever the cron interval. ``--minutes`` only bounds the very first (cold)
run, where there is no watermark yet, and acts as a floor if the feed's rolling window
has moved past our watermark. The watermark is written only after every record is
printed, so an aborted run re-fetches rather than skipping (at-least-once).

Stdlib only.
"""

import argparse
import json
import os
import pathlib
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

SOURCE = "melbourne_pedestrian_counts"
URL = "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets/pedestrian-counting-system-past-hour-counts-per-minute/records"
USER_AGENT = "my-pipeline-poc/0.1 (contact: you@example.com)"
PAGE_SIZE = 100
OFFSET_CEILING = 9900  # OpenDataSoft rejects offset + limit beyond 10000
DEFAULT_DATA_ROOT = "~/dev/data/extract"


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_watermark(path: pathlib.Path) -> str:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return ""
    watermark = state.get("sensing_datetime", "")
    if not isinstance(watermark, str):
        raise ValueError(f"malformed watermark state in {path}")
    return watermark


def save_watermark(path: pathlib.Path, watermark: str):
    """Publish the watermark atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps({"sensing_datetime": watermark}, indent=2) + "\n",
                      encoding="utf-8")
    os.replace(staged, path)


def _page(offset: int, timeout: int):
    query = urllib.parse.urlencode(
        {"limit": PAGE_SIZE, "offset": offset, "order_by": "sensing_datetime desc"}
    )
    request = urllib.request.Request(f"{URL}?{query}", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        document = json.load(response)
    rows = document.get("results")
    if not isinstance(rows, list):
        raise TypeError("Melbourne response is missing results")
    return rows


def fetch_counts(minutes: int = 90, timeout: int = 30, watermark: str = "", progress=None):
    """Yield rows newer than ``watermark``, newest first.

    ``minutes`` bounds the cold start and floors the walk if the publisher's rolling
    window has already moved past our watermark. ``progress`` receives the newest
    ``sensing_datetime`` seen so the caller can checkpoint it.
    """
    if minutes <= 0:
        return
    fetched_at = datetime.now(timezone.utc)
    floor = (fetched_at - timedelta(minutes=minutes)).isoformat()
    # Resume exactly where we stopped, unless the rolling window has outrun us.
    cutoff = max(watermark, floor) if watermark else floor
    stamp = fetched_at.isoformat()
    offset = 0
    while offset <= OFFSET_CEILING:
        rows = _page(offset, timeout)
        if not rows:
            return
        for row in rows:
            location_id = row.get("location_id")
            sensed_at = row.get("sensing_datetime")
            if location_id is None or not sensed_at:
                raise ValueError("pedestrian row is missing location_id or sensing_datetime")
            if sensed_at <= cutoff:
                return
            record = dict(row)
            record.update(
                {
                    "source": SOURCE,
                    "fetched_at": stamp,
                    "id": f"{location_id}:{sensed_at}",
                }
            )
            if progress is not None and sensed_at > progress.get("sensing_datetime", ""):
                progress["sensing_datetime"] = sensed_at
            yield record
        if len(rows) < PAGE_SIZE:
            return
        offset += PAGE_SIZE


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--minutes", type=int, default=90,
                        help="cold-start lookback, and floor if the rolling window outran us")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off window pull)")
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()

    path = state_path(args.state_file)
    watermark = "" if args.no_state else load_watermark(path)
    progress = {"sensing_datetime": watermark}
    for record in fetch_counts(args.minutes, args.timeout, watermark, progress):
        print(json.dumps(record, ensure_ascii=False))
    # Only after every record is written: an aborted run re-fetches instead of skipping.
    if not args.no_state and progress["sensing_datetime"] > watermark:
        save_watermark(path, progress["sensing_datetime"])


if __name__ == "__main__":
    main()
