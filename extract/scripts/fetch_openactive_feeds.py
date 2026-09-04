#!/usr/bin/env python3
"""OpenActive RPDE — bookable activity and facility inventory across every UK provider.

Discovers feeds from the official status dashboard (one request, generated from the
OpenActive data catalogs) and then walks each provider's RPDE pages, emitting every
``updated`` and ``deleted`` item with its provider, feed type, and feed position. The
dashboard listed 455 feeds across 151 providers when verified live 2026-09-04, of which
149 are Slot feeds.

RPDE is a *change* feed, not a collection: pages are ordered oldest-modified-first and
``next`` is a resumable cursor, so this checkpoints the cursor per feed and resumes from
it on the following run. Steady-state runs therefore emit only what actually changed
since the previous run instead of re-downloading a mostly-identical catalog every time.
The last page is reached when ``next`` repeats with no items.

Because a cursor can be resumed mid-feed, a run is bounded by a wall-clock budget rather
than by page or item caps: it walks until the budget is spent, saves its progress, and
picks up where it left off next time. Feeds are visited round-robin from the position
after the last one completed, so no feed starves during the initial backfill and nothing
is ever truncated. Cursors are only persisted after a run finishes writing its records,
so an aborted run re-fetches rather than skipping data (at-least-once).

Per-feed failures are reported on stderr and skipped; a feed that fails keeps its old
cursor and is retried next run. A run fails only if no feed produced records and every
feed failed. Feeds are CC BY 4.0 from their respective providers.

Stdlib only.
"""

import argparse
import json
import os
import pathlib
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

SOURCE = "openactive_rpde"
DASHBOARD = "https://status.openactive.io/"
FEED_TYPES = ("Slot", "FacilityUse", "SessionSeries", "ScheduledSession", "CourseInstance", "Event")
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
ANCHOR = re.compile(r'<a[^>]+name="(http[^"]+)"')
LINK = re.compile(r'<a[^>]+href="(http[^"]+)"[^>]*>([^<]*)</a>')
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the per-feed cursors live: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "openactive_rpde.json"


def load_state(path: pathlib.Path) -> dict:
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"cursors": {}, "rotate": {}}
    if not isinstance(state.get("cursors"), dict) or not isinstance(state.get("rotate"), dict):
        raise ValueError(f"malformed cursor state in {path}")
    return state


def save_state(path: pathlib.Path, state: dict):
    """Publish the cursor file atomically so a crash can't leave it half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_name(path.name + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(staged, path)


def discover_feeds(feed_type: str, timeout: int = 60):
    """Yield (provider, feed_url) pairs for one feed type, in dashboard order.

    The dashboard nests each provider's feed table inside its row, so this scans links
    in document order: a dataset-site link (also present as a named anchor) starts a
    provider block and the feed-type links that follow belong to it.
    """
    request = urllib.request.Request(DASHBOARD, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        html = response.read().decode("utf-8", "replace")
    anchors = set(ANCHOR.findall(html))
    provider, seen = None, set()
    for url, name in LINK.findall(html):
        label = name.strip()
        if url in anchors and label:
            provider = label
        elif label == feed_type and url not in seen:
            seen.add(url)
            yield provider or urllib.parse.urlparse(url).netloc, url
    if not seen:
        raise ValueError(f"dashboard exposed no {feed_type} feeds; page structure changed")


def walk_feed(provider: str, url: str, feed_type: str, fetched_at: str, cursor: str,
              deadline: float, timeout: int):
    """Walk one feed from ``cursor``, yielding records.

    Yields ``(record, next_cursor, at_end)`` so the caller can checkpoint progress even
    when the wall-clock budget stops the walk part-way through a feed.
    """
    page_url = cursor or url
    while True:
        request = urllib.request.Request(page_url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=timeout) as response:
            document = json.load(response)
        items = document.get("items")
        next_url = document.get("next")
        if not isinstance(items, list) or not isinstance(next_url, str):
            raise ValueError("RPDE page is missing items or next")
        next_url = urllib.parse.urljoin(page_url, next_url)
        at_end = not items or next_url == page_url
        for item in items:
            item_id = item.get("id")
            state = item.get("state")
            if item_id is None or state not in {"updated", "deleted"}:
                raise ValueError("RPDE item is missing a valid id or state")
            record = dict(item)
            record.update(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at,
                    "id": f"{feed_type}:{url}:{item_id}",
                    "item_id": item_id,
                    "provider": provider,
                    "feed_type": feed_type,
                    "feed_url": url,
                    "page_url": page_url,
                    "next": next_url,
                }
            )
            yield record, next_url, at_end
        if at_end:
            return
        page_url = next_url
        if time.monotonic() >= deadline:
            return


def fetch_openactive(feed_type: str = "Slot", budget_seconds: int = 2700, timeout: int = 60,
                     state: dict | None = None, max_feeds: int = 0):
    """Yield records for one feed type, updating ``state`` cursors in place."""
    fetched_at = datetime.now(timezone.utc).isoformat()
    state = state if state is not None else {"cursors": {}, "rotate": {}}
    cursors = state.setdefault("cursors", {})
    rotate = state.setdefault("rotate", {})

    feeds = list(discover_feeds(feed_type, timeout))
    if max_feeds > 0:
        feeds = feeds[:max_feeds]
    # Resume the round-robin after the last feed we finished, so the initial backfill
    # works through every provider instead of restarting on the same big feed forever.
    start = rotate.get(feed_type, 0) % len(feeds)
    ordered = feeds[start:] + feeds[:start]

    deadline = time.monotonic() + budget_seconds
    failures, produced, completed = 0, 0, 0
    for offset, (provider, url) in enumerate(ordered):
        if time.monotonic() >= deadline:
            break
        key = f"{feed_type}|{url}"
        try:
            reached_end = False
            for record, next_cursor, at_end in walk_feed(
                provider, url, feed_type, fetched_at, cursors.get(key, ""), deadline, timeout
            ):
                produced += 1
                cursors[key] = next_cursor
                reached_end = at_end
                yield record
            if reached_end:
                completed = offset + 1
        except (urllib.error.URLError, urllib.error.HTTPError, ValueError, TimeoutError,
                json.JSONDecodeError, OSError) as error:
            failures += 1
            print(f"{provider} <{url}>: {type(error).__name__}: {error}", file=sys.stderr)
    rotate[feed_type] = (start + completed) % len(feeds)

    if failures and not produced:
        raise RuntimeError(f"all {failures} {feed_type} feeds failed")
    if failures:
        print(f"{failures} of {len(feeds)} {feed_type} feeds failed", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--feed-type", default="Slot", choices=FEED_TYPES)
    parser.add_argument("--budget-seconds", type=int, default=2700,
                        help="wall-clock walking budget; progress is checkpointed and resumed")
    parser.add_argument("--max-feeds", type=int, default=0, help="0 harvests every discovered feed")
    parser.add_argument("--state-file", help="cursor file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write cursors (one-off full walk)")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    path = state_path(args.state_file)
    state = {"cursors": {}, "rotate": {}} if args.no_state else load_state(path)
    for record in fetch_openactive(args.feed_type, args.budget_seconds, args.timeout,
                                   state, args.max_feeds):
        print(json.dumps(record, ensure_ascii=False))
    # Only after every record is written: an aborted run re-fetches instead of skipping.
    if not args.no_state:
        save_state(path, state)


if __name__ == "__main__":
    main()
