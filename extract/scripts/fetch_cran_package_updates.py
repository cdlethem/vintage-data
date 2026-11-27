#!/usr/bin/env python3
"""CRAN — R packages newly published or updated since the previous run (keyless, HTML).

https://cran.r-project.org/web/packages/available_packages_by_date.html lists all
~26,000 contributed packages with the publication date of each package's *current*
version, newest first. Both brand-new packages and new releases of established
ones (a bugfix of a mature package moves it to today) appear, so this is a true
update stream, not just a new-arrivals list. Verified live 2026-11-27: the last
two weeks of the table held 38-89 rows per day.

'Current' per run = rows dated after the stored watermark date, plus the remainder
of the watermark date not yet emitted (the boundary set). The table is sorted
newest-first, so scanning stops at the first older row. State is written only
after every record is printed, so an aborted run re-fetches rather than skipping
(at-least-once). Package names are case-insensitive on CRAN, so `id` is the
lowercased name; the original spelling is kept in `package`.

Each emitted package's current version is fetched from its package page (one
extra request per row, ~70/day). A page that is still unreachable after retries
does not fail the run: the record carries `version: null` and the count is
reported on the VINTAGE_RUN_SUMMARY stderr line.

First run has no watermark and, as its baseline, covers rows dated today minus
`--days-back` (default 1) and later.

Stdlib only.
"""
import argparse
import errno
import json
import os
import pathlib
import re
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser

SOURCE = "cran_package_updates"
LIST_URL = "https://cran.r-project.org/web/packages/available_packages_by_date.html"
PACKAGE_URL = "https://cran.r-project.org/web/packages/{name}/index.html"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"

MAX_ATTEMPTS = 3
RETRY_DELAYS = (1, 2)
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}\Z")
VERSION_RE = re.compile(r"<td>\s*Version:\s*</td>\s*<td>\s*([^<]+?)\s*</td>", re.S)
LIST_HEADER = ("Date", "Package", "Title")


class ListTable(HTMLParser):
    """Collect the Date/Package/Title rows of the by-date package table."""

    def __init__(self):
        super().__init__()
        self.rows = []
        self.row = None
        self.cell = None

    def handle_starttag(self, tag, attrs):
        if tag == "tr":
            self.row = []
        elif tag in ("th", "td") and self.row is not None:
            self.cell = []

    def handle_data(self, data):
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag):
        if tag in ("th", "td") and self.cell is not None:
            self.row.append(" ".join("".join(self.cell).split()))
            self.cell = None
        elif tag == "tr" and self.row is not None:
            if any(self.row):
                self.rows.append(self.row)
            self.row = None


def parse_list_table(html: str | bytes):
    """Yield (date, package, title) from the by-date table, newest first.

    Fails (ValueError) if the expected table or a row shape is missing, so a
    markup change is never silently read as an empty day.
    """
    if isinstance(html, bytes):
        html = html.decode("utf-8", "replace")
    parser = ListTable()
    parser.feed(html)
    rows = [row for row in parser.rows if len(row) == 3]
    if not rows or tuple(rows[0]) != LIST_HEADER:
        raise ValueError("CRAN by-date table not found (expected Date/Package/Title header)")
    for date_cell, package, title in rows[1:]:
        if not DATE_RE.fullmatch(date_cell):
            raise ValueError(f"CRAN table row has an unparsable date {date_cell!r}")
        if not package:
            raise ValueError(f"CRAN table row for {date_cell} has an empty package name")
        yield date_cell, package, title


def is_timeout_error(error):
    """Return whether *error* is a real timeout, including urllib wrappers."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        if isinstance(error, (TimeoutError, socket.timeout)):
            return True
        if isinstance(error, OSError) and error.errno == errno.ETIMEDOUT:
            return True
        if isinstance(error, urllib.error.URLError):
            error = error.reason
        else:
            return False
    return False


def fetch_text(url: str, timeout: int = 60) -> str:
    """GET *url* and return its text; retry timeouts and 5xx/429, bounded."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    last_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as error:
            if error.code not in RETRIABLE_STATUSES:
                raise
            last_error = error
        except Exception as error:  # URLError, socket errors, timeouts
            if not is_timeout_error(error):
                raise
            last_error = error
        if attempt < MAX_ATTEMPTS:
            time.sleep(RETRY_DELAYS[attempt - 1])
    raise TimeoutError(
        f"CRAN request for {url} failed after {MAX_ATTEMPTS} attempts: {last_error}"
    ) from last_error


def collect_updates(rows, watermark: str, boundary: frozenset, floor: str):
    """Yield (date, package, title) rows inside the run's window, in table order.

    ``watermark`` (last fully emitted date) and ``boundary`` (names already
    emitted at that date) bound the run when a watermark exists; ``floor`` (the
    cold-start date) bounds it otherwise. The table is newest-first, so the
    first row older than the minimum date to emit ends the scan. A package name
    appearing twice (case variants) is emitted once.
    """
    seen = set()
    for row_date, package, title in rows:
        if row_date < floor:
            return
        key = package.lower()
        if key in seen:
            continue
        if watermark and row_date == watermark and key in boundary:
            continue
        seen.add(key)
        yield row_date, package, title


def package_version(package: str, timeout: int = 60):
    """Return the current version of *package*, or None if it cannot be read."""
    try:
        html = fetch_text(PACKAGE_URL.format(name=package), timeout=timeout)
    except Exception:
        return None
    match = VERSION_RE.search(html)
    return match.group(1) if match else None


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermark lives: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / f"{SOURCE}.json"


def load_state(path: pathlib.Path) -> dict:
    """Return {"date": "YYYY-MM-DD", "boundary": [name, ...]}."""
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days-back", type=int, default=1,
                        help="cold-start window in days; ignored once a watermark exists")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermark (one-off pull)")
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()

    path = state_path(args.state_file)
    state = {} if args.no_state else load_state(path)
    watermark = state.get("date", "")
    boundary = frozenset(name.lower() for name in state.get("boundary", []))
    if watermark:
        floor = watermark
    else:
        floor = (datetime.now(timezone.utc).date() - timedelta(days=args.days_back)).isoformat()

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = list(parse_list_table(fetch_text(LIST_URL, timeout=args.timeout)))

    newest = watermark
    emitted_on_newest: set[str] = set(boundary) if watermark else set()
    records = 0
    version_failures = 0
    for row_date, package, title in collect_updates(rows, watermark, boundary, floor):
        version = package_version(package, timeout=args.timeout)
        if version is None:
            version_failures += 1
        if row_date > newest:
            newest, emitted_on_newest = row_date, set()
        if row_date == newest:
            emitted_on_newest.add(package.lower())
        print(json.dumps({
            "source": SOURCE,
            "fetched_at": fetched_at,
            "id": package.lower(),
            "package": package,
            "title": title,
            "version": version,
            "published": row_date,
        }, ensure_ascii=False))
        records += 1

    # Only after every record is written: an aborted run re-fetches instead of
    # skipping. An empty pull leaves the watermark untouched (nothing new).
    if not args.no_state and newest:
        save_state(path, {"date": newest, "boundary": sorted(emitted_on_newest)})
    if records:
        print(SUMMARY_PREFIX + json.dumps(
            {"records": records, "version_fetch_failures": version_failures},
            separators=(",", ":"),
        ), file=sys.stderr)


if __name__ == "__main__":
    main()
