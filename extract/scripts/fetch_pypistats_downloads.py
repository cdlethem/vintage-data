#!/usr/bin/env python3
"""pypistats.org — daily PyPI download counts for a curated set of top packages.

For each package in ``DEFAULT_PACKAGES`` this fetches the ``/overall`` time
series (daily downloads, with and without mirror downloads) from the
pypistats.org JSON API and emits the rows newer than the stored per-package
watermark as NDJSON.

Data provenance: the numbers are the Python Software Foundation's publicly
available PyPI download stats (Google BigQuery), served by the PSF-hosted
pypistats.org. The API docs say all download data is updated once daily and
180 days of history are retained; verified against the live API, the newest
row in the series is the previous UTC day (as of 2026-11-22 05:40 UTC the
latest row was 2026-11-21) and the series reaches back exactly 180 days.
The API ignores start_date/end_date (a one-day range request returned the
full retained window), so rows are filtered to the wanted window here.

Etiquette: the API applies IP-based rate limiting and asks regular fetchers
to cache results and call each endpoint at most once per day. This fetcher
makes one bounded request per package per day, spaced 2 seconds apart, and
retries 429/5xx a few times with growing delay.

State: ``{"version": 1, "last_date": {package: "YYYY-MM-DD"}}`` under
``$EXTRACT_DATA_ROOT/state/pypistats_downloads.json``. The watermark advances
per package, after every record is written, so a package that failed this run
is backfilled from its own next run (up to the 180-day retention window).

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
from datetime import date, datetime, timedelta, timezone

SOURCE = "pypistats_downloads"
BASE = "https://pypistats.org/api/packages"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or (
    "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"
)
ATTRIBUTION = "pypistats.org (PyPI download stats hosted by the Python Software Foundation)"
DEFAULT_DATA_ROOT = "~/.local/share/vintage-data/extract"
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
CATEGORIES = ("with_mirrors", "without_mirrors")
# 180 days of history are retained upstream: a live pull returned 181 rows
# spanning exactly 180 days (oldest = newest - 180d).
RETENTION_DAYS = 180
DEFAULT_TIMEOUT = 30
MAX_TIMEOUT = 120
DEFAULT_RETRIES = 3
MAX_RETRIES = 5
DEFAULT_RETRY_DELAY = 5
MAX_RETRY_DELAY = 300
REQUEST_GAP_SECONDS = 2.0

# Curated snapshot of the most-downloaded packages on PyPI. pypistats.org
# exposes no ranking endpoint, so this list is fixed in code; refresh entries
# deliberately if the ecosystem shifts. Names must match PyPI exactly as
# pypistats stores them (lowercase normalized names: "jinja2", "pyyaml", ...).
DEFAULT_PACKAGES = (
    "requests",
    "flask",
    "django",
    "numpy",
    "pandas",
    "scipy",
    "scikit-learn",
    "matplotlib",
    "pip",
    "setuptools",
    "wheel",
    "cryptography",
    "boto3",
    "botocore",
    "pydantic",
    "pydantic-core",
    "typing-extensions",
    "urllib3",
    "certifi",
    "charset-normalizer",
    "idna",
    "click",
    "jinja2",
    "itsdangerous",
    "werkzeug",
    "sqlalchemy",
    "httpx",
    "anyio",
    "sniffio",
    "starlette",
    "uvicorn",
    "gunicorn",
    "fastapi",
    "tensorflow",
    "keras",
    "torch",
    "torchvision",
    "transformers",
    "tokenizers",
    "safetensors",
    "nltk",
    "spacy",
    "lxml",
    "psutil",
    "pillow",
    "openpyxl",
    "paramiko",
    "pyyaml",
    "pyjwt",
    "python-dotenv",
)


def state_path(explicit: str | None = None) -> pathlib.Path:
    """Where the watermarks live: outside the repo, beside the raw data."""
    if explicit:
        return pathlib.Path(explicit).expanduser()
    root = os.environ.get("EXTRACT_DATA_ROOT") or DEFAULT_DATA_ROOT
    return pathlib.Path(root).expanduser() / "state" / "pypistats_downloads.json"


def load_state(path: pathlib.Path) -> dict:
    if not path.is_file():
        return {}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        print(f"{SOURCE}: state file {path} unreadable; re-baselining", file=sys.stderr)
        return {}
    return state if isinstance(state, dict) else {}


def save_state(path: pathlib.Path, state: dict) -> None:
    """Publish the watermarks atomically so a crash can't leave them half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_suffix(path.suffix + ".tmp")
    staged.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(staged, path)


def validate_overall(document, package: str) -> list[dict]:
    """Validate one /overall response and return its raw data rows."""
    if not isinstance(document, dict):
        raise ValueError("pypistats response is not an object")
    if document.get("package") != package:
        raise ValueError(f"pypistats response package {document.get('package')!r} != {package!r}")
    if document.get("type") != "overall_downloads":
        raise ValueError(f"pypistats response type {document.get('type')!r} is not overall_downloads")
    rows = document.get("data")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"pypistats response for {package} has no data rows")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"pypistats row for {package} is not an object")
        if row.get("category") not in CATEGORIES:
            raise ValueError(f"pypistats row for {package} has invalid category {row.get('category')!r}")
        if not isinstance(row.get("date"), str):
            raise ValueError(f"pypistats row for {package} is malformed: {row!r}")
        try:
            date.fromisoformat(row["date"])
        except ValueError:
            raise ValueError(f"pypistats row for {package} has unparseable date {row['date']!r}")
        if not isinstance(row.get("downloads"), int) \
                or isinstance(row.get("downloads"), bool) or row["downloads"] < 0:
            raise ValueError(f"pypistats row for {package} is malformed: {row!r}")
    return rows


def fetch_overall(package: str, start_date: date, end_date: date,
                  timeout: int, retries: int, retry_delay: int) -> list[dict]:
    """Fetch one package's series, retrying only transient HTTP failures.

    The date parameters are sent for documentation/future-proofing, but the
    live API ignores them and returns the full retained window, so the
    caller filters rows to its own window.
    """
    params = urllib.parse.urlencode({
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    })
    request = urllib.request.Request(
        f"{BASE}/{urllib.parse.quote(package, safe='-/_.')}/overall?{params}",
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    delay = retry_delay
    for attempt in range(1, retries + 2):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                document = json.load(response)
            return validate_overall(document, package)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRIABLE_STATUSES or attempt > retries:
                raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            if attempt > retries:
                raise
        time.sleep(delay)
        delay = min(delay * 3, MAX_RETRY_DELAY)


def run(packages: tuple[str, ...], state: dict, state_file: pathlib.Path | None,
        end_date: date, timeout: int, retries: int, retry_delay: int) -> int:
    """Emit new rows per package; return a process exit code."""
    last_dates = state.get("last_date")
    if not isinstance(last_dates, dict):
        last_dates = {}
    fetched_at = datetime.now(timezone.utc).isoformat()
    oldest_floor = end_date - timedelta(days=RETENTION_DAYS)

    new_last_dates = dict(last_dates)
    rows_emitted = 0
    packages_fetched = 0
    packages_skipped = 0
    failed: list[str] = []
    requests_made = 0
    seen_dates: set[str] = set()

    for index, package in enumerate(packages):
        if index:
            time.sleep(REQUEST_GAP_SECONDS)
        watermark = last_dates.get(package)
        if not isinstance(watermark, str):
            # No watermark: baseline on the most recent completed day only.
            start_date = end_date
        else:
            try:
                watermark_date = date.fromisoformat(watermark)
            except ValueError:
                print(f"{SOURCE}: {package}: unreadable watermark {watermark!r}; re-baselining",
                      file=sys.stderr)
                watermark_date = end_date
            start_date = max(watermark_date + timedelta(days=1), oldest_floor)
        if start_date > end_date:
            packages_skipped += 1
            continue

        try:
            rows = fetch_overall(package, start_date, end_date, timeout, retries, retry_delay)
        except Exception as exc:  # noqa: BLE001 - one package must not sink the run
            failed.append(package)
            print(f"{SOURCE}: {package}: FAILED {exc}", file=sys.stderr)
            continue
        requests_made += 1

        package_rows = [row for row in rows if start_date <= date.fromisoformat(row["date"]) <= end_date]
        if not package_rows:
            # The full window came back but nothing fell in [start, end]:
            # the series is stale relative to the watermark (upstream gap or
            # date labels changed). Do not advance the watermark; the next
            # run retries, and a persistent condition makes the run fail.
            print(f"{SOURCE}: {package}: no rows in window {start_date}..{end_date}",
                  file=sys.stderr)
            continue
        for row in package_rows:
            rows_emitted += 1
            seen_dates.add(row["date"])
            print(json.dumps({
                "source": SOURCE,
                "id": f"{package}-{row['date']}-{row['category']}",
                "fetched_at": fetched_at,
                "package": package,
                "date": row["date"],
                "category": row["category"],
                "downloads": row["downloads"],
                "attribution": ATTRIBUTION,
                "raw": row,
            }, ensure_ascii=False))
        newest = max(date.fromisoformat(row["date"]) for row in package_rows)
        new_last_dates[package] = newest.isoformat()
        packages_fetched += 1

    summary = {
        "packages": len(packages),
        "fetched": packages_fetched,
        "skipped": packages_skipped,
        "failed": len(failed),
        "failed_packages": failed,
        "requests": requests_made,
        "rows": rows_emitted,
        "oldest_date": min(seen_dates) if seen_dates else None,
        "newest_date": max(seen_dates) if seen_dates else None,
    }

    # Only after every record is written: an aborted run re-fetches instead of
    # skipping. Failed packages keep their old watermark so the next run
    # backfills their gap from the 180-day window.
    if state_file is not None and (packages_fetched or packages_skipped):
        save_state(state_file, {"version": 1, "last_date": new_last_dates})
    print(SUMMARY_PREFIX + json.dumps(summary, separators=(",", ":")), file=sys.stderr)
    if not packages_fetched and not packages_skipped and packages:
        print(f"{SOURCE}: all {len(packages)} packages failed", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packages", help="comma-separated package list (default: curated top-50)")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--retries", type=int, default=DEFAULT_RETRIES)
    parser.add_argument("--retry-delay", type=int, default=DEFAULT_RETRY_DELAY,
                        help="initial retry delay in seconds; grows x3 per attempt")
    parser.add_argument("--state-file", help="watermark file; defaults under $EXTRACT_DATA_ROOT/state")
    parser.add_argument("--no-state", action="store_true",
                        help="ignore and do not write the watermarks (one-off pull)")
    args = parser.parse_args(argv)

    if not 1 <= args.timeout <= MAX_TIMEOUT:
        parser.error(f"--timeout must be between 1 and {MAX_TIMEOUT}")
    if not 0 <= args.retries <= MAX_RETRIES:
        parser.error(f"--retries must be between 0 and {MAX_RETRIES}")
    if not 1 <= args.retry_delay <= MAX_RETRY_DELAY:
        parser.error(f"--retry-delay must be between 1 and {MAX_RETRY_DELAY}")

    if args.packages:
        packages = tuple(p.strip() for p in args.packages.split(",") if p.strip())
        if not packages:
            parser.error("--packages is empty")
    else:
        packages = DEFAULT_PACKAGES

    state_file = None if args.no_state else state_path(args.state_file)
    state = {} if args.no_state else load_state(state_file)
    end_date = datetime.now(timezone.utc).date() - timedelta(days=1)
    sys.exit(run(packages, state, state_file, end_date, args.timeout, args.retries, args.retry_delay))


if __name__ == "__main__":
    main()
