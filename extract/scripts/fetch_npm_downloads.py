#!/usr/bin/env python3
"""npm download statistics — daily download counts for popular npm packages, keyless.

The npm registry (operated by GitHub) publishes per-package download counts
at https://api.npmjs.org/downloads. No key is required; the data updates
once per calendar day, with a lag of about one day.

Each run makes ONE bulk range request (npm allows up to 128 unscoped
packages per request) for the last WINDOW_DAYS completed UTC days covering
a fixed list of well-known packages, and emits one record per
(package, day):

  {"source": "npm_downloads", "fetched_at": "...", "id": "react:2026-11-07",
   "package": "react", "day": "2026-11-07", "downloads": 42621351}

The three-day window means every calendar day is captured by three
consecutive runs, so a late data publication or a single lost fetch is
recovered by the next run. RAW is insert-only; cross-run duplication is
expected.

A package present in the list but null in the response means the list is
stale (npm omits unknown packages without an error status), so the run
fails loudly rather than silently dropping it.

Verified live 2026-11-09 05:4x UTC: one bulk request returned 3 full days
for all 44 packages in ~0.3 s.

Stdlib only.
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

SOURCE = "npm_downloads"
BASE = "https://api.npmjs.org/downloads"
WINDOW_DAYS = 3
MAX_ATTEMPTS = 3
RETRY_DELAYS = (1.0, 2.0)
RETRIABLE_STATUS = frozenset({429, 500, 502, 503, 504})
SUMMARY_PREFIX = "VINTAGE_RUN_SUMMARY\t"
USER_AGENT = os.environ.get("EXTRACT_USER_AGENT") or "vintage-data/0.1 (+https://github.com/cdlethem/vintage-data)"

PACKAGES = (
    "react", "react-dom", "vue", "svelte", "preact", "solid-js",
    "jquery", "backbone", "next", "nuxt", "astro",
    "vite", "webpack", "esbuild", "rollup", "typescript", "tailwindcss",
    "redux", "zustand", "mobx",
    "lodash", "underscore", "moment", "dayjs", "date-fns",
    "chalk", "commander", "semver",
    "axios", "undici", "got", "node-fetch",
    "jest", "mocha", "vitest", "cypress", "puppeteer", "playwright",
    "mongoose", "sequelize", "prisma",
    "react-router", "bootstrap", "graphql",
)


def window(today) -> tuple[str, str]:
    """Return (start, end) ISO dates of the last completed UTC days."""
    return (today - timedelta(days=WINDOW_DAYS)).isoformat(), (today - timedelta(days=1)).isoformat()


def build_url(packages, start: str, end: str) -> str:
    return f"{BASE}/range/{start}:{end}/" + urllib.parse.quote(",".join(packages), safe=",-.")


def get_document(url: str, timeout: float) -> dict:
    """Fetch one JSON document, retrying transient failures on a fixed budget."""
    for attempt in range(MAX_ATTEMPTS):
        if attempt:
            time.sleep(RETRY_DELAYS[attempt - 1])
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRIABLE_STATUS or attempt == MAX_ATTEMPTS - 1:
                raise
        except (TimeoutError, urllib.error.URLError):
            if attempt == MAX_ATTEMPTS - 1:
                raise
    raise AssertionError("unreachable")


def parse_document(document, packages) -> list[tuple[str, str, int]]:
    """Validate the bulk response and return (package, day, downloads) rows."""
    if not isinstance(document, dict):
        raise ValueError("npm downloads response is not a JSON object")
    rows = []
    for package in packages:
        entry = document.get(package)
        if entry is None:
            raise ValueError(f"package {package!r} missing from response; stale package list")
        if not isinstance(entry, dict) or not isinstance(entry.get("downloads"), list):
            raise ValueError(f"malformed downloads entry for package {package!r}")
        for row in entry["downloads"]:
            if (
                not isinstance(row, dict)
                or not isinstance(row.get("day"), str)
                or not isinstance(row.get("downloads"), int)
                or isinstance(row.get("downloads"), bool)
                or row["downloads"] < 0
            ):
                raise ValueError(f"malformed downloads row for package {package!r}: {row!r}")
            rows.append((package, row["day"], row["downloads"]))
    return rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="npm download statistics, keyless")
    parser.add_argument("--timeout", type=float, default=30.0, help="per-request timeout in seconds")
    args = parser.parse_args(argv)

    fetched_at = datetime.now(timezone.utc)
    start, end = window(fetched_at.date())
    url = build_url(PACKAGES, start, end)
    records = 0
    try:
        document = get_document(url, args.timeout)
        rows = parse_document(document, PACKAGES)
    except Exception as exc:
        print(
            SUMMARY_PREFIX
            + json.dumps(
                {
                    "health": "failed",
                    "completeness": "failed",
                    "records": records,
                    "window": [start, end],
                    "packages": len(PACKAGES),
                    "error": str(exc)[:500],
                },
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1
    for package, day, downloads in sorted(rows, key=lambda row: (row[0], row[1])):
        print(
            json.dumps(
                {
                    "source": SOURCE,
                    "fetched_at": fetched_at.isoformat(),
                    "id": f"{package}:{day}",
                    "package": package,
                    "day": day,
                    "downloads": downloads,
                },
                ensure_ascii=False,
            )
        )
        records += 1
    print(
        SUMMARY_PREFIX
        + json.dumps(
            {
                "health": "healthy",
                "completeness": "complete",
                "records": records,
                "window": [start, end],
                "packages": len(PACKAGES),
                "requests": {"attempted": 1},
            },
            separators=(",", ":"),
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
