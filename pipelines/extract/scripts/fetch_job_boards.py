#!/usr/bin/env python3
"""Fetch a live-verified catalog of public company ATS job boards.

Airflow runs this once per provider: independent tasks, retries, timeout states,
sink manifests, and output partitions. Provider endpoints, pagination,
throttling, verification and record normalization are shared with the catalog
verification tool through ``job_boards_lib``.

The catalog is data, not executable code:
``pipelines/extract/catalogs/job_boards.json``. Every entry was queried live and
returned at least one open posting when admitted. Use
``tools/verify_job_boards.py`` before adding or refreshing entries.

Usage:
    fetch_job_boards.py --providers greenhouse
    fetch_job_boards.py --providers greenhouse,lever   # useful outside Airflow
    fetch_job_boards.py --board ashby openai            # ad-hoc smoke test

Stdlib only.
"""
from __future__ import annotations

import argparse
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from job_boards_lib import Board, FETCHERS, fetch, is_permanent_miss
from job_boards_lib.catalog import load_catalog


def fetch_board(board: Board, max_per_board: int = 10000) -> list[dict]:
    """Fetch one board atomically; a paging failure emits no partial board."""
    return list(fetch(board, max_per_board))


def fetch_catalog(boards: list[Board], workers: int = 12, max_per_board: int = 10000):
    """Fetch boards concurrently and tolerate isolated stale company tokens.

    An isolated company can migrate ATSs without making a provider unavailable.
    Conversely, a task must fail when a provider-wide outage affects nearly the
    whole catalog; otherwise Airflow would record a false success and no retry.
    """
    succeeded = failed = records = 0
    errors: list[str] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(fetch_board, board, max_per_board): board for board in boards
        }
        for future in as_completed(futures):
            board = futures[future]
            try:
                rows = future.result()
            except Exception as exc:  # keep unrelated company boards running
                failed += 1
                kind = "retired" if is_permanent_miss(exc) else "error"
                message = f"{board.provider}/{board.token}: {kind}: {exc!r}"
                errors.append(message)
                print(f"job_boards: skipping {message}", file=sys.stderr)
                continue
            succeeded += 1
            records += len(rows)
            yield from rows

    total = succeeded + failed
    print(
        f"job_boards: boards={total} succeeded={succeeded} failed={failed} records={records}",
        file=sys.stderr,
    )
    # All-failed is always a provider outage, even for a tiny provider catalog.
    # For larger catalogs, 80% across at least three boards separates normal
    # token churn from a broad upstream or response-contract failure.
    if failed == total or (failed >= 3 and failed / total >= 0.8):
        raise RuntimeError(
            f"provider-wide failure: {failed}/{total} boards failed; first errors: "
            + "; ".join(errors[:3])
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--providers", help="comma-separated provider subset")
    parser.add_argument("--max-per-board", type=int, default=10000)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--board", nargs=2, metavar=("PROVIDER", "TOKEN"))
    args = parser.parse_args()

    if args.max_per_board < 1:
        parser.error("--max-per-board must be positive")
    if args.workers < 1:
        parser.error("--workers must be positive")

    if args.board:
        provider, token = args.board
        if provider not in FETCHERS:
            parser.error(f"unknown provider {provider!r}; choose from {', '.join(FETCHERS)}")
        boards = [Board(provider, token, token, "US", "Software")]
    else:
        wanted = None
        if args.providers:
            wanted = {part.strip() for part in args.providers.split(",") if part.strip()}
            unknown = wanted - set(FETCHERS)
            if unknown:
                parser.error(f"unknown provider(s): {', '.join(sorted(unknown))}")
        boards = load_catalog(providers=wanted)
        if not boards:
            print("job_boards: catalog selection is empty", file=sys.stderr)
            return 0

    for row in fetch_catalog(boards, workers=args.workers, max_per_board=args.max_per_board):
        print(json.dumps(row, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
