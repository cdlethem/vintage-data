#!/usr/bin/env python3
"""Fetch a live-verified catalog of public company ATS job boards.

Airflow runs this once per provider: independent tasks, retries, timeout states,
sink manifests, and output partitions. Provider endpoints, pagination,
throttling, verification and record normalization are shared with the catalog
verification tool through ``job_boards_lib``.

The catalog is data, not executable code:
``extract/catalogs/job_boards.json``. Every entry was queried live and
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
from job_boards_lib.common import CLIENT


# The runner rejects summaries above 64 KiB of JSON payload. Keep the same
# limit here; failure details are optional, but aggregate counts are not.
SUMMARY_MAX_BYTES = 64 * 1024


def fetch_board(board: Board, max_per_board: int = 10000) -> list[dict]:
    """Fetch one board atomically; a paging failure emits no partial board."""
    return list(fetch(board, max_per_board))


def _fetch_board_with_stats(board: Board, max_per_board: int) -> list[dict]:
    CLIENT.begin_observation()
    try:
        rows = fetch_board(board, max_per_board)
    except Exception as exc:
        stats = CLIENT.request_stats()
        status = getattr(exc, "code", None)
        stats.update({"status": status, "error": f"{type(exc).__name__}: {exc}"[:500]})
        raise _TenantFailure(stats) from exc
    return rows


class _TenantFailure(RuntimeError):
    def __init__(self, stats: dict):
        self.stats = stats
        super().__init__(stats["error"])


def fetch_catalog(boards: list[Board], workers: int = 12, max_per_board: int = 10000):
    """Fetch tenants independently; retain every success exactly once.

    Workday's shared ``HttpClient`` policy supplies 4-way concurrency, 150 ms
    pacing, bounded Retry-After, and three attempts. A failed tenant makes the
    successful run partial; only an all/near-all provider failure is hard.
    """
    succeeded = failed = records = 0
    failures: list[dict] = []
    workers = min(workers, 4) if boards and all(b.provider == "workday" for b in boards) else workers
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {pool.submit(_fetch_board_with_stats, board, max_per_board): board for board in boards}
        for future in as_completed(futures):
            board = futures[future]
            tenant = {"tenant": board.token, "provider": board.provider}
            try:
                rows = future.result()
            except _TenantFailure as exc:
                failed += 1
                stats = exc.stats
                cause = exc.__cause__
                tenant.update({"outcome": "failed", "records": 0, "retry_count": stats["retries"],
                               "final_status": stats["status"], "error": stats["error"]})
                kind = "retired" if cause is not None and is_permanent_miss(cause) else "error"
                print(f"job_boards: skipping {board.provider}/{board.token}: {kind}: {stats['error']}",
                      file=sys.stderr)
                failures.append(tenant)
            else:
                succeeded += 1
                records += len(rows)
                yield from rows

    total = succeeded + failed
    broad_failure = total == 0 or failed == total or (failed >= 3 and failed / total >= 0.8)
    payload = {
        "health": "failed" if broad_failure else ("degraded" if failed else "healthy"),
        "completeness": "failed" if broad_failure else ("partial" if failed else "complete"),
        "records": records,
        "partitions": {"attempted": total, "succeeded": succeeded, "failed": failed,
                       "failures": []},
        "metrics": {"tenants_attempted": total, "tenants_succeeded": succeeded,
                    "tenants_failed": failed, "tenant_records_total": records},
    }
    failures_out = payload["partitions"]["failures"]
    # Measure with the exact JSON encoding used on stderr (including escaping).
    # Long or non-ASCII tokens/errors can exceed the cap even with only 100
    # entries. Retain the lexicographically earliest details that fit.
    budget = SUMMARY_MAX_BYTES - len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    for failure in sorted(failures, key=lambda t: t["tenant"])[:100]:
        size = len(json.dumps(failure, separators=(",", ":")).encode("utf-8"))
        size += bool(failures_out)  # comma between entries
        if size > budget:
            continue
        failures_out.append(failure)
        budget -= size
    print("VINTAGE_RUN_SUMMARY\t" + json.dumps(payload, separators=(",", ":")), file=sys.stderr)
    if broad_failure:
        raise RuntimeError(f"provider-wide failure: {failed}/{total} tenants failed")

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
