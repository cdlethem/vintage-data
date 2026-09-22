"""Regression test for the job-boards run-summary size contract.

    python3 extract/scripts/test_job_boards_summary.py

The test imports the extractor from this directory and the run-summary protocol
helpers from ``orchestration/include`` so it exercises the same parsing and
validation path as the production runner.

The defect under test: ``fetch_catalog`` used to emit one entry per tenant
(succeeded or failed) into ``metrics.tenant_results``, unbounded by catalog
size. Once a provider's catalog grew large enough, the reserved
``VINTAGE_RUN_SUMMARY`` stderr line exceeded
``run_metadata.SUMMARY_MAX_BYTES`` (64 KiB), the orchestration layer rejected
it as malformed, and the whole run -- including postings that were fetched
successfully -- was thrown away. This test drives ``fetch_catalog`` with a
stub catalog well past the old overflow threshold and asserts the emitted
summary still validates and stays under the cap, that per-tenant failure
detail is still present (bounded), and that the aggregate counts match the
simulated outcome.

This is an offline serialization test only. It does not exercise the live
Workday or Greenhouse endpoints and must not be read as evidence that those
providers currently publish manifests; that is a separate, human-run
acceptance gate.
"""
from __future__ import annotations

import contextlib
import io
import json
import pathlib
import sys
import unittest
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent.parent / "orchestration" / "include"))

import fetch_job_boards as fjb
from job_boards_lib.common import Board
from run_metadata import SUMMARY_PREFIX, extract_summary_line, validate_summary

# Comfortably past the point where the old one-entry-per-tenant metrics list
# overflowed the 64 KiB summary cap (each entry was ~100+ bytes of tenant,
# provider, outcome, records, retry_count, final_status, error).
TENANT_COUNT = 2000
FAIL_EVERY = 10  # -> 200 failures, safely past MAX_PARTITION_FAILURES (100).


def _stub_boards(n: int) -> list[Board]:
    return [Board("stub", f"tenant-{i:05d}", f"Company {i}", "US", "Software") for i in range(n)]


def _stub_fetch_board(board: Board, max_per_board: int) -> list[dict]:
    index = int(board.token.rsplit("-", 1)[-1])
    if index % FAIL_EVERY == 0:
        raise RuntimeError(f"synthetic failure for {board.token}")
    return [{"tenant": board.token, "title": "Software Engineer"}]


def _run_catalog(boards: list[Board]) -> tuple[list[dict], str]:
    """Drain fetch_catalog and return (rows, captured_stderr)."""
    stderr = io.StringIO()
    with mock.patch.object(fjb, "fetch_board", _stub_fetch_board):
        with contextlib.redirect_stderr(stderr):
            rows = list(fjb.fetch_catalog(boards, workers=16, max_per_board=10))
    return rows, stderr.getvalue()


class SummarySizeTest(unittest.TestCase):
    def test_large_catalog_emits_a_bounded_validating_summary(self):
        boards = _stub_boards(TENANT_COUNT)
        expected_failed_tokens = [
            board.token
            for board in boards
            if int(board.token.rsplit("-", 1)[-1]) % FAIL_EVERY == 0
        ]
        expected_failed = len(expected_failed_tokens)
        expected_succeeded = TENANT_COUNT - expected_failed
        expected_rows = [
            {"tenant": board.token, "title": "Software Engineer"}
            for board in boards
            if int(board.token.rsplit("-", 1)[-1]) % FAIL_EVERY != 0
        ]

        rows, captured = _run_catalog(boards)

        payload_json, remaining = extract_summary_line(captured)
        self.assertIsNotNone(payload_json, "no reserved run-summary line was emitted")
        self.assertNotIn(SUMMARY_PREFIX, remaining, "reserved line must be stripped from remaining stderr")

        payload_bytes = len(payload_json.encode("utf-8"))
        self.assertLess(payload_bytes, 64 * 1024, "summary must stay under the 64 KiB protocol cap")

        summary = json.loads(payload_json)
        validated = validate_summary(summary)  # raises RunSummaryError on malformed input

        # The unbounded per-tenant list must be gone entirely.
        self.assertNotIn("tenant_results", validated.get("metrics", {}))

        # Aggregate counters replace it and must match the simulated outcome.
        metrics = validated["metrics"]
        self.assertEqual(metrics["tenants_attempted"], TENANT_COUNT)
        self.assertEqual(metrics["tenants_succeeded"], expected_succeeded)
        self.assertEqual(metrics["tenants_failed"], expected_failed)
        self.assertEqual(metrics["tenant_records_total"], expected_succeeded)

        # partitions.* mirrors the same counts and stays the source of truth.
        partitions = validated["partitions"]
        self.assertEqual(partitions["attempted"], TENANT_COUNT)
        self.assertEqual(partitions["succeeded"], expected_succeeded)
        self.assertEqual(partitions["failed"], expected_failed)

        # Bounded per-tenant failure detail survives -- capped, not dropped.
        self.assertEqual(len(partitions["failures"]), min(expected_failed, 100))
        for failure in partitions["failures"]:
            self.assertEqual(failure["outcome"], "failed")
            self.assertIn("error", failure)
        self.assertEqual(
            [failure["tenant"] for failure in partitions["failures"]],
            sorted(expected_failed_tokens)[:100],
        )

        # Every non-failing tenant's row was actually yielded.
        self.assertEqual(sorted(rows, key=lambda row: row["tenant"]), expected_rows)

    def test_summary_line_is_the_sole_reserved_line(self):
        boards = _stub_boards(50)
        _, captured = _run_catalog(boards)
        reserved = [line for line in captured.splitlines() if line.startswith(SUMMARY_PREFIX)]
        self.assertEqual(len(reserved), 1)


if __name__ == "__main__":
    unittest.main()
