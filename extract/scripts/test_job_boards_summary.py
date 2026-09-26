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
successfully -- was thrown away. The existing aggregate-counter repair removed
the unbounded success list, but 100 retained failures with long or escaped
details can still overflow the line. These offline cases verify both paths:
successful rows survive, failure counts remain authoritative, and retained
failure details fit the runner's protocol limit.

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
from run_metadata import SUMMARY_MAX_BYTES, SUMMARY_PREFIX, extract_summary_line, validate_summary

# Comfortably past the point where the old one-entry-per-tenant metrics list
# overflowed the 64 KiB summary cap (each entry was ~100+ bytes of tenant,
# provider, outcome, records, retry_count, final_status, error).
TENANT_COUNT = 2000
FAIL_EVERY = 10  # -> 200 failures, safely past MAX_PARTITION_FAILURES (100).


def _long_error_fetch(board: Board, max_per_board: int) -> list[dict]:
    index = int(board.token.rsplit("-", 1)[-1])
    if index % FAIL_EVERY == 0:
        raise RuntimeError("synthetic failure " + "雪" * 600)
    return [{"tenant": board.token, "title": "Software Engineer"}]


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

    def test_long_failure_details_do_not_discard_workday_or_greenhouse_rows(self):
        for provider in ("workday", "greenhouse"):
            with self.subTest(provider=provider):
                boards = [Board(provider, board.token, board.company, board.country, board.industry)
                          for board in _stub_boards(TENANT_COUNT)]
                stderr = io.StringIO()
                with mock.patch.object(fjb, "fetch_board", _long_error_fetch):
                    with contextlib.redirect_stderr(stderr):
                        rows = list(fjb.fetch_catalog(boards, workers=16, max_per_board=10))

                payload_json, remaining = extract_summary_line(stderr.getvalue())
                self.assertIsNotNone(payload_json)
                self.assertLessEqual(len(payload_json.encode("utf-8")), SUMMARY_MAX_BYTES)
                summary = validate_summary(json.loads(payload_json))
                self.assertEqual(summary["health"], "degraded")
                self.assertEqual(summary["completeness"], "partial")
                self.assertEqual(summary["partitions"]["failed"], TENANT_COUNT // FAIL_EVERY)
                self.assertEqual(summary["partitions"]["succeeded"], len(rows))
                self.assertEqual(summary["metrics"]["tenant_records_total"], len(rows))
                self.assertEqual(
                    {row["tenant"] for row in rows},
                    {board.token for board in boards if int(board.token.rsplit("-", 1)[-1]) % FAIL_EVERY},
                )
                failures = summary["partitions"]["failures"]
                self.assertGreater(len(failures), 0)
                self.assertLess(len(failures), 100)
                self.assertEqual(
                    [failure["tenant"] for failure in failures],
                    [f"tenant-{index:05d}" for index in range(0, len(failures) * FAIL_EVERY, FAIL_EVERY)],
                )
                self.assertTrue(all(failure["error"].startswith("RuntimeError: synthetic failure")
                                    for failure in failures))
                self.assertEqual(remaining.count("job_boards: skipping " + provider + "/"),
                                 TENANT_COUNT // FAIL_EVERY)

    def test_provider_wide_failure_still_reports_failed_summary(self):
        boards = [Board("greenhouse", "tenant-00000", "Company", "US", "Software")]
        stderr = io.StringIO()
        with mock.patch.object(fjb, "fetch_board", _long_error_fetch):
            with contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(RuntimeError, "provider-wide failure: 1/1"):
                    list(fjb.fetch_catalog(boards))
        payload_json, remaining = extract_summary_line(stderr.getvalue())
        summary = validate_summary(json.loads(payload_json))
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["partitions"]["failed"], 1)
        self.assertEqual(summary["partitions"]["succeeded"], 0)
        self.assertEqual(summary["partitions"]["failures"][0]["tenant"], "tenant-00000")
        self.assertIn("skipping greenhouse/tenant-00000", remaining)

    def test_oversized_failure_token_does_not_hide_later_failure(self):
        boards = [Board("greenhouse", token, "Company", "US", "Software")
                  for token in ("a" + "雪" * 12000, "z-short", "success")]

        def fetch_one(board: Board, max_per_board: int) -> list[dict]:
            if board.token == "success":
                return [{"tenant": board.token}]
            raise RuntimeError("synthetic failure")

        stderr = io.StringIO()
        with mock.patch.object(fjb, "fetch_board", fetch_one):
            with contextlib.redirect_stderr(stderr):
                rows = list(fjb.fetch_catalog(boards))
        payload_json, remaining = extract_summary_line(stderr.getvalue())
        self.assertLessEqual(len(payload_json.encode("utf-8")), SUMMARY_MAX_BYTES)
        summary = validate_summary(json.loads(payload_json))
        self.assertEqual(rows, [{"tenant": "success"}])
        self.assertEqual(summary["partitions"]["failed"], 2)
        self.assertEqual(summary["partitions"]["succeeded"], 1)
        self.assertEqual([failure["tenant"] for failure in summary["partitions"]["failures"]],
                         ["z-short"])
        self.assertEqual(remaining.count("job_boards: skipping greenhouse/"), 2)

    def test_summary_line_is_the_sole_reserved_line(self):
        boards = _stub_boards(50)
        _, captured = _run_catalog(boards)
        reserved = [line for line in captured.splitlines() if line.startswith(SUMMARY_PREFIX)]
        self.assertEqual(len(reserved), 1)


if __name__ == "__main__":
    unittest.main()
