import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock


SCRIPT = Path(__file__).with_name("fetch_job_boards.py")
SPEC = importlib.util.spec_from_file_location("fetch_job_boards", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

RUN_METADATA_SCRIPT = Path(__file__).resolve().parents[2] / "orchestration" / "include" / "run_metadata.py"
RUN_METADATA_SPEC = importlib.util.spec_from_file_location("run_metadata", RUN_METADATA_SCRIPT)
RUN_METADATA = importlib.util.module_from_spec(RUN_METADATA_SPEC)
assert RUN_METADATA_SPEC.loader is not None
RUN_METADATA_SPEC.loader.exec_module(RUN_METADATA)


class FetchJobBoardsSummaryTests(unittest.TestCase):
    def test_high_cardinality_workday_outcomes_use_bounded_aggregate_summary(self):
        attempted = 600
        failed = 120
        boards = [
            MODULE.Board("workday", f"tenant-{index:04d}|wd5|Careers", f"Company {index}", "US", "Software")
            for index in range(attempted)
        ]

        def fetch_board(board, _limit):
            index = int(board.token.split("-", 1)[1].split("|", 1)[0])
            if index < failed:
                raise OSError("synthetic tenant failure " + "x" * 1_000)
            return [{"id": f"job-{index}"}]

        stderr = io.StringIO()
        with mock.patch.object(MODULE, "fetch_board", side_effect=fetch_board), \
             contextlib.redirect_stderr(stderr):
            rows = list(MODULE.fetch_catalog(boards, workers=32))

        lines = stderr.getvalue().splitlines()
        summaries = [line for line in lines if line.startswith(MODULE.SUMMARY_PREFIX)]
        self.assertEqual(len(summaries), 1)
        self.assertLess(len((summaries[0] + "\n").encode()), MODULE.MAX_SUMMARY_BYTES)
        summary_json, ordinary_stderr = RUN_METADATA.extract_summary_line(stderr.getvalue())
        self.assertIsNotNone(summary_json)
        summary = RUN_METADATA.validate_summary(json.loads(summary_json))
        diagnostics = ordinary_stderr.splitlines()

        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["metrics"]["records"], attempted - failed)
        self.assertEqual(summary["partitions"]["attempted"], attempted)
        self.assertEqual(summary["partitions"]["succeeded"], attempted - failed)
        self.assertEqual(summary["partitions"]["failed"], failed)
        self.assertEqual(len(summary["partitions"]["failures"]), MODULE.MAX_FAILURE_SAMPLES)
        self.assertEqual(
            [item["tenant"] for item in summary["partitions"]["failures"]],
            [f"tenant-{index:04d}|wd5|Careers" for index in range(MODULE.MAX_FAILURE_SAMPLES)],
        )
        self.assertTrue(all(item["provider"] == "workday"
                            and item["retry_count"] == 0
                            and item["final_status"] is None
                            and 0 < len(item["error"]) <= 128
                            for item in summary["partitions"]["failures"]))
        self.assertEqual(summary["metrics"]["failure_sample_count"], MODULE.MAX_FAILURE_SAMPLES)
        self.assertEqual(summary["metrics"]["failures_omitted"], failed - MODULE.MAX_FAILURE_SAMPLES)
        self.assertEqual(summary["metrics"]["retained_records"], attempted - failed)
        self.assertEqual(summary["metrics"]["failure_threshold"], {
            "minimum_failed": MODULE.FAILURE_THRESHOLD_COUNT,
            "minimum_ratio": MODULE.FAILURE_THRESHOLD_RATIO,
            "failed_count": failed,
            "attempted_count": attempted,
            "count_met": True,
            "ratio_met": False,
            "triggered": False,
        })
        self.assertNotIn("tenant_results", summary["metrics"])
        self.assertEqual(len(rows), attempted - failed)

        outcomes = [json.loads(line) for line in diagnostics]
        self.assertEqual(sum(item["outcome"] == "failed" for item in outcomes), failed)
        self.assertEqual(sum(item["outcome"] == "succeeded" for item in outcomes), attempted - failed)
        self.assertTrue(all("tenant" in item and "records" in item and "retry_count" in item
                            and "final_status" in item and "error" in item for item in outcomes))
        self.assertTrue(all(item["event"] == "job_board_tenant_result" for item in outcomes))


if __name__ == "__main__":
    unittest.main()