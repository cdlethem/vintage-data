"""Offline behavioral tests for the US Treasury yield curve extractor."""
import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_us_treasury_yield_curve.py"
SPEC = importlib.util.spec_from_file_location("fetch_us_treasury_yield_curve", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

HEADER = "Date," + ",".join(f'"{name}"' for name in MODULE.TENOR_COLUMNS)
TENOR_COUNT = len(MODULE.TENOR_COLUMNS)


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 9, 21, 15, 0, tzinfo=timezone.utc)


def csv_row(date="10/08/2026", values=None):
    values = values if values is not None else ["4.14"] * TENOR_COUNT
    return f"{date}," + ",".join(values)


def csv_body(*rows):
    return (HEADER + "\n" + "\n".join(rows) + "\n").encode("utf-8")


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def run_extractor(rows, *args):
    """Run main() against a canned CSV; return (exit code, records, summary)."""
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=Response(csv_body(*rows))
    ) as urlopen:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = MODULE.main(list(args))
    records = [json.loads(line) for line in stdout.getvalue().splitlines()]
    summary_lines = [line for line in stderr.getvalue().splitlines()
                     if line.startswith(MODULE.SUMMARY_PREFIX)]
    summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):])
    return result, records, summary, urlopen


class FetchUsTreasuryYieldCurveTests(unittest.TestCase):
    def write_state(self, directory, last_date):
        state = pathlib.Path(directory) / "state.json"
        state.write_text(json.dumps({"version": MODULE.STATE_VERSION, "last_date": last_date}) + "\n",
                         encoding="utf-8")
        return state

    def test_first_run_emits_full_year_in_date_order_and_writes_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, records, summary, _ = run_extractor(
                [csv_row("10/08/2026"), csv_row("10/06/2026"), csv_row("10/01/2026")],
                "--state-file", str(state),
            )
            self.assertEqual(result, 0)
            self.assertEqual([r["date"] for r in records], ["2026-10-01", "2026-10-06", "2026-10-08"])
            first = records[0]
            self.assertEqual(first["id"], "2026-10-01")
            self.assertEqual(first["source"], MODULE.SOURCE)
            self.assertEqual(first["mo1"], 4.14)
            self.assertEqual(first["yr10"], 4.14)
            self.assertEqual(summary["health"], "healthy")
            self.assertEqual(summary["records"], 3)
            self.assertEqual(summary["coverage"], {"year": 2026, "last_date_before": None, "last_date_after": "2026-10-08"})
            self.assertEqual(json.loads(state.read_text())["last_date"], "2026-10-08")

    def test_watermark_emits_only_rows_after_last_date(self):
        with tempfile.TemporaryDirectory() as directory:
            state = self.write_state(directory, "2026-10-02")
            result, records, summary, _ = run_extractor(
                [csv_row("10/01/2026"), csv_row("10/02/2026"), csv_row("10/05/2026"), csv_row("10/08/2026")],
                "--state-file", str(state),
            )
            self.assertEqual(result, 0)
            self.assertEqual([r["date"] for r in records], ["2026-10-05", "2026-10-08"])
            self.assertEqual(summary["coverage"]["last_date_before"], "2026-10-02")
            self.assertEqual(json.loads(state.read_text())["last_date"], "2026-10-08")

    def test_no_new_rows_reports_zero_and_leaves_state_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            state = self.write_state(directory, "2026-10-08")
            before = state.read_text()
            result, records, summary, _ = run_extractor(
                [csv_row("10/07/2026"), csv_row("10/08/2026")],
                "--state-file", str(state),
            )
            self.assertEqual(result, 0)
            self.assertEqual(records, [])
            self.assertEqual(summary["health"], "healthy")
            self.assertEqual(summary["records"], 0)
            self.assertEqual(summary["coverage"]["last_date_after"], "2026-10-08")
            self.assertEqual(state.read_text(), before)

    def test_late_published_row_after_watermark_is_picked_up(self):
        with tempfile.TemporaryDirectory() as directory:
            state = self.write_state(directory, "2026-10-02")
            # The 10/05 row was published after an earlier 21:15 UTC run; its date
            # exceeds the watermark, so the next run lands it.
            result, records, summary, _ = run_extractor(
                [csv_row("10/01/2026"), csv_row("10/02/2026"), csv_row("10/05/2026")],
                "--state-file", str(state),
            )
            self.assertEqual(result, 0)
            self.assertEqual([r["date"] for r in records], ["2026-10-05"])
            self.assertEqual(summary["records"], 1)

    def test_explicit_year_is_a_full_dump_without_touching_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, records, summary, _ = run_extractor(
                [csv_row("12/31/2025")],
                "--state-file", str(state), "--year", "2025",
            )
            self.assertEqual(result, 0)
            self.assertEqual([r["date"] for r in records], ["2025-12-31"])
            self.assertEqual(summary["coverage"]["year"], 2025)
            self.assertFalse(state.exists())

    def test_no_state_emits_full_year_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, records, summary, _ = run_extractor(
                [csv_row("10/08/2026")],
                "--no-state",
            )
            self.assertEqual(result, 0)
            self.assertEqual([r["date"] for r in records], ["2026-10-08"])
            self.assertFalse(state.exists())
            self.assertIsNone(summary["coverage"]["last_date_before"])

    def test_missing_tenor_column_fails_loudly_without_state_change(self):
        body = (HEADER.replace('"30 Yr"', '') + "\n" + csv_row() + "\n").encode("utf-8")
        with tempfile.TemporaryDirectory() as directory:
            state = self.write_state(directory, "2026-10-01")
            stdout, stderr = io.StringIO(), io.StringIO()
            with mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=Response(body)
            ):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    result = MODULE.main(["--state-file", str(state)])
            self.assertEqual(result, 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("30 Yr", stderr.getvalue())

    def test_duplicate_date_row_fails_loudly(self):
        self.assert_fails(csv_body(csv_row("10/08/2026"), csv_row("10/08/2026")))

    def test_row_outside_requested_year_fails_loudly(self):
        self.assert_fails(csv_body(csv_row("01/02/2026"), csv_row("12/31/2025")))

    def test_unparseable_date_fails_loudly(self):
        self.assert_fails(csv_body(csv_row("10/32/2026")))

    def test_malformed_state_fails_before_any_download(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            state.write_text("not json", encoding="utf-8")
            with mock.patch.object(MODULE.urllib.request, "urlopen",
                                   side_effect=AssertionError("unexpected download")):
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()) as err:
                    result = MODULE.main(["--state-file", str(state)])
            self.assertEqual(result, 1)
            self.assertIn("malformed state", err.getvalue())
            self.assertEqual(state.read_text(), "not json")

    def assert_fails(self, body):
        with tempfile.TemporaryDirectory() as directory:
            state = self.write_state(directory, "2026-10-01")
            stdout, stderr = io.StringIO(), io.StringIO()
            with mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=Response(body)
            ):
                with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    result = MODULE.main(["--state-file", str(state)])
            self.assertEqual(result, 1)
            self.assertEqual(stdout.getvalue(), "")
            summary = json.loads([line for line in stderr.getvalue().splitlines()
                                  if line.startswith(MODULE.SUMMARY_PREFIX)][0][len(MODULE.SUMMARY_PREFIX):])
            self.assertEqual(summary["health"], "failed")
            self.assertEqual(json.loads(state.read_text())["last_date"], "2026-10-01")


if __name__ == "__main__":
    unittest.main()
