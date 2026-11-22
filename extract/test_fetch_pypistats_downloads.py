import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
import urllib.error
from datetime import date, timedelta
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_pypistats_downloads.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "pypistats_downloads.yml"
SPEC = importlib.util.spec_from_file_location("fetch_pypistats_downloads", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def day_rows(date_str, downloads):
    """Two-category rows for one day, mirror downloads slightly lower."""
    return [
        {"category": "with_mirrors", "date": date_str, "downloads": downloads},
        {"category": "without_mirrors", "date": date_str, "downloads": downloads - 90000},
    ]


def overall_document(package, rows):
    return {"package": package, "type": "overall_downloads", "data": rows}


def series(package, start, end, base=4_000_000):
    """A day-indexed series covering start..end inclusive (ISO dates)."""
    rows = []
    current = start
    index = 0
    while current <= end:
        rows.extend(day_rows(current.isoformat(), base + index * 1000))
        current += timedelta(days=1)
        index += 1
    return rows


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_error(code):
    return urllib.error.HTTPError(
        "https://pypistats.org/api/packages/flask/overall", code, "error", {}, None
    )


class ValidateOverallTests(unittest.TestCase):
    def test_returns_rows_for_valid_document(self):
        document = overall_document("flask", day_rows("2026-11-21", 4_000_000))
        self.assertEqual(MODULE.validate_overall(document, "flask"), document["data"])

    def test_rejects_malformed_documents(self):
        good = day_rows("2026-11-21", 4_000_000)
        malformed = (
            ([], "not an object"),
            (overall_document("django", good), "package"),
            ({"package": "flask", "type": "system_downloads", "data": good}, "type"),
            ({"package": "flask", "type": "overall_downloads"}, "no data rows"),
            (overall_document("flask", {}), "no data rows"),
            (overall_document("flask", []), "no data rows"),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.validate_overall(document, "flask")

    def test_rejects_malformed_rows(self):
        def with_row(row):
            return overall_document("flask", [row])

        malformed_rows = (
            {"category": "mirrors", "date": "2026-11-21", "downloads": 1},
            {"category": "with_mirrors", "downloads": 1},
            {"category": "with_mirrors", "date": 20261121, "downloads": 1},
            {"category": "with_mirrors", "date": "2026-11-21", "downloads": "1"},
            {"category": "with_mirrors", "date": "2026-11-21", "downloads": True},
            {"category": "with_mirrors", "date": "2026-11-21", "downloads": -1},
            {"category": "with_mirrors", "date": "not-a-date", "downloads": 1},
        )
        for row in malformed_rows:
            with self.subTest(row=row):
                with self.assertRaisesRegex(ValueError, "malformed|category|unparseable"):
                    MODULE.validate_overall(with_row(row), "flask")


class FetchOverallTests(unittest.TestCase):
    def run_fetch(self, side_effect):
        with mock.patch.object(MODULE.time, "sleep") as sleep:
            with mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=side_effect
            ) as urlopen:
                rows = MODULE.fetch_overall(
                    "flask", date(2026, 11, 20), date(2026, 11, 21),
                    timeout=5, retries=2, retry_delay=1,
                )
        return rows, urlopen, sleep

    def test_retries_transient_then_succeeds(self):
        document = overall_document("flask", day_rows("2026-11-21", 4_000_000))
        rows, urlopen, sleep = self.run_fetch(
            [http_error(429), JsonResponse(document)]
        )
        self.assertEqual(rows, document["data"])
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1])

    def test_re_raises_original_error_after_retries(self):
        with mock.patch.object(MODULE.time, "sleep") as sleep:
            with mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=http_error(503),
            ) as urlopen:
                with self.assertRaises(urllib.error.HTTPError) as raised:
                    MODULE.fetch_overall(
                        "flask", date(2026, 11, 20), date(2026, 11, 21),
                        timeout=5, retries=2, retry_delay=1,
                    )
        self.assertEqual(raised.exception.code, 503)
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1, 3])

    def test_permanent_404_is_not_retried(self):
        with mock.patch.object(MODULE.time, "sleep") as sleep:
            with mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=http_error(404),
            ) as urlopen:
                with self.assertRaises(urllib.error.HTTPError):
                    MODULE.fetch_overall(
                        "flask", date(2026, 11, 20), date(2026, 11, 21),
                        timeout=5, retries=3, retry_delay=1,
                    )
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()


class RunTests(unittest.TestCase):
    END_DATE = date(2026, 11, 21)

    def run_fetcher(self, packages, state, fetch_side_effect, state_file):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(MODULE, "fetch_overall", side_effect=fetch_side_effect) as fetch:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = MODULE.run(
                    packages, state, state_file, self.END_DATE, 30, 3, 1
                )
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        summary_line = [
            line for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        summary = (
            json.loads(summary_line[0][len(MODULE.SUMMARY_PREFIX):])
            if summary_line
            else None
        )
        return exit_code, records, summary, fetch, stderr.getvalue()

    def test_emits_new_rows_and_advances_watermark(self):
        fetch = lambda package, start, end, timeout, retries, delay: (  # noqa: E731
            series(package, start, end)
        )
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, records, summary, _, _ = self.run_fetcher(
                ("flask", "requests"), {}, fetch, state_file
            )
            saved_state = json.loads(state_file.read_text())
        self.assertEqual(exit_code, 0)
        # Baseline (no watermark) collects exactly the newest day, both categories.
        self.assertEqual(len(records), 4)
        self.assertEqual(
            {record["id"] for record in records},
            {
                "flask-2026-11-21-with_mirrors",
                "flask-2026-11-21-without_mirrors",
                "requests-2026-11-21-with_mirrors",
                "requests-2026-11-21-without_mirrors",
            },
        )
        record = next(r for r in records if r["id"] == "flask-2026-11-21-with_mirrors")
        self.assertEqual(record["source"], "pypistats_downloads")
        self.assertEqual(record["package"], "flask")
        self.assertEqual(record["date"], "2026-11-21")
        self.assertEqual(record["downloads"], 4_000_000)
        self.assertEqual(record["raw"], {"category": "with_mirrors", "date": "2026-11-21", "downloads": 4_000_000})
        self.assertEqual(record["attribution"], MODULE.ATTRIBUTION)
        self.assertRegex(record["fetched_at"], r"\+00:00$")
        self.assertEqual(
            summary,
            {
                "packages": 2, "fetched": 2, "skipped": 0, "failed": 0,
                "failed_packages": [], "requests": 2, "rows": 4,
                "oldest_date": "2026-11-21", "newest_date": "2026-11-21",
            },
        )
        self.assertEqual(
            saved_state,
            {"version": 1, "last_date": {"flask": "2026-11-21", "requests": "2026-11-21"}},
        )

    def test_skips_packages_at_watermark_without_requests(self):
        state = {"version": 1, "last_date": {"flask": "2026-11-21"}}
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, records, summary, fetch, _ = self.run_fetcher(
                ("flask",), state, None, state_file
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(records, [])
        self.assertEqual(summary["skipped"], 1)
        self.assertEqual(summary["requests"], 0)
        fetch.assert_not_called()

    def test_backfills_gap_from_watermark_plus_one(self):
        state = {"version": 1, "last_date": {"flask": "2026-11-19"}}
        seen = {}

        def fetch(package, start, end, timeout, retries, delay):
            seen[package] = (start, end)
            return series(package, start, end)

        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, records, summary, _, _ = self.run_fetcher(
                ("flask",), state, fetch, state_file
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(seen["flask"], (date(2026, 11, 20), self.END_DATE))
        self.assertEqual(
            sorted(record["date"] for record in records),
            ["2026-11-20", "2026-11-20", "2026-11-21", "2026-11-21"],
        )
        self.assertEqual(summary["rows"], 4)

    def test_stale_watermark_is_clamped_to_retention_floor(self):
        state = {"version": 1, "last_date": {"flask": "2025-01-01"}}
        seen = {}

        def fetch(package, start, end, timeout, retries, delay):
            seen[package] = (start, end)
            return series(package, start, end)

        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, _, summary, _, _ = self.run_fetcher(
                ("flask",), state, fetch, state_file
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(
            seen["flask"][0], self.END_DATE - timedelta(days=MODULE.RETENTION_DAYS)
        )
        self.assertEqual(summary["failed"], 0)

    def test_failed_package_does_not_sink_run_and_keeps_watermark(self):
        def fetch(package, start, end, timeout, retries, delay):
            if package == "broken":
                raise RuntimeError("HTTP 503")
            return series(package, start, end)

        state = {"version": 1, "last_date": {"broken": "2026-11-15"}}
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, records, summary, _, _ = self.run_fetcher(
                ("flask", "broken"), state, fetch, state_file
            )
            saved = json.loads(state_file.read_text())
        self.assertEqual(exit_code, 0)
        self.assertEqual(summary["failed_packages"], ["broken"])
        # The failed package keeps its old watermark; the good one advances.
        self.assertEqual(saved["last_date"]["broken"], "2026-11-15")
        self.assertEqual(saved["last_date"]["flask"], "2026-11-21")

    def test_all_packages_failed_exits_nonzero_and_writes_no_state(self):
        def fetch(package, start, end, timeout, retries, delay):
            raise RuntimeError("HTTP 503")

        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, records, summary, _, stderr_text = self.run_fetcher(
                ("flask",), {}, fetch, state_file
            )
        self.assertEqual(exit_code, 1)
        self.assertEqual(records, [])
        self.assertEqual(summary["failed"], 1)
        self.assertIn("all 1 packages failed", stderr_text)
        self.assertFalse(state_file.exists())

    def test_no_rows_in_window_does_not_advance_watermark(self):
        def fetch(package, start, end, timeout, retries, delay):
            # Upstream batch is late: the newest row is older than the window.
            return series(package, date(2026, 11, 18), date(2026, 11, 19))

        state = {"version": 1, "last_date": {"flask": "2026-11-19"}}
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, records, summary, _, _ = self.run_fetcher(
                ("flask",), state, fetch, state_file
            )
        self.assertEqual(exit_code, 1)  # nothing fetched, nothing skipped
        self.assertEqual(records, [])
        self.assertFalse(state_file.exists())

    def test_no_state_file_means_no_state_saved(self):
        fetch = lambda package, start, end, timeout, retries, delay: (  # noqa: E731
            series(package, start, end)
        )
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            exit_code, records, _, _, _ = self.run_fetcher(
                ("flask",), {}, fetch, None
            )
        self.assertEqual(exit_code, 0)
        self.assertEqual(len(records), 2)
        self.assertFalse(state_file.exists())


class DefaultPackagesTests(unittest.TestCase):
    def test_curated_list_is_50_unique_lowercase_names(self):
        self.assertEqual(len(MODULE.DEFAULT_PACKAGES), 50)
        self.assertEqual(len(set(MODULE.DEFAULT_PACKAGES)), 50)
        for name in MODULE.DEFAULT_PACKAGES:
            self.assertRegex(name, r"^[a-z0-9][a-z0-9._-]*$")
        self.assertIn("requests", MODULE.DEFAULT_PACKAGES)
        self.assertIn("flask", MODULE.DEFAULT_PACKAGES)


class SourceYamlTests(unittest.TestCase):
    def test_yaml_points_at_fetcher_with_daily_staggered_schedule(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r"(?m)^name: pypistats_downloads$")
        self.assertRegex(text, r"(?m)^script: fetch_pypistats_downloads\.py$")
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertRegex(text, r'(?m)^schedule: "\d{1,2} \d{1,2} \* \* \*"')
        self.assertTrue(SCRIPT.is_file())


if __name__ == "__main__":
    unittest.main()
