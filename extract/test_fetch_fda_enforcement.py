import contextlib
import datetime
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
from unittest.mock import patch

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_fda_enforcement.py"
SOURCE_CONFIG = pathlib.Path(__file__).parent / "sources" / "fda_enforcement.yml"
SPEC = importlib.util.spec_from_file_location("fetch_fda_enforcement", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

TODAY = datetime.date(2026, 10, 21)


class FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def page_document(rows):
    return {"meta": {"results": {"total": len(rows)}}, "results": rows}


def row(report_date, event_id=1001, endpoint_extra=None):
    record = {
        "report_date": report_date,
        "event_id": event_id,
        "recall_number": f"R-{event_id}",
        "recalling_firm": "Example Foods LLC",
        "classification": "Class I",
        "product_description": "Frozen vegetables",
        "product_quantity": "200 cases",
        "distribution_pattern": "National",
        "reason_for_recall": "Listeria",
        "status": "Recall completed",
        "voluntary_mandated": "Voluntary",
    }
    if endpoint_extra:
        record.update(endpoint_extra)
    return record


class FakeOpen:
    """Yields canned urlopen results in order; counts calls."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def __call__(self, request, timeout=None):
        self.calls += 1
        if not self.responses:
            raise AssertionError("urlopen called more times than canned responses")
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        payload = json.dumps(page_document(response)).encode("utf-8")
        return FakeResponse(payload)


def http_error(code, body=b""):
    return urllib.error.HTTPError(
        "https://api.fda.gov/food/enforcement.json", code, "error", {}, io.BytesIO(body)
    )


def run_main(argv, responses):
    stdout, stderr = io.StringIO(), io.StringIO()
    fake = FakeOpen(responses)
    sleeper = []
    with patch.object(MODULE.urllib.request, "urlopen", fake), \
         patch.object(MODULE.time, "sleep", sleeper.append), \
         contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        result = MODULE.main(argv)
    return result, stdout.getvalue().splitlines(), stderr.getvalue().splitlines(), fake, sleeper


def parse_summary(stderr_lines):
    """The single VINTAGE_RUN_SUMMARY record on stderr, as a dict."""
    lines = [line for line in stderr_lines if line.startswith(MODULE.SUMMARY_PREFIX)]
    assert len(lines) == 1, f"expected one summary line, got {lines!r}"
    return json.loads(lines[0][len(MODULE.SUMMARY_PREFIX):])


class PlanRangesTests(unittest.TestCase):
    def test_initial_run_uses_lookback_window(self):
        ranges = MODULE.plan_ranges({}, TODAY, lookback_days=14)
        self.assertEqual(
            ranges,
            {"food": ("20261007", "20261021"), "drug": ("20261007", "20261021"),
             "device": ("20261007", "20261021")},
        )

    def test_watermark_starts_the_day_after(self):
        ranges = MODULE.plan_ranges({"food": "20261014"}, TODAY, lookback_days=14)
        self.assertEqual(ranges["food"], ("20261015", "20261021"))
        self.assertEqual(ranges["drug"], ("20261007", "20261021"))

    def test_watermark_at_today_yields_empty_range(self):
        ranges = MODULE.plan_ranges({"food": "20261021", "drug": "20261021",
                                     "device": "20261021"}, TODAY, lookback_days=14)
        for endpoint in MODULE.ENDPOINTS:
            self.assertIsNone(ranges[endpoint])


class FetchAllTests(unittest.TestCase):
    def run_fetch(self, responses, watermarks, lookback_days=14):
        counter = {"requests": 0}
        fake = FakeOpen(responses)
        with patch.object(MODULE.urllib.request, "urlopen", fake), \
             patch.object(MODULE.time, "sleep", lambda s: None):
            records, advanced, skipped = MODULE.fetch_all(
                watermarks, TODAY, lookback_days, 30, counter)
        return records, advanced, skipped, fake

    def test_emits_envelope_and_normalizes_fields(self):
        records, advanced, skipped, _ = self.run_fetch(
            [[row("20261014", 11), row("20261015", 12)], [], []], {})
        self.assertEqual(skipped, 0)
        self.assertEqual(len(records), 2)
        first = records[0]
        self.assertEqual(first["source"], MODULE.SOURCE)
        self.assertEqual(first["id"], "food:R-11")
        self.assertEqual(first["endpoint"], "food")
        self.assertIn("fetched_at", first)
        self.assertEqual(first["fetched_at"], records[1]["fetched_at"])
        self.assertEqual(first["recall_number"], "R-11")
        self.assertEqual(advanced, {"food": "20261021", "drug": "20261021",
                                     "device": "20261021"})

    def test_skips_rows_without_usable_keys(self):
        bad = row("20261015")
        bad["report_date"] = "15/10/2026"
        records, advanced, skipped, _ = self.run_fetch([[], [bad, row("20261015", 21)], []], {})
        self.assertEqual(skipped, 1)
        self.assertEqual([r["id"] for r in records], ["drug:R-21"])
        self.assertEqual(advanced, {"food": "20261021", "drug": "20261021",
                                     "device": "20261021"})

    def test_na_recall_number_falls_back_to_event_id(self):
        na = row("20261015", 31)
        na["recall_number"] = "N/A"
        records, _, skipped, _ = self.run_fetch([[na], [], []], {})
        self.assertEqual(skipped, 0)
        self.assertEqual(records[0]["id"], "food:event-31")

    def test_paginates_until_partial_page(self):
        full = [row("20261015", 1000 + i) for i in range(MODULE.PAGE_SIZE)]
        tail = [row("20261015", 2001)]
        records, _, skipped, fake = self.run_fetch([full, tail, [], []], {})
        self.assertEqual(skipped, 0)
        self.assertEqual(len(records), MODULE.PAGE_SIZE + 1)
        self.assertEqual(fake.calls, 4)  # two food pages, then drug + device

    def test_empty_ranges_make_no_requests(self):
        records, advanced, skipped, fake = self.run_fetch(
            [], {"food": "20261021", "drug": "20261021", "device": "20261021"})
        self.assertEqual((records, skipped, fake.calls), ([], 0, 0))
        self.assertEqual(advanced, {"food": "20261021", "drug": "20261021",
                                    "device": "20261021"})


class MainStateTests(unittest.TestCase):
    def test_first_run_writes_advanced_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            result, out, err, fake, _ = run_main(
                ["--state-file", str(state)],
                [[row("20261019", 1), row("20261020", 2)], [row("20261020", 3)], []])
            self.assertEqual(result, 0)
            self.assertEqual(len(out), 3)
            today = datetime.date.today().strftime("%Y%m%d")
            self.assertEqual(parse_summary(err),
                             {"health": "healthy", "completeness": "complete",
                              "records": 3, "requests": 3, "skipped": 0,
                              "coverage": {"watermarks": {"food": today, "drug": today,
                                                          "device": today}},
                              "state": str(state)})
            written = json.loads(state.read_text(encoding="utf-8"))
            self.assertEqual(written, {"version": 1, "source": "fda_enforcement",
                                       "watermarks": {"food": today, "drug": today,
                                                      "device": today}})

    def test_second_run_starts_after_watermark_and_is_empty(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            wm = (datetime.datetime.now(datetime.timezone.utc).date()
                  - datetime.timedelta(days=1)).strftime("%Y%m%d")
            state.write_text(json.dumps({"version": 1, "source": "fda_enforcement",
                                         "watermarks": {"food": wm, "drug": wm,
                                                        "device": wm}}),
                             encoding="utf-8")
            result, out, err, fake, _ = run_main(["--state-file", str(state)], [[], [], []])
            self.assertEqual(result, 0)
            self.assertEqual(out, [])
            self.assertEqual(fake.calls, 3)

    def test_404_no_matches_is_an_empty_success(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            wm = (datetime.datetime.now(datetime.timezone.utc).date()
                  - datetime.timedelta(days=1)).strftime("%Y%m%d")
            state.write_text(json.dumps({"version": 1, "source": "fda_enforcement",
                                         "watermarks": {"food": wm, "drug": wm,
                                                        "device": wm}}),
                             encoding="utf-8")
            no_match = b'{"error": {"code": "NOT_FOUND", "message": "No matches found!"}}'
            result, out, err, fake, _ = run_main(
                ["--state-file", str(state)],
                [http_error(404, no_match)] * 3)
            self.assertEqual(result, 0)
            self.assertEqual(out, [])
            parsed = parse_summary(err)
            self.assertEqual(parsed["health"], "healthy")
            self.assertEqual(parsed["records"], 0)
            today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
            written = json.loads(state.read_text(encoding="utf-8"))
            self.assertEqual(written["watermarks"], {"food": today, "drug": today,
                                                     "device": today})

    def test_failure_leaves_state_unwritten_and_reports_failed(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            result, out, err, fake, _ = run_main(
                ["--state-file", str(state)],
                [[row("20261019", 1)], http_error(500), http_error(500), http_error(500)])
            self.assertEqual(result, 1)
            self.assertEqual(out, [])  # records are emitted only after every endpoint succeeds
            self.assertFalse(state.exists())
            parsed = parse_summary(err)
            self.assertEqual(parsed["health"], "failed")
            self.assertEqual(parsed["completeness"], "incomplete")
            self.assertEqual(parsed["records"], 0)

    def test_no_state_never_touches_a_file(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            result, out, err, _, _ = run_main(["--no-state"], [[], [], []])
            self.assertEqual(result, 0)
            self.assertFalse(state.exists())
            self.assertEqual(parse_summary(err)["state"], "none")

    def test_no_state_and_state_file_are_mutually_exclusive(self):
        with self.assertRaises(SystemExit):
            run_main(["--no-state", "--state-file", "/tmp/never"], [[], [], []])

    def test_malformed_state_fails_without_network(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            state.write_text("not json", encoding="utf-8")
            result, out, err, fake, _ = run_main(["--state-file", str(state)], [[], [], []])
            self.assertEqual(result, 1)
            self.assertEqual(out, [])
            self.assertEqual(fake.calls, 0)

    def test_retries_500_then_succeeds(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            result, out, err, fake, sleeper = run_main(
                ["--state-file", str(state)],
                [http_error(500), [row("20261019", 1)], [], []])
            self.assertEqual(result, 0)
            self.assertEqual(fake.calls, 4)  # two food attempts + drug + device
            self.assertEqual(sleeper, [MODULE.RETRY_BACKOFF_SECONDS])

    def test_client_error_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "fda_enforcement.json"
            result, out, err, fake, sleeper = run_main(
                ["--state-file", str(state)],
                [http_error(400, b"bad query"), [], []])
            self.assertEqual(result, 1)
            self.assertEqual(fake.calls, 1)
            self.assertEqual(sleeper, [])
            self.assertIn("HTTP 400", parse_summary(err)["error"])


class SourceConfigTests(unittest.TestCase):
    def test_source_configuration(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r'(?m)^schedule: "33 2 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn("script: fetch_fda_enforcement.py", text)
        self.assertIn("args: []", text)
        self.assertIn("retries: 1", text)
        self.assertIn("CC0 1.0", text)
        self.assertIn("attribution_required: false", text)
        self.assertTrue(SCRIPT.is_file())


if __name__ == "__main__":
    unittest.main()
