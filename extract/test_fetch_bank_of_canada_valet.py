"""Tests for fetch_bank_of_canada_valet.py (mocked HTTP, no network)."""
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
from datetime import date
from unittest import mock

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_bank_of_canada_valet.py"
RUN_METADATA_PATH = pathlib.Path(__file__).parent.parent / "orchestration" / "include" / "run_metadata.py"


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


MODULE = _load(SCRIPT, "fetch_bank_of_canada_valet")
RUN_METADATA = _load(RUN_METADATA_PATH, "boc_test_run_metadata")

FX = "FXUSDCAD"
RATE = "AVG.INTWO"
MONTHLY = "V37151"

FIXTURE_DETAIL = {
    FX: {"label": "USD/CAD", "metadata": {"unit": "Canadian dollars per US dollar"}},
    RATE: {"label": "CORRA", "metadata": {"unit": "Percent"}},
    MONTHLY: {"label": "M1+", "metadata": {"unit": "CAD millions"}},
}


def fixed_today(when: date):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return when

    return FixedDate


def valet_doc(cells_by_date):
    """Build a Valet observations document.

    ``cells_by_date`` maps "YYYY-MM-DD" to {series: value_or_None}; a missing
    key means the series did not publish that date.
    """
    observations = []
    for period in sorted(cells_by_date):
        obs = {"d": period}
        for series, value in cells_by_date[period].items():
            if value is not None:
                obs[series] = {"v": str(value)}
        observations.append(obs)
    return {
        "terms": {"url": "https://www.bankofcanada.ca/terms/"},
        "seriesDetail": FIXTURE_DETAIL,
        "observations": observations,
    }


class FakeResponse:
    def __init__(self, body: bytes):
        self.status = 200
        self._body = body

    def read(self, n=-1):
        return self._body if n is None or n < 0 else self._body[:n]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def run_main(argv, document, http_error=None):
    """Run module.main with mocked HTTP and time; return (code, records, stderr, urls)."""
    stdout, stderr = io.StringIO(), io.StringIO()
    urls = []

    def fake_urlopen(req, timeout=None):
        urls.append(req.full_url)
        if http_error is not None:
            raise http_error
        return FakeResponse(json.dumps(document).encode("utf-8"))

    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen), \
         mock.patch.object(MODULE, "date", fixed_today(date(2026, 10, 14))), \
         mock.patch("sys.stdout", stdout), \
         mock.patch("sys.stderr", stderr):
        try:
            code = MODULE.main(argv)
        except SystemExit as exc:
            code = exc.code
    lines = [line for line in stdout.getvalue().splitlines() if line]
    return code, [json.loads(line) for line in lines], stderr.getvalue(), urls


def summary_from_stderr(stderr: str):
    payload, _ = RUN_METADATA.extract_summary_line(stderr)
    assert payload is not None, f"no VINTAGE_RUN_SUMMARY line in stderr: {stderr!r}"
    return RUN_METADATA.validate_summary(json.loads(payload))


class BankOfCanadaValetTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.state = pathlib.Path(self._dir.name) / "state.json"

    def tearDown(self):
        self._dir.cleanup()

    def test_cold_run_emits_all_rows_and_writes_watermarks(self):
        doc = valet_doc({
            "2026-10-09": {FX: "1.4271", RATE: "2.2800"},
            "2026-10-13": {FX: "1.4222"},  # rate lagging: no cell
        })
        code, records, stderr, urls = run_main(["--state-file", str(self.state)], doc)
        self.assertEqual(code, 0)
        self.assertEqual(len(records), 3)
        # Envelope: source, fetched_at, source-natural id.
        for rec in records:
            self.assertEqual(rec["source"], MODULE.SOURCE)
            self.assertIn("fetched_at", rec)
            self.assertRegex(rec["id"], r"^[A-Z0-9.]+:\d{4}-\d{2}-\d{2}$")
        by_id = {r["id"]: r for r in records}
        self.assertEqual(by_id[f"{FX}:2026-10-13"]["value"], 1.4222)
        self.assertEqual(by_id[f"{FX}:2026-10-13"]["unit"], "Canadian dollars per US dollar")
        self.assertEqual(by_id[f"{RATE}:2026-10-09"]["value"], 2.28)
        self.assertNotIn(f"{RATE}:2026-10-13", by_id)
        # One batched request; no explicit start date means cold 31-day window.
        self.assertEqual(len(urls), 1)
        self.assertIn("start_date=2026-09-13", urls[0])
        # Per-series watermarks: only dates that published advance.
        saved = json.loads(self.state.read_text())
        self.assertEqual(saved["series"][FX], "2026-10-13")
        self.assertEqual(saved["series"][RATE], "2026-10-09")
        self.assertNotIn(MONTHLY, saved["series"])
        summary = summary_from_stderr(stderr)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["metrics"]["records"], 3)

    def test_immediate_rerun_emits_nothing(self):
        doc = valet_doc({
            "2026-10-09": {FX: "1.4271", RATE: "2.2800"},
            "2026-10-13": {FX: "1.4222"},
        })
        code, records, _stderr, _urls = run_main(["--state-file", str(self.state)], doc)
        self.assertEqual(code, 0)
        self.assertEqual(len(records), 3)
        code, records, stderr, urls = run_main(["--state-file", str(self.state)], doc)
        self.assertEqual(code, 0)
        self.assertEqual(records, [])
        # Second run requests from the oldest stored watermark, not cold.
        self.assertIn("start_date=2026-10-09", urls[0])
        summary = summary_from_stderr(stderr)
        self.assertEqual(summary["metrics"]["records"], 0)

    def test_new_dates_emit_only_new_rows(self):
        base = valet_doc({"2026-10-09": {FX: "1.4271", RATE: "2.2800"}})
        code, records, _, _ = run_main(["--state-file", str(self.state)], base)
        self.assertEqual(len(records), 2)
        # BoC publishes a new day for the daily series; monthly series gets a
        # brand-new month while FX just adds one more day.
        extended = valet_doc({
            "2026-10-09": {FX: "1.4271", RATE: "2.2800"},
            "2026-10-13": {FX: "1.4222", RATE: "2.2600"},
            "2026-10-14": {FX: "1.4200", MONTHLY: "1820000"},
        })
        code, records, _, urls = run_main(["--state-file", str(self.state)], extended)
        self.assertEqual(code, 0)
        ids = {r["id"] for r in records}
        self.assertEqual(ids, {f"{FX}:2026-10-13", f"{RATE}:2026-10-13",
                               f"{FX}:2026-10-14", f"{MONTHLY}:2026-10-14"})
        saved = json.loads(self.state.read_text())
        self.assertEqual(saved["series"][MONTHLY], "2026-10-14")
        self.assertIn("start_date=2026-10-09", urls[0])

    def test_gap_days_are_picked_up(self):
        base = valet_doc({"2026-10-09": {FX: "1.4271"}})
        run_main(["--state-file", str(self.state)], base)
        # Watermark is 2026-10-09; the next run's window starts there, so the
        # two missing business days are re-requested and emitted.
        gapped = valet_doc({
            "2026-10-09": {FX: "1.4271"},
            "2026-10-13": {FX: "1.4222"},
            "2026-10-14": {FX: "1.4200"},
            "2026-10-15": {FX: "1.4180"},
        })
        code, records, _, urls = run_main(["--state-file", str(self.state)], gapped)
        self.assertEqual(code, 0)
        self.assertEqual([r["id"] for r in records],
                         [f"{FX}:2026-10-13", f"{FX}:2026-10-14", f"{FX}:2026-10-15"])
        self.assertIn("start_date=2026-10-09", urls[0])

    def test_missing_cell_does_not_advance_watermark(self):
        # A trailing date where the series has an explicit null keeps the
        # watermark behind it, so a later revision is still picked up.
        doc = valet_doc({"2026-10-13": {FX: "1.4222"}})
        doc["observations"].append({"d": "2026-10-14", FX: {"v": None}})
        code, records, _, _ = run_main(["--state-file", str(self.state)], doc)
        self.assertEqual(code, 0)
        self.assertEqual(len(records), 1)
        saved = json.loads(self.state.read_text())
        self.assertEqual(saved["series"][FX], "2026-10-13")
        # Next run re-requests from 2026-10-13 and picks up the revision.
        revised = valet_doc({
            "2026-10-13": {FX: "1.4222"},
            "2026-10-14": {FX: "1.4219"},
        })
        code, records, _, _ = run_main(["--state-file", str(self.state)], revised)
        self.assertEqual([r["id"] for r in records], [f"{FX}:2026-10-14"])

    def test_no_state_never_writes(self):
        doc = valet_doc({"2026-10-13": {FX: "1.4222"}})
        code, records, _, _ = run_main(["--no-state"], doc)
        self.assertEqual(code, 0)
        self.assertEqual(len(records), 1)
        self.assertFalse(self.state.exists())

    def test_stale_watermark_is_clamped_to_window(self):
        self.state.write_text(json.dumps(
            {"version": MODULE.STATE_VERSION, "series": {FX: "2026-01-01"}}
        ))
        doc = valet_doc({"2026-10-13": {FX: "1.4222"}})
        code, records, _, urls = run_main(["--state-file", str(self.state)], doc)
        self.assertEqual(code, 0)
        self.assertIn("start_date=2026-09-13", urls[0])
        self.assertEqual([r["id"] for r in records], [f"{FX}:2026-10-13"])

    def test_malformed_response_fails_and_keeps_state(self):
        self.state.write_text(json.dumps(
            {"version": MODULE.STATE_VERSION, "series": {FX: "2026-10-09"}}
        ))
        before = self.state.read_text()
        code, records, stderr, _ = run_main(
            ["--state-file", str(self.state)], {"message": "series not found"})
        self.assertEqual(code, 1)
        self.assertEqual(records, [])
        summary = summary_from_stderr(stderr)
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(self.state.read_text(), before)

    def test_http_404_fails_run(self):
        err = urllib.error.HTTPError(
            "https://www.bankofcanada.ca/valet/observations/FXUSDCAD/json",
            404, "Not Found", None, io.BytesIO(b'{"message": "unknown series"}'))
        code, records, stderr, _ = run_main(["--no-state"], {"observations": []},
                                            http_error=err)
        self.assertEqual(code, 1)
        self.assertEqual(records, [])
        summary = summary_from_stderr(stderr)
        self.assertEqual(summary["health"], "failed")
        self.assertIn("HTTP 404", summary["metrics"]["error"])

    def test_duplicate_json_keys_rejected(self):
        with self.assertRaises(MODULE.MalformedResponseError):
            MODULE.load_response(b'{"observations": [], "observations": []}')

    def test_non_numeric_value_kept_as_string_with_warning(self):
        doc = valet_doc({"2026-10-13": {FX: "1.4222"}})
        doc["observations"][0][RATE] = {"v": "n/a"}
        code, records, stderr, _ = run_main(["--no-state"], doc)
        self.assertEqual(code, 0)
        by_id = {r["id"]: r for r in records}
        self.assertEqual(by_id[f"{RATE}:2026-10-13"]["value"], "n/a")
        self.assertIn("non-numeric value", stderr)
        self.assertEqual(summary_from_stderr(stderr)["health"], "healthy")


if __name__ == "__main__":
    unittest.main()
