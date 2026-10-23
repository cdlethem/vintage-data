import contextlib
from datetime import datetime, timedelta, timezone
import importlib.util
import io
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest import mock
import urllib.error
from urllib.parse import unquote


SCRIPT = Path(__file__).parent / "scripts" / "fetch_ghcn_daily.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "ghcn_daily.yml"
SPEC = importlib.util.spec_from_file_location("fetch_ghcn_daily", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

STATION_A = "USW00012839"
STATION_B = "US1OKCV0091"


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def row(station, date, **elements):
    out = {"STATION": station, "DATE": date}
    for element, value in elements.items():
        if value is not None:
            out[element] = f"{value:7d}"
    return out


class FetchGhcnDailyTests(unittest.TestCase):
    def run_extractor(self, document, *args, urlopen_side_effect=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        if urlopen_side_effect is not None:
            patcher = mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=urlopen_side_effect
            )
        else:
            patcher = mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)
            )
        with patcher as urlopen, contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = MODULE.main(list(args))
        return result, stdout.getvalue().splitlines(), stderr.getvalue().splitlines(), urlopen

    def summary(self, stderr_lines):
        self.assertEqual(len(stderr_lines), 1, stderr_lines)
        self.assertTrue(stderr_lines[0].startswith("VINTAGE_RUN_SUMMARY\t"), stderr_lines)
        return json.loads(stderr_lines[0].split("\t", 1)[1])

    def test_success_normalizes_scale_and_advances_watermark(self):
        document = [
            row(STATION_A, "2026-10-18", TMP=273, TMAX=283, TMIN=233, PRCP=335),
            row(STATION_B, "2026-10-18", TMP=260, TMAX=300, TMIN=120, PRCP=0),
            row(STATION_B, "2026-10-17", TMP=255, TMAX=299, TMIN=111),
            row(STATION_A, "2026-10-17"),  # no elements: nothing to store
        ]
        today = datetime(2026, 10, 23, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            with mock.patch.object(MODULE, "datetime", wraps=datetime) as fake_dt:
                fake_dt.now.return_value = today
                result, stdout, stderr, _ = self.run_extractor(
                    document, "--state-file", str(state)
                )
            self.assertEqual(result, 0)
            self.assertEqual(len(stdout), 3)
            first = json.loads(stdout[0])  # US1... sorts before USW...
            self.assertEqual(first["station"], STATION_B)
            self.assertEqual(first["date"], "2026-10-17")
            self.assertEqual(first["id"], f"{STATION_B}-2026-10-17")
            self.assertEqual(first["temp_c"], 25.5)
            self.assertEqual(first["tmin_c"], 11.1)
            second = json.loads(stdout[1])
            self.assertEqual(second["station"], STATION_B)
            self.assertEqual(second["date"], "2026-10-18")
            self.assertEqual(second["precip_mm"], 0.0)  # PRCP 0 is a real zero
            third = json.loads(stdout[2])
            self.assertEqual(
                third,
                {
                    "source": "ghcn_daily",
                    "fetched_at": today.isoformat(timespec="seconds"),
                    "id": f"{STATION_A}-2026-10-18",
                    "station": STATION_A,
                    "station_name": "FL MIAMI INTL AP",
                    "latitude": 25.7881,
                    "longitude": -80.31,
                    "date": "2026-10-18",
                    "temp_c": 27.3,
                    "tmax_c": 28.3,
                    "tmin_c": 23.3,
                    "precip_mm": 33.5,
                    "snow_cm": None,
                    "snow_depth_cm": None,
                },
            )
            summary = self.summary(stderr)
            self.assertEqual(summary["health"], "healthy")
            self.assertEqual(summary["records"], 3)
            self.assertEqual(summary["coverage"]["watermark_after"], "2026-10-18")
            self.assertEqual(summary["coverage"]["skipped_empty_rows"], 1)
            self.assertEqual(json.loads(state.read_text())["last_date"], "2026-10-18")

    def test_rerun_with_advanced_watermark_emits_nothing_and_keeps_watermark(self):
        today = datetime(2026, 10, 23, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text(json.dumps({"last_date": "2026-10-18"}) + "\n")
            with mock.patch.object(MODULE, "datetime", wraps=datetime) as fake_dt:
                fake_dt.now.return_value = today
                result, stdout, stderr, _ = self.run_extractor(
                    [], "--state-file", str(state)
                )
            self.assertEqual(result, 0)
            self.assertEqual(stdout, [])
            summary = self.summary(stderr)
            self.assertEqual(summary["records"], 0)
            self.assertEqual(summary["coverage"]["watermark_before"], "2026-10-18")
            self.assertEqual(summary["coverage"]["watermark_after"], "2026-10-18")
            self.assertEqual(summary["coverage"]["window_start"], "2026-10-19")
            self.assertEqual(json.loads(state.read_text())["last_date"], "2026-10-18")

    def test_stale_watermark_window_is_bounded_by_lookback(self):
        today = datetime(2026, 10, 23, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            stale = (today - timedelta(days=100)).date().isoformat()
            state.write_text(json.dumps({"last_date": stale}) + "\n")
            with mock.patch.object(MODULE, "datetime", wraps=datetime) as fake_dt:
                fake_dt.now.return_value = today
                result, _, _, urlopen = self.run_extractor([], "--state-file", str(state))
            self.assertEqual(result, 0)
            url = unquote(urlopen.call_args[0][0].full_url)
            expected_start = (today.date() - timedelta(days=31)).isoformat()
            self.assertIn(f"startDate={expected_start}", url)
            self.assertIn(f"endDate={today.date().isoformat()}", url)
            self.assertIn("stations=USW00012839,", url)

    def test_request_covers_all_curated_stations_and_elements(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            result, _, _, urlopen = self.run_extractor([], "--state-file", str(state))
            self.assertEqual(result, 0)
            url = unquote(urlopen.call_args[0][0].full_url)
            for station in MODULE.STATIONS:
                self.assertIn(station, url)
            for element in ("TMP", "TMAX", "TMIN", "PRCP", "SNOW", "SNWD"):
                self.assertIn(element, url)
            self.assertIn("dataset=daily-summaries", url)

    def test_download_failure_fails_with_summary_and_no_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            result, stdout, stderr, _ = self.run_extractor(
                [], "--state-file", str(state),
                urlopen_side_effect=urllib.error.URLError("timed out"),
            )
            self.assertEqual(result, 1)
            self.assertEqual(stdout, [])
            summary = self.summary(stderr)
            self.assertEqual(summary["health"], "failed")
            self.assertIn("timed out", summary["error"])
            self.assertFalse(state.exists())

    def test_malformed_state_fails_without_request(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            state.write_text("not json")
            result, _, stderr, urlopen = self.run_extractor(
                [], "--state-file", str(state)
            )
            self.assertEqual(result, 1)
            urlopen.assert_not_called()
            self.assertEqual(self.summary(stderr)["health"], "failed")
            self.assertEqual(state.read_text(), "not json")

    def test_unknown_station_fails_run_and_writes_no_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            document = [row("XX000000001", "2026-10-18", TMP=273)]
            result, stdout, stderr, _ = self.run_extractor(document, "--state-file", str(state))
            self.assertEqual(result, 1)
            self.assertEqual(stdout, [])
            self.assertEqual(self.summary(stderr)["health"], "failed")
            self.assertFalse(state.exists())

    def test_duplicate_row_fails_run(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            document = [
                row(STATION_A, "2026-10-18", TMP=273),
                row(STATION_A, "2026-10-18", TMP=280),
            ]
            result, _, stderr, _ = self.run_extractor(document, "--state-file", str(state))
            self.assertEqual(result, 1)
            self.assertEqual(self.summary(stderr)["health"], "failed")
            self.assertFalse(state.exists())

    def test_date_outside_window_fails_run(self):
        today = datetime(2026, 10, 23, tzinfo=timezone.utc)
        with mock.patch.object(MODULE, "datetime", wraps=datetime) as fake_dt:
            fake_dt.now.return_value = today
            with tempfile.TemporaryDirectory() as directory:
                state = Path(directory) / "state.json"
                document = [row(STATION_A, "2026-11-01", TMP=273)]
                result, _, stderr, _ = self.run_extractor(document, "--state-file", str(state))
                self.assertEqual(result, 1)
                self.assertEqual(self.summary(stderr)["health"], "failed")
                self.assertFalse(state.exists())

    def test_sentinels_and_missing_elements_become_null(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            document = [row(STATION_A, "2026-10-18", TMP=273, TMAX=283, SNOW=-9999, SNWD=-999)]
            result, stdout, _, _ = self.run_extractor(document, "--state-file", str(state))
            self.assertEqual(result, 0)
            record = json.loads(stdout[0])
            self.assertEqual(record["temp_c"], 27.3)
            self.assertEqual(record["tmax_c"], 28.3)
            self.assertEqual(record["snow_cm"], None)
            self.assertEqual(record["snow_depth_cm"], None)

    def test_failed_watermark_save_does_not_report_success(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            document = [row(STATION_A, "2026-10-18", TMP=273)]
            with mock.patch.object(MODULE.os, "replace", side_effect=OSError("state unavailable")):
                result, stdout, stderr, _ = self.run_extractor(document, "--state-file", str(state))
            self.assertEqual(result, 1)
            self.assertEqual(len(stdout), 1)
            summary = self.summary(stderr)
            self.assertEqual(summary["health"], "failed")
            self.assertEqual(summary["records"], 1)
            self.assertFalse(state.exists())

    def test_no_state_mode_emits_without_writing_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state.json"
            document = [row(STATION_A, "2026-10-18", TMP=273)]
            result, stdout, _, _ = self.run_extractor(document, "--no-state")
            self.assertEqual(result, 0)
            self.assertEqual(len(stdout), 1)
            self.assertFalse(state.exists())

    def test_source_configuration_is_daily_staggered_and_paused(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r'(?m)^schedule: "33 2 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIsNone(re.search(r"(?m)^enabled:\s*true$", text))
        self.assertIn('args: ["--timeout", "60"]', text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("one request per run", text)
        self.assertIn("public domain", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("live NCEI source smoke", text)
        self.assertIn("read-only Airflow import checks", text)


if __name__ == "__main__":
    unittest.main()
