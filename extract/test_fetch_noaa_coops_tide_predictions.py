import contextlib
import importlib.util
import io
import json
from datetime import date
from pathlib import Path
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_noaa_coops_tide_predictions.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "noaa_coops_tide_predictions.yml"
SPEC = importlib.util.spec_from_file_location("fetch_noaa_coops_tide_predictions", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

STATION = "8518750"
OTHER = "9414290"
DAY = date(2026, 10, 7)


def predictions_for(begin: date, end: date, station: str):
    """Canned high/low rows for a date window, shaped like the datagetter API."""
    rows = []
    cursor = begin
    while cursor <= end:
        for hour, tide_type, height in ((4, "L", -0.019), (10, "H", 1.568), (16, "L", 0.117), (22, "H", 1.629)):
            rows.append({"t": cursor.strftime(f"%Y-%m-%d {hour:02d}:00"), "v": height, "type": tide_type})
        cursor = cursor.fromordinal(cursor.toordinal() + 1)
    return {"predictions": rows}


class JsonResponse(io.BytesIO):
    def __init__(self, document, status=200):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def station_of(url: str) -> str:
    return MODULE.urllib.parse.parse_qs(MODULE.urllib.parse.urlparse(url).query)["station"][0]


class FetchNoaaCoopsTidePredictionsTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.state_file = Path(self._temp.name) / "state.json"

    def run_main(self, documents, argv):
        """Run main() with a per-station canned document; return (rc, stdout, stderr, requests)."""
        requests = []

        def urlopen(request, timeout=None):
            url = request.full_url
            requests.append((url, request, timeout))
            document = documents.get(station_of(url))
            if isinstance(document, Exception):
                raise document
            return JsonResponse(document)

        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen), \
                mock.patch.object(MODULE.time, "sleep"):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                rc = MODULE.main(argv)
        return rc, stdout.getvalue(), stderr.getvalue(), requests

    def argv(self, today=None, extra=None):
        return ["--stations", f"{STATION},{OTHER}", "--request-delay", "0",
                "--state-file", str(self.state_file), "--today", today or DAY.isoformat(), *(extra or [])]

    def test_emits_valid_ndjson_with_stable_ids_and_metadata(self):
        documents = {s: predictions_for(DAY, DAY, s) for s in (STATION, OTHER)}
        rc, out, err, _ = self.run_main(documents, self.argv())
        self.assertEqual(rc, 0)

        lines = [json.loads(line) for line in out.splitlines()]
        self.assertEqual(len(lines), 8)
        record = lines[0]
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["id"], f"{STATION}|{DAY.isoformat()}T04:00+00:00")
        self.assertEqual(record["station"], STATION)
        self.assertEqual(record["station_name"], MODULE.STATIONS[STATION]["name"])
        self.assertEqual(record["height_m"], -0.019)
        self.assertEqual(record["type"], "L")
        self.assertEqual(record["datum"], "MLLW")
        self.assertRegex(record["fetched_at"], r"\+00:00$")
        self.assertEqual({r["fetched_at"] for r in lines}, {record["fetched_at"]})
        summary = json.loads(err.split(MODULE.SUMMARY_PREFIX, 1)[1])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["partitions"]["failed"], 0)

    def test_request_uses_datagetter_parameters_and_user_agent(self):
        yesterday = DAY.fromordinal(DAY.toordinal() - 1)
        self.state_file.write_text(json.dumps({
            "version": MODULE.STATE_VERSION, "source": MODULE.SOURCE,
            "stations": {STATION: yesterday.isoformat(), OTHER: yesterday.isoformat()},
        }), encoding="utf-8")
        documents = {s: predictions_for(DAY, DAY, s) for s in (STATION, OTHER)}
        _, _, _, requests = self.run_main(documents, self.argv() + ["--timeout", "25"])
        self.assertEqual(len(requests), 2)
        url, request, timeout = requests[0]
        self.assertTrue(url.startswith(MODULE.BASE + "?"))
        params = MODULE.urllib.parse.parse_qs(MODULE.urllib.parse.urlparse(url).query)
        self.assertEqual(params["product"], ["predictions"])
        self.assertEqual(params["interval"], ["hilo"])
        self.assertEqual(params["datum"], ["MLLW"])
        self.assertEqual(params["time_zone"], ["gmt"])
        self.assertEqual(params["units"], ["metric"])
        self.assertEqual(params["application"], ["vintage-data"])
        self.assertEqual(params["format"], ["json"])
        self.assertEqual(params["begin_date"], ["20261007"])
        self.assertEqual(params["end_date"], ["20261007"])
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(timeout, 25)

    def test_first_run_uses_lookback_baseline_and_writes_state(self):
        documents = {s: predictions_for(DAY.fromordinal(DAY.toordinal() - 2), DAY, s) for s in (STATION, OTHER)}
        rc, out, _, requests = self.run_main(documents, self.argv())
        self.assertEqual(rc, 0)
        url, _, _ = requests[0]
        params = MODULE.urllib.parse.parse_qs(MODULE.urllib.parse.urlparse(url).query)
        self.assertEqual(params["begin_date"], ["20261005"])
        self.assertEqual(len(out.splitlines()), 24)  # 2 stations x 3 days x 4 tides

        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["stations"][STATION], DAY.isoformat())
        self.assertEqual(state["stations"][OTHER], DAY.isoformat())

    def test_second_run_next_day_emits_only_the_new_day(self):
        documents = {s: predictions_for(DAY, DAY, s) for s in (STATION, OTHER)}
        self.run_main(documents, self.argv())
        next_day = DAY.fromordinal(DAY.toordinal() + 1)
        rc, out, _, requests = self.run_main(documents, self.argv(today=next_day.isoformat()))
        self.assertEqual(rc, 0)
        url, _, _ = requests[0]
        params = MODULE.urllib.parse.parse_qs(MODULE.urllib.parse.urlparse(url).query)
        self.assertEqual(params["begin_date"], [next_day.strftime("%Y%m%d")])
        self.assertEqual(len(out.splitlines()), 8)  # only the new day, no duplicates

    def test_catch_up_after_missed_days_is_gap_free(self):
        documents = {s: predictions_for(DAY, DAY, s) for s in (STATION, OTHER)}
        self.run_main(documents, self.argv())
        fifth = DAY.fromordinal(DAY.toordinal() + 4)
        begin, end = DAY.fromordinal(DAY.toordinal() + 1), fifth
        documents = {s: predictions_for(begin, end, s) for s in (STATION, OTHER)}
        rc, out, _, requests = self.run_main(documents, self.argv(today=fifth.isoformat()))
        self.assertEqual(rc, 0)
        self.assertEqual(len(requests), 2)  # 4-day window fits one request per station
        url, _, _ = requests[0]
        params = MODULE.urllib.parse.parse_qs(MODULE.urllib.parse.urlparse(url).query)
        self.assertEqual(params["begin_date"], [begin.strftime("%Y%m%d")])
        self.assertEqual(params["end_date"], [end.strftime("%Y%m%d")])
        self.assertEqual(len(out.splitlines()), 32)  # 2 stations x 4 days x 4 tides

    def test_failed_station_keeps_watermark_and_run_stays_degraded(self):
        import urllib.error
        ok = {STATION: predictions_for(DAY, DAY, STATION)}
        rc, out, err, _ = self.run_main(
            {STATION: ok[STATION], OTHER: urllib.error.HTTPError(MODULE.BASE, 503, "unavailable", None, None)},
            self.argv() + ["--retries", "0"],
        )
        self.assertEqual(rc, 0)  # partial success publishes; failed station retries next run
        self.assertEqual(len(out.splitlines()), 4)

        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["stations"][STATION], DAY.isoformat())
        self.assertNotIn(OTHER, state["stations"])
        summary = json.loads(err.split(MODULE.SUMMARY_PREFIX, 1)[1])
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["partitions"], {
            "attempted": 2, "succeeded": 1, "failed": 1,
            "failures": [{"station": OTHER, "error": "HTTP 503"}],
        })

    def test_all_stations_failed_exits_1_and_writes_no_state(self):
        import urllib.error
        documents = {s: urllib.error.HTTPError(MODULE.BASE, 429, "rate limited", None, None) for s in (STATION, OTHER)}
        rc, out, err, _ = self.run_main(documents, self.argv() + ["--retries", "0"])
        self.assertEqual(rc, 1)
        self.assertEqual(out, "")
        self.assertFalse(self.state_file.exists())
        summary = json.loads(err.split(MODULE.SUMMARY_PREFIX, 1)[1])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        self.assertEqual(summary["partitions"]["failed"], 2)

    def test_retries_retriable_status_with_bounded_backoff(self):
        import urllib.error
        attempts = []
        flaky = {STATION: predictions_for(DAY, DAY, STATION), OTHER: None}
        def urlopen(request, timeout=None):
            attempts.append(request.full_url)
            if station_of(request.full_url) == OTHER and len([a for a in attempts if a == request.full_url]) < 3:
                raise urllib.error.HTTPError(MODULE.BASE, 503, "unavailable", None, None)
            return JsonResponse(flaky[station_of(request.full_url)])

        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urlopen), \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                rc = MODULE.main(self.argv())
        self.assertEqual(rc, 0)
        self.assertEqual(len([a for a in attempts if OTHER in a]), 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list if call.args and call.args[0] >= 2], [2, 4])

    def test_window_longer_than_api_chunk_splits_requests(self):
        documents = {s: predictions_for(DAY, DAY, s) for s in (STATION, OTHER)}
        self.run_main(documents, self.argv())
        far = DAY.fromordinal(DAY.toordinal() + 100)
        rc, _, _, requests = self.run_main(documents, self.argv(today=far.isoformat()))
        self.assertEqual(rc, 0)
        # 100-day catch-up per station: one 92-day chunk plus one 8-day chunk
        self.assertEqual(len(requests), 4)

    def test_no_state_mode_never_touches_the_state_file(self):
        documents = {s: predictions_for(DAY.fromordinal(DAY.toordinal() - 2), DAY, s) for s in (STATION, OTHER)}
        rc, out, _, requests = self.run_main(documents, ["--stations", STATION, "--no-state",
                                                        "--request-delay", "0",
                                                        "--today", DAY.isoformat()])
        self.assertEqual(rc, 0)
        self.assertEqual(len(out.splitlines()), 12)
        self.assertFalse(self.state_file.exists())
        url, _, _ = requests[0]
        params = MODULE.urllib.parse.parse_qs(MODULE.urllib.parse.urlparse(url).query)
        self.assertEqual(params["begin_date"], ["20261005"])

    def test_malformed_entries_fail_the_station_not_the_run(self):
        bad = {
            "t": "not-a-time", "v": "1.0", "type": "H",
        }
        documents = {STATION: {"predictions": [bad]}, OTHER: predictions_for(DAY, DAY, OTHER)}
        rc, out, err, _ = self.run_main(documents, self.argv() + ["--retries", "0"])
        self.assertEqual(rc, 0)
        self.assertEqual(len(out.splitlines()), 4)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertNotIn(STATION, state["stations"])
        summary = json.loads(err.split(MODULE.SUMMARY_PREFIX, 1)[1])
        self.assertEqual(summary["partitions"]["failures"], [{"station": STATION, "error": "bad tide time 'not-a-time'"}])

    def test_rejects_unknown_station_and_invalid_options_before_http(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            rc, _, _, _ = self.run_main({}, ["--stations", "9999999", "--request-delay", "0",
                                             "--state-file", str(self.state_file), "--today", DAY.isoformat()])
            self.assertEqual(rc, 1)
            urlopen.assert_not_called()
        for bad in (["--timeout", "0"], ["--retries", "6"], ["--request-delay", "-1"],
                    ["--no-state", "--state-file", str(self.state_file)]):
            with self.subTest(bad=bad):
                with self.assertRaises(SystemExit):
                    MODULE.main(self.argv() + bad)

    def test_source_yaml_contract(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertIn('name: noaa_coops_tide_predictions', text)
        self.assertIn('script: fetch_noaa_coops_tide_predictions.py', text)
        self.assertRegex(text, r'(?m)^schedule: "38 5 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn('args: ["--lookback-days", "2", "--timeout", "30", "--retries", "2", "--request-delay", "1"]', text)
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 10", text)
        self.assertIn("sink: local", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("United States government work", text)


if __name__ == "__main__":
    unittest.main()
