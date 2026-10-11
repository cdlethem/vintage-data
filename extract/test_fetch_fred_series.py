"""Behavioral tests for fetch_fred_series.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import sys
import tempfile
import unittest
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

SCRIPT = Path(__file__).parent / "scripts" / "fetch_fred_series.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "fred_series.yml"

SPEC = importlib.util.spec_from_file_location("fetch_fred_series", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

TODAY = date(2026, 10, 11)
NOW = datetime(2026, 10, 11, 22, 45, tzinfo=timezone.utc)
SERIES_IDS = tuple(series_id for series_id, _ in MODULE.SERIES)


class FakeResponse:
    def __init__(self, body: bytes):
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def csv_body(series_id: str, rows: list[tuple[str, str]]) -> bytes:
    lines = [f"observation_date,{series_id}"]
    lines.extend(f"{obs},{value}" for obs, value in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def http_error(status: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        "https://fred.stlouisfed.org/graph/fredgraph.csv", status, "synthetic",
        {}, io.BytesIO(b"error"),
    )


class ScriptedTransport:
    """Fake urlopen: queued per-series responses, request log, window log."""

    def __init__(self, bodies: dict[str, list[object]]):
        self.bodies = {series: list(responses) for series, responses in bodies.items()}
        self.requests: list[str] = []
        self.sleep_log: list[float] = []

    def __call__(self, request, timeout=None):
        url = request.full_url
        self.requests.append(url)
        query = MODULE.urllib.parse.parse_qs(MODULE.urllib.parse.urlparse(url).query)
        series = query["id"][0]
        queue = self.bodies.get(series)
        if not queue:
            raise AssertionError(f"unexpected request for {series}: {url}")
        response = queue.pop(0)
        if isinstance(response, Exception):
            raise response
        return FakeResponse(response)

    def sleep(self, seconds: float) -> None:
        self.sleep_log.append(seconds)

    def windows(self) -> dict[str, tuple[str, str]]:
        """Requested (cosd, cefd) per series, in request order."""
        result: dict[str, tuple[str, str]] = {}
        for url in self.requests:
            query = MODULE.urllib.parse.parse_qs(
                MODULE.urllib.parse.urlparse(url).query
            )
            result[query["id"][0]] = (query["cosd"][0], query["cefd"][0])
        return result


def all_series_bodies(start: date, end: date) -> dict[str, list[object]]:
    """One happy ascending CSV per series covering [start, end]."""
    days = (end - start).days + 1
    bodies: dict[str, list[object]] = {}
    for index, series_id in enumerate(SERIES_IDS):
        rows = [
            ((start + timedelta(days=offset)).isoformat(), f"{100 + index}.{offset % 10}")
            for offset in range(days)
        ]
        bodies[series_id] = [csv_body(series_id, rows)]
    return bodies


def run_main(argv: list[str], transport: ScriptedTransport) -> tuple[int, list[dict], dict | None]:
    """Run main() in-process; return (exit_code, records, summary)."""
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", transport), \
         mock.patch("time.sleep", side_effect=transport.sleep), \
         contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            MODULE.main(argv)
            code = 0
        except SystemExit as exc:
            code = exc.code if isinstance(exc.code, int) else 1
    records = [json.loads(line) for line in stdout.getvalue().splitlines() if line.strip()]
    summary = None
    for line in stderr.getvalue().splitlines():
        if line.startswith(MODULE.SUMMARY_PREFIX):
            summary = json.loads(line[len(MODULE.SUMMARY_PREFIX):])
    return code, records, summary


class FetchFredSeriesTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.state_file = str(Path(self._tmp.name) / "fred_series.json")

    def tearDown(self):
        self._tmp.cleanup()
    def fetch_direct(self, state: dict, transport: ScriptedTransport):
        with mock.patch.object(MODULE.urllib.request, "urlopen", transport):
            return MODULE.fetch_series(
                SERIES_IDS, TODAY, 365, 14, state,
                timeout=30, retries=3, retry_delay=5.0, request_interval=3.0,
                sleep=transport.sleep, now=lambda: NOW,
            )

    def test_bootstrap_requests_full_window_per_series(self):
        start = TODAY - timedelta(days=365)
        transport = ScriptedTransport(all_series_bodies(start, TODAY))
        records, state, requested, failures = self.fetch_direct(
            {"version": 1, "watermarks": {}}, transport
        )
        self.assertEqual(requested, len(SERIES_IDS))
        self.assertEqual(failures, [])
        self.assertEqual(len(records), len(SERIES_IDS) * 366)
        windows = transport.windows()
        self.assertEqual(set(windows), set(SERIES_IDS))
        for series_id in SERIES_IDS:
            self.assertEqual(windows[series_id], (start.isoformat(), TODAY.isoformat()))
        # Envelope contract: source, fetched_at, id present and consistent.
        first = records[0]
        self.assertEqual(first["source"], "fred_series")
        self.assertEqual(first["id"], f"{SERIES_IDS[0]}:{start.isoformat()}")
        self.assertEqual(first["fetched_at"], NOW.isoformat())
        self.assertEqual(first["value"], 100.0)
        for record in records:
            self.assertEqual(record["source"], "fred_series")
            self.assertEqual(record["fetched_at"], NOW.isoformat())
            self.assertTrue(record["id"].startswith(record["series_id"] + ":"))
        # Watermarks land on the last observed date of each series.
        self.assertEqual(set(state["watermarks"]), set(SERIES_IDS))
        for series_id in SERIES_IDS:
            self.assertEqual(state["watermarks"][series_id], TODAY.isoformat())

    def test_replay_after_bootstrap_emits_nothing(self):
        start = TODAY - timedelta(days=365)
        _, state, _, _ = self.fetch_direct(
            {"version": 1, "watermarks": {}},
            ScriptedTransport(all_series_bodies(start, TODAY)),
        )
        # Second run: every series is up to date; no request is even sent.
        transport = ScriptedTransport(
            all_series_bodies(TODAY - timedelta(days=14), TODAY)
        )
        records, state_after, requested, failures = self.fetch_direct(state, transport)
        self.assertEqual(records, [])
        self.assertEqual(requested, 0)
        self.assertEqual(failures, [])
        self.assertEqual(transport.requests, [])
        self.assertEqual(state_after["watermarks"], state["watermarks"])

    def test_incremental_run_emits_only_new_dates(self):
        old = TODAY - timedelta(days=10)
        watermark_state = {
            "version": 1,
            "watermarks": {series_id: old.isoformat() for series_id in SERIES_IDS},
        }
        bodies: dict[str, list[object]] = {}
        for series_id in SERIES_IDS:
            rows = [
                ((TODAY - timedelta(days=offset)).isoformat(), f"1.{offset}")
                for offset in range(5, -1, -1)
            ]
            bodies[series_id] = [csv_body(series_id, rows)]
        transport = ScriptedTransport(bodies)
        records, state_after, requested, failures = self.fetch_direct(
            watermark_state, transport
        )
        self.assertEqual(requested, len(SERIES_IDS))
        self.assertEqual(failures, [])
        # Exactly the six dates after the watermark, for every series.
        expected = {
            f"{series_id}:{day.isoformat()}"
            for series_id in SERIES_IDS
            for day in (TODAY - timedelta(days=offset) for offset in range(6))
        }
        self.assertEqual({r["id"] for r in records}, expected)
        for series_id in SERIES_IDS:
            self.assertEqual(state_after["watermarks"][series_id], TODAY.isoformat())

    def test_missing_value_placeholder_is_null(self):
        bodies = {
            series_id: [csv_body(series_id, [("2026-10-09", "."), ("2026-10-10", "3.25")])]
            for series_id in SERIES_IDS
        }
        transport = ScriptedTransport(bodies)
        records, _, _, _ = self.fetch_direct({"version": 1, "watermarks": {}}, transport)
        by_id = {r["id"]: r["value"] for r in records}
        self.assertIsNone(by_id[f"{SERIES_IDS[0]}:2026-10-09"])
        self.assertEqual(by_id[f"{SERIES_IDS[0]}:2026-10-10"], 3.25)

    def test_trailing_empty_cell_is_unpublished_period(self):
        # A monthly series returns the current, not-yet-published period with
        # an empty cell: it must not be emitted and must not advance the
        # watermark (the value gets fetched once it publishes).
        bodies = {
            series_id: [csv_body(series_id, [("2026-09-01", "310.4"), ("2026-10-01", "")])]
            for series_id in SERIES_IDS
        }
        transport = ScriptedTransport(bodies)
        records, state, _, failures = self.fetch_direct(
            {"version": 1, "watermarks": {}}, transport
        )
        self.assertEqual(failures, [])
        ids = {r["id"] for r in records}
        self.assertIn(f"{SERIES_IDS[0]}:2026-09-01", ids)
        self.assertNotIn(f"{SERIES_IDS[0]}:2026-10-01", ids)
        for series_id in SERIES_IDS:
            self.assertEqual(state["watermarks"][series_id], "2026-09-01")

    def test_mid_window_empty_cell_is_skipped(self):
        # FRED pads monthly responses with not-yet-published periods (empty
        # cells) that can sit mid-window: those rows are skipped, the rest are
        # emitted, and the watermark stops at the last published value.
        bodies = all_series_bodies(TODAY - timedelta(days=14), TODAY)
        series_id = SERIES_IDS[1]
        bodies[series_id] = [csv_body(
            series_id,
            [("2026-10-01", "1.0"), ("2026-10-06", ""), ("2026-10-09", "2.0")],
        )]
        transport = ScriptedTransport(bodies)
        records, state, _, failures = self.fetch_direct(
            {"version": 1, "watermarks": {}}, transport
        )
        self.assertEqual(failures, [])
        ids = {r["id"] for r in records}
        self.assertIn(f"{series_id}:2026-10-01", ids)
        self.assertIn(f"{series_id}:2026-10-09", ids)
        self.assertNotIn(f"{series_id}:2026-10-06", ids)
        self.assertEqual(state["watermarks"][series_id], "2026-10-09")

    def test_out_of_window_padding_rows_are_dropped(self):
        # FRED includes anchor rows outside the requested window in monthly
        # responses; raw is insert-only, so they must not be emitted and must
        # not move the watermark.
        start = TODAY - timedelta(days=14)
        bodies = all_series_bodies(start, TODAY)
        series_id = SERIES_IDS[2]
        bodies[series_id] = [csv_body(
            series_id,
            [("1952-12-01", ""), ("2025-10-01", "1.2"), ("2026-10-01", "7.2")],
        )]
        transport = ScriptedTransport(bodies)
        records, state, _, failures = self.fetch_direct(
            {"version": 1, "watermarks": {}}, transport
        )
        self.assertEqual(failures, [])
        ids = {r["id"] for r in records}
        self.assertIn(f"{series_id}:2026-10-01", ids)
        self.assertNotIn(f"{series_id}:1952-12-01", ids)
        self.assertNotIn(f"{series_id}:2025-10-01", ids)
        self.assertEqual(state["watermarks"][series_id], "2026-10-01")

    def test_partial_failure_is_degraded_and_keeps_failed_watermark(self):
        start = TODAY - timedelta(days=14)
        bodies = all_series_bodies(start, TODAY)
        broken = SERIES_IDS[0]
        bodies[broken] = [http_error(503)] * 4  # 1 try + 3 retries
        transport = ScriptedTransport(bodies)
        code, records, summary = run_main(
            ["--state-file", self.state_file, "--request-interval", "0",
             "--retry-delay", "0"],
            transport,
        )
        self.assertEqual(code, 0)
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["requests"]["failed"], 1)
        self.assertEqual(summary["metrics"]["failures"][0]["series"], broken)
        self.assertEqual(len(records), (len(SERIES_IDS) - 1) * 15)
        state = json.loads(Path(self.state_file).read_text())
        self.assertNotIn(broken, state["watermarks"])
        self.assertEqual(state["watermarks"][SERIES_IDS[1]], TODAY.isoformat())

    def test_all_failures_exit_1_without_advancing_watermarks(self):
        start = TODAY - timedelta(days=14)
        bodies = {series_id: [http_error(503)] * 4 for series_id in SERIES_IDS}
        transport = ScriptedTransport(bodies)
        code, records, summary = run_main(
            ["--state-file", self.state_file, "--request-interval", "0",
             "--retry-delay", "0"],
            transport,
        )
        self.assertEqual(code, 1)
        self.assertEqual(records, [])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["completeness"], "failed")
        state = json.loads(Path(self.state_file).read_text())
        self.assertEqual(state["watermarks"], {})

    def test_http_404_is_not_retried(self):
        bodies = {series_id: [http_error(404)] for series_id in SERIES_IDS}
        transport = ScriptedTransport(bodies)
        code, _, _ = run_main(
            ["--no-state", "--request-interval", "0", "--retries", "3"],
            transport,
        )
        self.assertEqual(code, 1)
        # 25 series x exactly one attempt each.
        self.assertEqual(len(transport.requests), len(SERIES_IDS))

    def test_transient_429_then_success(self):
        start = TODAY - timedelta(days=14)
        bodies = all_series_bodies(start, TODAY)
        series_id = SERIES_IDS[0]
        bodies[series_id] = [http_error(429), bodies[series_id][0]]
        transport = ScriptedTransport(bodies)
        code, records, summary = run_main(
            ["--no-state", "--request-interval", "0", "--retries", "3",
             "--retry-delay", "1"],
            transport,
        )
        self.assertEqual(code, 0)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(len(records), len(SERIES_IDS) * 15)
        self.assertEqual(len(transport.requests), len(SERIES_IDS) + 1)
        self.assertEqual(len([s for s in transport.sleep_log if s > 0]), 1)  # one backoff

    def test_header_mismatch_fails_that_series_only(self):
        start = TODAY - timedelta(days=14)
        bodies = all_series_bodies(start, TODAY)
        series_id = SERIES_IDS[0]
        # FRED answering with a different (or missing) series column.
        bodies[series_id] = [csv_body("NOT_THIS_SERIES", [(TODAY.isoformat(), "1.0")])]
        transport = ScriptedTransport(bodies)
        code, records, summary = run_main(
            ["--no-state", "--request-interval", "0"],
            transport,
        )
        self.assertEqual(code, 0)
        self.assertEqual(summary["requests"]["failed"], 1)
        self.assertEqual(summary["metrics"]["failures"][0]["series"], series_id)
        self.assertNotIn(f"{series_id}:{TODAY.isoformat()}", {r["id"] for r in records})

    def test_non_ascending_dates_fail_that_series(self):
        start = TODAY - timedelta(days=14)
        bodies = all_series_bodies(start, TODAY)
        series_id = SERIES_IDS[0]
        bodies[series_id] = [csv_body(series_id, [
            (TODAY.isoformat(), "1.0"),
            ((TODAY - timedelta(days=1)).isoformat(), "2.0"),  # out of order
        ])]
        transport = ScriptedTransport(bodies)
        code, _, summary = run_main(
            ["--no-state", "--request-interval", "0"],
            transport,
        )
        self.assertEqual(code, 0)
        self.assertEqual(summary["requests"]["failed"], 1)
        self.assertEqual(summary["metrics"]["failures"][0]["series"], series_id)

    def test_corrupt_state_fails_cleanly(self):
        Path(self.state_file).write_text(
            json.dumps({"watermarks": {SERIES_IDS[0]: "not-a-date"}})
        )
        transport = ScriptedTransport({})
        code, records, summary = run_main(
            ["--state-file", self.state_file],
            transport,
        )
        self.assertEqual(code, 1)
        self.assertIsNone(summary)
        self.assertEqual(records, [])
        self.assertEqual(transport.requests, [])

    def test_no_state_and_state_file_are_mutually_exclusive(self):
        transport = ScriptedTransport({})
        code, records, summary = run_main(
            ["--no-state", "--state-file", self.state_file],
            transport,
        )
        self.assertEqual(code, 1)
        self.assertIsNone(summary)
        self.assertEqual(records, [])

    def test_no_state_run_writes_nothing(self):
        start = TODAY - timedelta(days=14)
        bodies = all_series_bodies(start, TODAY)
        transport = ScriptedTransport(bodies)
        code, records, summary = run_main(
            ["--no-state", "--request-interval", "0"],
            transport,
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(records), len(SERIES_IDS) * 15)
        self.assertFalse(Path(self.state_file).exists())


class SourceYamlTests(unittest.TestCase):
    def test_source_yaml_is_well_formed(self):
        import yaml

        config = yaml.safe_load(SOURCE_CONFIG.read_text())
        for key in ("name", "script", "schedule"):
            self.assertIn(key, config)
        self.assertEqual(config["name"], "fred_series")
        self.assertEqual(config["script"], "fetch_fred_series.py")
        self.assertTrue(SCRIPT.exists())
        cron = config["schedule"].split()
        self.assertEqual(len(cron), 5, "schedule must be a five-field UTC cron")
        self.assertTrue(config["enabled"] is True)
        for key in ("cadence_note", "rate_limit", "licence", "attribution_required"):
            self.assertIn(key, config)
        self.assertTrue(config["attribution_required"])


if __name__ == "__main__":
    unittest.main()
