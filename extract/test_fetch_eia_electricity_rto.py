"""Behavioral tests for fetch_eia_electricity_rto.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
import unittest.mock
import urllib.error
import urllib.parse
import yaml
from datetime import datetime, timezone

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_eia_electricity_rto.py"
SOURCE_CONFIG = pathlib.Path(__file__).parent / "sources" / "eia_electricity_rto.yml"
SPEC = importlib.util.spec_from_file_location("fetch_eia_electricity_rto", SCRIPT)
eia = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(eia)

# Latest published hour at probe time; EIA-930 trails real time by ~a day.
NOW = datetime(2026, 10, 3, 15, 25, tzinfo=timezone.utc)
LATEST = "2026-10-02T06"
LATEST_DT = datetime(2026, 10, 2, 6, tzinfo=timezone.utc)


def row(period, respondent, fueltype, value="375", **extra):
    base = {
        "period": period,
        "respondent": respondent,
        "respondent-name": f"{respondent} name",
        "fueltype": fueltype,
        "type-name": f"{fueltype} name",
        "value": value,
        "value-units": "megawatthours",
    }
    base.update(extra)
    return base


def probe(latest=LATEST):
    return {"response": {"total": 1, "data": [row(latest, "AVA", "NG")]}}


def page(rows, total):
    # EIA sends the page total as a numeric string, as it does for values.
    return {"response": {"total": str(total), "frequency": "hourly", "data": rows}}


def query(url):
    return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))


class ScriptedTransport:
    """Serves a scripted latest-probe then a scripted list of range pages in order."""

    def __init__(self, pages, *, latest=LATEST):
        self.pages = list(pages)
        self.latest = latest
        self.urls = []

    def __call__(self, url, timeout):
        self.urls.append(url)
        params = query(url)
        if params.get("sort[0][direction]") == "desc":
            return probe(self.latest)
        if not self.pages:
            raise AssertionError(f"unexpected range request: {url}")
        return self.pages.pop(0)


class FakeClock:
    def __init__(self):
        self.value = 1000.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, delay):
        self.value += delay
        self.sleeps.append(delay)


def parse_summary(stderr_text):
    lines = [
        line for line in stderr_text.splitlines()
        if line.startswith(eia.SUMMARY_PREFIX)
    ]
    assert len(lines) == 1, f"expected exactly one summary line, got {lines!r}"
    return json.loads(lines[0][len(eia.SUMMARY_PREFIX):])


def read_ndjson(text):
    return [json.loads(line) for line in text.splitlines() if line]


class FetchEiaElectricityRtoTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = pathlib.Path(self._tmp.name) / "state" / "eia_electricity_rto.json"

    def write_state(self, last_collected_period):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps({
            "version": 1,
            "source": "eia_electricity_rto",
            "last_collected_period": last_collected_period,
        }), encoding="utf-8")

    def run_fetcher(self, transport, *, state_file=None, page_size=5000):
        out = io.StringIO()
        err = io.StringIO()
        clock = FakeClock()

        def _run():
            return eia.run(
                path=None if state_file is None else (
                    self.state_file if state_file == "default" else pathlib.Path(state_file)
                ),
                output=out,
                page_size=page_size,
                transport=transport,
                now=lambda: NOW,
                sleep=clock.sleep,
                monotonic=clock.monotonic,
            )

        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            count = _run()
        return count, out.getvalue(), err.getvalue(), clock

    def test_first_run_collects_lookback_and_saves_watermark(self):
        rows = [
            row("2026-10-02T05", "AVA", "NG"),
            row("2026-10-02T05", "AVA", "SUN", value="12.5"),
            row("2026-10-02T06", "CISO", "COL"),
        ]
        transport = ScriptedTransport([page(rows, total=3)])
        count, out, err, _ = self.run_fetcher(transport, state_file="default")

        self.assertEqual(count, 3)
        records = read_ndjson(out)
        self.assertEqual(len(records), 3)
        for record in records:
            self.assertEqual(record["source"], "eia_electricity_rto")
            self.assertEqual(record["fetched_at"], "2026-10-03T15:25:00Z")
            self.assertIn("id", record)
        self.assertEqual(records[0]["id"], "2026-10-02T05|AVA|NG")
        self.assertEqual(records[1]["value_mwh"], 12.5)
        self.assertIsInstance(records[2]["value_mwh"], int)

        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_collected_period"], LATEST)

        summary = parse_summary(err)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["requests"], {"attempted": 2, "succeeded": 2})
        self.assertEqual(summary["state_change"], {"last_collected_period": LATEST})
        self.assertEqual(summary["metrics"]["records"], 3)

    def test_first_run_lookback_starts_two_days_before_latest(self):
        transport = ScriptedTransport([page([], total=0)])
        _, _, _, _ = self.run_fetcher(transport, state_file="default")

        range_url = next(u for u in transport.urls if query(u).get("start"))
        params = query(range_url)
        # latest 2026-10-02T06 minus the 2-day default lookback, plus one hour
        self.assertEqual(params["start"], "2026-09-30T07")
        self.assertEqual(params["end"], LATEST)
        self.assertEqual(params["frequency"], "hourly")
        self.assertEqual(params["length"], "5000")
        self.assertEqual(params["offset"], "0")

    def test_resume_requests_only_periods_after_watermark(self):
        self.write_state("2026-10-01T06")
        rows = [row("2026-10-01T07", "AVA", "NG"), row("2026-10-02T06", "AVA", "NG")]
        transport = ScriptedTransport([page(rows, total=2)])
        count, out, err, _ = self.run_fetcher(transport, state_file="default")

        self.assertEqual(count, 2)
        params = query(next(u for u in transport.urls if query(u).get("start")))
        self.assertEqual(params["start"], "2026-10-01T07")  # watermark + 1h, exclusive
        self.assertEqual(params["end"], LATEST)
        self.assertEqual(parse_summary(err)["state_change"]["last_collected_period"], LATEST)

    def test_no_new_periods_is_a_quiet_success(self):
        self.write_state(LATEST)
        transport = ScriptedTransport([])
        count, out, err, _ = self.run_fetcher(transport, state_file="default")

        self.assertEqual(count, 0)
        self.assertEqual(out, "")
        self.assertEqual(len(transport.urls), 1)  # probe only, no range fetch
        summary = parse_summary(err)
        self.assertEqual(summary["metrics"]["records"], 0)
        self.assertEqual(summary["state_change"], {})
        self.assertEqual(
            json.loads(self.state_file.read_text(encoding="utf-8"))["last_collected_period"],
            LATEST,
        )

    def test_no_state_does_not_read_or_write_persistence(self):
        self.write_state("2026-09-01T00")
        rows = [row("2026-10-02T06", "AVA", "NG")]
        transport = ScriptedTransport([page(rows, total=1)])
        count, _, err, _ = self.run_fetcher(transport, state_file=None)

        self.assertEqual(count, 1)
        # Without a watermark the lookback applies, so the stale file is ignored
        # and the full window is re-collected; the file must be untouched.
        self.assertEqual(
            json.loads(self.state_file.read_text(encoding="utf-8"))["last_collected_period"],
            "2026-09-01T00",
        )
        self.assertEqual(parse_summary(err)["state_change"], {})

    def test_paginates_until_total_and_catches_total_drift(self):
        page_one = [row("2026-10-02T05", f"BA{i:02d}", "NG") for i in range(3)]
        page_two = [row("2026-10-02T06", "CISO", "NG")]
        transport = ScriptedTransport([page(page_one, total=4), page(page_two, total=4)])
        count, out, _, _ = self.run_fetcher(transport, state_file=None, page_size=3)

        self.assertEqual(count, 4)
        offsets = [query(u).get("offset") for u in transport.urls if query(u).get("start")]
        self.assertEqual(offsets, ["0", "3"])

        drifted = ScriptedTransport([page(page_one, total=4), page(page_two, total=3)])
        with self.assertRaises(ValueError, msg="total changed mid-pagination"):
            self.run_fetcher(drifted, state_file=None, page_size=3)

    def test_rejects_row_outside_requested_range(self):
        transport = ScriptedTransport([page([row("2026-10-03T00", "AVA", "NG")], total=1)])
        with self.assertRaises(ValueError, msg="period outside requested range"):
            self.run_fetcher(transport, state_file=None)

    def test_rejects_non_numeric_value(self):
        transport = ScriptedTransport([page([row("2026-10-02T06", "AVA", "NG", value="abc")], total=1)])
        with self.assertRaises(ValueError, msg="non-numeric value"):
            self.run_fetcher(transport, state_file=None)

    def test_rejects_duplicate_rows(self):
        duplicate = row("2026-10-02T06", "AVA", "NG")
        transport = ScriptedTransport([page([duplicate, dict(duplicate)], total=2)])
        with self.assertRaises(ValueError, msg="duplicate row"):
            self.run_fetcher(transport, state_file=None)

    def test_rejects_bad_state_version(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps({"version": 99}), encoding="utf-8")
        with self.assertRaises(ValueError, msg="unsupported state version"):
            self.run_fetcher(ScriptedTransport([page([], total=0)]), state_file="default")

    def test_pacing_sleeps_between_requests(self):
        rows = [row("2026-10-02T05", "AVA", "NG"), row("2026-10-02T06", "AVA", "NG")]
        transport = ScriptedTransport([page(rows, total=2)])
        _, _, _, clock = self.run_fetcher(transport, state_file=None)

        # Two requests (probe + one page) at a 1.5s minimum interval.
        self.assertEqual(clock.sleeps, [eia.MIN_REQUEST_INTERVAL])

    def test_request_json_fails_fast_on_429(self):
        sleeps = []
        calls = {"n": 0}

        def urlopen(request, timeout=None):
            calls["n"] += 1
            raise urllib.error.HTTPError(
                "https://eia", 429, "throttled", {"Retry-After": "65070"}, io.BytesIO(b"")
            )

        with unittest.mock.patch.object(eia.urllib.request, "urlopen", urlopen):
            with self.assertRaises(eia.RateLimited) as ctx:
                eia.request_json("https://eia", timeout=5, sleep=sleeps.append)

        # The shared DEMO_KEY quota must not be burned by in-process retries.
        self.assertEqual(calls["n"], 1)
        self.assertEqual(sleeps, [])
        self.assertEqual(ctx.exception.retry_after, 65070.0)

    def test_429_mid_catchup_saves_partial_watermark_and_reports_degraded(self):
        page_one = [row("2026-10-02T05", f"BA{i:02d}", "NG") for i in range(3)]
        page_two = [row("2026-10-02T06", "CISO", "NG")]

        class QuotaTransport(ScriptedTransport):
            def __call__(self, url, timeout):
                params = query(url)
                if params.get("sort[0][direction]") == "desc":
                    return super().__call__(url, timeout)
                if int(params.get("offset", 0)) != 0:
                    raise eia.RateLimited(65070)
                return super().__call__(url, timeout)

        transport = QuotaTransport([page(page_one, total=4), page(page_two, total=4)])
        count, out, err, _ = self.run_fetcher(transport, state_file="default", page_size=3)

        # Only page one was collected and the 429 was not retried.
        self.assertEqual(count, 3)
        range_urls = [u for u in transport.urls if query(u).get("start")]
        self.assertEqual([query(u).get("offset") for u in range_urls], ["0"])
        self.assertEqual(len(read_ndjson(out)), 3)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_collected_period"], "2026-10-02T05")
        summary = parse_summary(err)
        self.assertEqual(summary["health"], "degraded")
        self.assertEqual(summary["completeness"], "partial")
        self.assertEqual(summary["state_change"], {"last_collected_period": "2026-10-02T05"})
        self.assertEqual(summary["metrics"]["records"], 3)
        self.assertEqual(summary["metrics"]["period_end"], "2026-10-02T05")

    def test_429_on_first_page_fails_without_state(self):
        page_one = [row("2026-10-02T05", "BA00", "NG")]

        class QuotaTransport(ScriptedTransport):
            def __call__(self, url, timeout):
                params = query(url)
                if params.get("sort[0][direction]") == "desc":
                    return super().__call__(url, timeout)
                raise eia.RateLimited(65070)

        transport = QuotaTransport([page(page_one, total=1)])
        with self.assertRaises(eia.RateLimited):
            self.run_fetcher(transport, state_file="default")
        self.assertFalse(self.state_file.exists())

    def test_request_json_gives_up_after_max_retries(self):
        def urlopen(request, timeout=None):
            raise urllib.error.HTTPError("https://eia", 503, "down", {}, io.BytesIO(b""))

        with unittest.mock.patch.object(eia.urllib.request, "urlopen", urlopen):
            with self.assertRaises(urllib.error.HTTPError):
                eia.request_json("https://eia", timeout=5, sleep=lambda _d: None)

    def test_mutually_exclusive_state_flags(self):
        with self.assertRaises(SystemExit):
            with unittest.mock.patch.object(sys, "argv",
                                            ["fetch_eia_electricity_rto.py",
                                             "--no-state", "--state-file", str(self.state_file)]):
                eia.main()

    def test_source_yaml_is_valid_configuration(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        for key in ("name", "script", "schedule", "enabled"):
            self.assertIn(key, config)
        self.assertEqual(config["name"], "eia_electricity_rto")
        self.assertTrue(
            (SCRIPT.parent / config["script"]).is_file(),
            f"script {config['script']} missing",
        )
        self.assertRegex(config["schedule"], r"^[\d*]+ [\d*]+ [\d*]+ [\d*]+ [\d*]+$")
        self.assertTrue(config["enabled"])
        self.assertIn("licence", config)
        self.assertIn("rate_limit", config)
        self.assertTrue(config["attribution_required"])


if __name__ == "__main__":
    unittest.main()
