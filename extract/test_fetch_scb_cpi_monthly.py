"""Behavioral tests for fetch_scb_cpi_monthly.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_scb_cpi_monthly.py"
SOURCE_CONFIG = pathlib.Path(__file__).parent / "sources" / "scb_cpi_monthly.yml"
SPEC = importlib.util.spec_from_file_location("fetch_scb_cpi_monthly", SCRIPT)
scb = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(scb)

NOW = datetime(2026, 11, 3, 5, 45, tzinfo=timezone.utc)


def p(year, month):
    return f"{year}M{month:02d}"


def add_months(period, delta):
    year, month = int(period[:4]), int(period[5:])
    index = year * 12 + month - 1 + delta
    return p(index // 12, index % 12 + 1)


def table_info(last_period):
    return {
        "id": "TAB6596",
        "label": "Consumer Price Index (CPI), total 2020=100. Month 1980M01-2026M09",
        "updated": "2026-10-14T06:00:00Z",
        "lastPeriod": last_period,
    }


def data_doc(periods, values):
    """A json-stat2 dataset: flat value array aligned to Tid category positions."""
    ordered = sorted(periods)
    index = {period: position for position, period in enumerate(ordered)}
    return {
        "class": "dataset",
        "size": [1, len(ordered)],
        "dimension": {
            "ContentsCode": {
                "category": {
                    "index": {"00000808": 0},
                    "label": {"00000808": "CPI, fixed index numbers"},
                }
            },
            "Tid": {
                "category": {
                    "index": index,
                    "label": {period: period for period in ordered},
                }
            },
        },
        "value": list(values),
    }


def window_periods(last, n):
    return [add_months(last, offset) for offset in range(-(n - 1), 1)]


class ScriptedTransport:
    """Serve the table summary and one data document, in request order."""

    def __init__(self, info, data=None, fail=None):
        self.info = info
        self.data = data
        self.fail = fail
        self.requests = []

    def __call__(self, url, timeout):
        self.requests.append(url)
        if "/data?" in url:
            if self.fail is not None:
                raise self.fail
            if self.data is None:
                raise AssertionError(f"unexpected data request: {url}")
            return self.data
        if self.info is None:
            raise AssertionError(f"unexpected table-summary request: {url}")
        return self.info


class ScbCpiMonthlyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = pathlib.Path(self._tmp.name) / "state" / "scb_cpi_monthly.json"

    def write_state(self, last_period):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps({"version": 1, "source": "scb_cpi_monthly",
                        "last_period": last_period}),
            encoding="utf-8",
        )

    def run_fetcher(self, transport, *, state_file=None, **overrides):
        out = io.StringIO()
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            count = scb.run(
                path=state_file,
                output=out,
                transport=transport,
                now=lambda: NOW,
                **overrides,
            )
        return count, out.getvalue(), err.getvalue()

    def records(self, output):
        return [json.loads(line) for line in output.splitlines() if line]

    def summary(self, stderr):
        for line in stderr.splitlines():
            if line.startswith(scb.SUMMARY_PREFIX):
                return json.loads(line[len(scb.SUMMARY_PREFIX):])
        raise AssertionError(f"no VINTAGE_RUN_SUMMARY line in: {stderr!r}")

    def test_first_run_backfills_the_initial_window(self):
        last = "2026M09"
        periods = window_periods(last, 12)
        transport = ScriptedTransport(table_info(last),
                                      data_doc(periods, [120.0 + i for i in range(12)]))
        count, output, stderr = self.run_fetcher(transport, state_file=self.state_file)

        self.assertEqual(count, 12)
        records = self.records(output)
        self.assertEqual([r["id"] for r in records], periods)  # oldest first
        self.assertEqual(len({r["fetched_at"] for r in records}), 1)  # stamped once per run
        self.assertEqual(records[0]["fetched_at"], "2026-11-03T05:45:00Z")
        self.assertEqual(records[-1]["value"], 131.0)
        self.assertEqual(records[-1]["metric"], "CPI, fixed index numbers")
        self.assertEqual(records[-1]["table_updated"], "2026-10-14T06:00:00Z")
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state, {"version": 1, "source": "scb_cpi_monthly",
                                 "last_period": last})
        summary = self.summary(stderr)
        self.assertEqual(summary["metrics"]["periods"], 12)
        self.assertEqual(summary["metrics"]["missed_months"], 0)
        self.assertEqual(summary["state_change"], {"last_period": last})

    def test_stateful_run_reemits_the_lookback_window(self):
        last = "2026M09"
        periods = window_periods(last, 3)
        transport = ScriptedTransport(table_info(last),
                                      data_doc(periods, [125.17, 124.85, 125.94]))
        self.write_state(last)
        before = self.state_file.read_text(encoding="utf-8")
        count, output, _ = self.run_fetcher(transport, state_file=self.state_file)

        self.assertEqual(count, 3)
        self.assertEqual([r["id"] for r in self.records(output)], periods)
        # Watermark did not move, so the state file is left untouched.
        self.assertEqual(self.state_file.read_text(encoding="utf-8"), before)

    def test_new_month_advances_the_watermark(self):
        periods = ["2026M08", "2026M09", "2026M10"]
        transport = ScriptedTransport(table_info("2026M10"),
                                      data_doc(periods, [125.30, 125.94, 126.41]))
        self.write_state("2026M08")
        count, output, _ = self.run_fetcher(transport, state_file=self.state_file)

        self.assertEqual(count, 3)
        self.assertEqual([r["id"] for r in self.records(output)], periods)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_period"], "2026M10")

    def test_gap_beyond_the_window_is_reported(self):
        periods = window_periods("2026M09", 3)  # 2026M07..2026M09
        transport = ScriptedTransport(table_info("2026M09"),
                                      data_doc(periods, [125.17, 124.85, 125.94]))
        self.write_state("2026M05")  # two months behind the window start
        count, output, stderr = self.run_fetcher(transport, state_file=self.state_file)

        self.assertEqual(count, 3)
        self.assertIn("were not re-collected", stderr)
        self.assertEqual(self.summary(stderr)["metrics"]["missed_months"], 1)

    def test_missing_cells_are_counted_not_emitted(self):
        periods = window_periods("2026M09", 3)
        # JSON null is what the live API returns for months before the 2020=100
        # reference series starts; "n" is the other missing marker.
        transport = ScriptedTransport(table_info("2026M09"),
                                      data_doc(periods, [None, "n", 125.94]))
        count, output, stderr = self.run_fetcher(transport, state_file=self.state_file,
                                                 initial_lookback_months=3)

        self.assertEqual(count, 1)
        self.assertEqual([r["id"] for r in self.records(output)], ["2026M09"])
        self.assertEqual(self.summary(stderr)["metrics"]["missing_values"], 2)

    def test_no_state_writes_no_file(self):
        last = "2026M09"
        periods = window_periods(last, 12)
        transport = ScriptedTransport(table_info(last),
                                      data_doc(periods, [120.0 + i for i in range(12)]))
        count, output, _ = self.run_fetcher(transport, state_file=None)

        self.assertEqual(count, 12)
        self.assertFalse(self.state_file.exists())

    def test_http_error_on_data_request_leaves_state_untouched(self):
        transport = ScriptedTransport(
            table_info("2026M09"),
            data=data_doc(window_periods("2026M09", 3), [1.0, 2.0, 3.0]),
            fail=urllib.error.HTTPError(
                "https://statistikdatabasen.scb.se/api/v2/tables/TAB6596/data",
                500, "Internal Server Error", None, None),
        )
        self.write_state("2026M09")
        with self.assertRaises(urllib.error.HTTPError):
            self.run_fetcher(transport, state_file=self.state_file)
        # The watermark must survive the failed run verbatim.
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_period"], "2026M09")

    def test_dimension_mismatch_is_rejected(self):
        # Data for 4 periods while only 3 were requested.
        transport = ScriptedTransport(
            table_info("2026M09"),
            data_doc(window_periods("2026M09", 3) + ["2026M10"], [1.0, 2.0, 3.0, 4.0]))
        with self.assertRaisesRegex(ValueError, "does not match the requested periods"):
            self.run_fetcher(transport, state_file=self.state_file)

    def test_malformed_state_is_rejected(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps(
            {"version": 9, "source": "scb_cpi_monthly", "last_period": None}),
            encoding="utf-8")
        transport = ScriptedTransport(table_info("2026M09"))
        with self.assertRaisesRegex(ValueError, "unexpected version"):
            self.run_fetcher(transport, state_file=self.state_file)

    def test_argument_bounds(self):
        for bad in (dict(lookback_months=0), dict(lookback_months=25),
                    dict(initial_lookback_months=0), dict(initial_lookback_months=25),
                    dict(timeout=0)):
            with self.assertRaises(ValueError):
                self.run_fetcher(ScriptedTransport(table_info("2026M09")),
                                 state_file=self.state_file, **bad)

    def test_source_config_references_this_fetcher(self):
        import yaml
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], scb.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertEqual(len(config["schedule"].split()), 5)
        self.assertTrue(config["enabled"])


if __name__ == "__main__":
    unittest.main()
