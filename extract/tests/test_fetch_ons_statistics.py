import importlib.util
import io
import json
import pathlib
import socket
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_ons_statistics.py"
SPEC = importlib.util.spec_from_file_location("fetch_ons_statistics", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

URI_INDEX = "/economy/inflationandpriceindices/timeseries/d7bt/mm23"
URI_RATE = "/economy/inflationandpriceindices/timeseries/d7oe/mm23"


MONTH_FULL = {"JAN": "January", "FEB": "February", "MAR": "March", "APR": "April",
              "MAY": "May", "JUN": "June", "JUL": "July", "AUG": "August",
              "SEP": "September", "OCT": "October", "NOV": "November", "DEC": "December"}


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def month_row(year, month, value, update="2026-09-15T23:00:00.000Z"):
    label = f"{year} {month[:3].upper()}"
    return {
        "date": label,
        "value": value,
        "label": label,
        "year": str(year),
        "month": MONTH_FULL[month],
        "quarter": "",
        "sourceDataset": "MM23",
        "updateDate": update,
    }


def year_row(year, value, update="2026-01-21T00:00:00.000Z"):
    return {
        "date": str(year),
        "value": value,
        "label": str(year),
        "year": str(year),
        "month": "",
        "quarter": "",
        "sourceDataset": "MM23",
        "updateDate": update,
    }


def series_document(uri, bucket, rows, title="CPI INDEX 00: ALL ITEMS 2015=100"):
    return {
        "years": rows if bucket == "years" else [],
        "quarters": rows if bucket == "quarters" else [],
        "months": rows if bucket == "months" else [],
        "sourceDatasets": [],
        "type": "timeseries",
        "uri": uri,
        "description": {
            "title": title,
            "unit": "%",
            "preUnit": "",
            "nextRelease": "15 Oct 2026",
            "releaseDate": "15 Sep 2026",
            "datasetId": uri.rsplit("/", 2)[-2],
            "contact": {"name": "Consumer Price Inflation team"},
        },
    }


def fetch_with(document, uris, periods=36, effects=()):
    """Run fetch_ons_statistics with one canned response for the first series.

    *effects* holds extra canned responses (or exceptions) served in order
    for the remaining series.
    """
    payloads = [Response(json.dumps(document).encode()), *list(effects)]
    urlopen = mock.Mock(side_effect=lambda *a, **k: payloads.pop(0))
    with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
         mock.patch.object(MODULE.time, "sleep") as sleep:
        records, stats = MODULE.fetch_ons_statistics("ons_cpi", uris, periods, timeout=5)
    return records, stats, urlopen, sleep


class ParseSeriesTests(unittest.TestCase):
    def test_normalizes_envelope_and_bounds_to_most_recent_periods(self):
        rows = [month_row(2026, name, str(i)) for i, name in enumerate(
            ("JAN", "FEB", "MAR", "APR", "MAY", "JUN"), start=1)]
        document = series_document(URI_INDEX, "months", rows)
        records, stats, urlopen, sleep = fetch_with(document, [URI_INDEX], periods=3)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()
        self.assertEqual(stats, {
            "succeeded": 1, "failed": 0, "retries": 0,
            "payload_bytes": len(json.dumps(document).encode()),
        })
        self.assertEqual(len(records), 3)
        self.assertEqual([record["period"] for record in records],
                         ["2026 APR", "2026 MAY", "2026 JUN"])
        last = records[-1]
        self.assertEqual(last["source"], "ons_cpi")
        self.assertTrue(last["fetched_at"].endswith("Z"))
        self.assertEqual(last["id"], "d7bt:mm23:2026 JUN")
        self.assertEqual(last["value"], 6.0)
        self.assertIsInstance(last["value"], float)
        self.assertEqual(last["series"]["title"], "CPI INDEX 00: ALL ITEMS 2015=100")
        self.assertEqual(last["series"]["frequency"], "months")
        self.assertEqual(last["series"]["dataset"], "d7bt")
        self.assertEqual(last["series"]["series_id"], "mm23")
        self.assertEqual(last["update_date"], "2026-09-15T23:00:00.000Z")

    def test_empty_value_row_is_skipped(self):
        rows = [month_row(2026, "JAN", "1"), month_row(2026, "FEB", ""), month_row(2026, "MAR", "3")]
        records, *_ = fetch_with(series_document(URI_INDEX, "months", rows), [URI_INDEX])
        self.assertEqual([record["period"] for record in records], ["2026 JAN", "2026 MAR"])

    def test_years_bucket_uses_annual_periods(self):
        rows = [year_row(year, "5.2") for year in (2024, 2025)]
        records, *_ = fetch_with(series_document(URI_RATE, "years", rows, title="CPI ANNUAL RATE 00: ALL ITEMS 2015=100"), [URI_RATE])
        self.assertEqual([record["id"] for record in records], ["d7oe:mm23:2024", "d7oe:mm23:2025"])
        self.assertEqual(records[0]["series"]["frequency"], "years")
        self.assertIsNone(records[0]["month"])

    def test_multiple_series_keep_argument_order(self):
        index_rows = [month_row(2026, "AUG", "143.6")]
        rate_rows = [month_row(2026, "AUG", "0.2")]
        records, stats, urlopen, _ = fetch_with(
            series_document(URI_INDEX, "months", index_rows),
            [URI_INDEX, URI_RATE], periods=12,
            effects=[Response(json.dumps(series_document(URI_RATE, "months", rate_rows, title="CPI MONTHLY RATE 00: ALL ITEMS 2015=100")).encode())])
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(stats["succeeded"], 2)
        self.assertEqual([record["series"]["uri"] for record in records], [URI_INDEX, URI_RATE])

    def test_unsorted_input_is_chronologically_ordered(self):
        rows = [
            month_row(2026, "DEC", "3"),
            month_row(2025, "JUL", "1"),
            month_row(2026, "AUG", "2"),
        ]
        records, *_ = fetch_with(series_document(URI_INDEX, "months", rows), [URI_INDEX])
        self.assertEqual([record["period"] for record in records],
                         ["2025 JUL", "2026 AUG", "2026 DEC"])


class ValidationTests(unittest.TestCase):
    def fail_without_retries(self, document, uris=(URI_INDEX,)):
        urlopen = mock.Mock(return_value=Response(json.dumps(document).encode()))
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(MODULE.OnsFetchError):
                MODULE.fetch_ons_statistics("ons_cpi", list(uris), 36, timeout=5)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_uri_mismatch_fails(self):
        doc = series_document(URI_INDEX, "months", [month_row(2026, "AUG", "1")])
        doc["uri"] = "/economy/inflationandpriceindices/timeseries/other/mm23"
        self.fail_without_retries(doc)

    def test_non_timeseries_type_fails(self):
        doc = series_document(URI_INDEX, "months", [month_row(2026, "AUG", "1")])
        doc["type"] = "dataset"
        self.fail_without_retries(doc)

    def test_missing_title_fails(self):
        doc = series_document(URI_INDEX, "months", [month_row(2026, "AUG", "1")])
        doc["description"]["title"] = "  "
        self.fail_without_retries(doc)

    def test_empty_buckets_fail(self):
        self.fail_without_retries(series_document(URI_INDEX, "months", []))

    def test_all_values_missing_fails(self):
        rows = [month_row(2026, "AUG", ""), month_row(2026, "SEP", "")]
        self.fail_without_retries(series_document(URI_INDEX, "months", rows))

    def test_non_numeric_value_fails(self):
        rows = [month_row(2026, "AUG", "n/a")]
        self.fail_without_retries(series_document(URI_INDEX, "months", rows))

    def test_bad_uri_shape_fails(self):
        with self.assertRaises(MODULE.OnsFetchError):
            MODULE.fetch_ons_statistics("ons_cpi", ["/economy/datasets/d7bt"], 36, timeout=5)

    def test_duplicate_series_rejected(self):
        with self.assertRaises(ValueError):
            MODULE.fetch_ons_statistics("ons_cpi", [URI_INDEX, URI_INDEX], 36, timeout=5)

    def test_no_series_rejected(self):
        with self.assertRaises(ValueError):
            MODULE.fetch_ons_statistics("ons_cpi", [], 36, timeout=5)


class RetryTests(unittest.TestCase):
    def document(self):
        return Response(json.dumps(series_document(URI_INDEX, "months", [month_row(2026, "AUG", "1")])).encode())

    def test_retries_timeout_then_succeeds(self):
        urlopen = mock.Mock(side_effect=[TimeoutError("timed out"), self.document()])
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep") as sleep:
            records, stats = MODULE.fetch_ons_statistics("ons_cpi", [URI_INDEX], 36, timeout=5)
        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(stats["retries"], 1)
        sleep.assert_called_once_with(1)

    def test_retries_urllib_wrapped_timeout_then_succeeds(self):
        urlopen = mock.Mock(side_effect=[
            urllib.error.URLError(socket.timeout("timed out")),
            self.document(),
        ])
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep") as sleep:
            records, stats = MODULE.fetch_ons_statistics("ons_cpi", [URI_INDEX], 36, timeout=5)
        self.assertEqual(len(records), 1)
        sleep.assert_called_once_with(1)
        self.assertEqual(stats["retries"], 1)

    def test_timeout_exhaustion_is_bounded(self):
        urlopen = mock.Mock(side_effect=TimeoutError("timed out"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(MODULE.OnsFetchError) as ctx:
                MODULE.fetch_ons_statistics("ons_cpi", [URI_INDEX], 36, timeout=5)
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])
        self.assertEqual(ctx.exception.stats, {"succeeded": 0, "failed": 1, "retries": 0, "payload_bytes": 0})

    def test_http_error_fails_without_retries(self):
        error = urllib.error.HTTPError(MODULE.API, 404, "not found", {}, None)
        urlopen = mock.Mock(side_effect=error)
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(MODULE.OnsFetchError):
                MODULE.fetch_ons_statistics("ons_cpi", [URI_INDEX], 36, timeout=5)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_non_timeout_transport_error_fails_without_retries(self):
        urlopen = mock.Mock(side_effect=urllib.error.URLError("temporary failure in name resolution"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(MODULE.OnsFetchError):
                MODULE.fetch_ons_statistics("ons_cpi", [URI_INDEX], 36, timeout=5)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_second_series_failure_carries_partial_stats(self):
        # First series succeeds; the second times out through its full retry budget.
        urlopen = mock.Mock(side_effect=[
            self.document(),
            TimeoutError("timed out"),
            TimeoutError("timed out"),
            TimeoutError("timed out"),
        ])
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(MODULE.OnsFetchError) as ctx:
                MODULE.fetch_ons_statistics("ons_cpi", [URI_INDEX, URI_RATE], 36, timeout=5)
        self.assertEqual(urlopen.call_count, 4)  # one success + three failed attempts
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])
        self.assertEqual(ctx.exception.stats["succeeded"], 1)
        self.assertEqual(ctx.exception.stats["failed"], 1)


class MainTests(unittest.TestCase):
    def document(self):
        return Response(json.dumps(series_document(URI_INDEX, "months", [
            month_row(2026, "JUL", "143.1"), month_row(2026, "AUG", "143.6"),
        ])).encode())

    def run_main(self, argv, urlopen):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", urlopen), \
             mock.patch.object(MODULE.time, "sleep"), \
             redirect_stdout(out), redirect_stderr(err):
            status = MODULE.main(argv)
        return status, out.getvalue(), err.getvalue()

    def test_success_emits_ndjson_and_summary(self):
        urlopen = mock.Mock(side_effect=[self.document()])
        status, out, err = self.run_main(
            ["ons_cpi", "--series", URI_INDEX, "--periods", "36", "--timeout", "60"], urlopen)
        self.assertEqual(status, 0)
        lines = out.strip().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            record = json.loads(line)
            self.assertIn("source", record)
            self.assertIn("fetched_at", record)
            self.assertIn("id", record)
        summary_line = [line for line in err.splitlines() if line.startswith(MODULE.SUMMARY_PREFIX)]
        self.assertEqual(len(summary_line), 1)
        summary = json.loads(summary_line[0][len(MODULE.SUMMARY_PREFIX):])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 2)
        self.assertEqual(summary["requests"]["succeeded"], 1)

    def test_failure_returns_one_and_reports_stats(self):
        urlopen = mock.Mock(side_effect=TimeoutError("timed out"))
        status, out, err = self.run_main(
            ["ons_cpi", "--series", URI_INDEX, "--series", URI_RATE], urlopen)
        self.assertEqual(status, 1)
        self.assertEqual(out, "")
        summary_line = [line for line in err.splitlines() if line.startswith(MODULE.SUMMARY_PREFIX)]
        summary = json.loads(summary_line[0][len(MODULE.SUMMARY_PREFIX):])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["requests"]["attempted"], 2)
        self.assertEqual(summary["requests"]["failed"], 1)
        self.assertIn("timed out", summary["error"])


if __name__ == "__main__":
    unittest.main()
