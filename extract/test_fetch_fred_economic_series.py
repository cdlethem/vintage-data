import contextlib
import importlib.util
import io
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_fred_economic_series.py"
SPEC = importlib.util.spec_from_file_location(
    "fetch_fred_economic_series", SCRIPT_PATH
)
fetch_fred = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fetch_fred)

WINDOW_END = date(2026, 11, 13)
WINDOW_START = WINDOW_END - timedelta(days=180)


def csv_body(series_id, rows):
    return "observation_date,{0}\n{1}".format(
        series_id, "\n".join("{0},{1}".format(d, v) for d, v in rows)
    )


def parse(series_id, rows):
    return fetch_fred.parse_series_csv(
        series_id,
        csv_body(series_id, rows),
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        fetched_at="2026-11-13T12:00:00+00:00",
    )


class CsvResponse(io.BytesIO):
    def __init__(self, text):
        super().__init__(text.encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class ParseSeriesCsvTests(unittest.TestCase):
    def test_parses_observation_with_stable_id_and_series_name(self):
        records, missing = parse(
            "UNRATE", [("2026-11-06", "4.2"), ("2026-11-07", "4.2")]
        )

        self.assertEqual(missing, 0)
        self.assertEqual(len(records), 2)
        record = records[0]
        self.assertEqual(record["source"], "fred_economic_series")
        self.assertEqual(record["fetched_at"], "2026-11-13T12:00:00+00:00")
        self.assertEqual(record["id"], "UNRATE:2026-11-06")
        self.assertEqual(record["series_id"], "UNRATE")
        self.assertEqual(record["series_name"], "Unemployment Rate")
        self.assertEqual(record["date"], "2026-11-06")
        self.assertEqual(record["value"], 4.2)
        self.assertEqual(records[1]["id"], "UNRATE:2026-11-07")

    def test_skips_missing_values_and_counts_them(self):
        records, missing = parse(
            "T10Y2Y", [("2026-11-11", ""), ("2026-11-12", "."), ("2026-11-13", "2.04")]
        )

        self.assertEqual(missing, 2)
        self.assertEqual([r["date"] for r in records], ["2026-11-13"])
        self.assertEqual(records[0]["value"], 2.04)

    def test_drops_out_of_window_rows_when_server_ignores_range(self):
        records, missing = parse(
            "DTWEXBGS",
            [
                ("2006-01-02", "101.4155"),
                ("2015-06-30", "98.3210"),
                ("2026-11-05", "121.8803"),
                ("2026-11-06", "121.8803"),
            ],
        )

        self.assertEqual(missing, 0)
        self.assertEqual(
            [r["date"] for r in records], ["2026-11-05", "2026-11-06"]
        )

    def test_rejects_future_dated_observation(self):
        with self.assertRaisesRegex(RuntimeError, "future-dated"):
            parse("DFF", [("2026-11-14", "4.33")])

    def test_rejects_unexpected_header(self):
        for body in (
            "date,UNRATE\n2026-11-06,4.2\n",
            "observation_date,CPIAUCSL\n2026-11-06,331.5\n",
            "",
        ):
            with self.subTest(body=body):
                with self.assertRaisesRegex(RuntimeError, "unexpected CSV header"):
                    fetch_fred.parse_series_csv(
                        "UNRATE",
                        body,
                        window_start=WINDOW_START,
                        window_end=WINDOW_END,
                        fetched_at="2026-11-13T12:00:00+00:00",
                    )

    def test_rejects_malformed_rows(self):
        cases = (
            (("2026-11-06", "4.2,9"), "malformed row"),
            (("2026/11/06", "4.2"), "invalid date"),
            (("2026-13-06", "4.2"), "invalid date"),
            (("2026-11-06", "4.2abc"), "non-numeric"),
            (("2026-11-06", "nan"), "non-finite"),
            (("2026-11-06", "inf"), "non-finite"),
        )
        for rows, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(RuntimeError, message):
                    parse("UNRATE", [rows])

    def test_rejects_empty_window(self):
        with self.assertRaisesRegex(RuntimeError, "no observations in the window"):
            parse("GDP", [("2020-09-30", "21428.0")])


class FetchFredEconomicSeriesTests(unittest.TestCase):
    def _responses(self):
        today = date(2026, 11, 13)
        return [
            CsvResponse(csv_body(series_id, [(today.isoformat(), "1.0")]))
            for series_id in fetch_fred.SERIES_IDS
        ]

    def test_default_basket_hits_one_request_per_series_with_range_params(self):
        responses = self._responses()
        with patch.object(
            fetch_fred.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            records = list(
                fetch_fred.fetch_fred_economic_series(
                    window_start=WINDOW_START, window_end=WINDOW_END
                )
            )

        self.assertEqual(len(records), 6)
        self.assertEqual(urlopen.call_count, 6)
        self.assertEqual(
            [r["series_id"] for r in records], list(fetch_fred.SERIES_IDS)
        )
        request = urlopen.call_args_list[0].args[0]
        self.assertEqual(
            request.full_url,
            "https://fred.stlouisfed.org/graph/fredgraph.csv"
            "?id=UNRATE&cosd=2026-05-17&coed=2026-11-13",
        )
        self.assertEqual(request.headers["User-agent"], fetch_fred.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 30})

    def test_rejects_non_allowlisted_series_before_http(self):
        with patch.object(fetch_fred.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(ValueError, "not allowlisted"):
                list(
                    fetch_fred.fetch_fred_economic_series(
                        window_start=WINDOW_START,
                        window_end=WINDOW_END,
                        series_ids=("UNRATE", "BOGUS"),
                    )
                )
        urlopen.assert_not_called()

    def test_http_failures_propagate(self):
        error = urllib.error.HTTPError(fetch_fred.URL, 404, "not found", {}, None)
        with patch.object(fetch_fred.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                list(
                    fetch_fred.fetch_fred_economic_series(
                        window_start=WINDOW_START,
                        window_end=WINDOW_END,
                        series_ids=("UNRATE",),
                    )
                )
        self.assertEqual(caught.exception.code, 404)


class MainTests(unittest.TestCase):
    def test_main_emits_contract_ndjson(self):
        today = datetime.now(timezone.utc).date()
        responses = [
            CsvResponse(csv_body("UNRATE", [(today.isoformat(), "4.2")]))
        ] + [
            CsvResponse(csv_body(series_id, [(today.isoformat(), "1.0")]))
            for series_id in fetch_fred.SERIES_IDS[1:]
        ]
        stdout = io.StringIO()
        with patch.object(
            fetch_fred.urllib.request, "urlopen", side_effect=responses
        ):
            with contextlib.redirect_stdout(stdout):
                fetch_fred.main([])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 6)
        record = json.loads(lines[0])
        self.assertEqual(record["id"], f"UNRATE:{today.isoformat()}")
        self.assertEqual(record["value"], 4.2)
        self.assertEqual(
            [json.loads(line)["series_id"] for line in lines],
            list(fetch_fred.SERIES_IDS),
        )


if __name__ == "__main__":
    unittest.main()
