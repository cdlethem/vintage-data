import contextlib
from datetime import date, datetime, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_nasa_power_daily.py"
SOURCE_PATH = Path(__file__).parent / "sources" / "nasa_power_daily.yml"
SPEC = importlib.util.spec_from_file_location("fetch_nasa_power_daily", SCRIPT_PATH)
fetch_power = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fetch_power)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(parameter_series, *, fill_value=-999.0, units=None):
    if units is None:
        units = {parameter: "unit" for parameter in parameter_series}
    return {
        "type": "Feature",
        "properties": {"parameter": parameter_series},
        "header": {"fill_value": fill_value},
        "parameters": {
            parameter: {"longname": parameter, "units": units[parameter]}
            for parameter in parameter_series
        },
    }


def metadata(start="2026-08-01", end="2026-08-01"):
    return fetch_power.request_metadata(
        fetch_power.parse_date(start),
        fetch_power.parse_date(end),
        38.9072,
        -77.0369,
        "AG",
        ("T2M", "PRECTOTCORR"),
        "JSON",
        "UTC",
    )


def configured_args():
    for line in SOURCE_PATH.read_text(encoding="utf-8").splitlines():
        if line.startswith("args: "):
            return json.loads(line.removeprefix("args: "))
    raise AssertionError("source configuration has no args")


class FetchNasaPowerDailyTests(unittest.TestCase):
    def test_normalizes_values_units_fill_values_and_stable_ids(self):
        document = response(
            {
                "T2M": {"20260801": 24.5, "20260802": -999.0},
                "PRECTOTCORR": {"20260801": 3.25, "20260802": 0.0},
            },
            units={"T2M": "C", "PRECTOTCORR": "mm/day"},
        )
        records = list(
            fetch_power.parse_response(
                document, metadata(end="2026-08-02"), "2026-08-03T00:01:02+00:00"
            )
        )

        self.assertEqual(
            [record["id"] for record in records],
            ["AG:38.9072:-77.0369:UTC:20260801", "AG:38.9072:-77.0369:UTC:20260802"],
        )
        self.assertEqual(records[0]["source"], "nasa_power_daily")
        self.assertEqual(records[0]["fetched_at"], "2026-08-03T00:01:02+00:00")
        self.assertEqual(records[0]["date"], "2026-08-01")
        self.assertEqual(records[0]["values"], {"T2M": 24.5, "PRECTOTCORR": 3.25})
        self.assertEqual(records[1]["values"], {"T2M": None, "PRECTOTCORR": 0.0})
        self.assertEqual(records[0]["units"], {"T2M": "C", "PRECTOTCORR": "mm/day"})
        self.assertEqual(records[0]["fill_value"], -999.0)
        self.assertEqual(records[0]["request"], metadata(end="2026-08-02"))

    def test_stable_id_is_independent_of_parameter_order_and_fetched_at(self):
        first = response(
            {"T2M": {"20260801": 1}, "PRECTOTCORR": {"20260801": 2}}
        )
        second = response(
            {"PRECTOTCORR": {"20260801": 2}, "T2M": {"20260801": 1}}
        )
        first_record = next(fetch_power.parse_response(first, metadata(), "first"))
        reversed_metadata = fetch_power.request_metadata(
            date(2026, 8, 1),
            date(2026, 8, 1),
            38.9072,
            -77.0369,
            "AG",
            ("PRECTOTCORR", "T2M"),
            "JSON",
            "UTC",
        )
        second_record = next(
            fetch_power.parse_response(second, reversed_metadata, "second")
        )
        self.assertEqual(first_record["id"], second_record["id"])

    def test_empty_parameter_data_emits_no_records(self):
        document = response({"T2M": {}, "PRECTOTCORR": {}})
        self.assertEqual(list(fetch_power.parse_response(document, metadata(), "stamp")), [])

    def test_rejects_malformed_responses(self):
        valid_series = {"T2M": {"20260801": 1}, "PRECTOTCORR": {"20260801": 2}}
        cases = (
            (None, "root must be an object"),
            ({}, "properties must be an object"),
            ({"properties": {}}, "properties.parameter must be an object"),
            (
                response({"T2M": {"20260801": 1}}),
                "missing parameter series PRECTOTCORR",
            ),
            (
                response(
                    {"T2M": {"20260801": 1}, "PRECTOTCORR": {"20260802": 2}}
                ),
                "inconsistent dates",
            ),
            (
                response({"T2M": {"bad": 1}, "PRECTOTCORR": {"bad": 2}}),
                "invalid observation date",
            ),
            (
                response(
                    {"T2M": {"20260801": "warm"}, "PRECTOTCORR": {"20260801": 2}}
                ),
                "must be numeric",
            ),
            (
                response(valid_series, units={"T2M": "", "PRECTOTCORR": "mm/day"}),
                "units must be a non-empty string",
            ),
        )
        for document, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(RuntimeError, message):
                    list(fetch_power.parse_response(document, metadata(), "stamp"))

    def test_request_has_explicit_bounded_point_parameters(self):
        document = response({"T2M": {}, "PRECTOTCORR": {}})
        with patch.object(
            fetch_power.urllib.request, "urlopen", return_value=JsonResponse(document)
        ) as urlopen, patch.object(
            fetch_power,
            "utc_now",
            return_value=datetime(2026, 8, 3, 1, 2, 3, tzinfo=timezone.utc),
        ):
            records = list(
                fetch_power.fetch_nasa_power_daily(
                    date(2026, 8, 1),
                    date(2026, 8, 2),
                    latitude=38.9072,
                    longitude=-77.0369,
                    community="AG",
                    parameters=("T2M", "PRECTOTCORR"),
                    output_format="JSON",
                    time_standard="UTC",
                    timeout=19,
                )
            )

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 19})
        self.assertEqual(request.headers["Accept"], "application/json")
        self.assertTrue(request.headers["User-agent"])
        parsed = urllib.parse.urlparse(request.full_url)
        self.assertEqual(f"{parsed.scheme}://{parsed.netloc}{parsed.path}", fetch_power.URL)
        self.assertEqual(
            urllib.parse.parse_qs(parsed.query),
            {
                "start": ["20260801"],
                "end": ["20260802"],
                "latitude": ["38.9072"],
                "longitude": ["-77.0369"],
                "community": ["AG"],
                "parameters": ["T2M,PRECTOTCORR"],
                "format": ["JSON"],
                "time-standard": ["UTC"],
            },
        )

    def test_oversized_and_reversed_ranges_fail_before_http(self):
        for start, end, message in (
            (date(2025, 1, 1), date(2026, 1, 2), "at most 366"),
            (date(2026, 8, 2), date(2026, 8, 1), "must not be after"),
        ):
            with self.subTest(message=message), patch.object(
                fetch_power.urllib.request, "urlopen"
            ) as urlopen:
                with self.assertRaisesRegex(ValueError, message):
                    list(
                        fetch_power.fetch_nasa_power_daily(
                            start,
                            end,
                            latitude=0,
                            longitude=0,
                            community="AG",
                            parameters=("T2M",),
                            output_format="JSON",
                            time_standard="UTC",
                        )
                    )
                urlopen.assert_not_called()

    def test_http_and_malformed_json_failures_are_visible(self):
        error = urllib.error.HTTPError(fetch_power.URL, 503, "unavailable", {}, None)
        arguments = {
            "latitude": 0,
            "longitude": 0,
            "community": "AG",
            "parameters": ("T2M",),
            "output_format": "JSON",
            "time_standard": "UTC",
        }
        with patch.object(fetch_power.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                list(fetch_power.fetch_nasa_power_daily(date(2026, 8, 1), date(2026, 8, 1), **arguments))
        self.assertEqual(caught.exception.code, 503)

        with patch.object(
            fetch_power.urllib.request,
            "urlopen",
            return_value=io.BytesIO(b"not json"),
        ):
            with self.assertRaises(json.JSONDecodeError):
                list(fetch_power.fetch_nasa_power_daily(date(2026, 8, 1), date(2026, 8, 1), **arguments))

    def test_configured_args_fetch_previous_complete_utc_day_without_state(self):
        args = configured_args()
        self.assertIn("--smoke", args)
        self.assertNotIn("--start", args)
        self.assertNotIn("--end", args)
        document = response(
            {"T2M": {"20251231": 7.5}, "PRECTOTCORR": {"20251231": -999.0}},
            units={"T2M": "C", "PRECTOTCORR": "mm/day"},
        )
        fixed_now = datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
        stdout = io.StringIO()

        with tempfile.TemporaryDirectory() as temporary_directory:
            previous_cwd = os.getcwd()
            os.chdir(temporary_directory)
            try:
                before = set(Path(".").iterdir())
                with patch.object(fetch_power, "utc_now", return_value=fixed_now), patch.object(
                    fetch_power.urllib.request,
                    "urlopen",
                    return_value=JsonResponse(document),
                ) as urlopen, contextlib.redirect_stdout(stdout):
                    fetch_power.main(args)
                after = set(Path(".").iterdir())
            finally:
                os.chdir(previous_cwd)

        self.assertEqual(after, before)
        urlopen.assert_called_once()
        query = urllib.parse.parse_qs(
            urllib.parse.urlparse(urlopen.call_args.args[0].full_url).query
        )
        self.assertEqual(query["start"], ["20251231"])
        self.assertEqual(query["end"], ["20251231"])
        self.assertEqual(query["latitude"], ["38.9072"])
        self.assertEqual(query["longitude"], ["-77.0369"])
        self.assertEqual(query["community"], ["AG"])
        self.assertEqual(query["parameters"], ["T2M,PRECTOTCORR"])
        self.assertEqual(query["format"], ["JSON"])
        self.assertEqual(query["time-standard"], ["UTC"])

        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(
            records,
            [
                {
                    "id": "AG:38.9072:-77.0369:UTC:20251231",
                    "source": "nasa_power_daily",
                    "fetched_at": "2026-01-01T00:00:01+00:00",
                    "date": "2025-12-31",
                    "latitude": 38.9072,
                    "longitude": -77.0369,
                    "community": "AG",
                    "time_standard": "UTC",
                    "values": {"T2M": 7.5, "PRECTOTCORR": None},
                    "units": {"T2M": "C", "PRECTOTCORR": "mm/day"},
                    "fill_value": -999.0,
                    "request": {
                        "start": "2025-12-31",
                        "end": "2025-12-31",
                        "latitude": 38.9072,
                        "longitude": -77.0369,
                        "community": "AG",
                        "parameters": ["T2M", "PRECTOTCORR"],
                        "format": "JSON",
                        "time_standard": "UTC",
                    },
                }
            ],
        )

    def test_main_supports_explicit_date_range(self):
        document = response({"T2M": {"20260228": 1, "20260301": 2}})
        stdout = io.StringIO()
        with patch.object(
            fetch_power.urllib.request, "urlopen", return_value=JsonResponse(document)
        ), patch.object(
            fetch_power,
            "utc_now",
            return_value=datetime(2026, 3, 2, tzinfo=timezone.utc),
        ), contextlib.redirect_stdout(stdout):
            fetch_power.main(
                [
                    "--start", "2026-02-28",
                    "--end", "2026-03-01",
                    "--latitude", "0",
                    "--longitude", "0",
                    "--community", "RE",
                    "--parameters", "T2M",
                    "--format", "JSON",
                    "--time-standard", "LST",
                ]
            )
        self.assertEqual(
            [json.loads(line)["date"] for line in stdout.getvalue().splitlines()],
            ["2026-02-28", "2026-03-01"],
        )


if __name__ == "__main__":
    unittest.main()
