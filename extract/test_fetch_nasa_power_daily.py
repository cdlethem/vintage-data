import contextlib
from datetime import date
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
import urllib.error
import urllib.parse
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_nasa_power_daily.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "nasa_power_daily.yml"
SPEC = importlib.util.spec_from_file_location("fetch_nasa_power_daily", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

CONFIGURED_ARGV = [
    "--latitude",
    "38.9072",
    "--longitude",
    "-77.0369",
    "--parameters",
    "T2M,T2M_MAX,T2M_MIN,PRECTOTCORR",
    "--community",
    "AG",
    "--time-standard",
    "UTC",
    "--timeout",
    "30",
    "--smoke",
]


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def power_document(
    series=None,
    *,
    fill_value=-999,
    parameter="T2M",
    units="C",
):
    if series is None:
        series = {"20260915": 22.5, "20260916": fill_value}
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-77.0369, 38.9072, 12.0]},
        "properties": {"parameter": {parameter: series}},
        "header": {
            "title": "NASA/POWER CERES/MERRA2 Native Resolution Daily Data",
            "api": {"version": "v2.8.0", "name": "POWER Daily API"},
            "sources": ["POWER", "MERRA2"],
            "fill_value": fill_value,
            "time_standard": "UTC",
        },
        "messages": [],
        "parameters": {parameter: {"units": units, "longname": "Temperature at 2 Meters"}},
    }


def strict_loads(value):
    return json.loads(
        value,
        parse_constant=lambda constant: (_ for _ in ()).throw(
            ValueError(f"non-standard JSON constant: {constant}")
        ),
    )


class FetchNasaPowerDailyTests(unittest.TestCase):
    def fetch(self, document, **overrides):
        arguments = {
            "latitude": 38.9072,
            "longitude": -77.0369,
            "parameters": "T2M",
            "community": "AG",
            "time_standard": "UTC",
            "start": "20260915",
            "end": "20260916",
            "timeout": 17,
        }
        arguments.update(overrides)
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            records = MODULE.fetch_daily(**arguments)
        return records, urlopen

    def run_main(self, document, argv=None):
        stdout = io.StringIO()
        stderr = io.StringIO()
        if argv is None:
            argv = [
                "--latitude",
                "38.9072",
                "--longitude",
                "-77.0369",
                "--parameters",
                "T2M",
                "--start",
                "20260915",
                "--end",
                "20260916",
            ]
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.main(argv)
        return stdout.getvalue(), stderr.getvalue()

    def assert_main_rejects_without_output(self, document, message):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    MODULE.main(
                        [
                            "--parameters",
                            "T2M",
                            "--start",
                            "20260915",
                            "--end",
                            "20260916",
                        ]
                    )
        self.assertEqual(raised.exception.code, 2)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn(message, stderr.getvalue())

    def test_normalizes_observations_envelope_metadata_and_fill_values(self):
        records, _ = self.fetch(power_document())

        self.assertEqual(len(records), 2)
        first, missing = records
        self.assertEqual(first["source"], "nasa_power_daily")
        self.assertRegex(first["fetched_at"], r"\+00:00$")
        self.assertEqual(first["date"], "2026-09-15")
        self.assertEqual(first["parameter"], "T2M")
        self.assertEqual(first["value"], 22.5)
        self.assertEqual(first["units"], "C")
        self.assertEqual(first["fill_value"], -999)
        self.assertEqual(first["latitude"], 38.9072)
        self.assertEqual(first["longitude"], -77.0369)
        self.assertEqual(
            first["request"],
            {
                "latitude": 38.9072,
                "longitude": -77.0369,
                "parameters": ["T2M"],
                "community": "AG",
                "time_standard": "UTC",
                "start": "20260915",
                "end": "20260916",
            },
        )
        self.assertEqual(first["parameter_metadata"]["longname"], "Temperature at 2 Meters")
        self.assertEqual(first["header"]["sources"], ["POWER", "MERRA2"])
        self.assertIsNone(missing["value"])
        self.assertEqual(first["fetched_at"], missing["fetched_at"])

    def test_genuine_numeric_fill_sentinels_normalize_to_null_and_emit_strict_json(self):
        for fill_value, observed in ((1, 1), (0, 0), (-999.0, -999)):
            with self.subTest(fill_value=fill_value, observed=observed):
                document = power_document(
                    {"20260915": observed, "20260916": None},
                    fill_value=fill_value,
                )
                stdout, stderr = self.run_main(document)
                self.assertEqual(stderr, "")
                lines = stdout.splitlines()
                self.assertEqual(len(lines), 2)
                parsed = [strict_loads(line) for line in lines]
                self.assertEqual([record["value"] for record in parsed], [None, None])
                self.assertEqual([record["fill_value"] for record in parsed], [fill_value, fill_value])

    def test_rejects_non_finite_fill_sentinels_visibly_without_invalid_ndjson(self):
        for fill_value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(fill_value=fill_value):
                self.assert_main_rejects_without_output(
                    power_document(fill_value=fill_value),
                    "header.fill_value must be a finite number",
                )

    def test_rejects_boolean_fill_sentinels_visibly_without_output(self):
        for fill_value in (True, False):
            with self.subTest(fill_value=fill_value):
                self.assert_main_rejects_without_output(
                    power_document(fill_value=fill_value),
                    "header.fill_value must be a finite number",
                )

    def test_rejects_boolean_observations_before_fill_comparison(self):
        for fill_value, observed in ((1, True), (0, False)):
            with self.subTest(fill_value=fill_value, observed=observed):
                self.assert_main_rejects_without_output(
                    power_document(
                        {"20260915": observed, "20260916": fill_value},
                        fill_value=fill_value,
                    ),
                    "observation 20260915 must be numeric or null",
                )

    def test_rejects_other_invalid_observation_types_and_non_finite_values(self):
        for value in ("22.5", [], {}, float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                message = "must be finite or null" if isinstance(value, float) else "must be numeric or null"
                self.assert_main_rejects_without_output(
                    power_document({"20260915": value}),
                    message,
                )

    def test_request_has_explicit_bounds_format_headers_and_settings(self):
        records, urlopen = self.fetch(power_document({}), timeout=17)

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        parsed = urllib.parse.urlparse(request.full_url)
        query = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "power.larc.nasa.gov")
        self.assertEqual(query["parameters"], ["T2M"])
        self.assertEqual(query["community"], ["AG"])
        self.assertEqual(query["latitude"], ["38.9072"])
        self.assertEqual(query["longitude"], ["-77.0369"])
        self.assertEqual(query["start"], ["20260915"])
        self.assertEqual(query["end"], ["20260916"])
        self.assertEqual(query["format"], ["JSON"])
        self.assertEqual(query["time-standard"], ["UTC"])
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_explicit_date_range_is_inclusive_and_allows_31_days(self):
        series = {"20260101": 10, "20260131": 20}
        records, urlopen = self.fetch(
            power_document(series),
            start="20260101",
            end="20260131",
        )
        self.assertEqual([record["date"] for record in records], ["2026-01-01", "2026-01-31"])
        query = urllib.parse.parse_qs(urllib.parse.urlparse(urlopen.call_args.args[0].full_url).query)
        self.assertEqual(query["start"], ["20260101"])
        self.assertEqual(query["end"], ["20260131"])

    def test_invalid_request_bounds_fail_before_http(self):
        cases = (
            ({"latitude": True}, "latitude must be a finite number"),
            ({"latitude": 91}, "latitude must be between"),
            ({"longitude": -181}, "longitude must be between"),
            ({"start": "20260230"}, "valid YYYYMMDD"),
            ({"start": "20260917", "end": "20260916"}, "start must not be later"),
            ({"start": "20260101", "end": "20260201"}, "must not exceed 31 days"),
            ({"timeout": 0}, "timeout must be a positive"),
        )
        for overrides, message in cases:
            with self.subTest(overrides=overrides):
                arguments = {
                    "parameters": "T2M",
                    "start": "20260915",
                    "end": "20260916",
                }
                arguments.update(overrides)
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.fetch_daily(**arguments)
                urlopen.assert_not_called()

    def test_stable_ids_ignore_fetch_time_value_units_and_fill_value(self):
        first, _ = self.fetch(power_document({"20260915": 20}, fill_value=-999, units="C"))
        second, _ = self.fetch(power_document({"20260915": 68}, fill_value=-888, units="F"))

        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertNotEqual(first[0]["value"], second[0]["value"])

    def test_ids_distinguish_point_parameter_and_date(self):
        base = MODULE._stable_id(38.9072, -77.0369, "T2M", "20260915")
        variants = {
            MODULE._stable_id(38.9073, -77.0369, "T2M", "20260915"),
            MODULE._stable_id(38.9072, -77.0370, "T2M", "20260915"),
            MODULE._stable_id(38.9072, -77.0369, "T2M_MAX", "20260915"),
            MODULE._stable_id(38.9072, -77.0369, "T2M", "20260916"),
        }
        self.assertNotIn(base, variants)
        self.assertEqual(len(variants), 4)

    def test_empty_parameter_series_emits_no_records(self):
        records, _ = self.fetch(power_document({}))
        self.assertEqual(records, [])

    def test_rejects_malformed_responses(self):
        valid = power_document()
        malformed = (
            ([], TypeError, "must be a JSON object"),
            ({}, TypeError, "missing header object"),
            ({**valid, "header": []}, TypeError, "missing header object"),
            ({**valid, "header": {}}, ValueError, "header.fill_value"),
            ({**valid, "properties": {}}, TypeError, "properties.parameter"),
            ({**valid, "properties": {"parameter": {"T2M": []}}}, TypeError, "missing series"),
            ({**valid, "parameters": []}, TypeError, "parameters metadata"),
            ({**valid, "parameters": {}}, TypeError, "missing metadata"),
            (
                {**valid, "parameters": {"T2M": {"units": ""}}},
                ValueError,
                "missing units",
            ),
            (
                {**valid, "properties": {"parameter": {"T2M": {"bad-date": 1}}}},
                ValueError,
                "observation date",
            ),
            (
                {**valid, "properties": {"parameter": {"T2M": {"20260917": 1}}}},
                ValueError,
                "outside the requested range",
            ),
        )
        for document, exception, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(exception, re.escape(message)):
                    self.fetch(document)

    def test_malformed_response_fails_through_entry_point_without_partial_ndjson(self):
        document = power_document({"20260915": 22, "20260916": "bad"})
        self.assert_main_rejects_without_output(document, "must be numeric or null")

    def test_http_failures_remain_visible_and_emit_no_output(self):
        error = urllib.error.HTTPError(MODULE.API_URL, 503, "unavailable", {}, None)
        stdout = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
            with contextlib.redirect_stdout(stdout):
                with self.assertRaises(urllib.error.HTTPError):
                    MODULE.main(
                        [
                            "--parameters",
                            "T2M",
                            "--start",
                            "20260915",
                            "--end",
                            "20260916",
                        ]
                    )
        error.close()
        self.assertEqual(stdout.getvalue(), "")

    def test_configured_smoke_uses_previous_complete_utc_day_and_no_state_writes(self):
        parameters = MODULE.DEFAULT_PARAMETERS
        document = power_document({"20251231": 12}, parameter=parameters[0])
        document["properties"]["parameter"].update({name: {"20251231": 1} for name in parameters[1:]})
        document["parameters"].update(
            {name: {"units": "unit", "longname": name} for name in parameters[1:]}
        )
        stdout = io.StringIO()
        with mock.patch.object(MODULE, "_utc_today", return_value=date(2026, 1, 1)):
            with mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(document),
            ) as urlopen:
                with mock.patch("builtins.open") as file_open:
                    with contextlib.redirect_stdout(stdout):
                        MODULE.main(CONFIGURED_ARGV)
        file_open.assert_not_called()
        query = urllib.parse.parse_qs(urllib.parse.urlparse(urlopen.call_args.args[0].full_url).query)
        self.assertEqual(query["start"], ["20251231"])
        self.assertEqual(query["end"], ["20251231"])
        self.assertEqual(query["time-standard"], ["UTC"])
        self.assertEqual(len(stdout.getvalue().splitlines()), 4)
        for line in stdout.getvalue().splitlines():
            strict_loads(line)

    def test_smoke_and_explicit_dates_are_mutually_exclusive(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            MODULE._resolved_range("20260915", "20260916", True)

    def test_source_configuration_stays_daily_disabled_and_matches_regression_argv(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        configured_args = json.loads(re.search(r"(?m)^args: (\[.*\])$", text).group(1))

        self.assertEqual(configured_args, CONFIGURED_ARGV)
        self.assertRegex(text, r'(?m)^schedule: "23 10 \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIsNone(re.search(r"(?m)^enabled:\s*true$", text))
        self.assertIn("attribution_required: true", text)
        self.assertIn("one bounded serial request", text)
        self.assertIn("read-only Airflow import check", text)


if __name__ == "__main__":
    unittest.main()
