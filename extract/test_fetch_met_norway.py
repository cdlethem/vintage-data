import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock

SCRIPT = Path(__file__).parent / "scripts" / "fetch_met_norway.py"
SPEC = importlib.util.spec_from_file_location("fetch_met_norway", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def fixture_document(hours=3):
    rows = []
    for i in range(hours):
        rows.append(
            {
                "time": f"2026-10-12T06:{i:02d}:00Z",
                "data": {
                    "instant": {
                        "details": {
                            "air_temperature": 3.0 + i,
                            "air_pressure_at_sea_level": 1017.2,
                            "cloud_area_fraction": 9.1,
                            "relative_humidity": 84.7,
                            "wind_speed": 0.8,
                            "wind_from_direction": 9.0,
                        }
                    },
                    "next_1_hours": {
                        "summary": {"symbol_code": "clearsky_night"},
                        "details": {"precipitation_amount": 0.0},
                    },
                    "next_6_hours": {
                        "summary": {"symbol_code": "cloudy"},
                        "details": {"precipitation_amount": 1.5},
                    },
                    "next_12_hours": {
                        "summary": {"symbol_code": "lightrain"},
                        "details": {},
                    },
                },
            }
        )
    return {
        "type": "Feature",
        "properties": {
            "meta": {"updated_at": "2026-10-12T05:32:51Z", "units": {}},
            "timeseries": rows,
        },
    }


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def run_capture(**kwargs):
    """Run the fetcher with mocked HTTP; return (records, summary_dict, urlopen)."""
    kwargs.setdefault("delay", 0)
    responses = [JsonResponse(fixture_document()) for _ in MODULE.CITIES]
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            succeeded, failures = MODULE.run(**kwargs)
    records = [json.loads(line) for line in stdout.getvalue().splitlines()]
    summary_line = next(
        line for line in stderr.getvalue().splitlines()
        if line.startswith(MODULE.SUMMARY_PREFIX)
    )
    summary = json.loads(summary_line[len(MODULE.SUMMARY_PREFIX):])
    return records, summary, failures, succeeded, urlopen


class FetchMetNorwayTests(unittest.TestCase):
    def test_emits_one_record_per_forecast_hour_with_envelope(self):
        records, summary, failures, succeeded, _ = run_capture()

        self.assertEqual(succeeded, len(MODULE.CITIES))
        self.assertEqual(failures, [])
        self.assertEqual(len(records), 3 * len(MODULE.CITIES))
        record = records[0]
        slug = MODULE.CITIES[0][0]
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["id"], f"{slug}:2026-10-12T06:00:00Z")
        self.assertRegex(record["fetched_at"], r"\+00:00$")
        self.assertEqual(record["city"], slug)
        self.assertEqual(record["model_run_time"], "2026-10-12T05:32:51Z")
        self.assertEqual(record["air_temperature"], 3.0)
        self.assertEqual(record["wind_speed"], 0.8)
        self.assertEqual(record["symbol_code_1h"], "clearsky_night")
        self.assertEqual(record["symbol_code_6h"], "cloudy")
        self.assertEqual(record["symbol_code_12h"], "lightrain")
        self.assertEqual(record["precipitation_amount_6h"], 1.5)
        self.assertIsNone(record["precipitation_amount_12h"])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(
            summary["requests"],
            {"attempted": 10, "succeeded": 10, "failed": 0},
        )

    def test_uses_one_utc_fetch_timestamp_per_run(self):
        records, _, _, _, _ = run_capture()

        self.assertEqual(len({r["fetched_at"] for r in records}), 1)

    def test_makes_one_bounded_request_per_city_with_user_agent(self):
        _, _, _, _, urlopen = run_capture(timeout=17)

        self.assertEqual(urlopen.call_count, len(MODULE.CITIES))
        first = urlopen.call_args_list[0]
        request = first.args[0]
        slug, lat, lon, altitude = MODULE.CITIES[0]
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args_list[0].kwargs, {"timeout": 17})
        self.assertEqual(request.full_url, f"{MODULE.BASE}?lat={lat:.4f}&lon={lon:.4f}&altitude={altitude}")

    def test_missing_optional_blocks_become_nulls(self):
        document = fixture_document(hours=1)
        del document["properties"]["timeseries"][0]["data"]["next_6_hours"]
        stdout, stderr = io.StringIO(), io.StringIO()
        responses = [JsonResponse(document) if i == 0 else JsonResponse(fixture_document()) for i in range(len(MODULE.CITIES))]
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                succeeded, failures = MODULE.run(delay=0)
        self.assertEqual(succeeded, len(MODULE.CITIES))
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        first = records[0]
        self.assertIsNone(first["symbol_code_6h"])
        self.assertIsNone(first["precipitation_amount_6h"])

    def test_malformed_document_fails_only_that_city(self):
        responses = [JsonResponse(fixture_document()) for _ in MODULE.CITIES]
        stdout, stderr = io.StringIO(), io.StringIO()
        responses[2] = JsonResponse({"properties": {}})
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                succeeded, failures = MODULE.run(delay=0)
        self.assertEqual(succeeded, len(MODULE.CITIES) - 1)
        self.assertEqual(failures, [{"city": MODULE.CITIES[2][0], "error": MODULE.CITIES[2][0] + ": response has no timeseries"}])
        self.assertEqual(urlopen.call_count, len(MODULE.CITIES))
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(len(records), 3 * (len(MODULE.CITIES) - 1))
        self.assertNotIn(MODULE.CITIES[2][0], {r["city"] for r in records})

    def test_all_cities_fail_raises(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=OSError("connection refused")
        ):
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(RuntimeError, "all cities failed"):
                    MODULE.run(delay=0)
        self.assertEqual(stdout.getvalue(), "")

    def test_invalid_timeout_fails_before_http(self):
        for timeout in (True, 0, -1, 1.5, MODULE.MAX_TIMEOUT + 1):
            with self.subTest(timeout=timeout):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with contextlib.redirect_stdout(io.StringIO()):
                        with self.assertRaisesRegex(ValueError, "timeout"):
                            MODULE.run(delay=0, timeout=timeout)
                urlopen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
