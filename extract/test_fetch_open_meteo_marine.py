import contextlib
import importlib.util
import io
import json
from pathlib import Path
import urllib.parse
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_open_meteo_marine.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "open_meteo_marine.yml"
SPEC = importlib.util.spec_from_file_location("fetch_open_meteo_marine", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def point_response(point, times, variables):
    """Build one Open-Meteo marine location response for the given times."""
    hourly = {"time": list(times)}
    units = {"time": "iso8601"}
    for index, variable in enumerate(variables):
        hourly[variable] = [10.0 + index + 0.5 for _ in times]
        units[variable] = "m" if "height" in variable else ("s" if "period" in variable else "unit")
    return {
        "latitude": point.latitude,
        "longitude": point.longitude,
        "elevation": 0.0,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "hourly": hourly,
        "hourly_units": units,
    }


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


TIMES = ["2026-10-16T22:00", "2026-10-16T23:00", "2026-10-17T00:00"]
POINT = MODULE.Point("mid_atlantic", "Mid-Atlantic", 40.0, -40.0)
POINT_B = MODULE.Point("indian_ocean", "Indian Ocean", -20.0, 80.0)



class FetchOpenMeteoMarineTests(unittest.TestCase):
    def fetch(self, document, **kwargs):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)
        ) as urlopen:
            records = list(
                MODULE.fetch_marine(
                    points=(POINT,),
                    fetched_at="2026-10-17T00:27:00+00:00",
                    **kwargs,
                )
            )
        return records, urlopen

    def test_emits_one_record_per_valid_hour_with_stable_revision_id(self):
        records, _ = self.fetch(point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES))

        self.assertEqual(len(records), len(TIMES))
        record = records[0]
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["fetched_at"], "2026-10-17T00:27:00+00:00")
        self.assertEqual(record["valid_at"], "2026-10-16T22:00:00+00:00")
        self.assertEqual(record["point_id"], "mid_atlantic")
        self.assertEqual(record["point_name"], "Mid-Atlantic")
        self.assertEqual(record["latitude"], 40.0)
        self.assertEqual(record["longitude"], -40.0)
        self.assertEqual(record["elevation"], 0.0)
        self.assertEqual(record["utc_offset_seconds"], 0)
        self.assertEqual(record["provider"]["model"], "best_match")
        self.assertEqual(record["units"]["wave_height"], "m")
        self.assertEqual(record["units"]["wave_period"], "s")
        self.assertEqual(
            record["id"],
            MODULE._stable_revision_id("mid_atlantic", record["valid_at"], record["fetched_at"]),
        )
        self.assertEqual(
            record["marine"],
            {
                "wave_height": 10.5,
                "wave_period": 11.5,
                "wave_direction": 12.5,
                "wind_wave_height": 13.5,
                "swell_wave_height": 14.5,
                "sea_surface_temperature": 15.5,
            },
        )

    def test_uses_one_utc_collection_timestamp_per_run(self):
        records, _ = self.fetch(point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES))

        self.assertEqual(len({r["fetched_at"] for r in records}), 1)
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")

    def test_makes_one_bounded_request_with_configured_headers_and_window(self):
        records, urlopen = self.fetch(point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES), timeout=17)

        self.assertEqual(len(records), 3)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertTrue(request.full_url.startswith(MODULE.API_URL + "?"))
        query = {
            urllib.parse.unquote(k): urllib.parse.unquote(v)
            for k, v in urllib.parse.parse_qsl(request.full_url.split("?", 1)[1])
        }
        self.assertEqual(query["latitude"], "40.0")
        self.assertEqual(query["longitude"], "-40.0")
        self.assertEqual(query["hourly"], ",".join(MODULE.HOURLY_VARIABLES))
        self.assertEqual(query["past_days"], "1")
        self.assertEqual(query["forecast_days"], "1")
        self.assertEqual(query["timezone"], "UTC")
        self.assertEqual(query["models"], "best_match")
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 17})

    def test_batches_points_and_delays_between_batches(self):
        three = (POINT, POINT_B, MODULE.Point("north_sea", "North Sea", 58.0, 3.0))
        documents = [
            [point_response(p, TIMES, MODULE.HOURLY_VARIABLES) for p in (POINT, POINT_B)],
            [point_response(three[2], TIMES, MODULE.HOURLY_VARIABLES)],
        ]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[JsonResponse(documents[0]), JsonResponse(documents[1])]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(
                MODULE.fetch_marine(
                    points=three,
                    batch_size=2,
                    request_delay=1.5,
                    fetched_at="2026-10-17T00:27:00+00:00",
                )
            )

        self.assertEqual(len(records), 9)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(1.5)

    def test_invalid_window_and_transport_parameters_fail_before_http(self):
        bad_windows = (
            dict(past_days=True),
            dict(past_days=0, forecast_days=0),
            dict(past_days=3),
            dict(forecast_days=-1),
            dict(forecast_days=3),
            dict(batch_size=0),
            dict(batch_size=11),
            dict(batch_size=True),
            dict(timeout=0),
            dict(timeout=float("nan")),
            dict(request_delay=-1),
        )
        for kwargs in bad_windows:
            with self.subTest(kwargs=kwargs):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex((TypeError, ValueError), "."):
                        list(
                            MODULE.fetch_marine(
                                points=(POINT,),
                                fetched_at="2026-10-17T00:27:00+00:00",
                                **kwargs,
                            )
                        )
                urlopen.assert_not_called()

    def test_rejects_duplicate_points_and_unknown_variables_before_http(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(ValueError, "duplicate point id"):
                list(
                    MODULE.fetch_marine(
                        points=(POINT, MODULE.Point("mid_atlantic", "Copy", 41.0, -40.0)),
                        fetched_at="2026-10-17T00:27:00+00:00",
                    )
                )
            with self.assertRaisesRegex(ValueError, "not allowlisted"):
                list(
                    MODULE.fetch_marine(
                        points=(POINT,),
                        variables=("wave_height", "sea_surface_height"),
                        fetched_at="2026-10-17T00:27:00+00:00",
                    )
                )
        urlopen.assert_not_called()

    def test_rejects_malformed_response_documents(self):
        good = point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES)
        malformed = (
            ([], "returned 0 point responses"),
            ("not an object", "must be a JSON list"),
            ({"error": True, "reason": "boom"}, "Open-Meteo request failed: boom"),
            ([{**good, "error": True, "reason": "point failed"}], "point response 0 failed"),
            ([{**good, "latitude": 91.0}], "outside valid bounds"),
            ([{**good, "utc_offset_seconds": 3600}], "not UTC"),
            ([{**good, "timezone": "Etc/GMT+1"}], "timezone must be GMT"),
            ([{**good, "hourly": None}], "missing hourly object"),
            ([{**good, "hourly_units": None}], "missing hourly_units object"),
            (
                [{**good, "hourly": {**good["hourly"], "time": [TIMES[0], TIMES[1], TIMES[0]]}}],
                "duplicate valid times",
            ),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(
                    (TypeError, ValueError, MODULE.OpenMeteoAPIError), message
                ):
                    self.fetch(document)

    def test_rejects_truncated_series_non_finite_values_and_bad_timestamps(self):
        truncated = point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES)
        truncated["hourly"]["wave_height"].pop()
        with self.assertRaisesRegex(ValueError, "expected 3"):
            self.fetch(truncated)

        nan = point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES)
        nan["hourly"]["sea_surface_temperature"][1] = float("nan")
        with self.assertRaisesRegex(ValueError, "finite number"):
            self.fetch(nan)

        boolean = point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES)
        boolean["hourly"]["wave_direction"][0] = True
        with self.assertRaises(TypeError):
            self.fetch(boolean)

        bad_time = point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES)
        bad_time["hourly"]["time"][2] = "not a timestamp"
        with self.assertRaisesRegex(ValueError, "ISO-8601"):
            self.fetch(bad_time)

    def test_accepts_null_values_as_missing_data(self):
        document = point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES)
        document["hourly"]["wave_period"][1] = None

        records, _ = self.fetch(document)

        self.assertIsNone(records[1]["marine"]["wave_period"])
        self.assertEqual(records[1]["marine"]["wave_height"], 10.5)

    def test_accepts_offset_bearing_and_zulu_timestamps_as_utc(self):
        document = point_response(
            POINT,
            ["2026-10-16T22:00:00+00:00", "2026-10-16T23:00:00Z", "2026-10-17T02:00:00+02:00"],
            MODULE.HOURLY_VARIABLES,
        )

        records, _ = self.fetch(document)

        self.assertEqual(
            [r["valid_at"] for r in records],
            [
                "2026-10-16T22:00:00+00:00",
                "2026-10-16T23:00:00+00:00",
                "2026-10-17T00:00:00+00:00",
            ],
        )

    def test_main_emits_valid_ndjson_and_run_summary(self):
        document = point_response(POINT, TIMES, MODULE.HOURLY_VARIABLES)
        stdout, stderr = io.StringIO(), io.StringIO()

        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                MODULE.main(["--point", "mid_atlantic,Mid-Atlantic,40.0,-40.0", "--timeout", "23"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 3)
        for line in lines:
            record = json.loads(line)
            self.assertEqual(record["source"], MODULE.SOURCE)
            self.assertIn("id", record)
            self.assertIn("fetched_at", record)

        summary_lines = [
            line
            for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        self.assertEqual(len(summary_lines), 1)
        summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX) :])
        self.assertEqual(summary["source"], MODULE.SOURCE)
        self.assertEqual(summary["records"], 3)
        self.assertEqual(summary["points"], 1)
        self.assertEqual(summary["variables"], 6)
        self.assertEqual(summary["past_days"], 1)
        self.assertEqual(summary["forecast_days"], 1)

    def test_source_configuration_is_hourly_enabled_and_attribution_cleared(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "27 \* \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: true$")
        self.assertIn(
            'args: ["--past-days", "1", "--forecast-days", "1", "--batch-size", "10", "--timeout", "30", "--request-delay", "1"]',
            text,
        )
        self.assertIn("retries: 1", text)
        self.assertIn("timeout_minutes: 5", text)
        self.assertIn("48-hour sliding window", text)
        self.assertIn("captured twice", text)
        self.assertIn("600 calls/minute", text)
        self.assertIn("24/day", text)
        self.assertIn("CC BY 4.0", text)
        self.assertIn("attribution_required: true", text)
        self.assertIn("Open-Meteo Marine API", text)


if __name__ == "__main__":
    unittest.main()
