#!/usr/bin/env python3
import io
import pathlib
import sys
import unittest
import urllib.error
import urllib.parse
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import fetch_open_meteo_weather as weather


LOCATIONS = (
    {"location_id": "alpha", "name": "Alpha", "latitude": 43.0, "longitude": -89.0},
    {"location_id": "beta", "name": "Beta", "latitude": 44.0, "longitude": -88.0},
)
VARIABLES = ("temperature_2m", "precipitation", "weather_code")
FETCHED_AT = "2026-09-13T08:00:00Z"


def forecast(temperature, precipitation, weather_code):
    return {
        "latitude": 43.0,
        "longitude": -89.0,
        "generationtime_ms": 0.12,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 260.0,
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "precipitation": "mm",
            "weather_code": "wmo code",
        },
        "hourly": {
            "time": ["2026-09-13T09:00", "2026-09-13T10:00"],
            "temperature_2m": [temperature, temperature + 1],
            "precipitation": [precipitation, 0.0],
            "weather_code": [weather_code, 1],
        },
    }


class WeatherExtractorTests(unittest.TestCase):
    def test_parses_batched_response_with_revision_identity_and_units(self):
        rows = weather.rows_from_response(
            [forecast(12.5, 0.4, 61), forecast(15.0, 0.0, 3)],
            LOCATIONS,
            VARIABLES,
            FETCHED_AT,
        )

        self.assertEqual(len(rows), 4)
        first = rows[0]
        self.assertEqual(first["source"], "open_meteo_weather")
        self.assertEqual(first["forecast_issued_at"], FETCHED_AT)
        self.assertEqual(first["forecast_valid_at"], "2026-09-13T09:00:00Z")
        self.assertEqual(first["location_id"], "alpha")
        self.assertEqual(first["latitude"], 43.0)
        self.assertEqual(first["units"], {"temperature_2m": "°C", "precipitation": "mm", "weather_code": "wmo code"})
        self.assertEqual(first["temperature_2m"], 12.5)
        self.assertEqual(rows[2]["location_id"], "beta")
        self.assertNotEqual(first["id"], rows[2]["id"])
        later_revision = weather.rows_from_response([forecast(12.5, 0.4, 61), forecast(15.0, 0.0, 3)], LOCATIONS, VARIABLES, "2026-09-13T09:00:00Z")
        self.assertNotEqual(first["id"], later_revision[0]["id"])

    def test_empty_response_emits_no_rows(self):
        self.assertEqual(weather.rows_from_response([], LOCATIONS, VARIABLES, FETCHED_AT), [])

    def test_rejects_misaligned_hourly_values(self):
        response = forecast(12.5, 0.4, 61)
        response["hourly"]["precipitation"] = [0.4]
        with self.assertRaisesRegex(ValueError, "does not match hourly time"):
            weather.rows_from_response(response, (LOCATIONS[0],), VARIABLES, FETCHED_AT)

    def test_bounded_request_uses_utc_and_batched_coordinates(self):
        url = weather.build_url(LOCATIONS, VARIABLES, forecast_days=3)
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        self.assertEqual(query["latitude"], ["43.0,44.0"])
        self.assertEqual(query["longitude"], ["-89.0,-88.0"])
        self.assertEqual(query["hourly"], ["temperature_2m,precipitation,weather_code"])
        self.assertEqual(query["timezone"], ["UTC"])
        with self.assertRaisesRegex(ValueError, "between 1 and 7"):
            weather.build_url(LOCATIONS, VARIABLES, forecast_days=8)
        with self.assertRaisesRegex(ValueError, "between 1 and 10"):
            weather.build_url((), VARIABLES)

    def test_http_error_propagates_without_emitting_partial_data(self):
        error = urllib.error.HTTPError(weather.API_URL, 429, "Too Many Requests", None, io.BytesIO())
        with patch.object(weather.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError):
                weather.fetch_forecast(LOCATIONS, VARIABLES, fetched_at=FETCHED_AT)


if __name__ == "__main__":
    unittest.main()
