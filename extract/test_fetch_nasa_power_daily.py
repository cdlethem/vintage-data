import importlib.util
import json
from pathlib import Path
import unittest
import urllib.error


SCRIPT = Path(__file__).with_name("scripts") / "fetch_nasa_power_daily.py"
SPEC = importlib.util.spec_from_file_location("fetch_nasa_power_daily", SCRIPT)
power = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(power)


class FakeResponse:
    status = 200

    def __init__(self, document):
        self.payload = json.dumps(document).encode("utf-8")

    def read(self):
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


def fixture():
    return {
        "header": {"fill_value": -999.0},
        "parameters": {
            "T2M": {"units": "C"},
            "PRECTOTCORR": {"units": "mm/day"},
        },
        "properties": {
            "parameter": {
                "T2M": {"20260101": 11.25, "20260102": -999.0},
                "PRECTOTCORR": {"20260101": 0.3, "20260102": 1.1},
            }
        },
    }


class NasaPowerDailyTests(unittest.TestCase):
    def request(self):
        return power.build_request(
            "38.9072", "-77.0369", "2026-01-01", "2026-01-02",
            "RE", "T2M,PRECTOTCORR", "JSON", "UTC",
        )

    def test_normalizes_one_record_per_returned_day(self):
        records = list(power.normalize_response(
            fixture(), self.request(), fetched_at="2026-01-03T00:00:00+00:00"
        ))

        self.assertEqual([record["date"] for record in records], ["2026-01-01", "2026-01-02"])
        self.assertEqual(records[0]["parameters"], {"T2M": 11.25, "PRECTOTCORR": 0.3})
        self.assertEqual(records[1]["parameters"]["T2M"], -999.0)
        self.assertEqual(records[0]["units"], {"T2M": "C", "PRECTOTCORR": "mm/day"})
        self.assertEqual(records[0]["fill_value"], -999.0)
        self.assertEqual(records[0]["location"], {"latitude": 38.9072, "longitude": -77.0369})
        self.assertEqual(records[0]["request"]["parameters"], ["T2M", "PRECTOTCORR"])
        self.assertEqual(records[0]["source"], "nasa_power_daily")

    def test_ids_are_stable_across_fetches(self):
        first = list(power.normalize_response(fixture(), self.request(), fetched_at="2026-01-03T00:00:00+00:00"))
        second = list(power.normalize_response(fixture(), self.request(), fetched_at="2030-01-03T00:00:00+00:00"))

        self.assertEqual(
            [record["id"] for record in first],
            ["nasa_power_daily:RE:UTC:38.9072:-77.0369:20260101", "nasa_power_daily:RE:UTC:38.9072:-77.0369:20260102"],
        )
        self.assertEqual([record["id"] for record in first], [record["id"] for record in second])
        self.assertNotEqual(first[0]["fetched_at"], second[0]["fetched_at"])

    def test_empty_parameter_data_emits_no_records(self):
        document = fixture()
        document["properties"]["parameter"] = {"T2M": {}, "PRECTOTCORR": {}}

        self.assertEqual(list(power.normalize_response(document, self.request())), [])

    def test_malformed_response_fails_visibly(self):
        document = fixture()
        del document["parameters"]["T2M"]["units"]

        with self.assertRaisesRegex(power.PowerError, "missing units for T2M"):
            list(power.normalize_response(document, self.request()))

    def test_request_bounds_and_format_are_validated(self):
        with self.assertRaisesRegex(power.PowerError, "exceeds"):
            power.build_request("0", "0", "2024-01-01", "2025-01-01", "RE", "T2M")
        with self.assertRaisesRegex(power.PowerError, "end date"):
            power.build_request("0", "0", "2026-01-02", "2026-01-01", "RE", "T2M")
        with self.assertRaisesRegex(power.PowerError, "only JSON"):
            power.build_request("0", "0", "2026-01-01", "2026-01-01", "RE", "T2M", "CSV")

    def test_fetch_uses_one_unpaginated_http_request(self):
        calls = []

        def opener(request, timeout):
            calls.append((request.full_url, timeout))
            return FakeResponse(fixture())

        records = list(power.fetch_daily(
            "38.9072", "-77.0369", "2026-01-01", "2026-01-02", "RE",
            "T2M,PRECTOTCORR", opener=opener, fetched_at="2026-01-03T00:00:00+00:00",
        ))

        self.assertEqual(len(calls), 1)
        self.assertEqual(len(records), 2)
        self.assertIn("time-standard=UTC", calls[0][0])
        self.assertIn("parameters=T2M%2CPRECTOTCORR", calls[0][0])

    def test_rate_limit_is_not_silently_accepted(self):
        request = self.request()

        def opener(_request, timeout):
            raise urllib.error.HTTPError("https://example.invalid", 429, "rate limited", {}, None)

        with self.assertRaisesRegex(power.PowerError, "rate limit"):
            power.fetch_document(request, opener=opener)


if __name__ == "__main__":
    unittest.main()
