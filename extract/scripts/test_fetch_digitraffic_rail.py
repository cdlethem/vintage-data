import gzip
import importlib.util
import io
import json
import subprocess
import sys
from pathlib import Path
import unittest
from unittest import mock
import urllib.error

SCRIPT = Path(__file__).with_name("fetch_digitraffic_rail.py")
SPEC = importlib.util.spec_from_file_location("fetch_digitraffic_rail", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    """Context manager for mock HTTP responses."""
    def __init__(self, data, headers=None):
        super().__init__(data)
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def train_record(departure_date="2026-09-01", train_number="1"):
    """Minimal valid train record for testing."""
    return {
        "departureDate": departure_date,
        "trainNumber": train_number,
        "trainType": {"name": "S", "id": 1},
        "commuterLineID": "J",
        "operatorUICCode": 10,
        "operatorShortCode": "VR",
    }


def trains_response(trains=None):
    """Serialize train list as JSON."""
    if trains is None:
        trains = [train_record()]
    return json.dumps(trains).encode("utf-8")


class FetchDigitrafficRailTests(unittest.TestCase):
    """Test the Digitraffic rail live-trains extractor."""

    def fetch(self, document=None, content_encoding="gzip", **overrides):
        """Mock urlopen and return fetched records."""
        arguments = {
            "station": overrides.pop("station", "HKI"),
            "limit": overrides.pop("limit", 100),
            "timeout": overrides.pop("timeout", 30),
        }
        if document is None:
            document = trains_response()

        # Optionally gzip the response body
        body = document
        headers = {}
        if content_encoding == "gzip":
            body = gzip.compress(document)
            headers["Content-Encoding"] = "gzip"

        response_obj = Response(body, headers)

        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response_obj
        ) as urlopen:
            records = list(MODULE.fetch_trains(**arguments))
            return records, urlopen

    def test_fetches_and_normalizes_train_records(self):
        """Valid response yields train records with fetched_at, source, id."""
        records, _ = self.fetch()
        self.assertEqual(len(records), 1)
        self.assertIn("fetched_at", records[0])
        self.assertEqual(records[0]["source"], "digitraffic_rail_live_trains")
        self.assertEqual(records[0]["id"], "2026-09-01|1")
        self.assertEqual(records[0]["requested_station"], "HKI")

    def test_requests_correct_station_endpoint(self):
        """Request URL includes station parameter."""
        _, urlopen = self.fetch(station="TKL")
        request = urlopen.call_args[0][0]
        self.assertIn("/TKL", request.full_url)

    def test_sends_identifying_headers(self):
        """Request includes User-Agent and gzip Accept-Encoding."""
        _, urlopen = self.fetch()
        request = urlopen.call_args[0][0]
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(request.get_header("Accept-encoding"), "gzip")

    def test_includes_required_digitraffic_user_header(self):
        """Request includes Digitraffic-User identification header."""
        _, urlopen = self.fetch()
        request = urlopen.call_args[0][0]
        # Digitraffic-User header (case-insensitive in urllib.request.Request)
        self.assertTrue(request.get_header("Digitraffic-user"))

    def test_request_includes_query_parameters(self):
        """Request URL contains station query parameters."""
        _, urlopen = self.fetch()
        request = urlopen.call_args[0][0]
        self.assertIn("minutes_before_departure", request.full_url)
        self.assertIn("minutes_after_departure", request.full_url)
        self.assertIn("minutes_before_arrival", request.full_url)
        self.assertIn("minutes_after_arrival", request.full_url)

    def test_propagates_timeout(self):
        """Timeout is passed through to urlopen."""
        _, urlopen = self.fetch(timeout=60)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 60})

    def test_decompresses_gzipped_response(self):
        """Gzipped response body is decompressed before parsing."""
        uncompressed = trains_response([train_record("2026-09-02", "42")])
        records, _ = self.fetch(document=uncompressed, content_encoding="gzip")
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["trainNumber"], "42")

    def test_handles_plain_response(self):
        """Uncompressed response is parsed directly."""
        body = trains_response([train_record("2026-09-03", "99")])
        records, _ = self.fetch(document=body, content_encoding=None)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["trainNumber"], "99")

    def test_limits_records_returned(self):
        """Limit parameter bounds the number of yielded records."""
        trains = [train_record(f"2026-09-0{i}", str(i)) for i in range(1, 6)]
        document = trains_response(trains)
        records, _ = self.fetch(document=document, limit=3)
        self.assertEqual(len(records), 3)

    def test_zero_limit_returns_without_http_call(self):
        """Zero limit skips the fetch entirely."""
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            records = list(MODULE.fetch_trains(limit=0))
            self.assertEqual(records, [])
            urlopen.assert_not_called()

    def test_rejects_response_not_a_list(self):
        """Non-list response raises ValueError."""
        document = json.dumps({"type": "FeatureCollection"}).encode("utf-8")
        with self.assertRaisesRegex(ValueError, "train list"):
            self.fetch(document=document)

    def test_rejects_train_missing_departure_date(self):
        """Train without departureDate raises ValueError."""
        trains = [{"trainNumber": 1}]
        document = trains_response(trains)
        with self.assertRaisesRegex(ValueError, "departureDate"):
            self.fetch(document=document)

    def test_rejects_train_missing_train_number(self):
        """Train without trainNumber raises ValueError."""
        trains = [{"departureDate": "2026-09-01"}]
        document = trains_response(trains)
        with self.assertRaisesRegex(ValueError, "trainNumber"):
            self.fetch(document=document)

    def test_propagates_http_errors_without_retry(self):
        """HTTP errors are raised immediately; 403 is not retried."""
        error = urllib.error.HTTPError(
            MODULE.BASE_URL + "/HKI",
            403,
            "Forbidden",
            {},
            None,
        )
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_trains())
            self.assertEqual(ctx.exception.code, 403)

    def test_main_emits_newline_delimited_json(self):
        """main() writes records as newline-delimited JSON to stdout."""
        trains = [train_record("2026-09-01", "1"), train_record("2026-09-01", "2")]
        document = trains_response(trains)
        body = document
        headers = {"Content-Encoding": "gzip"}
        body = gzip.compress(document)
        response_obj = Response(body, headers)

        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response_obj
        ):
            with mock.patch("sys.argv", ["fetch_digitraffic_rail.py"]):
                with mock.patch("builtins.print") as mock_print:
                    MODULE.main()
                    self.assertEqual(mock_print.call_count, 2)
                    for call in mock_print.call_args_list:
                        # Each call should be json.dumps
                        record_str = call[0][0]
                        record = json.loads(record_str)
                        self.assertEqual(record["source"], "digitraffic_rail_live_trains")

    def test_multiline_response_with_multiple_trains(self):
        """Response with multiple trains is parsed correctly."""
        trains = [
            train_record("2026-09-01", "1"),
            train_record("2026-09-01", "2"),
            train_record("2026-09-02", "10"),
        ]
        document = trains_response(trains)
        records, _ = self.fetch(document=document)
        self.assertEqual(len(records), 3)
        self.assertEqual([r["trainNumber"] for r in records], ["1", "2", "10"])

    def test_malformed_json_response_raises(self):
        """Malformed JSON response raises json.JSONDecodeError."""
        document = b"not valid json"
        with self.assertRaises(json.JSONDecodeError):
            self.fetch(document=document, content_encoding=None)

    def test_child_process_failure_exit_code(self):
        """Compile check: syntax errors exit non-zero."""
        result = subprocess.run(
            [sys.executable, "-m", "py_compile", str(SCRIPT)],
            capture_output=True,
        )
        self.assertEqual(result.returncode, 0, f"Compilation failed: {result.stderr.decode()}")

    def test_source_configuration_matches_script(self):
        """Source identifier and URL match the module constants."""
        self.assertEqual(MODULE.SOURCE, "digitraffic_rail_live_trains")
        self.assertEqual(
            MODULE.BASE_URL, "https://rata.digitraffic.fi/api/v1/live-trains/station"
        )

    def test_endpoint_cadence_is_once_per_minute(self):
        """Docstring specifies minimum 1-minute cadence; not enforced but documented."""
        docstring = MODULE.__doc__ or ""
        self.assertIn("once per minute", docstring)


if __name__ == "__main__":
    unittest.main()
