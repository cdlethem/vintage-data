import gzip
import importlib.util
import io
import json
import urllib.error
import urllib.parse
from pathlib import Path
import unittest
from unittest import mock

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

def status_line_timeout(secret):
    """Create a wrapped timeout from the real HTTP status-line reader."""
    response = mock.Mock()
    response.fp.readline.side_effect = TimeoutError(secret)
    try:
        MODULE.http.client.HTTPResponse._read_status(response)
    except TimeoutError as error:
        return urllib.error.URLError(error)


def connection_timeout(secret):
    """Create a wrapped timeout without a status-line frame."""
    try:
        raise TimeoutError(secret)
    except TimeoutError as error:
        return urllib.error.URLError(error)



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


    def test_requests_exact_station_endpoint_and_query_contract(self):
        """Request targets the HTTPS station endpoint with its exact time window."""
        _, urlopen = self.fetch(station="tkl /?")
        request = urlopen.call_args[0][0]
        url = urllib.parse.urlsplit(request.full_url)

        self.assertEqual(url.scheme, "https")
        self.assertEqual(url.hostname, "rata.digitraffic.fi")
        self.assertEqual(url.path, "/api/v1/live-trains/station/TKL%20%2F%3F")

        query = urllib.parse.parse_qsl(url.query, keep_blank_values=True)
        query_keys = [key for key, _ in query]
        self.assertEqual(len(query_keys), len(set(query_keys)))
        self.assertEqual(
            dict(query),
            {
                "minutes_before_departure": "0",
                "minutes_after_departure": "60",
                "minutes_before_arrival": "0",
                "minutes_after_arrival": "60",
            },
        )

    def test_sends_exact_request_headers(self):
        """Request uses the configured identifiers and expected response encodings."""
        _, urlopen = self.fetch()
        request = urlopen.call_args[0][0]

        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(request.get_header("Accept-encoding"), "gzip")
        self.assertEqual(request.get_header("Digitraffic-user"), MODULE.USER_AGENT)
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)

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

    def test_non_positive_limit_returns_without_http_call(self):
        """Non-positive limits skip the fetch entirely."""
        for limit in (0, -1):
            with self.subTest(limit=limit):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    records = list(MODULE.fetch_trains(limit=limit))
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
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=error
        ) as urlopen:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_trains())
            urlopen.assert_called_once()
            self.assertIs(ctx.exception, error)
            self.assertEqual(ctx.exception.code, 403)
        error.close()

    def test_retries_status_line_timeout_once_after_station_cadence(self):
        """Only a response-status timeout earns a delayed second request."""
        secret = "https://user:password@example.invalid/?token=secret"
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[
            status_line_timeout(secret), Response(trains_response()),
        ]) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                with mock.patch.object(MODULE.sys, "stderr", new_callable=io.StringIO) as stderr:
                    records = list(MODULE.fetch_trains(timeout=12))
        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual([call.kwargs for call in urlopen.call_args_list], [
            {"timeout": 12}, {"timeout": 12},
        ])
        sleep.assert_called_once_with(60)
        self.assertIn("phase=open category=timeout", stderr.getvalue())
        self.assertNotIn(secret, stderr.getvalue())

    def test_repeated_status_line_timeout_stops_after_two_requests(self):
        """Repeated response-status timeouts preserve the final failure."""
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[
            status_line_timeout("secret"), status_line_timeout("secret"),
        ]) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                with mock.patch.object(MODULE.sys, "stderr", new_callable=io.StringIO) as stderr:
                    with self.assertRaises(urllib.error.URLError):
                        list(MODULE.fetch_trains())
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(60)
        self.assertEqual(stderr.getvalue().count("phase=open category=timeout"), 2)

    def test_connection_timeout_is_not_retried(self):
        """A connection timeout cannot be mistaken for a status-line timeout."""
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=connection_timeout("secret")) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                with self.assertRaises(urllib.error.URLError):
                    list(MODULE.fetch_trains())
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_status_line_timeout_with_nonfinite_timeout_is_not_retried(self):
        """Never make an additional request without a positive finite timeout."""
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=status_line_timeout("secret")) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                with self.assertRaises(urllib.error.URLError):
                    list(MODULE.fetch_trains(timeout=float("inf")))
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_request_failures_report_elapsed_category_without_secrets(self):
        """Opening and reading failures have bounded, redacted diagnostics."""
        secret = "https://user:password@example.invalid/private?token=secret"
        failures = (
            (TimeoutError(secret), "timeout", "open"),
            (urllib.error.URLError(TimeoutError(secret)), "timeout", "open"),
            (urllib.error.URLError(secret), "url_error", "open"),
            (urllib.error.HTTPError(secret, 503, secret, {}, None), "http_error", "open"),
            (TimeoutError(secret), "timeout", "read"),
            (OSError(secret), "request_error", "read"),
        )
        for error, category, phase in failures:
            with self.subTest(category=category, phase=phase):
                response = Response(b"[]")
                response.read = mock.Mock(side_effect=error)
                urlopen_result = response if phase == "read" else None
                with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error if phase == "open" else None, return_value=urlopen_result) as urlopen:
                    with mock.patch.object(MODULE.time, "monotonic", side_effect=[10.0, 10.125]):
                        with mock.patch.object(MODULE.sys, "stderr", new_callable=io.StringIO) as stderr:
                            with self.assertRaises(type(error)) as caught:
                                list(MODULE.fetch_trains())
                self.assertIs(caught.exception, error)
                urlopen.assert_called_once()
                self.assertEqual(stderr.getvalue(), f"{MODULE.SOURCE} request_failed phase={phase} category={category} elapsed_ms=125\n")
                self.assertNotIn(secret, stderr.getvalue())
                if isinstance(error, urllib.error.HTTPError):
                    error.close()

    def test_elapsed_time_is_clamped(self):
        """Unexpected clock values cannot produce unbounded or negative output."""
        for end, expected in ((9.0, 0), (1_000_000_000.0, 999_999_999)):
            with self.subTest(end=end):
                with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=TimeoutError("secret")):
                    with mock.patch.object(MODULE.time, "monotonic", side_effect=[10.0, end]):
                        with mock.patch.object(MODULE.sys, "stderr", new_callable=io.StringIO) as stderr:
                            with self.assertRaises(TimeoutError):
                                list(MODULE.fetch_trains())
                self.assertEqual(stderr.getvalue(), f"{MODULE.SOURCE} request_failed phase=open category=timeout elapsed_ms={expected}\n")

    def test_main_hides_original_exception_text(self):
        """CLI errors retain a nonzero exit without printing authenticated URLs."""
        secret = "https://user:password@example.invalid/?token=secret"
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urllib.error.URLError(secret)):
            with mock.patch("sys.argv", ["fetch_digitraffic_rail.py"]):
                with mock.patch.object(MODULE.sys, "stderr", new_callable=io.StringIO) as stderr:
                    with self.assertRaises(SystemExit) as caught:
                        MODULE.main()
        self.assertEqual(caught.exception.code, 1)
        self.assertIn("category=url_error", stderr.getvalue())
        self.assertIn("extraction_failed", stderr.getvalue())
        self.assertNotIn(secret, stderr.getvalue())

    def test_main_does_not_print_invalid_response_body(self):
        """Validation failure exits without echoing the server response."""
        secret = "private response body token=secret"
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(secret.encode())):
            with mock.patch("sys.argv", ["fetch_digitraffic_rail.py"]):
                with mock.patch.object(MODULE.sys, "stderr", new_callable=io.StringIO) as stderr:
                    with self.assertRaises(SystemExit) as caught:
                        MODULE.main()
        self.assertEqual(caught.exception.code, 1)
        self.assertEqual(stderr.getvalue(), f"{MODULE.SOURCE} extraction_failed\n")

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
