import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_drivebc_events.py"
SPEC = importlib.util.spec_from_file_location("fetch_drivebc_events", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def http_transient(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.URL, code, "error", headers, io.BytesIO(b"origin failure"))


def event(event_id="1", **overrides):
    document = {
        "id": event_id,
        "headline": "Lane closed",
        "status": "active",
        "event_type": "construction",
        "created": "2026-11-17T12:00:00Z",
        "updated": "2026-11-17T12:00:00Z",
    }
    document.update(overrides)
    return document


class FetchDrivebcEventsTests(unittest.TestCase):
    def test_yields_normalized_records_from_json(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"events": [event()]})) as urlopen:
            records = list(MODULE.fetch_events(500))
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "drivebc_open511_events")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], "1")
        self.assertEqual(record["headline"], "Lane closed")

    def test_request_sends_format_and_limit(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"events": []})) as urlopen:
            list(MODULE.fetch_events(100))
        url = urlopen.call_args_list[0].args[0].full_url
        self.assertEqual(url, MODULE.URL + "?format=json&limit=100")

    def test_request_is_clamped_to_upstream_limit_cap(self):
        # Upstream deterministically 500s for limit>500 (error_code 1005);
        # the clamp keeps the request within the documented cap.
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"events": [event()]})) as urlopen:
            list(MODULE.fetch_events(1000))
        url = urlopen.call_args_list[0].args[0].full_url
        self.assertEqual(url, MODULE.URL + "?format=json&limit=500")

    def test_zero_limit_makes_no_request(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            self.assertEqual(list(MODULE.fetch_events(0)), [])
        urlopen.assert_not_called()

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response({"events": [event()]})]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_events(500))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
                self.assertEqual(urlopen.call_count, 2)

    def test_retries_500_with_bounded_retry_after_values(self):
        cases = (
            (None, MODULE.RETRY_BASE_DELAY),
            ("not-a-delay", MODULE.RETRY_BASE_DELAY),
            ("2", 2),
            ("900", MODULE.MAX_RETRY_DELAY),
        )
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(500, retry_after), response({"events": [event()]})]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_events(500))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
                self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(500, "1")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_events(500))
            self.assertEqual(ctx.exception.code, 500)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response({"events": [event()]})]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_events(500))), 1)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_events(500))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.URL, 404, "Not Found", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_events(500))
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_rejects_response_missing_events_list(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"error": "boom"})):
            with self.assertRaises(TypeError):
                list(MODULE.fetch_events(500))

    def test_rejects_event_missing_id(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"events": [event(id="")]})):
            with self.assertRaises(ValueError):
                list(MODULE.fetch_events(500))


if __name__ == "__main__":
    unittest.main()
