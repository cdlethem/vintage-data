import contextlib
import importlib.util
import io
import json
import urllib.error
from pathlib import Path
import unittest
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_zenodo.py"
SPEC = importlib.util.spec_from_file_location("fetch_zenodo", SCRIPT_PATH)
fetch_zenodo = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetch_zenodo)


class JsonResponse:
    def __init__(self, document):
        self._body = io.StringIO(json.dumps(document))

    def __enter__(self):
        return self._body

    def __exit__(self, exc_type, exc_value, traceback):
        self._body.close()


class TextResponse:
    def __init__(self, body):
        self._body = io.StringIO(body)

    def __enter__(self):
        return self._body

    def __exit__(self, exc_type, exc_value, traceback):
        self._body.close()


class FetchZenodoTests(unittest.TestCase):
    def document(self):
        return {
            "hits": {
                "hits": [{
                    "id": 123,
                    "doi": "10.5281/zenodo.123",
                    "conceptdoi": "10.5281/zenodo.100",
                    "created": "2026-09-13T06:17:00Z",
                    "metadata": {
                        "title": "Example dataset",
                        "publication_date": "2026-09-12",
                        "resource_type": {"type": "dataset"},
                        "access_right": "open",
                        "license": {"id": "cc-by-4.0"},
                        "creators": [{"name": "Ada Lovelace"}],
                        "keywords": ["example"],
                    },
                }],
            },
        }

    def test_constants_are_configured_correctly(self):
        """Verify the new configuration constants are set."""
        self.assertEqual(fetch_zenodo.REQUEST_TIMEOUT_SECONDS, 60)
        self.assertEqual(fetch_zenodo.MAX_ATTEMPTS, 5)
        self.assertEqual(fetch_zenodo.RETRY_DELAYS_SECONDS, (2, 5, 10))

    def test_timeout_exhaustion_after_five_attempts(self):
        """Verify MAX_ATTEMPTS=5 exhaustion: 5 urlopen calls with 4 sleep calls of delays 2,5,10,2."""
        stderr = io.StringIO()
        secret = "secret-query-value"
        response_body = "secret-response-body"

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=TimeoutError(f"read timed out: {response_body}")) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaisesRegex(TimeoutError, "read timed out"):
                        list(fetch_zenodo.fetch_recent(query=secret))

        # MAX_ATTEMPTS=5 means 5 urlopen calls
        self.assertEqual(urlopen.call_count, 5)
        # 4 sleeps: after attempts 1,2,3,4 (no sleep after attempt 5)
        # Delays are RETRY_DELAYS_SECONDS = (2, 5, 10), cycling if needed
        self.assertEqual(sleep.call_args_list, [
            unittest.mock.call(2),
            unittest.mock.call(5),
            unittest.mock.call(10),
            unittest.mock.call(2),
        ])
        request = urlopen.call_args.args[0]
        self.assertIn("q=secret-query-value", request.full_url)
        # Verify timeout_seconds=60 in diagnostic
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 60)
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic, {
            "attempt_count": 5,
            "event": "zenodo_request_failed",
            "request": {
                "endpoint": "https://zenodo.org/api/records",
                "query_present": True,
                "timeout_seconds": 60,
            },
            "terminal_error": {"class": "TimeoutError"},
        })
        self.assertNotIn(secret, stderr.getvalue())
        self.assertNotIn(response_body, stderr.getvalue())

    def test_read_timeout_recovers_on_second_attempt(self):
        """Verify timeout recovery with new delay of 2 seconds."""
        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[TimeoutError("connection timed out"), JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        # First delay is RETRY_DELAYS_SECONDS[0] = 2
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(2)])
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 60)

    def test_wrapped_connection_timeout_recovers_on_second_attempt(self):
        """Verify URLError-wrapped timeout recovery with new delay of 2 seconds."""
        wrapped_timeout = urllib.error.URLError(TimeoutError("read timed out"))

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[wrapped_timeout, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        # First delay is RETRY_DELAYS_SECONDS[0] = 2
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(2)])

    def test_timeout_sequence_uses_correct_backoff_delays(self):
        """Verify backoff sequence: 2, 5, 10, 2 for attempts 1-4 before final attempt 5."""
        stderr = io.StringIO()

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=TimeoutError("timeout")) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(TimeoutError):
                        list(fetch_zenodo.fetch_recent())

        # Verify the exact delay sequence
        self.assertEqual(sleep.call_args_list, [
            unittest.mock.call(2),   # After attempt 1, RETRY_DELAYS_SECONDS[0]
            unittest.mock.call(5),   # After attempt 2, RETRY_DELAYS_SECONDS[1]
            unittest.mock.call(10),  # After attempt 3, RETRY_DELAYS_SECONDS[2]
            unittest.mock.call(2),   # After attempt 4, RETRY_DELAYS_SECONDS[0] (cycle)
        ])

    def test_non_timeout_connection_error_is_not_retried(self):
        """Verify non-timeout errors don't trigger retry and timeout_seconds=60 is logged."""
        stderr = io.StringIO()
        error = urllib.error.URLError(ConnectionRefusedError("secret connection detail"))

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=error) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(urllib.error.URLError):
                        list(fetch_zenodo.fetch_recent())

        urlopen.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(json.loads(stderr.getvalue())["attempt_count"], 1)
        self.assertEqual(json.loads(stderr.getvalue())["request"]["timeout_seconds"], 60)
        self.assertEqual(json.loads(stderr.getvalue())["terminal_error"], {
            "class": "URLError",
            "reason_class": "ConnectionRefusedError",
        })
        self.assertNotIn("secret connection detail", stderr.getvalue())

    def test_parse_failure_is_not_retried(self):
        """Verify JSON parse failures don't retry and timeout_seconds=60 is logged."""
        stderr = io.StringIO()

        with patch.object(fetch_zenodo.urllib.request, "urlopen", return_value=TextResponse("not json")) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(json.JSONDecodeError):
                        list(fetch_zenodo.fetch_recent())

        urlopen.assert_called_once()
        sleep.assert_not_called()
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], 1)
        self.assertEqual(diagnostic["request"]["timeout_seconds"], 60)
        self.assertEqual(diagnostic["terminal_error"], {"class": "JSONDecodeError"})

    def test_schema_failure_is_not_retried(self):
        """Verify schema validation failures don't retry and timeout_seconds=60 is logged."""
        stderr = io.StringIO()

        with patch.object(fetch_zenodo.urllib.request, "urlopen", return_value=JsonResponse({"hits": {}})) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaisesRegex(ValueError, "hits list"):
                        list(fetch_zenodo.fetch_recent())

        urlopen.assert_called_once()
        sleep.assert_not_called()
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], 1)
        self.assertEqual(diagnostic["request"]["timeout_seconds"], 60)
        self.assertEqual(diagnostic["terminal_error"], {"class": "ValueError"})

    def test_http_error_with_timeout_reason_is_not_retried(self):
        """Verify HTTPError with TimeoutError reason doesn't retry and timeout_seconds=60 is logged."""
        stderr = io.StringIO()
        stdout = io.StringIO()
        error = urllib.error.HTTPError(
            "https://example.invalid/?token=secret",
            500,
            TimeoutError("secret timeout detail"),
            {},
            None,
        )

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=error) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(stdout):
                    with self.assertRaises(urllib.error.HTTPError):
                        fetch_zenodo.main(["1"])

        urlopen.assert_called_once()
        sleep.assert_not_called()
        self.assertEqual(stdout.getvalue(), "")
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic, {
            "attempt_count": 1,
            "event": "zenodo_request_failed",
            "request": {
                "endpoint": "https://zenodo.org/api/records",
                "query_present": False,
                "timeout_seconds": 60,
            },
            "terminal_error": {
                "class": "HTTPError",
                "reason_class": "TimeoutError",
            },
        })
        self.assertNotIn("token=secret", stderr.getvalue())
        self.assertNotIn("secret timeout detail", stderr.getvalue())

    def test_success_emits_expected_ndjson_record_with_new_timeout(self):
        """Verify successful response and timeout_seconds=60 in request."""
        stdout = io.StringIO()

        with patch.object(fetch_zenodo.urllib.request, "urlopen", return_value=JsonResponse(self.document())) as urlopen:
            with contextlib.redirect_stdout(stdout):
                fetch_zenodo.main(["1"])

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://zenodo.org/api/records?sort=newest&size=1")
        self.assertEqual(request.get_header("User-agent"), fetch_zenodo.USER_AGENT)
        self.assertEqual(request.get_header("Accept"), "application/json")
        # Verify new timeout_seconds=60
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 60)
        record = json.loads(stdout.getvalue())
        self.assertEqual(record["source"], "zenodo")
        self.assertEqual(record["id"], 123)
        self.assertEqual(record["doi"], "10.5281/zenodo.123")
        self.assertEqual(record["title"], "Example dataset")
        self.assertEqual(record["creators"], ["Ada Lovelace"])
        self.assertEqual(record["keywords"], ["example"])


if __name__ == "__main__":
    unittest.main()
