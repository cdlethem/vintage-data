BASE_URL = "https://zenodo.org/api/records"
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


def terminal_diagnostic(stderr_text):
    """Parse the bounded terminal diagnostic; per-retry notices precede it."""
    return json.loads(stderr_text.strip().splitlines()[-1])


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
        """Verify the bounded-retry configuration constants are set."""
        self.assertEqual(fetch_zenodo.REQUEST_TIMEOUT_SECONDS, 60)
        self.assertEqual(fetch_zenodo.MAX_TRANSIENT_ATTEMPTS, 10)
        self.assertEqual(fetch_zenodo.DEFAULT_RETRY_DELAY_SECONDS, 5)
        self.assertEqual(fetch_zenodo.MAX_RETRY_DELAY_SECONDS, 30)
        self.assertEqual(fetch_zenodo.MAX_RETRY_CUMULATIVE_DELAY_SECONDS, 480)

    def test_timeout_exhaustion_after_bounded_attempts(self):
        """Verify bounded exhaustion: 10 attempts, exponential delays capped at 30s."""
        stderr = io.StringIO()
        secret = "secret-query-value"
        response_body = "secret-response-body"

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=TimeoutError(f"read timed out: {response_body}")) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaisesRegex(TimeoutError, "read timed out"):
                        list(fetch_zenodo.fetch_recent(query=secret))

        # MAX_TRANSIENT_ATTEMPTS=10 means 10 urlopen calls
        self.assertEqual(urlopen.call_count, fetch_zenodo.MAX_TRANSIENT_ATTEMPTS)
        # 9 sleeps: after attempts 1-9 (no sleep after the final attempt).
        # Delays double 5, 10, 20 then cap at MAX_RETRY_DELAY_SECONDS=30.
        self.assertEqual(sleep.call_args_list, [
            unittest.mock.call(5),
            unittest.mock.call(10),
            unittest.mock.call(20),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
        ])
        request = urlopen.call_args.args[0]
        self.assertIn("q=secret-query-value", request.full_url)
        # Verify timeout_seconds=60 in diagnostic
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 60)
        diagnostic = terminal_diagnostic(stderr.getvalue())
        self.assertEqual(diagnostic, {
            "attempt_count": fetch_zenodo.MAX_TRANSIENT_ATTEMPTS,
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

    def test_http_504_recovers_on_second_attempt(self):
        error = urllib.error.HTTPError(BASE_URL, 504, "Gateway Timeout", {}, None)
        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[error, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list,
                         [unittest.mock.call(fetch_zenodo.DEFAULT_RETRY_DELAY_SECONDS)])

    def test_http_504_exhaustion_raises_after_bounded_attempts(self):
        stderr = io.StringIO()
        error = urllib.error.HTTPError(BASE_URL, 504, "Gateway Timeout", {}, None)

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=error) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(urllib.error.HTTPError):
                        list(fetch_zenodo.fetch_recent())

        self.assertEqual(urlopen.call_count, fetch_zenodo.MAX_TRANSIENT_ATTEMPTS)
        self.assertEqual(sleep.call_args_list, [
            unittest.mock.call(5),
            unittest.mock.call(10),
            unittest.mock.call(20),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
        ])
        diagnostic = terminal_diagnostic(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], fetch_zenodo.MAX_TRANSIENT_ATTEMPTS)
        self.assertEqual(diagnostic["terminal_error"]["class"], "HTTPError")

    def test_http_500_recovers_on_second_attempt(self):
        error = urllib.error.HTTPError(BASE_URL, 500, "INTERNAL SERVER ERROR", {}, None)
        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[error, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list,
                         [unittest.mock.call(fetch_zenodo.DEFAULT_RETRY_DELAY_SECONDS)])

    def test_transient_http_codes_are_retried(self):
        for code in (429, 500, 502, 503):
            with self.subTest(code=code):
                error = urllib.error.HTTPError(BASE_URL, code, "error", {}, None)
                with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[error, JsonResponse(self.document())]) as urlopen:
                    with patch.object(fetch_zenodo.time, "sleep") as sleep:
                        records = list(fetch_zenodo.fetch_recent())

                self.assertEqual(records[0]["id"], 123)
                self.assertEqual(urlopen.call_count, 2)
                self.assertEqual(sleep.call_args_list,
                                 [unittest.mock.call(fetch_zenodo.DEFAULT_RETRY_DELAY_SECONDS)])

    def test_http_500_exhaustion_raises_after_bounded_attempts(self):
        stderr = io.StringIO()
        error = urllib.error.HTTPError(BASE_URL, 500, "INTERNAL SERVER ERROR", {}, None)

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=error) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(urllib.error.HTTPError):
                        list(fetch_zenodo.fetch_recent())

        self.assertEqual(urlopen.call_count, fetch_zenodo.MAX_TRANSIENT_ATTEMPTS)
        self.assertEqual(sleep.call_args_list, [
            unittest.mock.call(5),
            unittest.mock.call(10),
            unittest.mock.call(20),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
        ])
        diagnostic = terminal_diagnostic(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], fetch_zenodo.MAX_TRANSIENT_ATTEMPTS)
        self.assertEqual(diagnostic["terminal_error"]["class"], "HTTPError")

    def test_http_400_is_not_retried(self):
        stderr = io.StringIO()
        error = urllib.error.HTTPError(BASE_URL, 400, "Bad Request", {}, None)

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=error) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(urllib.error.HTTPError):
                        list(fetch_zenodo.fetch_recent())

        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(sleep.call_args_list, [])
        diagnostic = terminal_diagnostic(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], 1)
        self.assertEqual(diagnostic["terminal_error"]["class"], "HTTPError")


    def test_read_timeout_recovers_on_second_attempt(self):
        """Verify timeout recovery with the default 5-second backoff delay."""
        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[TimeoutError("connection timed out"), JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        # First delay is DEFAULT_RETRY_DELAY_SECONDS = 5
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(5)])
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 60)

    def test_wrapped_connection_timeout_recovers_on_second_attempt(self):
        """Verify URLError-wrapped timeout recovery with the 5-second delay."""
        wrapped_timeout = urllib.error.URLError(TimeoutError("read timed out"))

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[wrapped_timeout, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        # First delay is DEFAULT_RETRY_DELAY_SECONDS = 5
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(5)])

    def test_timeout_sequence_uses_exponential_backoff_delays(self):
        """Verify backoff doubles from 5s and caps at 30 seconds."""
        stderr = io.StringIO()

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=TimeoutError("timeout")) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(TimeoutError):
                        list(fetch_zenodo.fetch_recent())

        # Verify the exact delay sequence
        self.assertEqual(sleep.call_args_list, [
            unittest.mock.call(5),   # After attempt 1
            unittest.mock.call(10),  # After attempt 2
            unittest.mock.call(20),  # After attempt 3
            unittest.mock.call(30),  # After attempt 4, capped at MAX_RETRY_DELAY_SECONDS
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
            unittest.mock.call(30),
        ])
        self.assertNotEqual(sleep.call_args_list, [
            unittest.mock.call(5),
            unittest.mock.call(10),
            unittest.mock.call(20),
            unittest.mock.call(5),
        ] + [unittest.mock.call(30)] * 5)

    def test_non_timeout_connection_error_recovers_on_second_attempt(self):
        """Verify transport errors (DNS blips, resets) retry with the standard backoff."""
        stderr = io.StringIO()
        error = urllib.error.URLError(ConnectionRefusedError("secret connection detail"))

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[error, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(5)])
        self.assertNotIn("secret connection detail", stderr.getvalue())

    def test_connection_error_exhaustion_raises_after_bounded_attempts(self):
        """Verify persistent transport failures fail after the bounded attempts."""
        stderr = io.StringIO()
        error = urllib.error.URLError(ConnectionRefusedError("secret connection detail"))

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=error) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(urllib.error.URLError):
                        list(fetch_zenodo.fetch_recent())

        self.assertEqual(urlopen.call_count, fetch_zenodo.MAX_TRANSIENT_ATTEMPTS)
        self.assertEqual(len(sleep.call_args_list), fetch_zenodo.MAX_TRANSIENT_ATTEMPTS - 1)
        diagnostic = terminal_diagnostic(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], fetch_zenodo.MAX_TRANSIENT_ATTEMPTS)
        self.assertEqual(diagnostic["terminal_error"], {
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
        diagnostic = terminal_diagnostic(stderr.getvalue())
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
        diagnostic = terminal_diagnostic(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], 1)
        self.assertEqual(diagnostic["request"]["timeout_seconds"], 60)
        self.assertEqual(diagnostic["terminal_error"], {"class": "ValueError"})

    def test_http_error_with_timeout_reason_is_not_retried(self):
        """Verify HTTPError with TimeoutError reason doesn't retry and timeout_seconds=60 is logged."""
        stderr = io.StringIO()
        stdout = io.StringIO()
        error = urllib.error.HTTPError(
            "https://example.invalid/?token=secret",
            400,
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
        diagnostic = terminal_diagnostic(stderr.getvalue())
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

    def test_retry_after_header_is_honored(self):
        stderr = io.StringIO()
        headers = {"Retry-After": "7"}
        error = urllib.error.HTTPError(BASE_URL, 503, "unavailable", headers, None)

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[error, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(7)])

    def test_retry_after_header_is_clamped(self):
        stderr = io.StringIO()
        headers = {"Retry-After": "999"}
        error = urllib.error.HTTPError(BASE_URL, 503, "unavailable", headers, None)

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[error, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(sleep.call_args_list,
                         [unittest.mock.call(fetch_zenodo.MAX_RETRY_DELAY_SECONDS)])

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
