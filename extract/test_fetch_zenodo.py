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

    def test_timeout_exhaustion_reports_bounded_terminal_context(self):
        stderr = io.StringIO()
        secret = "secret-query-value"
        response_body = "secret-response-body"

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=TimeoutError(f"read timed out: {response_body}")) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaisesRegex(TimeoutError, "read timed out"):
                        list(fetch_zenodo.fetch_recent(query=secret))

        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(1), unittest.mock.call(2)])
        request = urlopen.call_args.args[0]
        self.assertIn("q=secret-query-value", request.full_url)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic, {
            "attempt_count": 3,
            "event": "zenodo_request_failed",
            "request": {
                "endpoint": "https://zenodo.org/api/records",
                "query_present": True,
                "timeout_seconds": 30,
            },
            "terminal_error": {"class": "TimeoutError"},
        })
        self.assertNotIn(secret, stderr.getvalue())
        self.assertNotIn(response_body, stderr.getvalue())

    def test_read_timeout_recovers_on_second_attempt(self):
        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[TimeoutError("connection timed out"), JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(1)])
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)

    def test_wrapped_connection_timeout_recovers_on_second_attempt(self):
        wrapped_timeout = urllib.error.URLError(TimeoutError("read timed out"))

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=[wrapped_timeout, JsonResponse(self.document())]) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                records = list(fetch_zenodo.fetch_recent())

        self.assertEqual(records[0]["id"], 123)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(1)])

    def test_non_timeout_connection_error_is_not_retried(self):
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
        self.assertEqual(json.loads(stderr.getvalue())["terminal_error"], {
            "class": "URLError",
            "reason_class": "ConnectionRefusedError",
        })
        self.assertNotIn("secret connection detail", stderr.getvalue())

    def test_parse_failure_is_not_retried(self):
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
        self.assertEqual(diagnostic["terminal_error"], {"class": "JSONDecodeError"})

    def test_schema_failure_is_not_retried(self):
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
        self.assertEqual(diagnostic["terminal_error"], {"class": "ValueError"})


    def test_http_error_is_not_retried(self):
        stderr = io.StringIO()
        error = urllib.error.HTTPError("https://example.invalid/?token=secret", 500, "server error", {}, None)

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=error) as urlopen:
            with patch.object(fetch_zenodo.time, "sleep") as sleep:
                with contextlib.redirect_stderr(stderr):
                    with self.assertRaises(urllib.error.HTTPError):
                        list(fetch_zenodo.fetch_recent())

        urlopen.assert_called_once()
        sleep.assert_not_called()
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic["attempt_count"], 1)
        self.assertEqual(diagnostic["terminal_error"], {
            "class": "HTTPError",
            "reason_class": "str",
        })
        self.assertNotIn("token=secret", stderr.getvalue())

    def test_success_emits_expected_ndjson_record(self):
        stdout = io.StringIO()

        with patch.object(fetch_zenodo.urllib.request, "urlopen", return_value=JsonResponse(self.document())) as urlopen:
            with contextlib.redirect_stdout(stdout):
                fetch_zenodo.main(["1"])

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://zenodo.org/api/records?sort=newest&size=1")
        self.assertEqual(request.get_header("User-agent"), fetch_zenodo.USER_AGENT)
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
        record = json.loads(stdout.getvalue())
        self.assertEqual(record["source"], "zenodo")
        self.assertEqual(record["id"], 123)
        self.assertEqual(record["doi"], "10.5281/zenodo.123")
        self.assertEqual(record["title"], "Example dataset")
        self.assertEqual(record["creators"], ["Ada Lovelace"])
        self.assertEqual(record["keywords"], ["example"])


if __name__ == "__main__":
    unittest.main()
