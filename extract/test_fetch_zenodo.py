import contextlib
import importlib.util
import io
import json
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


class FetchZenodoTests(unittest.TestCase):
    def test_read_timeout_reports_bounded_request_context_and_reraises(self):
        stderr = io.StringIO()
        secret = "secret-query-value"
        response_body = "secret-response-body"

        with patch.object(fetch_zenodo.urllib.request, "urlopen", side_effect=TimeoutError(f"read timed out: {response_body}")) as urlopen:
            with contextlib.redirect_stderr(stderr):
                with self.assertRaisesRegex(TimeoutError, "read timed out"):
                    list(fetch_zenodo.fetch_recent(query=secret))

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
        self.assertIn("q=secret-query-value", request.full_url)
        diagnostic = json.loads(stderr.getvalue())
        self.assertEqual(diagnostic, {
            "event": "zenodo_read_timeout",
            "exception_class": "TimeoutError",
            "request": {
                "endpoint": "https://zenodo.org/api/records",
                "query_present": True,
                "timeout_seconds": 30,
            },
        })
        self.assertNotIn(secret, stderr.getvalue())
        self.assertNotIn(response_body, stderr.getvalue())

    def test_success_emits_expected_ndjson_record(self):
        document = {
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
        stdout = io.StringIO()

        with patch.object(fetch_zenodo.urllib.request, "urlopen", return_value=JsonResponse(document)) as urlopen:
            with contextlib.redirect_stdout(stdout):
                fetch_zenodo.main(["1"])

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, "https://zenodo.org/api/records?sort=newest&size=1")
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
