import contextlib
import http.client
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock
import urllib.error


SCRIPT = Path(__file__).with_name("fetch_celestrak_socrates.py")
SPEC = importlib.util.spec_from_file_location("fetch_celestrak_socrates", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

HEADERS = [
    "NORAD_CAT_ID_1", "NORAD_CAT_ID_2", "OBJECT_NAME_1", "OBJECT_NAME_2", "TCA",
    "TCA_RANGE", "TCA_RELATIVE_SPEED", "MAX_PROB", "DILUTION",
]


def csv_document(rows=1):
    values = []
    for index in range(rows):
        values.append(",".join([str(100 + index), str(200 + index), "OBJECT_A", "OBJECT_B",
                                f"2026-09-17T00:{index:02d}:00Z", "1.5", "12.3", "0.0001", ""]))
    return (",".join(HEADERS) + "\n" + "\n".join(values) + "\n").encode()


class Response:
    def __init__(self, chunks, status=200, content_length=None):
        self.chunks = list(chunks)
        self.status = status
        self.headers = {} if content_length is None else {"Content-Length": str(content_length)}
        self.read_sizes = []

    def read(self, size):
        self.read_sizes.append(size)
        item = self.chunks.pop(0) if self.chunks else b""
        if isinstance(item, BaseException):
            raise item
        return item

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class Clock:
    def __init__(self):
        self.value = 0.0

    def monotonic(self):
        value = self.value
        self.value += 0.125
        return value

    def sleep(self, seconds):
        self.value += seconds


class FetchCelestrakSocratesTests(unittest.TestCase):
    def setUp(self):
        MODULE.LAST_REQUEST.clear()

    def test_complete_multichunk_transfer_publishes_top_100_with_metrics(self):
        document = csv_document(101)
        chunks = [document[:37], document[37:111], document[111:]]
        response = Response(chunks, content_length=len(document))
        clock = Clock()
        stdout = io.StringIO()
        stderr = io.StringIO()

        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response) as urlopen, \
             mock.patch.object(MODULE.time, "monotonic", clock.monotonic), \
             mock.patch.object(MODULE.time, "sleep", clock.sleep), \
             contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "csv", "--limit", "100"]), 0)

        self.assertEqual(len(stdout.getvalue().splitlines()), 100)
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, f"{MODULE.BASE}/sort-maxProb.csv")
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 60})
        self.assertEqual(response.read_sizes, [MODULE.CHUNK_BYTES] * 4)
        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        metrics = summary["metrics"]
        self.assertEqual(metrics["status"], 200)
        self.assertEqual(metrics["attempts"], 1)
        self.assertEqual(metrics["received_bytes"], len(document))
        self.assertEqual(metrics["received_chunks"], 3)
        self.assertEqual(metrics["attempt_metrics"][0], {
            "attempt": 1, "status": 200, "elapsed_to_headers_s": 0.125,
            "received_bytes": len(document), "received_chunks": 3, "elapsed_s": 0.25,
        })
        self.assertEqual(metrics["total_elapsed_s"], 0.5)
        self.assertEqual(metrics["parsed_rows"], 100)
        self.assertTrue(metrics["parse_complete"])

    def test_failures_before_headers_retry_with_documented_delays_and_redact_error(self):
        clock = Clock()
        error = urllib.error.URLError("https://user:password@example.invalid/?token=topsecret " + "x" * 400)
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[error, error, error]) as urlopen, \
             mock.patch.object(MODULE.time, "monotonic", clock.monotonic), \
             mock.patch.object(MODULE.time, "sleep", side_effect=clock.sleep) as sleep, \
             contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "csv"]), 1)

        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])
        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        metrics = summary["metrics"]
        self.assertEqual(metrics["failure_phase"], "before_headers")
        self.assertEqual(metrics["received_bytes"], 0)
        self.assertEqual(metrics["received_chunks"], 0)
        self.assertEqual(len(summary["error"]), MODULE.MAX_ERROR_CHARS)
        self.assertNotIn("password", summary["error"])
        self.assertNotIn("topsecret", summary["error"])
        self.assertEqual([attempt["elapsed_to_headers_s"] for attempt in metrics["attempt_metrics"]], [None] * 3)

    def test_interrupted_body_counts_only_received_bytes_and_publishes_nothing(self):
        prefix = csv_document(1)[:20]
        responses = [Response([prefix, OSError("connection reset")]) for _ in range(3)]
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses), \
             mock.patch.object(MODULE.time, "sleep"), contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "csv"]), 1)

        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        metrics = summary["metrics"]
        self.assertEqual(metrics["failure_phase"], "body_transfer")
        self.assertEqual(metrics["received_bytes"], len(prefix) * 3)
        self.assertEqual(metrics["received_chunks"], 3)
        self.assertEqual(summary["records"], 0)
        self.assertEqual(metrics["attempt_metrics"][0]["status"], 200)
        self.assertIsNotNone(metrics["attempt_metrics"][0]["elapsed_to_headers_s"])

    def test_incomplete_content_length_is_never_parsed_or_published(self):
        document = csv_document(1)
        responses = [Response([document], content_length=len(document) + 1) for _ in range(3)]
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses), \
             mock.patch.object(MODULE.time, "sleep"), contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "csv"]), 1)

        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        self.assertEqual(summary["metrics"]["failure_phase"], "incomplete_transfer")
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["metrics"]["received_bytes"], len(document) * 3)

    def test_invalid_csv_headers_fail_after_complete_transfer_without_publication(self):
        document = b"WRONG,HEADERS\n1,2\n"
        response = Response([document], content_length=len(document))
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response), \
             contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "csv"]), 1)

        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        self.assertEqual(summary["metrics"]["failure_phase"], "csv_header_validation")
        self.assertEqual(summary["metrics"]["received_bytes"], len(document))
        self.assertEqual(summary["records"], 0)

    def test_incomplete_read_partial_data_is_diagnostic_only(self):
        partial = b"abc"
        responses = [Response([http.client.IncompleteRead(partial, 10)]) for _ in range(3)]
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses), \
             mock.patch.object(MODULE.time, "sleep"):
            with self.assertRaisesRegex(RuntimeError, "body_transfer"):
                MODULE._get("https://example.invalid")
        self.assertEqual(MODULE.LAST_REQUEST["received_bytes"], len(partial) * 3)
        self.assertEqual(MODULE.LAST_REQUEST["received_chunks"], 3)


if __name__ == "__main__":
    unittest.main()
