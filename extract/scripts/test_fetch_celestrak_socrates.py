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

def valid_then_malformed_csv_document():
    return csv_document(1) + b"101,201,OBJECT_C,OBJECT_D,2026-09-17T00:01:00Z,invalid,12.3,0.0001,\n"

def table_document(rows=1, headers=HEADERS, malformed_row=None):
    heading = "".join(f"<th>{header}</th>" for header in headers)
    body = []
    for index in range(rows):
        cells = [str(100 + index), str(200 + index), "OBJECT &amp; A", "OBJECT_B",
                 f"2026-09-17T00:{index:02d}:00Z", "1.5", "12.3", "0.0001", ""]
        if index == malformed_row:
            cells.pop()
        body.append("<tr>" + "".join(f"<td><span>{cell}</span></td>" for cell in cells) + "</tr>")
    return ("<html><table><tr><th>Navigation</th></tr></table><table><tr>" + heading +
            "</tr>" + "".join(body) + "</table></html>").encode()


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
        self.assertEqual(sleep.call_args_list, [mock.call(2), mock.call(4)])
        self.assertEqual([call.kwargs for call in urlopen.call_args_list], [{"timeout": 60}] * 3)
        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        metrics = summary["metrics"]
        self.assertEqual(metrics["failure_phase"], "before_headers")
        self.assertEqual(metrics["received_bytes"], 0)
        self.assertEqual(metrics["received_chunks"], 0)
        self.assertEqual(len(summary["error"]), MODULE.MAX_ERROR_CHARS)
        self.assertNotIn("password", summary["error"])
        self.assertNotIn("topsecret", summary["error"])
        self.assertEqual([attempt["elapsed_to_headers_s"] for attempt in metrics["attempt_metrics"]], [None] * 3)
        self.assertEqual([attempt["elapsed_s"] for attempt in metrics["attempt_metrics"]], [0.125] * 3)
        self.assertEqual(metrics["total_elapsed_s"], 7.125)

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

    def test_malformed_selected_row_blocks_all_csv_publication(self):
        document = valid_then_malformed_csv_document()
        response = Response([document], content_length=len(document))
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response), \
             contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "csv", "--limit", "100"]), 1)

        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["metrics"]["failure_phase"], "csv_row_validation")
        self.assertEqual(summary["metrics"]["status"], 200)
        self.assertEqual(summary["metrics"]["received_bytes"], len(document))

    def test_table_mode_fetches_at_most_100_max_probability_rows_without_changing_csv_default(self):
        document = table_document(100)
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response([document], content_length=len(document))) as urlopen, \
             contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "table"]), 0)

        self.assertEqual(urlopen.call_args.args[0].full_url,
                         f"{MODULE.BASE}/table-socrates.php?NAME=,&ORDER=MAXPROB&MAX=100")
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(len(records), 100)
        self.assertEqual(records[0]["id"], "100:200:2026-09-17T00:00:00Z")
        self.assertEqual(records[0]["object_name_1"], "OBJECT & A")
        self.assertEqual(records[-1]["norad_id_2"], "299")
        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["metrics"]["pair_count"], 100)
        self.assertEqual(summary["metrics"]["parsed_rows"], 100)

    def test_default_mode_remains_csv_and_table_limit_is_bounded(self):
        csv_payload = csv_document(1)
        table_payload = table_document(2)
        for args, payload, url, expected in (
            ([], csv_payload, f"{MODULE.BASE}/sort-maxProb.csv", 1),
            (["--mode", "table", "--limit", "2"], table_payload,
             f"{MODULE.BASE}/table-socrates.php?NAME=,&ORDER=MAXPROB&MAX=2", 2),
        ):
            with self.subTest(args=args), \
                 mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response([payload])) as urlopen, \
                 contextlib.redirect_stdout(io.StringIO()) as stdout, \
                 contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(MODULE.main(args), 0)
                self.assertEqual(len(stdout.getvalue().splitlines()), expected)
                self.assertEqual(urlopen.call_args.args[0].full_url, url)

    def test_table_mode_rejects_unsupported_sort_and_limit_before_request(self):
        for sort, limit in (("minRange", 100), ("maxProb", 101), ("maxProb", 0), ("maxProb", -1)):
            with self.subTest(sort=sort, limit=limit), \
                 mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen, \
                 contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(MODULE.main(["--mode", "table", "--sort", sort, "--limit", str(limit)]), 1)
                urlopen.assert_not_called()

    def test_invalid_table_schema_or_row_publishes_nothing(self):
        cases = [
            (table_document(headers=HEADERS[:-1]), "table_header_validation"),
            (table_document(rows=2, malformed_row=1), "table_row_validation"),
            (table_document(rows=0), "table_row_validation"),
            (table_document(rows=101), "table_row_validation"),
            (table_document(rows=1).replace(b"<td><span>1.5", b"<td><span>invalid"), "table_row_validation"),
            (table_document(rows=1).replace(b"</table></html>", b"<tr></tr></table></html>"), "table_row_validation"),
        ]
        for document, phase in cases:
            with self.subTest(phase=phase, document_length=len(document)):
                stdout, stderr = io.StringIO(), io.StringIO()
                with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response([document])), \
                     contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    self.assertEqual(MODULE.main(["--mode", "table"]), 1)
                self.assertEqual(stdout.getvalue(), "")
                summary = json.loads(stderr.getvalue().split("\t", 1)[1])
                self.assertEqual(summary["records"], 0)
                self.assertEqual(summary["metrics"]["failure_phase"], phase)

    def test_table_http_status_failure_never_parses_body(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response([table_document()], status=503)), \
             contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(["--mode", "table"]), 1)
        self.assertEqual(stdout.getvalue(), "")
        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        self.assertEqual(summary["metrics"]["status"], 503)
        self.assertEqual(summary["metrics"]["failure_phase"], "http_response")

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
