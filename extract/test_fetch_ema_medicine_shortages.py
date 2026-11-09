import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_ema_medicine_shortages.py"
SPEC = importlib.util.spec_from_file_location("fetch_ema_medicine_shortages", SCRIPT)
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
    return urllib.error.HTTPError(MODULE.URL, code, "error", headers, io.BytesIO(b"edge throttled"))


def shortage_row(record_id):
    return {
        "category": "Human",
        "medicine_affected": "Emend",
        "supply_shortage_status": "Shortage",
        "international_non_proprietary_name_inn_or_common_name": "Aprepitant",
        "therapeutic_area_mesh": "Neoplasms",
        "pharmaceutical_forms_affected": "Injection, solution for infusion",
        "strengths_affected": "125mg",
        "availability_of_alternatives": "yes",
        "start_of_shortage_date": "2026-10-01",
        "expected_resolution_date": "2026-12-01",
        "expected_resolution": "yes",
        "first_published_date": "2026-10-01",
        "last_updated_date": "2026-11-01",
        "shortage_url": record_id,
    }


def snapshot_document(count=1):
    rows = [shortage_row(f"https://www.ema.europa.eu/shortage/{number + 1}") for number in range(count)]
    return {
        "meta": {"total_records": len(rows), "timestamp": "2026-11-09T18:15:36Z"},
        "data": rows,
    }


class FetchEmaMedicineShortagesTests(unittest.TestCase):
    def test_fetches_snapshot_on_first_attempt(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(snapshot_document())
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            document, last_modified, digest = MODULE.fetch_snapshot(30, None)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()
        self.assertEqual(document["meta"]["total_records"], 1)
        self.assertIsNone(last_modified)
        self.assertEqual(len(digest), 64)

    def test_recovers_from_transient_http_failures(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response(snapshot_document())]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                document, _, _ = MODULE.fetch_snapshot(30, None)
                self.assertEqual(document["meta"]["total_records"], 1)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
                self.assertEqual(urlopen.call_count, 2)

    def test_honours_bounded_retry_after_header(self):
        cases = (
            (None, MODULE.RETRY_BASE_DELAY),
            ("not-a-delay", MODULE.RETRY_BASE_DELAY),
            ("2", 2),
            ("900", MODULE.MAX_RETRY_DELAY),
        )
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(429, retry_after), response(snapshot_document())]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                MODULE.fetch_snapshot(30, None)
                self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
                self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(429, "1")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(RuntimeError) as ctx:
                MODULE.fetch_snapshot(30, None)
            self.assertIn("status=429", str(ctx.exception))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response(snapshot_document())]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            MODULE.fetch_snapshot(30, None)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(RuntimeError) as ctx:
                MODULE.fetch_snapshot(30, None)
            self.assertIn("timed out", str(ctx.exception))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.URL, 404, "Not Found", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(RuntimeError) as ctx:
                MODULE.fetch_snapshot(30, None)
            self.assertIn("status=404", str(ctx.exception))
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_returns_none_for_304_without_retrying(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=http_transient(304)
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertIsNone(MODULE.fetch_snapshot(30, "Mon, 09 Nov 2026 17:15:09 GMT"))
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_if_modified_since_is_sent_when_last_modified_known(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(snapshot_document())
        ) as urlopen:
            MODULE.fetch_snapshot(30, "Mon, 09 Nov 2026 17:15:09 GMT")
        request = urlopen.call_args_list[0].args[0]
        self.assertEqual(request.get_header("If-modified-since"), "Mon, 09 Nov 2026 17:15:09 GMT")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)


if __name__ == "__main__":
    unittest.main()
