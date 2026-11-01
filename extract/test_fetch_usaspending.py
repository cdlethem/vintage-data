import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_usaspending.py"
SPEC = importlib.util.spec_from_file_location("fetch_usaspending", SCRIPT)
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


def response(results):
    return Response(json.dumps({"results": results}).encode("utf-8"))


def http_transient(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.URL, code, "error", headers, io.BytesIO(b"origin failure"))


def award(award_id="CONT_AWD_0001", **overrides):
    document = {
        "generated_internal_id": f"CONT_AWD_{award_id}_0001_-NONE-_-NONE-",
        "internal_id": award_id,
        "Award ID": award_id,
        "Recipient Name": "EXAMPLE CONTRACTOR LLC",
        "Start Date": "2026-01-15",
        "Last Modified Date": "2026-10-30 23:50:16",
        "Award Amount": 1234567.89,
        "Awarding Agency": "Department of the Interior",
    }
    document.update(overrides)
    return document


class FetchUsaspendingTests(unittest.TestCase):
    def test_yields_normalized_records_from_json(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([award()])) as urlopen:
            records = list(MODULE.fetch_recent(days=30, limit=25))
        self.assertEqual(urlopen.call_count, 1)
        request = urlopen.call_args_list[0].args[0]
        self.assertEqual(request.full_url, MODULE.URL)
        self.assertEqual(request.get_method(), "POST")
        body = json.loads(request.data.decode("utf-8"))
        self.assertEqual(body["sort"], "Last Modified Date")
        self.assertEqual(body["order"], "desc")
        self.assertEqual(body["limit"], 25)
        self.assertEqual(body["page"], 1)
        self.assertEqual(body["filters"]["award_type_codes"], ["A", "B", "C", "D"])
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "usaspending")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], "CONT_AWD_CONT_AWD_0001_0001_-NONE-_-NONE-")
        self.assertEqual(record["award_id"], "CONT_AWD_0001")
        self.assertEqual(record["recipient"], "EXAMPLE CONTRACTOR LLC")
        self.assertEqual(record["start_date"], "2026-01-15")
        self.assertEqual(record["last_modified_date"], "2026-10-30 23:50:16")
        self.assertEqual(record["award_amount"], 1234567.89)
        self.assertEqual(record["awarding_agency"], "Department of the Interior")

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response([award()])]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_recent(days=30, limit=25))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
                self.assertEqual(urlopen.call_count, 2)

    def test_retries_502_with_bounded_retry_after_values(self):
        cases = (
            (None, MODULE.RETRY_BASE_DELAY),
            ("not-a-delay", MODULE.RETRY_BASE_DELAY),
            ("2", 2),
            ("900", MODULE.MAX_RETRY_DELAY),
        )
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(502, retry_after), response([award()])]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_recent(days=30, limit=25))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
                self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(502, "1")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_recent(days=30, limit=25))
            self.assertEqual(ctx.exception.code, 502)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response([award()])]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_recent(days=30, limit=25))), 1)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_read_timeout_and_stops_after_finite_attempts(self):
        # The 2026-11-01 failure: a single 30s read timeout lost the whole run.
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out"), response([award()])]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_recent(days=30, limit=25))), 1)
            self.assertEqual(urlopen.call_count, 2)
            self.assertEqual(sleep.call_count, 1)
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_recent(days=30, limit=25))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.URL, 404, "Not Found", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_recent(days=30, limit=25))
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
