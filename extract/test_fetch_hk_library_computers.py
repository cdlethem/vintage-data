import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_hk_library_computers.py"
SPEC = importlib.util.spec_from_file_location("fetch_hk_library_computers", SCRIPT)
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


def http_transient(code):
    return urllib.error.HTTPError(MODULE.URL, code, "error", {}, io.BytesIO(b"origin failure"))


def library(code="K1", with_sessions=True):
    document = {
        "libraryCode": code,
        "libraryDisplayName": "Kennedy Town Public Library",
        "district": "Eastern",
        "isOpen": True,
        "lastUpdateDate": "2026-10-07 15:00:00",
    }
    if with_sessions:
        document["sessionList"] = [{
            "sessionStart": "09:00",
            "sessionEnd": "10:00",
            "workstationGroup": [{
                "groupId": "g1",
                "groupName": "General PC",
                "availableWktNumber": 3,
            }],
        }]
    return document


class FetchHkLibraryComputersTests(unittest.TestCase):
    def test_yields_library_and_availability_records(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([library()])):
            records = list(MODULE.fetch_availability(limit=2000, timeout=30))
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["record_type"], "library")
        self.assertEqual(records[1]["record_type"], "availability")
        for record in records:
            self.assertEqual(record["source"], MODULE.SOURCE)
            self.assertIn("fetched_at", record)
            self.assertIn("id", record)

    def test_recovers_from_transient_read_timeouts(self):
        side_effect = [
            TimeoutError("The read operation timed out"),
            TimeoutError("The read operation timed out"),
            response([library()]),
        ]
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=side_effect) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_availability(limit=2000, timeout=30))
        self.assertEqual(len(records), 2)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(
            sleep.call_args_list,
            [mock.call(MODULE.RETRY_BASE_DELAY), mock.call(MODULE.RETRY_BASE_DELAY * 2)],
        )

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS,
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_availability(limit=2000, timeout=30))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_retries_transient_http_503_then_succeeds(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(503), response([library()])],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_availability(limit=2000, timeout=30))
        self.assertEqual(len(records), 2)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])

    def test_fails_immediately_for_non_retryable_http_errors(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=http_transient(404)) as urlopen, \
                mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_availability(limit=2000, timeout=30))
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_rejects_non_list_documents(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"libraryCode": "K1"})):
            with self.assertRaises(TypeError):
                list(MODULE.fetch_availability(limit=2000, timeout=30))


if __name__ == "__main__":
    unittest.main()
