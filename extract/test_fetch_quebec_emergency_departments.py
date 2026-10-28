import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_quebec_emergency_departments.py"
SPEC = importlib.util.spec_from_file_location("fetch_quebec_emergency_departments", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

CSV_BODY = (
    "RSS,No_permis_installation,Nom_installation,Total_census\n"
    "01, 1234 ,H\u00f4tel-Dieu de Qu\u00e9bec,57\n"
    "02,5678,Centre hospitalier de Laval,31\n"
).encode("cp1252")


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(body=CSV_BODY):
    return Response(body)


def http_transient(code):
    return urllib.error.HTTPError(MODULE.URL, code, "error", {}, io.BytesIO(b"origin failure"))


class FetchQuebecEmergencyDepartmentsTests(unittest.TestCase):
    def test_yields_normalized_records_from_csv(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response()) as urlopen:
            records = list(MODULE.fetch_departments())
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(len(records), 2)
        record = records[0]
        self.assertEqual(record["source"], "quebec_emergency_departments")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], "1234")
        self.assertEqual(record["No_permis_installation"], "1234")
        self.assertEqual(record["Total_census"], "57")

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response()]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_departments())), 2)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
                self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response()]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_departments())), 2)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_read_timeout(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out"), response()]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_departments())), 2)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(502)] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_departments())
            self.assertEqual(ctx.exception.code, 502)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(
            sleep.call_args_list,
            [mock.call(MODULE.RETRY_BASE_DELAY), mock.call(MODULE.RETRY_BASE_DELAY * 2)],
        )

    def test_stops_after_finite_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_departments())
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.URL, 404, "Not Found", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_departments())
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_still_rejects_missing_facility_identifiers(self):
        body = "RSS,Nom_installation\n9050,Somewhere\n".encode("cp1252")
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(body)):
            with self.assertRaises(ValueError):
                list(MODULE.fetch_departments())


if __name__ == "__main__":
    unittest.main()
