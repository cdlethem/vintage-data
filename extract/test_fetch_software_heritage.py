import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_software_heritage.py"
SPEC = importlib.util.spec_from_file_location("fetch_software_heritage", SCRIPT)
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
    return urllib.error.HTTPError(MODULE.BASE, code, "error", headers, io.BytesIO(b"origin failure"))


def origin(url="https://github.com/torvalds/linux"):
    return {
        "url": url,
        "visit_types": ["git"],
        "has_visits": True,
        "nb_visits": 478,
        "last_visit_date": "2026-09-02T03:44:11Z",
    }


class FetchSoftwareHeritageTests(unittest.TestCase):
    def test_yields_normalized_records_from_search(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([origin()])) as urlopen:
            records = list(MODULE.search_origins("linux"))
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "software_heritage_origin")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], "https://github.com/torvalds/linux")
        self.assertEqual(record["visit_types"], ["git"])
        self.assertTrue(record["has_visits"])
        self.assertEqual(record["nb_visits"], 478)
        self.assertEqual(record["last_visit_date"], "2026-09-02T03:44:11Z")

    def test_query_is_percent_encoded(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([origin()])) as urlopen:
            list(MODULE.search_origins("torvalds/linux"))
        url = urlopen.call_args_list[0].args[0].full_url
        self.assertEqual(url, MODULE.BASE + "/origin/search/torvalds%2Flinux/?limit=20")

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response([origin()])]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.search_origins("linux"))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
                self.assertEqual(urlopen.call_count, 2)

    def test_retries_with_bounded_retry_after_values(self):
        cases = (
            (None, MODULE.RETRY_BASE_DELAY),
            ("not-a-delay", MODULE.RETRY_BASE_DELAY),
            ("2", 2),
            ("900", MODULE.MAX_RETRY_DELAY),
        )
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(502, retry_after), response([origin()])]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.search_origins("linux"))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
                self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out"), response([origin()])]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.search_origins("linux"))), 1)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.search_origins("linux"))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.BASE, 404, "Not Found", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.search_origins("linux"))
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_visit_endpoint_recovers_from_transient_failure(self):
        visit = {
            "origin": "https://github.com/torvalds/linux",
            "visit": 478,
            "date": "2026-09-02T03:44:11Z",
            "status": "full",
            "type": "git",
            "snapshot": "0123456789abcdef",
        }
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response(visit)]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_latest_visit("https://github.com/torvalds/linux"))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["source"], "software_heritage_visit")
        self.assertEqual(records[0]["id"], "https://github.com/torvalds/linux:478")
        self.assertEqual(records[0]["visit_date"], "2026-09-02T03:44:11Z")
        self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
        self.assertEqual(urlopen.call_count, 2)


if __name__ == "__main__":
    unittest.main()
