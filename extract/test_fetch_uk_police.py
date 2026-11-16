import importlib.util
import io
import json
import pathlib
import socket
import unittest
import urllib.error
import urllib.request
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_uk_police.py"
SPEC = importlib.util.spec_from_file_location("fetch_uk_police", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

URL = f"{MODULE.BASE}/crimes-street-dates"


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
    return urllib.error.HTTPError(URL, code, "error", {}, io.BytesIO(b"origin failure"))


def request():
    return urllib.request.Request(URL, headers={"User-Agent": MODULE.USER_AGENT})


class UlopenRetryTests(unittest.TestCase):
    def test_recovers_from_transient_dns_blip(self):
        failure = urllib.error.URLError(
            socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")
        )
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[failure, response({})]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with MODULE._urlopen(request(), 60):
                pass
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=[http_transient(code), response({})]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                with MODULE._urlopen(request(), 60):
                    pass
                self.assertEqual(urlopen.call_count, 2)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])

    def test_recovers_from_read_timeout(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out"), response({})],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with MODULE._urlopen(request(), 60):
                pass
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])

    def test_stops_after_finite_transient_attempts(self):
        failure = urllib.error.URLError(
            socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")
        )
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[failure] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.URLError):
                MODULE._urlopen(request(), 60)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)
        self.assertEqual(
            sleep.call_args_list,
            [mock.call(MODULE.RETRY_BASE_DELAY * (2**attempt)) for attempt in range(MODULE.MAX_ATTEMPTS - 1)],
        )

    def test_404_is_not_retried_and_maps_to_empty_list(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=http_transient(404)
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(MODULE._get("crimes-street/all-crime", lat=52.6, lng=-1.13), [])
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_get_uses_retried_open(self):
        failure = urllib.error.URLError(
            socket.gaierror(socket.EAI_AGAIN, "Temporary failure in name resolution")
        )
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[failure, response([{"date": "2026-09"}])],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep"):
            self.assertEqual(MODULE.available_months(), ["2026-09"])
        self.assertEqual(urlopen.call_count, 2)


if __name__ == "__main__":
    unittest.main()
