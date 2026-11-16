"""Regression tests for the shared job-board HttpClient transport retries.

    python3 extract/scripts/test_job_boards_common.py

The 2026-11-16 ``extract__job_feed_getonboard`` run died on a one-off DNS
failure (``socket.gaierror: [Errno -3] Temporary failure in name
resolution``): ``HttpClient.request`` retried transient HTTP statuses
(429/5xx) but let transport-level ``urllib.error.URLError`` propagate, so a
single network blip lost the whole run. These offline cases pin the bounded
transport retry: transient URLError/timeout failures are retried with
backoff and a persistent failure surfaces after a finite number of attempts.
"""
from __future__ import annotations

import io
import json
import pathlib
import socket
import sys
import unittest
import urllib.error
from unittest import mock

HERE = pathlib.Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import job_boards_lib.common as common  # noqa: E402
from job_boards_lib.common import CLIENT  # noqa: E402


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


def dns_error():
    return urllib.error.URLError(
        socket.gaierror(-3, "Temporary failure in name resolution")
    )


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError("https://example.invalid", code, "error",
                                  headers, io.BytesIO(b"origin failure"))


class HttpClientTransportRetryTest(unittest.TestCase):
    def test_recovers_from_transient_dns_failure(self):
        CLIENT.begin_observation()
        with mock.patch.object(common.urllib.request, "urlopen",
                               side_effect=[dns_error(), response({"ok": True})]) as urlopen, \
             mock.patch.object(common.time, "sleep") as sleep:
            self.assertEqual(CLIENT.json("getonboard", "https://www.getonbrd.com/"),
                             {"ok": True})
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(1)])
        stats = CLIENT.request_stats()
        self.assertEqual(stats["retries"], 1)
        self.assertEqual(stats["statuses"], [200])

    def test_recovers_from_transient_read_timeout(self):
        with mock.patch.object(common.urllib.request, "urlopen",
                               side_effect=[TimeoutError("The read operation timed out"),
                                            response({"ok": True})]) as urlopen, \
             mock.patch.object(common.time, "sleep") as sleep:
            self.assertEqual(CLIENT.json("getonboard", "https://www.getonbrd.com/"),
                             {"ok": True})
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(1)])

    def test_stops_after_finite_transport_failures(self):
        with mock.patch.object(common.urllib.request, "urlopen",
                               side_effect=[dns_error(), dns_error(), dns_error()]) as urlopen, \
             mock.patch.object(common.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.URLError):
                CLIENT.request("getonboard", "https://www.getonbrd.com/")
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(1), mock.call(2)])

    def test_does_not_retry_permanent_http_errors(self):
        with mock.patch.object(common.urllib.request, "urlopen",
                               side_effect=http_error(404)) as urlopen, \
             mock.patch.object(common.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                CLIENT.request("getonboard", "https://www.getonbrd.com/")
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_still_retries_transient_http_statuses(self):
        with mock.patch.object(common.urllib.request, "urlopen",
                               side_effect=[http_error(503), response({"ok": True})]) as urlopen, \
             mock.patch.object(common.time, "sleep") as sleep:
            self.assertEqual(CLIENT.json("getonboard", "https://www.getonbrd.com/"),
                             {"ok": True})
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(1)])


if __name__ == "__main__":
    unittest.main()
