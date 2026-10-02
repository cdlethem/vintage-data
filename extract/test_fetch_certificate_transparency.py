import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_certificate_transparency.py"
SPEC = importlib.util.spec_from_file_location("fetch_certificate_transparency", SCRIPT)
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


def certificate(cert_id="1", **overrides):
    document = {
        "id": cert_id,
        "issuer_name": "Let's Encrypt Authority X3",
        "common_name": "*.wikipedia.org",
        "name_value": "*.wikipedia.org\nwikipedia.org",
        "not_before": "2026-07-01 00:00:00",
        "not_after": "2026-09-29 00:00:00",
        "serial_number": "01:02:03",
        "result_count": "236",
    }
    document.update(overrides)
    return document


class FetchCertificateTransparencyTests(unittest.TestCase):
    def test_yields_normalized_records_from_json(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([certificate()])) as urlopen:
            records = list(MODULE.fetch_certs("wikipedia.org"))
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "certificate_transparency")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], "1")
        self.assertEqual(record["common_name"], "*.wikipedia.org")
        self.assertEqual(record["names"], ["*.wikipedia.org", "wikipedia.org"])
        self.assertEqual(record["issuer"], "Let's Encrypt Authority X3")
        self.assertEqual(record["not_before"], "2026-07-01 00:00:00")
        self.assertEqual(record["not_after"], "2026-09-29 00:00:00")
        self.assertEqual(record["serial_number"], "01:02:03")

    def test_wildcard_query_is_percent_encoded(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([certificate()])) as urlopen:
            list(MODULE.fetch_certs("wikipedia.org", wildcard=True))
        url = urlopen.call_args_list[0].args[0].full_url
        self.assertEqual(url, MODULE.BASE + "?q=%25.wikipedia.org&output=json")

    def test_plain_query_is_sent_verbatim(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([certificate()])) as urlopen:
            list(MODULE.fetch_certs("wikipedia.org"))
        url = urlopen.call_args_list[0].args[0].full_url
        self.assertEqual(url, MODULE.BASE + "?q=wikipedia.org&output=json")

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response([certificate()])]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_certs("wikipedia.org"))), 1)
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
                side_effect=[http_transient(502, retry_after), response([certificate()])]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_certs("wikipedia.org"))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
                self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(502, "1")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_certs("wikipedia.org"))
            self.assertEqual(ctx.exception.code, 502)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response([certificate()])]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_certs("wikipedia.org"))), 1)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_certs("wikipedia.org"))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(MODULE.BASE, 404, "Not Found", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_certs("wikipedia.org"))
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_treats_non_json_body_as_empty_without_retrying(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=Response(b"<html>Bad Gateway</html>")
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(list(MODULE.fetch_certs("wikipedia.org")), [])
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
