import contextlib
import importlib.util
import io
import json
import pathlib
import ssl
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_radio_browser.py"
SPEC = importlib.util.spec_from_file_location("fetch_radio_browser", SCRIPT)
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


def servers_list(names):
    return response([{"name": name} for name in names])


def ssl_dead_mirror():
    return urllib.error.URLError(
        ssl.SSLCertVerificationError(
            "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: "
            "self-signed certificate"))


BAD = "bad1.example"
GOOD = "good1.example"


class RadioBrowserMirrorFailoverTests(unittest.TestCase):
    def setUp(self):
        MODULE._candidates = None
        MODULE._server = None

    def test_fails_over_to_next_mirror_on_dead_tls(self):
        # Regression: the upstream server list once contained a host serving a
        # self-signed certificate; a run that randomly picked it died on the
        # first request. A dead mirror must cost at most one attempt.
        rows = [{"stationuuid": "u1", "name": "St", "countrycode": "DE",
                 "language": "german", "clickcount": 10, "clicktrend": 1,
                 "votes": 2, "lastcheckok": True, "lastchecktime": 0}]
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=[servers_list([BAD, GOOD]),
                                            ssl_dead_mirror(),
                                            response(rows)]) as urlopen, \
                mock.patch.object(MODULE.random, "choice", return_value=BAD), \
                contextlib.redirect_stderr(stderr):
            document = MODULE._get("stations/search", limit=3)
        self.assertEqual(len(document), 1)
        self.assertEqual(urlopen.call_count, 3)
        urls = [call.args[0].full_url for call in urlopen.call_args_list]
        self.assertEqual(urls[1], f"https://{BAD}/json/stations/search?limit=3")
        self.assertEqual(urls[2], f"https://{GOOD}/json/stations/search?limit=3")
        self.assertEqual(MODULE._server, f"https://{GOOD}")
        self.assertIn("unavailable", stderr.getvalue())
        self.assertIn("trying next mirror", stderr.getvalue())

    def test_http_error_is_not_rotated(self):
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=[servers_list([BAD, GOOD]),
                                            urllib.error.HTTPError(
                                                "u", 404, "Not Found",
                                                {}, io.BytesIO(b""))]) as urlopen, \
                mock.patch.object(MODULE.random, "choice", return_value=BAD), \
                contextlib.redirect_stderr(stderr):
            with self.assertRaises(urllib.error.HTTPError):
                MODULE._get("no/such/endpoint")
        # One list call, one endpoint attempt: a mirror that answered is not
        # rotated around.
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(stderr.getvalue(), "")

    def test_all_mirrors_dead_raises_with_last_error(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=[servers_list([BAD, GOOD]),
                                            ssl_dead_mirror(),
                                            ssl_dead_mirror(),
                                            ssl_dead_mirror()]) as urlopen, \
                mock.patch.object(MODULE.random, "choice", return_value=BAD), \
                contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(RuntimeError) as ctx:
                MODULE._get("stations/search", limit=3)
        # List call plus one attempt per candidate: BAD, GOOD, FALLBACK.
        self.assertEqual(urlopen.call_count, 4)
        self.assertIsInstance(ctx.exception.__cause__, urllib.error.URLError)

    def test_fallback_used_when_server_list_down(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=[urllib.error.URLError("list down"),
                                            response([{"name": "S"}])]) as urlopen:
            MODULE._get("stations/search", limit=3)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(
            urlopen.call_args_list[1].args[0].full_url,
            MODULE.FALLBACK + "/json/stations/search?limit=3")

    def test_non_json_body_fails_over(self):
        rows = [{"name": "S"}]
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               side_effect=[servers_list([BAD, GOOD]),
                                            Response(b"<html>Bad Gateway</html>"),
                                            response(rows)]) as urlopen, \
                mock.patch.object(MODULE.random, "choice", return_value=BAD), \
                contextlib.redirect_stderr(io.StringIO()):
            document = MODULE._get("stations/search", limit=3)
        self.assertEqual(document, rows)
        self.assertEqual(urlopen.call_count, 3)


if __name__ == "__main__":
    unittest.main()
