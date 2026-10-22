import importlib.util
import io
import unittest
from pathlib import Path
from unittest.mock import patch
import urllib.error

SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_mpc_neocp.py"

SPEC = importlib.util.spec_from_file_location("fetch_mpc_neocp", SCRIPT_PATH)
mpc_neocp = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(mpc_neocp)

PAGE = b"""<html><body><table>
<tr><th>Temp Desig</th><th>Score</th><th>H</th></tr>
<tr><td>X91327</td><td>22</td><td>24.5</td></tr>
</table></body></html>
"""


class RawResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class QueueOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request.full_url, timeout))
        if not self.responses:
            raise AssertionError(f"unexpected HTTP request: {request.full_url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return RawResponse(response)


def connection_timeout():
    return urllib.error.URLError(TimeoutError("timed out"))


def http_error(code):
    return urllib.error.HTTPError(mpc_neocp.URL, code, f"HTTP {code}", None, None)


class FetchMpcNeocpRetryTests(unittest.TestCase):
    def run_fetch(self, responses):
        opener = QueueOpener(responses)
        sleeps = []
        with patch.object(mpc_neocp.urllib.request, "urlopen", opener), \
             patch.object(mpc_neocp.time, "sleep", sleeps.append):
            records = list(mpc_neocp.fetch_candidates(100, 5))
        return records, opener, sleeps

    def test_transient_timeouts_retried_then_succeed(self):
        records, opener, sleeps = self.run_fetch(
            [connection_timeout(), connection_timeout(), PAGE]
        )
        self.assertEqual(1, len(records))
        self.assertEqual("X91327", records[0]["id"])
        self.assertEqual(mpc_neocp.SOURCE, records[0]["source"])
        self.assertEqual(3, len(opener.requests))
        self.assertEqual([1.0, 2.0], sleeps)

    def test_retryable_503_retried_then_succeeds(self):
        records, opener, sleeps = self.run_fetch([http_error(503), PAGE])
        self.assertEqual(1, len(records))
        self.assertEqual(2, len(opener.requests))
        self.assertEqual([1.0], sleeps)

    def test_persistent_timeout_exhausts_attempts_and_raises(self):
        with self.assertRaises(urllib.error.URLError):
            self.run_fetch([connection_timeout()] * mpc_neocp.MAX_ATTEMPTS)

    def test_exhausted_attempts_make_bounded_requests(self):
        opener = QueueOpener([connection_timeout()] * mpc_neocp.MAX_ATTEMPTS)
        with patch.object(mpc_neocp.urllib.request, "urlopen", opener), \
             patch.object(mpc_neocp.time, "sleep", lambda _delay: None):
            with self.assertRaises(urllib.error.URLError):
                list(mpc_neocp.fetch_candidates(100, 5))
        self.assertEqual(mpc_neocp.MAX_ATTEMPTS, len(opener.requests))

    def test_non_retryable_http_error_fails_fast(self):
        opener = QueueOpener([http_error(404)])
        sleeps = []
        with patch.object(mpc_neocp.urllib.request, "urlopen", opener), \
             patch.object(mpc_neocp.time, "sleep", sleeps.append):
            with self.assertRaises(urllib.error.HTTPError) as context:
                list(mpc_neocp.fetch_candidates(100, 5))
        self.assertEqual(404, context.exception.code)
        self.assertEqual(1, len(opener.requests))
        self.assertEqual([], sleeps)


if __name__ == "__main__":
    unittest.main()
