from __future__ import annotations

import contextlib
import importlib.util
import io
import json
import unittest
from pathlib import Path
from unittest import mock
import urllib.error


SCRIPT = Path(__file__).resolve().parent / "scripts" / "fetch_crossref_new_dois.py"
SPEC = importlib.util.spec_from_file_location("fetch_crossref_new_dois", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

URL = "https://api.crossref.org/works"


class RawResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def work(doi: str, created: str) -> dict:
    return {"DOI": doi, "created": {"date-time": created}}


def page(items, next_cursor=None) -> dict:
    message = {"items": items, "total-results": len(items)}
    if next_cursor is not None:
        message["next-cursor"] = next_cursor
    return {"message": message}


def http_error(code: int, headers=None) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(URL, code, "error", headers or {}, None)


class QueueOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        if not self.responses:
            raise AssertionError(f"unexpected HTTP request: {request.full_url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return RawResponse(json.dumps(response).encode("utf-8"))


class FetchCrossrefNewDoisRetryTest(unittest.TestCase):
    def run_fetch(self, opener, **kwargs):
        sleeps = []
        with mock.patch.object(MODULE.urllib.request, "urlopen", opener), \
                mock.patch.object(MODULE.time, "sleep", sleeps.append), \
                contextlib.redirect_stderr(io.StringIO()):
            records = list(MODULE.fetch_new_works(**kwargs))
        return records, sleeps

    def test_transient_500_retries_then_succeeds(self):
        opener = QueueOpener([
            http_error(500),
            page([work("10.1/first", "2026-10-08T07:45:00")], next_cursor="c2"),
            page([work("10.1/second", "2026-10-08T07:46:00")]),
        ])
        records, sleeps = self.run_fetch(
            opener, hours_back=1, watermark="2026-10-08T07:44:00")
        self.assertEqual([r["id"] for r in records], ["10.1/first", "10.1/second"])
        self.assertEqual(len(opener.requests), 3)
        self.assertEqual(sleeps, [5.0])  # base delay before the successful retry

    def test_persistent_500_exhausts_attempts(self):
        opener = QueueOpener([http_error(500)] * MODULE.MAX_ATTEMPTS)
        with mock.patch.object(MODULE.urllib.request, "urlopen", opener), \
                mock.patch.object(MODULE.time, "sleep", lambda _s: None), \
                contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_new_works(hours_back=1,
                                            watermark="2026-10-08T07:44:00"))
        self.assertEqual(ctx.exception.code, 500)
        self.assertEqual(len(opener.requests), MODULE.MAX_ATTEMPTS)

    def test_non_transient_404_not_retried(self):
        opener = QueueOpener([http_error(404), page([])])
        sleeps = []
        with mock.patch.object(MODULE.urllib.request, "urlopen", opener), \
                mock.patch.object(MODULE.time, "sleep", sleeps.append), \
                contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_new_works(hours_back=1,
                                            watermark="2026-10-08T07:44:00"))
        self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(len(opener.requests), 1)
        self.assertEqual(sleeps, [])

    def test_429_honors_bounded_retry_after(self):
        opener = QueueOpener([
            http_error(429, headers={"Retry-After": "7"}),
            page([work("10.1/third", "2026-10-08T07:47:00")]),
        ])
        records, sleeps = self.run_fetch(
            opener, hours_back=1, watermark="2026-10-08T07:44:00")
        self.assertEqual([r["id"] for r in records], ["10.1/third"])
        self.assertEqual(sleeps, [7.0])

    def test_retry_after_capped_at_max(self):
        opener = QueueOpener([
            http_error(429, headers={"Retry-After": "900"}),
            page([work("10.1/fourth", "2026-10-08T07:48:00")]),
        ])
        _, sleeps = self.run_fetch(
            opener, hours_back=1, watermark="2026-10-08T07:44:00")
        self.assertEqual(sleeps, [MODULE.MAX_RETRY_DELAY])


if __name__ == "__main__":
    unittest.main()
