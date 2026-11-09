import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_stackexchange.py"
SPEC = importlib.util.spec_from_file_location("fetch_stackexchange", SCRIPT)
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


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.BASE, code, "error", headers, io.BytesIO(b"origin failure"))


def question_doc(count=2, quota=299):
    return {
        "quota_remaining": quota,
        "items": [
            {
                "question_id": 1000 + i,
                "title": f"Question {i}",
                "tags": ["python"],
                "creation_date": 1762700000 + i,
                "owner": {"display_name": "user", "user_type": "registered"},
                "score": 0,
                "view_count": 1,
                "answer_count": 0,
                "is_answered": False,
                "link": f"https://stackoverflow.com/questions/{1000 + i}",
            }
            for i in range(count)
        ],
    }


class FetchStackexchangeTests(unittest.TestCase):
    def test_recovers_from_521_with_bounded_backoff(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[http_error(521), http_error(521), response(question_doc())],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_recent("stackoverflow", pagesize=25))
        self.assertEqual([r["id"] for r in records], [1000, 1001])
        self.assertEqual(records[0]["quota_remaining"], 299)
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleep.call_args_list, [mock.call(5.0), mock.call(10.0)])

    def test_fails_after_finite_521_attempts_with_status_surfaced(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_error(521)] * MODULE.MAX_ATTEMPTS,
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_recent("stackoverflow", pagesize=25))
        self.assertEqual(ctx.exception.code, 521)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_other_http_errors_fail_immediately(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=[http_error(404)]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_recent("stackoverflow", pagesize=25))
        self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(sleep.call_count, 0)

    def test_connection_errors_are_retried(self):
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[urllib.error.URLError("read timed out"), response(question_doc(1))],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_recent("stackoverflow", pagesize=25))
        self.assertEqual([r["id"] for r in records], [1000])
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(sleep.call_args_list, [mock.call(5.0)])

    def test_retry_after_header_respected_and_capped(self):
        cases = ((None, 5.0), ("not-a-delay", 5.0), ("12", 12.0), ("900", 30.0))
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after), mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                side_effect=[http_error(503, retry_after), response(question_doc(1))],
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                list(MODULE.fetch_recent("stackoverflow", pagesize=25))
            self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
            self.assertEqual(urlopen.call_count, 2)


if __name__ == "__main__":
    unittest.main()
