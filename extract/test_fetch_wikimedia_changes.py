import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_wikimedia_changes.py"
API = "https://en.wikipedia.org/w/api.php"
SPEC = importlib.util.spec_from_file_location("fetch_wikimedia_changes", SCRIPT)
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
    return urllib.error.HTTPError(API, code, "error", headers, io.BytesIO(b"wikimedia error"))


def recent_change(**overrides):
    record = {
        "rcid": 786712345,
        "ns": 0,
        "title": "Example article",
        "timestamp": "2026-10-28T03:15:42Z",
        "type": "edit",
        "user": "SomeEditor",
        "comment": "added citation",
        "oldlen": 12345,
        "newlen": 12400,
    }
    record.update(overrides)
    return record


def page(rows, cont=None):
    document = {"query": {"recentchanges": rows}}
    if cont:
        document["continue"] = {"rccontinue": cont}
    return document


class FetchWikimediaChangesTests(unittest.TestCase):
    def test_yields_normalized_records_from_json(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            return_value=response(page([recent_change(), recent_change(rcid=2, bot=True, minor=True, tags=["visualedit"])],))
        ) as urlopen:
            records = list(MODULE.fetch_recent_changes("en", limit=50))
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(len(records), 2)
        record = records[0]
        self.assertEqual(record["source"], "wikimedia_recentchanges")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], 786712345)
        self.assertEqual(record["wiki"], "en.wikipedia")
        self.assertEqual(record["ts"], "2026-10-28T03:15:42Z")
        self.assertEqual(record["type"], "edit")
        self.assertEqual(record["title"], "Example article")
        self.assertEqual(record["user"], "SomeEditor")
        self.assertFalse(record["bot"])
        self.assertFalse(record["minor"])
        self.assertEqual(record["comment"], "added citation")
        self.assertEqual(record["bytes_delta"], 55)
        self.assertEqual(record["tags"], [])
        second = records[1]
        self.assertEqual(second["id"], 2)
        self.assertTrue(second["bot"])
        self.assertTrue(second["minor"])
        self.assertEqual(second["tags"], ["visualedit"])

    def test_bytes_delta_is_none_when_lengths_missing(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               return_value=response(page([recent_change(oldlen=None, newlen=None)]))):
            records = list(MODULE.fetch_recent_changes("en", limit=50))
        self.assertIsNone(records[0]["bytes_delta"])

    def test_sends_required_user_agent(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen",
                               return_value=response(page([]))) as urlopen:
            list(MODULE.fetch_recent_changes("en", limit=50))
        request = urlopen.call_args_list[0].args[0]
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)

    def test_recovers_from_transient_503(self):
        # The failure mode from the 2026-10-28 task log: a brief 503 window on
        # en.wikipedia recentchanges must not lose the run.
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(503, "8"), response(page([recent_change()]))]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = list(MODULE.fetch_recent_changes("en", limit=50))
        self.assertEqual(len(records), 1)
        self.assertEqual(sleep.call_args_list, [mock.call(8)])
        self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response(page([recent_change()]))]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_recent_changes("en", limit=50))), 1)
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
                side_effect=[http_transient(503, retry_after), response(page([recent_change()]))]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_recent_changes("en", limit=50))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
                self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(503)] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_recent_changes("en", limit=50))
            self.assertEqual(ctx.exception.code, 503)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response(page([recent_change()]))]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_recent_changes("en", limit=50))), 1)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_recent_changes("en", limit=50))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        error = urllib.error.HTTPError(API, 404, "Not Found", {}, io.BytesIO(b"down"))
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, mock.patch.object(
            MODULE.time, "sleep"
        ) as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_recent_changes("en", limit=50))
            self.assertEqual(ctx.exception.code, 404)
        self.assertEqual(urlopen.call_count, 1)
        sleep.assert_not_called()

    def test_follows_rccontinue_pagination(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[response(page([recent_change(rcid=1)], cont="20261028031542|786712344")),
                         response(page([recent_change(rcid=2)]))]
        ) as urlopen:
            records = list(MODULE.fetch_recent_changes("en", limit=50))
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual([r["id"] for r in records], [1, 2])
        first_url = urlopen.call_args_list[0].args[0].full_url
        self.assertNotIn("rccontinue", first_url)
        second_url = urlopen.call_args_list[1].args[0].full_url
        self.assertIn("rccontinue=20261028031542%7C786712344", second_url)


if __name__ == "__main__":
    unittest.main()
