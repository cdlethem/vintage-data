import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest import mock


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_bluesky_appview.py"
SPEC = importlib.util.spec_from_file_location("fetch_bluesky_appview", SCRIPT)
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


def author_feed(post):
    return {"feed": [{"post": post}]}


def http_transient(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.APPVIEW, code, "error", headers, io.BytesIO(b"origin failure"))


def post(uri="at://abc", handle="example.bsky.social", text="hello", **overrides):
    document = {
        "uri": uri,
        "author": {"handle": handle},
        "record": {"createdAt": "2026-08-01T00:00:00.000Z", "text": text, "langs": ["en"]},
        "likeCount": 3,
        "repostCount": 1,
        "replyCount": 2,
        "quoteCount": 0,
        "bookmarkCount": 1,
    }
    document.update(overrides)
    return document


class FetchBlueskyAppviewTests(unittest.TestCase):
    def test_yields_normalized_records_from_json(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(author_feed(post()))) as urlopen:
            records = list(MODULE.fetch_author_feed("example.bsky.social"))
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "bluesky")
        self.assertTrue(record["fetched_at"].endswith("+00:00"))
        self.assertEqual(record["id"], "at://abc")
        self.assertEqual(record["author"], "example.bsky.social")
        self.assertEqual(record["created"], "2026-08-01T00:00:00.000Z")
        self.assertEqual(record["text"], "hello")
        self.assertEqual(record["langs"], ["en"])
        self.assertEqual(record["likes"], 3)
        self.assertEqual(record["reposts"], 1)
        self.assertEqual(record["replies"], 2)
        self.assertEqual(record["quotes"], 0)
        self.assertEqual(record["bookmarks"], 1)

    def test_recovers_from_transient_read_timeouts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out"), response(author_feed(post()))]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_author_feed("example.bsky.social"))), 1)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_transient_server_errors(self):
        for code in (429, 500, 502, 503, 504):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(code), response(author_feed(post()))]
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                self.assertEqual(len(list(MODULE.fetch_author_feed("example.bsky.social"))), 1)
                self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
                self.assertEqual(urlopen.call_count, 2)

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[urllib.error.URLError("connection reset by peer"), response(author_feed(post()))]
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            self.assertEqual(len(list(MODULE.fetch_author_feed("example.bsky.social"))), 1)
            self.assertEqual(sleep.call_args_list, [mock.call(MODULE.RETRY_BASE_DELAY)])
            self.assertEqual(urlopen.call_count, 2)

    def test_stops_after_finite_read_timeouts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("The read operation timed out")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                list(MODULE.fetch_author_feed("example.bsky.social"))
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_stops_after_finite_transient_server_errors(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(502, "1")] * MODULE.MAX_ATTEMPTS
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                list(MODULE.fetch_author_feed("example.bsky.social"))
            self.assertEqual(ctx.exception.code, 502)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        for code in (401, 403, 404):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=http_transient(code)
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                with self.assertRaises(urllib.error.HTTPError) as ctx:
                    list(MODULE.fetch_author_feed("example.bsky.social"))
                self.assertEqual(ctx.exception.code, code)
            self.assertEqual(urlopen.call_count, 1)
            sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main()
