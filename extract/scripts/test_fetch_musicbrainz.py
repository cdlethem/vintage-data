import io
import importlib.util
import json
from datetime import datetime, timezone
from email.message import Message
from pathlib import Path
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).with_name("fetch_musicbrainz.py")
SPEC = importlib.util.spec_from_file_location("fetch_musicbrainz", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class Clock:
    def __init__(self, now=0.0):
        self.now = now
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.now += delay


class FixedDatetime(datetime):
    @classmethod
    def now(cls, tz=None):
        value = cls(2026, 9, 17, 12, 0, tzinfo=timezone.utc)
        return value if tz is not None else value.replace(tzinfo=None)


def http_error(status=503, retry_after=None, body=b"unsafe response body"):
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError(
        "https://musicbrainz.org/private-query", status, "service unavailable", headers, io.BytesIO(body)
    )


def release(**changes):
    value = {
        "id": "release-1",
        "title": "A Release",
        "artist-credit": [{"name": "Artist"}],
        "release-group": {"primary-type": "Album"},
        "date": "2026-09-17",
        "country": "US",
        "status": "Official",
        "track-count": 10,
        "score": 100,
    }
    value.update(changes)
    return value


class FetchMusicBrainzTests(unittest.TestCase):
    def setUp(self):
        MODULE._last_request[0] = None

    def get(self, responses, *, url="https://musicbrainz.org/ws/2/release/?query=date%3A2026-09-17"):
        clock = Clock()
        with mock.patch.object(MODULE.time, "monotonic", clock.monotonic), mock.patch.object(
            MODULE.time, "sleep", clock.sleep
        ), mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
            result = MODULE._get(url)
        return result, clock, urlopen

    def test_successful_output_and_query_parameters_are_unchanged(self):
        clock = Clock()
        document = {"releases": [release()]}
        with mock.patch.object(MODULE, "datetime", FixedDatetime), mock.patch.object(
            MODULE.time, "monotonic", clock.monotonic
        ), mock.patch.object(MODULE.time, "sleep", clock.sleep), mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)
        ) as urlopen:
            records = list(MODULE.search_releases("date:2026-09-17", limit=7, offset=3))

        self.assertEqual(records, [{
            "source": "musicbrainz", "fetched_at": "2026-09-17T12:00:00+00:00",
            "id": "release-1", "title": "A Release", "artists": ["Artist"],
            "date": "2026-09-17", "country": "US", "status": "Official",
            "release_group_type": "Album", "track_count": 10, "score": 100,
        }])
        request = urlopen.call_args.args[0]
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 30)
        self.assertEqual(request.headers["User-agent"], MODULE.USER_AGENT)
        self.assertEqual(request.headers["Accept"], "application/json")
        params = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
        self.assertEqual(params, {"query": ["date:2026-09-17"], "fmt": ["json"],
                                  "limit": ["7"], "offset": ["3"]})

    def test_valid_integer_and_http_date_retry_after_recover(self):
        cases = (("4", 4.0), ("Wed, 17 Sep 2026 12:00:05 GMT", 5.0))
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after):
                self.setUp()
                clock = Clock()
                with mock.patch.object(MODULE, "datetime", FixedDatetime), mock.patch.object(
                    MODULE.time, "monotonic", clock.monotonic
                ), mock.patch.object(MODULE.time, "sleep", clock.sleep), mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=[http_error(retry_after=retry_after), JsonResponse({"releases": []})],
                ) as urlopen:
                    self.assertEqual(MODULE._get("https://example.test/query"), {"releases": []})
                self.assertEqual(clock.sleeps, [expected_delay])
                self.assertEqual(urlopen.call_count, 2)
                self.assertEqual(urlopen.call_args_list[0].args[0].full_url,
                                 urlopen.call_args_list[1].args[0].full_url)

    def test_malformed_numeric_retry_after_uses_attempt_fallbacks(self):
        malformed_values = ("-1", "+1", "4.5", "1e2", " 4 ", "\t4")
        for retry_after in malformed_values:
            with self.subTest(retry_after=retry_after):
                self.setUp()
                result, clock, urlopen = self.get([
                    http_error(retry_after=retry_after), http_error(retry_after=retry_after),
                    JsonResponse({"releases": []}),
                ])
                self.assertEqual(result, {"releases": []})
                self.assertEqual(clock.sleeps, [1.0, 2.0])
                self.assertEqual(urlopen.call_count, 3)

    def test_expired_zero_missing_and_excessive_retry_after_are_bounded(self):
        cases = ((None, 1.0), ("not-a-date", 1.0),
                 ("Wed, 17 Sep 2026 11:59:59 GMT", 1.0), ("0", 1.0), ("999", 30.0))
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after):
                self.setUp()
                clock = Clock()
                with mock.patch.object(MODULE, "datetime", FixedDatetime), mock.patch.object(
                    MODULE.time, "monotonic", clock.monotonic
                ), mock.patch.object(MODULE.time, "sleep", clock.sleep), mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=[http_error(retry_after=retry_after), JsonResponse({"releases": []})],
                ):
                    MODULE._get("https://example.test/query")
                self.assertEqual(clock.sleeps, [expected_delay])
    def test_third_attempt_recovery_and_exhaustion_are_capped_and_redacted(self):
        self.setUp()
        result, clock, urlopen = self.get([
            http_error(retry_after="30"), http_error(retry_after="30"), JsonResponse({"releases": []})
        ])
        self.assertEqual(result, {"releases": []})
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(clock.sleeps, [30.0, 30.0])
        self.assertEqual(sum(clock.sleeps), 60.0)

        self.setUp()
        clock = Clock()
        with mock.patch.object(MODULE.time, "monotonic", clock.monotonic), mock.patch.object(
            MODULE.time, "sleep", clock.sleep
        ), mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[
            http_error(body=b"token=do-not-disclose"), http_error(), http_error()
        ]) as urlopen:
            with self.assertRaisesRegex(RuntimeError, r"HTTP 503 after 3 attempts") as raised:
                MODULE._get("https://musicbrainz.org/ws/2/release/?query=private")
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(clock.sleeps, [1.0, 2.0])
        self.assertIsNone(raised.exception.__cause__)
        self.assertNotIn("private", str(raised.exception))
        self.assertNotIn("token", str(raised.exception))

    def test_pacing_covers_failed_attempts_and_separate_queries_without_extra_sleep(self):
        self.setUp()
        clock = Clock()
        with mock.patch.object(MODULE.time, "monotonic", clock.monotonic), mock.patch.object(
            MODULE.time, "sleep", clock.sleep
        ), mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[
            http_error(retry_after="0"), JsonResponse({"releases": []}), JsonResponse({"releases": []})
        ]) as urlopen:
            MODULE._get("https://example.test/first")
            MODULE._get("https://example.test/second")
        self.assertEqual(clock.sleeps, [1.0, 1.0])
        self.assertEqual([call.args[0].full_url for call in urlopen.call_args_list], [
            "https://example.test/first", "https://example.test/first", "https://example.test/second",
        ])
        self.assertEqual(MODULE._last_request[0], 2.0)

    def test_non_503_transport_json_and_malformed_records_fail_without_retry(self):
        cases = (
            (http_error(status=500), MODULE._get),
            (urllib.error.URLError("offline"), MODULE._get),
            (io.BytesIO(b"not json"), MODULE._get),
            (JsonResponse({"releases": [{}]}), lambda url: list(MODULE.search_releases("date:2026-09-17"))),
        )
        for response, operation in cases:
            with self.subTest(response=type(response).__name__):
                self.setUp()
                clock = Clock()
                with mock.patch.object(MODULE.time, "monotonic", clock.monotonic), mock.patch.object(
                    MODULE.time, "sleep", clock.sleep
                ), mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[response]) as urlopen:
                    with self.assertRaises(Exception):
                        operation("https://example.test/query")
                self.assertEqual(urlopen.call_count, 1)
                self.assertEqual(clock.sleeps, [])


if __name__ == "__main__":
    unittest.main()
