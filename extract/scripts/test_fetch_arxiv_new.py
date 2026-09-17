import importlib.util
import io
import pathlib
import urllib.error
from datetime import datetime, timedelta, timezone
from email.message import Message
from unittest import TestCase, mock


SCRIPT = pathlib.Path(__file__).with_name("fetch_arxiv_new.py")
SPEC = importlib.util.spec_from_file_location("fetch_arxiv_new", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
NOW = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)
ATOM = b"""<?xml version='1.0' encoding='UTF-8'?>
<feed xmlns='http://www.w3.org/2005/Atom' xmlns:arxiv='http://arxiv.org/schemas/atom'>
  <entry>
    <id>http://arxiv.org/abs/2609.00001v1</id>
    <published>2026-09-17T10:00:00Z</published>
    <updated>2026-09-17T10:00:00Z</updated>
    <title> A useful\n paper </title>
    <summary> An abstract\n with detail. </summary>
    <author><name>Ada Lovelace</name></author>
    <category term='cs.AI'/>
    <arxiv:primary_category term='cs.AI'/>
  </entry>
</feed>"""
MALFORMED_XML = b"<feed><entry>"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def http_error(status, retry_after=None):
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError("http://invalid.test/private-query", status, "failure", headers, io.BytesIO())


def fetch(responses, *, sleep=None):
    if sleep is None:
        sleep = mock.Mock()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
        records = list(MODULE.fetch_new_papers(sleep=sleep, clock=lambda: NOW))
    return records, urlopen, sleep


class FetchArxivNewTests(TestCase):
    def test_immediate_success_preserves_response_schema_and_request_contract(self):
        records, urlopen, sleep = fetch([Response(ATOM)])

        self.assertEqual(records, [{
            "source": "arxiv",
            "fetched_at": NOW.isoformat(),
            "id": "http://arxiv.org/abs/2609.00001v1",
            "published": "2026-09-17T10:00:00Z",
            "updated": "2026-09-17T10:00:00Z",
            "title": "A useful paper",
            "abstract": "An abstract with detail.",
            "authors": ["Ada Lovelace"],
            "primary_category": "cs.AI",
            "categories": ["cs.AI"],
        }])
        request = urlopen.call_args.args[0]
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 60})
        sleep.assert_not_called()

    def test_429_then_success_reuses_identical_query_and_equivalent_records(self):
        immediate, _, _ = fetch([Response(ATOM)])
        records, urlopen, sleep = fetch([http_error(429, "8"), Response(ATOM)])

        self.assertEqual(records, immediate)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(
            [call.args[0].full_url for call in urlopen.call_args_list],
            [urlopen.call_args_list[0].args[0].full_url] * 2,
        )
        sleep.assert_called_once_with(8)

    def test_retry_after_delta_seconds_is_courteous_and_capped(self):
        _, _, sleep = fetch([http_error(429, "1"), Response(ATOM)])
        sleep.assert_called_once_with(3)

        _, _, sleep = fetch([http_error(429, "120"), Response(ATOM)])
        sleep.assert_called_once_with(30)

    def test_retry_after_http_dates_are_calculated_from_injected_clock(self):
        future = (NOW + timedelta(seconds=12)).strftime("%a, %d %b %Y %H:%M:%S GMT")
        _, _, sleep = fetch([http_error(429, future), Response(ATOM)])
        sleep.assert_called_once_with(12)

        past = (NOW - timedelta(seconds=1)).strftime("%a, %d %b %Y %H:%M:%S GMT")
        _, _, sleep = fetch([http_error(429, past), Response(ATOM)])
        sleep.assert_called_once_with(3)

    def test_missing_or_malformed_retry_after_uses_minimum_delay(self):
        for retry_after in (None, "not-a-date", "-1"):
            with self.subTest(retry_after=retry_after):
                _, _, sleep = fetch([http_error(429, retry_after), Response(ATOM)])
                sleep.assert_called_once_with(3)

    def test_cumulative_sleep_cap_applies_across_retries(self):
        sleeps = []
        records, urlopen, _ = fetch(
            [http_error(429, "30"), http_error(429, "30"), Response(ATOM)], sleep=sleeps.append
        )

        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleeps, [30, 30])
        self.assertLessEqual(sum(sleeps), 60)

    def test_429_exhaustion_closes_errors_without_final_sleep_or_url_diagnostic(self):
        errors = [http_error(429, "30") for _ in range(3)]
        sleep = mock.Mock()
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=errors):
            with mock.patch("sys.stderr", stderr):
                with self.assertRaises(urllib.error.HTTPError):
                    list(MODULE.fetch_new_papers(sleep=sleep, clock=lambda: NOW))

        self.assertEqual(sleep.call_args_list, [mock.call(30), mock.call(30)])
        self.assertTrue(all(error.fp.closed for error in errors))
        diagnostic = stderr.getvalue()
        self.assertIn("HTTP 429 after 3 attempt(s)", diagnostic)
        self.assertLess(len(diagnostic), 200)
        self.assertNotIn("private-query", diagnostic)

    def test_non_429_failure_is_immediate_and_diagnostic_is_bounded(self):
        error = http_error(503)
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen:
            with mock.patch("sys.stderr", stderr):
                with self.assertRaises(urllib.error.HTTPError):
                    list(MODULE.fetch_new_papers(sleep=mock.Mock(), clock=lambda: NOW))

        urlopen.assert_called_once()
        self.assertTrue(error.fp.closed)
        diagnostic = stderr.getvalue()
        self.assertIn("HTTP 503 after 1 attempt(s)", diagnostic)
        self.assertLess(len(diagnostic), 200)
        self.assertNotIn("private-query", diagnostic)

    def test_transport_failure_is_immediate_and_has_safe_failure_category(self):
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=urllib.error.URLError("secret endpoint")) as urlopen:
            with mock.patch("sys.stderr", stderr):
                with self.assertRaises(urllib.error.URLError):
                    list(MODULE.fetch_new_papers(sleep=mock.Mock(), clock=lambda: NOW))

        urlopen.assert_called_once()
        diagnostic = stderr.getvalue()
        self.assertIn("transport failure after 1 attempt(s)", diagnostic)
        self.assertLess(len(diagnostic), 200)
        self.assertNotIn("secret endpoint", diagnostic)

    def test_malformed_xml_is_not_retried(self):
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(MALFORMED_XML)) as urlopen:
            with self.assertRaises(MODULE.ET.ParseError):
                list(MODULE.fetch_new_papers(sleep=mock.Mock(), clock=lambda: NOW))

        urlopen.assert_called_once()

    def test_invalid_record_is_not_retried(self):
        invalid_atom = ATOM.replace(b"<id>http://arxiv.org/abs/2609.00001v1</id>", b"<id></id>")
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(invalid_atom)) as urlopen:
            with self.assertRaisesRegex(ValueError, "arXiv entry missing id"):
                list(MODULE.fetch_new_papers(sleep=mock.Mock(), clock=lambda: NOW))

        urlopen.assert_called_once()


if __name__ == "__main__":
    import unittest
    unittest.main()
