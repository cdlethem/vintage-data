import contextlib
import importlib.util
import io
import json
import pathlib
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone
from unittest import mock


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_ted_notices.py"
SPEC = importlib.util.spec_from_file_location("fetch_ted_notices", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __init__(self, body, *, status=200, headers=None):
        super().__init__(body)
        self.status = status
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def feed(*items):
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<rss version="2.0"><channel>'
        '<title>TED | All notices | EN</title>'
        + "".join(items)
        + "</channel></rss>"
    ).encode("utf-8")


def item(number, year, title="Spain - Construction work", pub="30/10/2026 (UTC+01)", deadline=None,
         notice_type="Contract or concession notice - standard regime", description=None):
    if description is None:
        description = f"Publication date: {pub}"
        if deadline:
            description += f"; Deadline for receipt of tenders: {deadline}"
        description += f"; Notice type: {notice_type}"
    return (
        f"    <item>\n"
        f"      <title>{number}-{year}: {title}</title>\n"
        f"      <link>https://ted.europa.eu/en/notice/{number}-{year}</link>\n"
        f"      <description>{description}</description>\n"
        f"    </item>"
    )


def http_transient(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.GENERATE_URL, code, "error", headers, io.BytesIO(b"origin failure"))


class FixedDateTime(datetime):
    @classmethod
    def now(cls, tz=None):
        return cls(2026, 10, 31, 12, 34, 56, tzinfo=timezone.utc)


def run_main(argv, body, sleep_patch=True):
    stdout, stderr = io.StringIO(), io.StringIO()
    urlopen_mock = mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(body))
    sleep_mock = mock.patch.object(MODULE.time, "sleep")
    with urlopen_mock as urlopen, (sleep_mock if sleep_patch else mock.patch.object(MODULE.time, "sleep")), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(sys, "argv", argv), \
            contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = MODULE.main()
    return code, stdout.getvalue(), stderr.getvalue(), urlopen


def summary(stderr_text):
    for line in stderr_text.splitlines():
        if line.startswith(MODULE.SUMMARY_PREFIX):
            return json.loads(line[len(MODULE.SUMMARY_PREFIX):])
    return None


class ParseItemsTests(unittest.TestCase):
    def test_parses_items_with_and_without_deadlines(self):
        body = feed(
            item("756001", "2026"),
            item("756002", "2026", deadline="30/11/2026 (UTC+01), 30/11/2026 (UTC+01)"),
            item("756003", "2026", pub="30/10/2026 (UTC+00)",
                 notice_type="Contract or concession award notice \u2013 standard regime"),
        )
        items = MODULE.parse_items(body)
        self.assertEqual(
            items[0],
            {
                "id": "756001-2026",
                "title": "Spain - Construction work",
                "url": "https://ted.europa.eu/en/notice/756001-2026",
                "publication_date": "2026-10-30",
                "notice_type": "Contract or concession notice - standard regime",
                "tender_deadline": None,
            },
        )
        self.assertEqual(items[1]["tender_deadline"], "2026-11-30")
        self.assertEqual(items[2]["notice_type"], "Contract or concession award notice \u2013 standard regime")

    def test_takes_the_earliest_deadline_of_a_multi_lot_notice(self):
        items = MODULE.parse_items(
            feed(item("756002", "2026", deadline="12/11/2026 (UTC+01), 30/11/2026 (UTC+01)"))
        )
        self.assertEqual(items[0]["tender_deadline"], "2026-11-12")

    def test_rejects_unrecognised_notice_title(self):
        body = feed(item("756001", "2026", description="Publication date: 30/10/2026 (UTC+01)"))
        body = body.replace(b"756001-2026: Spain", b"Award notice without id: Spain")
        with self.assertRaisesRegex(MODULE.FeedParseError, "unrecognised notice title"):
            MODULE.parse_items(body)

    def test_rejects_missing_publication_date(self):
        with self.assertRaisesRegex(MODULE.FeedParseError, "unrecognised publication date"):
            MODULE.parse_items(feed(item("756001", "2026", description="Notice type: something")))

    def test_rejects_out_of_order_ids(self):
        body = feed(item("756002", "2026"), item("756001", "2026"))
        with self.assertRaisesRegex(MODULE.FeedParseError, "ascending"):
            MODULE.parse_items(body)

    def test_rejects_duplicate_ids(self):
        body = feed(item("756001", "2026"), item("756001", "2026"))
        with self.assertRaisesRegex(MODULE.FeedParseError, "duplicate"):
            MODULE.parse_items(body)

    def test_rejects_malformed_xml(self):
        with self.assertRaisesRegex(MODULE.FeedParseError, "not well-formed"):
            MODULE.parse_items(b"<rss><channel>")

    def test_unescapes_double_encoded_entities_in_title_and_description(self):
        body = feed(
            item(
                "756063",
                "2026",
                title="Italy \u2013 Snow-clearing \u2013 PROCEDURA L&amp;#8217;AFFIDAMENTO",
                notice_type="Contract or concession award notice &amp;#8211; standard regime",
            )
        )
        items = MODULE.parse_items(body)
        self.assertEqual(
            items[0]["title"], "Italy \u2013 Snow-clearing \u2013 PROCEDURA L\u2019AFFIDAMENTO"
        )
        self.assertEqual(
            items[0]["notice_type"], "Contract or concession award notice \u2013 standard regime"
        )


class FetchFeedTests(unittest.TestCase):
    def test_recovers_from_503_with_bounded_retry_after(self):
        cases = ((None, 1), ("not-a-delay", 1), ("900", 30))
        for retry_after, expected_delay in cases:
            with self.subTest(retry_after=retry_after), mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(503, retry_after), Response(feed(item("1", "2026")))],
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                MODULE.fetch_feed("5000", 17)
            self.assertEqual(sleep.call_args_list, [mock.call(expected_delay)])
            self.assertEqual(urlopen.call_count, 2)

    def test_retries_the_identical_url(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(502, "2"), Response(feed(item("1", "2026")))],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep"):
            MODULE.fetch_feed("5000", 23)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(urlopen.call_args_list[0][0][0].full_url, urlopen.call_args_list[1][0][0].full_url)

    def test_stops_after_finite_transient_attempts(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[http_transient(503, "1")] * MODULE.MAX_ATTEMPTS,
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(MODULE.FeedFetchError, "failed after 4 attempts"):
                MODULE.fetch_feed("5000", 9)
        self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
        self.assertEqual(sleep.call_count, MODULE.MAX_ATTEMPTS - 1)

    def test_fails_immediately_for_non_retryable_http_errors(self):
        for code in (400, 404):
            with self.subTest(code=code), mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=http_transient(code)
            ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
                with self.assertRaisesRegex(MODULE.FeedFetchError, f"HTTP {code}"):
                    MODULE.fetch_feed("5000", 9)
            self.assertEqual(urlopen.call_count, 1)
            sleep.assert_not_called()

    def test_recovers_from_transient_connection_failures(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen",
            side_effect=[TimeoutError("read timed out"), Response(feed(item("1", "2026")))],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            MODULE.fetch_feed("5000", 17)
        self.assertEqual(sleep.call_args_list, [mock.call(1)])
        self.assertEqual(urlopen.call_count, 2)


class MainTests(unittest.TestCase):
    def test_first_run_emits_full_window_and_writes_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            code, out, err, _ = run_main(
                [str(SCRIPT), "--state-file", str(state_path)],
                feed(item("756001", "2026"), item("756002", "2026"), item("756003", "2026")),
            )
            self.assertEqual(code, 0)
            records = [json.loads(line) for line in out.splitlines()]
            self.assertEqual([r["id"] for r in records], ["756001-2026", "756002-2026", "756003-2026"])
            for record in records:
                self.assertEqual(record["source"], MODULE.SOURCE)
                self.assertEqual(record["fetched_at"], "2026-10-31T12:34:56Z")
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state, {"version": 1, "source": MODULE.SOURCE, "last_id": "756003-2026"})
            digest = summary(err)
            self.assertEqual(digest["health"], "healthy")
            self.assertEqual(digest["completeness"], "complete")
            self.assertEqual(digest["metrics"]["mode"], "first-run snapshot")
            self.assertEqual(digest["metrics"]["emitted"], 3)
            self.assertFalse(digest["metrics"]["gap"])

    def test_incremental_run_emits_only_newer_notices(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(
                json.dumps({"version": 1, "source": MODULE.SOURCE, "last_id": "756001-2026"}), encoding="utf-8"
            )
            code, out, err, _ = run_main(
                [str(SCRIPT), "--state-file", str(state_path)],
                feed(item("756001", "2026"), item("756002", "2026"), item("756003", "2026")),
            )
            self.assertEqual(code, 0)
            self.assertEqual(
                [json.loads(line)["id"] for line in out.splitlines()],
                ["756002-2026", "756003-2026"],
            )
            state = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(state["last_id"], "756003-2026")
            digest = summary(err)
            self.assertEqual(digest["metrics"]["mode"], "incremental")
            self.assertEqual(digest["metrics"]["emitted"], 2)

    def test_incremental_run_orders_across_year_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(
                json.dumps({"version": 1, "source": MODULE.SOURCE, "last_id": "999999-2026"}), encoding="utf-8"
            )
            code, out, err, _ = run_main(
                [str(SCRIPT), "--state-file", str(state_path)], feed(item("1", "2027"))
            )
            self.assertEqual(code, 0)
            self.assertEqual(
                [json.loads(line)["id"] for line in out.splitlines()], ["1-2027"]
            )
            self.assertEqual(json.loads(state_path.read_text(encoding="utf-8"))["last_id"], "1-2027")

    def test_reports_gap_when_window_skips_watermark(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            state_path.write_text(
                json.dumps({"version": 1, "source": MODULE.SOURCE, "last_id": "100-2026"}), encoding="utf-8"
            )
            code, out, err, _ = run_main(
                [str(SCRIPT), "--state-file", str(state_path)],
                feed(item("200", "2026"), item("201", "2026")),
            )
            self.assertEqual(code, 0)
            self.assertIn("lost", err)
            digest = summary(err)
            self.assertEqual(digest["completeness"], "partial")
            self.assertEqual(digest["health"], "healthy")
            self.assertTrue(digest["metrics"]["gap"])
            self.assertEqual(
                [json.loads(line)["id"] for line in out.splitlines()], ["200-2026", "201-2026"]
            )

    def test_no_state_emits_full_window_without_writing_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            code, out, err, _ = run_main(
                [str(SCRIPT), "--no-state"], feed(item("756001", "2026"), item("756002", "2026"))
            )
            self.assertEqual(code, 0)
            self.assertEqual(len(out.splitlines()), 2)
            self.assertFalse(state_path.exists())
            self.assertEqual(summary(err)["metrics"]["mode"], "snapshot")

    def test_keeps_watermark_and_reports_partial_on_empty_feed(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            original = json.dumps({"version": 1, "source": MODULE.SOURCE, "last_id": "756001-2026"})
            state_path.write_text(original, encoding="utf-8")
            code, out, err, _ = run_main([str(SCRIPT), "--state-file", str(state_path)], feed())
            self.assertEqual(code, 0)
            self.assertEqual(out, "")
            self.assertEqual(state_path.read_text(encoding="utf-8"), original)
            digest = summary(err)
            self.assertEqual(digest["completeness"], "partial")
            self.assertEqual(digest["health"], "degraded")
            self.assertEqual(digest["requests"], {"attempted": 1, "succeeded": 1, "failed": 0, "payload_bytes": len(feed())})

    def test_bad_item_fails_run_without_advancing_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            original = json.dumps({"version": 1, "source": MODULE.SOURCE, "last_id": "756001-2026"})
            state_path.write_text(original, encoding="utf-8")
            body = feed(item("756002", "2026"), item("756003", "2026", description="garbage"))
            code, out, err, _ = run_main([str(SCRIPT), "--state-file", str(state_path)], body)
            self.assertEqual(code, 1)
            self.assertEqual(out, "")
            self.assertEqual(state_path.read_text(encoding="utf-8"), original)
            digest = summary(err)
            self.assertEqual(digest["health"], "failed")
            self.assertEqual(digest["completeness"], "failed")
            self.assertEqual(digest["requests"], {"attempted": 1, "succeeded": 0, "failed": 1})

    def test_transient_exhaustion_fails_run_with_summary_and_keeps_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            original = json.dumps({"version": 1, "source": MODULE.SOURCE, "last_id": "756001-2026"})
            state_path.write_text(original, encoding="utf-8")
            stdout, stderr = io.StringIO(), io.StringIO()
            with mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[http_transient(503, "1")] * MODULE.MAX_ATTEMPTS,
            ), mock.patch.object(MODULE.time, "sleep"), mock.patch.object(sys, "argv",
                [str(SCRIPT), "--state-file", str(state_path)]), contextlib.redirect_stdout(stdout), \
                    contextlib.redirect_stderr(stderr):
                code = MODULE.main()
            self.assertEqual(code, 1)
            self.assertEqual(stdout.getvalue(), "")
            self.assertEqual(state_path.read_text(encoding="utf-8"), original)
            digest = summary(stderr.getvalue())
            self.assertEqual(digest["health"], "failed")
            self.assertIn("failed after 4 attempts", digest["metrics"]["error"])

    def test_rejects_invalid_state_shape(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            for bad in (
                {"version": 1, "source": MODULE.SOURCE, "last_id": "not-an-id"},
                {"version": 2, "source": MODULE.SOURCE, "last_id": "1-2026"},
                {"version": 1, "source": "other", "last_id": "1-2026"},
                {"version": 1, "source": MODULE.SOURCE, "last_id": None},
            ):
                with self.subTest(bad=bad):
                    state_path.write_text(json.dumps(bad), encoding="utf-8")
                    with self.assertRaisesRegex(RuntimeError, "unexpected"):
                        MODULE.load_state(state_path)

    def test_missing_state_is_none(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(MODULE.load_state(pathlib.Path(directory) / "absent.json"))

    def test_state_path_must_be_outside_repository(self):
        with self.assertRaisesRegex(ValueError, "outside the repository"):
            MODULE.validate_state_path(str(MODULE.REPOSITORY_ROOT / "state" / "ted_notices.json"))


if __name__ == "__main__":
    unittest.main()
