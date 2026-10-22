#!/usr/bin/env python3
"""Offline behavioral tests for the EU publications (CELLAR ingestion feed) extractor."""
import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
import urllib.parse
from datetime import datetime, timedelta, timezone
from unittest import mock
import yaml

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_eu_publications.py"
SOURCE_CONFIG = pathlib.Path(__file__).parent / "sources" / "eu_publications.yml"
SPEC = importlib.util.spec_from_file_location("fetch_eu_publications", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

NOTIF_ENTRY = "http://publications.europa.eu/rss/notificationEntry"
NOTIF_REQ = "http://publications.europa.eu/rss/notificationRequest"


class XmlResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def feed_page(items, more=False):
    """Build one RSS page. items: (guid, cellar_id, root_cellar_id, date, change_type, identifiers)."""
    entries = []
    for guid, cellar, root_cellar, date, change_type, identifiers in items:
        id_els = "".join(
            f"<notifEntry:identifier>{i}</notifEntry:identifier>" for i in identifiers
        )
        root_id_els = "".join(
            f"<notifEntry:rootIdentifier>{i}</notifEntry:rootIdentifier>" for i in identifiers
        )
        entries.append(
            "<item>"
            f"<guid isPermaLink=\"false\">{guid}</guid>"
            f"<notifEntry:cellarId>{cellar}</notifEntry:cellarId>"
            f"<notifEntry:rootCellarId>{root_cellar}</notifEntry:rootCellarId>"
            f"<notifEntry:type>{change_type}</notifEntry:type>"
            f"<notifEntry:wemiClass>WORK</notifEntry:wemiClass>"
            f"<notifEntry:identifiers>{id_els}</notifEntry:identifiers>"
            f"<notifEntry:rootIdentifiers>{root_id_els}</notifEntry:rootIdentifiers>"
            f"<notifEntry:date>{date}</notifEntry:date>"
            "</item>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<rss version="2.0" xmlns:notifReq="{NOTIF_REQ}" xmlns:notifEntry="{NOTIF_ENTRY}">'
        "<channel>"
        "<title>IngestionNotificationMessagesResponse</title>"
        "<notifReq:page>1</notifReq:page>"
        f"<notifReq:moreEntries>{'true' if more else 'false'}</notifReq:moreEntries>"
        + "".join(entries)
        + "</channel></rss>"
    ).encode("utf-8")


def item(guid=None, date="2026-10-22T03:00:00.000+02:00", change_type="create",
         identifiers=("celex:02026R1234", "oj:OJ:L_2026123")):
    guid = guid or "cellar:aaaa1111-2222-3333-4444-555566667777_" + date
    cellar = guid.rsplit("_", 1)[0]
    return (guid, cellar, cellar, date, change_type, list(identifiers))


def http_error(code, reason="error"):
    return urllib.error.HTTPError(MODULE.FEED_URL, code, reason, {}, io.BytesIO(b""))


class FetchEuPublicationsTests(unittest.TestCase):
    def run_main(self, pages, *args):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen",
                side_effect=[XmlResponse(p) for p in pages],
            ) as urlopen,
            mock.patch.object(MODULE.time, "sleep") as sleep,
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            result = MODULE.main(list(args))
        summary_lines = [
            line for line in stderr.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):]) if summary_lines else None
        return result, stdout.getvalue().splitlines(), summary, urlopen, sleep

    def request_params(self, urlopen, index=0):
        request = urlopen.call_args_list[index].args[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(parsed.scheme + "://" + parsed.netloc + parsed.path, MODULE.FEED_URL)
        return urllib.parse.parse_qs(parsed.query)

    def test_emits_envelope_with_expected_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, summary, _, _ = self.run_main(
                [feed_page([item()])], "--state-file", str(state))
            stored = json.loads(state.read_text())
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 1)
        record = json.loads(output[0])
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["id"], "cellar:aaaa1111-2222-3333-4444-555566667777_2026-10-22T03:00:00.000+02:00")
        self.assertEqual(record["cellar_id"], "cellar:aaaa1111-2222-3333-4444-555566667777")
        self.assertEqual(record["root_cellar_id"], record["cellar_id"])
        self.assertEqual(record["change_type"], "create")
        self.assertEqual(record["wemi_class"], "work")
        self.assertEqual(record["publication_date"], "2026-10-22T03:00:00.000+02:00")
        self.assertEqual(record["identifiers"], ["celex:02026R1234", "oj:OJ:L_2026123"])
        self.assertEqual(record["identifier_kinds"], ["celex", "oj"])
        self.assertRegex(record["fetched_at"], r"\+00:00$")
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 1)
        # The watermark is stored UTC-normalized from the feed's CEST date.
        self.assertEqual(summary["coverage"]["watermark_after"], "2026-10-22T01:00:00+00:00")
        self.assertEqual(stored["watermark"], "2026-10-22T01:00:00+00:00")
        self.assertEqual(stored["boundary"], [record["id"]])

    def test_drops_ted_notices_but_still_advances_watermark(self):
        ted = item(
            guid="cellar:bbbb0000-1111-2222-3333-444455556666_2026-10-22T04:00:00.000+02:00",
            date="2026-10-22T04:00:00.000+02:00",
            identifiers=("ted:2026-1234",),
        )
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, summary, _, _ = self.run_main(
                [feed_page([item(), ted])], "--state-file", str(state))
            stored = json.loads(state.read_text())
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 1)
        self.assertNotEqual(json.loads(output[0])["id"], ted[0])
        self.assertEqual(summary["health"], "healthy")
        # The later TED notice still pushed the watermark to 04:00 CEST.
        self.assertEqual(summary["coverage"]["watermark_after"], "2026-10-22T02:00:00+00:00")
        self.assertEqual(stored["boundary"], [ted[0]])

    def test_follows_pagination_until_more_entries_false(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, summary, urlopen, _ = self.run_main(
                [feed_page([item()], more=True),
                 feed_page([item(date="2026-10-22T03:30:00.000+02:00")])],
                "--state-file", str(state))
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 2)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(summary["requests"]["attempted"], 2)
        self.assertEqual(self.request_params(urlopen, 1)["page"], ["2"])

    def test_stops_at_max_pages(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, summary, urlopen, _ = self.run_main(
                [feed_page([item()], more=True),
                 feed_page([item(date="2026-10-22T03:30:00.000+02:00")], more=True),
                 feed_page([item(date="2026-10-22T04:00:00.000+02:00")], more=True)],
                "--state-file", str(state), "--max-pages", "2")
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 2)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(summary["requests"]["attempted"], 2)

    def test_boundary_dedup_uses_stored_watermark(self):
        # g1 was emitted at the watermark timestamp in the previous run; the
        # inclusive re-query returns it again and it must not re-emit.
        g1 = item(guid="cellar:cc000000-0000-0000-0000-000000000001_2026-10-22T03:00:00.000+02:00")
        g2 = item(guid="cellar:cc000000-0000-0000-0000-000000000002_2026-10-22T03:00:00.000+02:00")
        g3 = item(guid="cellar:cc000000-0000-0000-0000-000000000003_2026-10-22T03:10:00.000+02:00",
                  date="2026-10-22T03:10:00.000+02:00")
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            state.write_text(json.dumps({
                "watermark": "2026-10-22T01:00:00+00:00", "boundary": [g1[0]]}))
            result, output, summary, urlopen, _ = self.run_main(
                [feed_page([g1, g2, g3])], "--state-file", str(state))
            stored = json.loads(state.read_text())
        self.assertEqual(result, 0)
        self.assertEqual([json.loads(line)["id"] for line in output], [g2[0], g3[0]])
        # The re-query starts exactly at the stored watermark.
        self.assertEqual(self.request_params(urlopen)["startDate"], ["2026-10-22T01:00:00+00:00"])
        self.assertEqual(stored["watermark"], "2026-10-22T01:10:00+00:00")
        self.assertEqual(stored["boundary"], [g3[0]])

    def test_retries_transient_503_then_succeeds(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            with (
                mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=[http_error(503, "Service Unavailable"),
                                 XmlResponse(feed_page([item()]))],
                ) as urlopen,
                mock.patch.object(MODULE.time, "sleep") as sleep,
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                result = MODULE.main(["--state-file", str(state)])
        self.assertEqual(result, 0)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(MODULE.RETRY_DELAYS_SECONDS[0])

    def test_non_transient_http_error_fails_immediately(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            with (
                mock.patch.object(MODULE.urllib.request, "urlopen",
                                  side_effect=http_error(404, "Not Found")) as urlopen,
                mock.patch.object(MODULE.time, "sleep") as sleep,
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                result = MODULE.main(["--state-file", str(state)])
            self.assertEqual(result, 1)
            urlopen.assert_called_once()
            sleep.assert_not_called()
            self.assertFalse(state.exists())

    def test_exhausted_retries_fail_run_without_state(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            stdout, stderr = io.StringIO(), io.StringIO()
            with (
                mock.patch.object(
                    MODULE.urllib.request, "urlopen",
                    side_effect=[http_error(503, "Service Unavailable")] * MODULE.MAX_ATTEMPTS,
                ) as urlopen,
                mock.patch.object(MODULE.time, "sleep"),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                result = MODULE.main(["--state-file", str(state)])
            self.assertEqual(result, 1)
            self.assertEqual(stdout.getvalue(), "")
            summary_line = [line for line in stderr.getvalue().splitlines()
                            if line.startswith(MODULE.SUMMARY_PREFIX)][0]
            summary = json.loads(summary_line[len(MODULE.SUMMARY_PREFIX):])
            self.assertEqual(summary["health"], "failed")
            self.assertEqual(summary["records"], 0)
            self.assertEqual(urlopen.call_count, MODULE.MAX_ATTEMPTS)
            self.assertFalse(state.exists())

    def test_malformed_item_fails_run_without_state(self):
        bad = item()
        page = feed_page([bad]).replace(
            b"<notifEntry:date>2026-10-22T03:00:00.000+02:00</notifEntry:date>", b"")
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, summary, _, _ = self.run_main([page], "--state-file", str(state))
            self.assertFalse(state.exists())
        self.assertEqual(result, 1)
        self.assertEqual(output, [])
        self.assertIn("no parseable date", summary["error"])

    def test_malformed_state_fails_before_download(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            state.write_text("not json")
            with (
                mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen,
                contextlib.redirect_stdout(io.StringIO()),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                result = MODULE.main(["--state-file", str(state)])
            self.assertEqual(result, 1)
            urlopen.assert_not_called()
            self.assertEqual(state.read_text(), "not json")

    def test_empty_feed_is_valid_empty_run(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, summary, urlopen, _ = self.run_main(
                [feed_page([])], "--state-file", str(state))
            self.assertFalse(state.exists())
        self.assertEqual(result, 0)
        self.assertEqual(output, [])
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["health"], "healthy")

    def test_no_state_uses_lookback_and_writes_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, output, summary, urlopen, _ = self.run_main(
                [feed_page([item()])], "--no-state", "--state-file", str(state))
            self.assertFalse(state.exists())
        self.assertEqual(result, 0)
        self.assertEqual(len(output), 1)
        start = datetime.fromisoformat(self.request_params(urlopen)["startDate"][0])
        age = datetime.now(timezone.utc) - start
        self.assertGreater(age, timedelta(hours=23))
        self.assertLess(age, timedelta(hours=25))

    def test_sends_expected_query_and_user_agent(self):
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            _, _, _, urlopen, _ = self.run_main(
                [feed_page([item()])], "--state-file", str(state))
        request = urlopen.call_args.args[0]
        params = self.request_params(urlopen)
        self.assertEqual(params["type"], ["create,update"])
        self.assertEqual(params["wemiClasses"], ["work"])
        self.assertEqual(params["page"], ["1"])
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)

    def test_source_config_is_daily_and_attributed(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], "eu_publications")
        self.assertEqual(config["script"], "fetch_eu_publications.py")
        self.assertEqual(len(config["schedule"].split()), 5)
        self.assertTrue(config["enabled"])
        self.assertTrue(config.get("rate_limit"))
        self.assertTrue(config.get("licence"))
        self.assertTrue(config.get("attribution_required"))

    def test_stored_watermark_stays_millisecond_precise(self):
        # The feed's startDate answers 400 for microsecond precision or a
        # 'Z' suffix, so the watermark we store and re-send must never have
        # more than three fractional-second digits.
        millisecond = item(date="2026-10-22T03:00:09.435+02:00")
        with tempfile.TemporaryDirectory() as directory:
            state = pathlib.Path(directory) / "state.json"
            result, _, summary, urlopen, _ = self.run_main(
                [feed_page([millisecond])], "--state-file", str(state))
            stored = json.loads(state.read_text())
        self.assertEqual(result, 0)
        self.assertEqual(stored["watermark"], "2026-10-22T01:00:09.435+00:00")
        self.assertEqual(summary["coverage"]["watermark_after"], "2026-10-22T01:00:09.435+00:00")
        start = datetime.fromisoformat(self.request_params(urlopen)["startDate"][0])
        age = datetime.now(timezone.utc) - start
        self.assertGreater(age, timedelta(hours=23))
        self.assertLess(age, timedelta(hours=25))


if __name__ == "__main__":
    unittest.main()
