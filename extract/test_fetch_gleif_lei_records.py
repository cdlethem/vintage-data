"""Behavioral tests for fetch_gleif_lei_records.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path
from datetime import date, datetime, timezone
import urllib.parse

SCRIPT = Path(__file__).parent / "scripts" / "fetch_gleif_lei_records.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "gleif_lei_records.yml"
SPEC = importlib.util.spec_from_file_location("fetch_gleif_lei_records", SCRIPT)
gleif = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(gleif)

# NOW is a weekday afternoon; the most recent completed UTC day is 2026-09-29.
NOW = datetime(2026, 9, 30, 14, 0, tzinfo=timezone.utc)
GOLDEN = "2026-09-30T00:00:00Z"
def next_url(day_str):
    """Server-echoed continuation URL: filter value kept, operator omitted."""
    return (
        "https://api.gleif.org/api/v1/lei-records?filter%5Bregistration.lastUpdateDate%5D"
        f"={day_str}&page%5Bcursor%5D=token&page%5Bsize%5D=200"
    )


NEXT_URL = next_url("2026-09-29")


def resource(lei, ts):
    return {
        "type": "lei-records",
        "id": lei,
        "attributes": {
            "lei": lei,
            "entity": {"legalName": {"name": f"Entity {lei[:4]}", "language": "en"}},
            "registration": {"lastUpdateDate": ts, "status": "ISSUED"},
        },
        "relationships": {
            "direct-parent": {
                "links": {
                    "related": (
                        f"https://api.gleif.org/api/v1/lei-records/{lei}/direct-parent-relationship"
                    )
                }
            }
        },
    }


def page(records, next_url=None, *, with_next_key=True, golden=GOLDEN):
    """A lei-records page. with_next_key=False omits links.next entirely,
    as the live API does on the terminal cursor page."""
    links = {}
    if with_next_key:
        links["next"] = next_url
    return {
        "meta": {"goldenCopy": {"publishDate": golden}},
        "links": links,
        "data": records,
    }


class ScriptedTransport:
    """Serve a scripted list of documents per collected day, in request order."""

    def __init__(self, days):
        self.days = days
        self.requests = []

    def __call__(self, url, timeout):
        self.requests.append(url)
        query = urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query)
        # The first URL of a day walk carries filter[...lastUpdateDate]>=; the
        # server-echoed continuation links drop the operator.
        day = next(value for key, value in query
                   if key.startswith("filter[registration.lastUpdateDate]"))
        if day not in self.days or not self.days[day]:
            raise AssertionError(f"unexpected request for day {day}: {url}")
        return self.days[day].pop(0)


class FakeClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, delay):
        self.value += delay
        self.sleeps.append(delay)


class GleifLeiRecordsTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.state_file = Path(self._tmp.name) / "state" / "gleif_lei_records.json"

    def write_state(self, last_collected_date):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(
            json.dumps(
                {
                    "version": 1,
                    "source": "gleif_lei_records",
                    "last_collected_date": last_collected_date,
                }
            ),
            encoding="utf-8",
        )

    def run_fetcher(self, transport, *, state_file=None, now=NOW, **overrides):
        out = io.StringIO()
        err = io.StringIO()
        clock = FakeClock()
        with contextlib.redirect_stderr(err):
            count = gleif.run(
                path=state_file,
                output=out,
                transport=transport,
                now=lambda: now,
                monotonic=clock.monotonic,
                sleep=clock.sleep,
                **overrides,
            )
        return count, out.getvalue(), err.getvalue(), clock

    def parsed_summary(self, stderr_text):
        lines = [line for line in stderr_text.splitlines() if line.startswith(gleif.SUMMARY_PREFIX)]
        self.assertEqual(len(lines), 1)
        return json.loads(lines[0][len(gleif.SUMMARY_PREFIX):])

    def test_initial_run_collects_only_completed_days_oldest_first(self):
        transport = ScriptedTransport({
            "2026-09-27": [
                page([resource("25490010000000000001", "2026-09-27T00:00:01Z"),
                       resource("25490010000000000002", "2026-09-27T00:00:02Z")], next_url=next_url("2026-09-27")),
                page([], with_next_key=False),
            ],
            "2026-09-28": [page([resource("25490010000000000003", "2026-09-28T05:00:00Z")], next_url=None)],
            "2026-09-29": [page([resource("25490010000000000004", "2026-09-29T01:00:00Z"),
                                 resource("25490010000000000005", "2026-09-29T01:00:01Z")],
                                with_next_key=False)],
        })
        count, stdout, stderr, clock = self.run_fetcher(transport, state_file=self.state_file, lookback_days=3)

        self.assertEqual(count, 5)
        records = [json.loads(line) for line in stdout.splitlines()]
        self.assertEqual([r["id"] for r in records], [
            "25490010000000000001", "25490010000000000002",
            "25490010000000000003",
            "25490010000000000004", "25490010000000000005",
        ])
        self.assertEqual([r["updated_date"] for r in records], [
            "2026-09-27", "2026-09-27", "2026-09-28", "2026-09-29", "2026-09-29",
        ])
        self.assertTrue(all(r["source"] == "gleif_lei_records" for r in records))
        self.assertEqual({r["fetched_at"] for r in records}, {"2026-09-30T14:00:00Z"})

        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_collected_date"], "2026-09-29")

        summary = self.parsed_summary(stderr)
        self.assertEqual(summary["requests"], {"attempted": 4, "succeeded": 4})
        self.assertEqual(summary["state_change"], {"last_collected_date": "2026-09-29"})
        self.assertEqual(summary["metrics"]["days"], ["2026-09-27", "2026-09-28", "2026-09-29"])
        self.assertFalse(summary["metrics"]["truncated"])

        # Each day walk starts from the documented cursor-first URL.
        self.assertEqual(transport.requests[0], gleif.build_day_url(date(2026, 9, 27), 200))
        self.assertEqual(transport.requests[2], gleif.build_day_url(date(2026, 9, 28), 200))
        self.assertEqual(transport.requests[3], gleif.build_day_url(date(2026, 9, 29), 200))

    def test_initial_url_uses_day_filter_and_cursor_pagination(self):
        self.assertEqual(
            gleif.build_day_url(date(2026, 9, 29), 200),
            "https://api.gleif.org/api/v1/lei-records?"
            "filter%5Bregistration.lastUpdateDate%5D%3E%3D=2026-09-29"
            "&sort=registration.lastUpdateDate&page%5Bcursor%5D=%2A&page%5Bsize%5D=200",
        )

    def test_no_state_run_writes_no_state_file(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T01:00:00Z")], with_next_key=False)]})
        count, stdout, _, _ = self.run_fetcher(transport, state_file=None, lookback_days=1)
        self.assertEqual(count, 1)
        self.assertFalse(self.state_file.exists())

    def test_up_to_date_run_is_a_noop(self):
        self.write_state("2026-09-29")
        before = self.state_file.read_bytes()
        transport = ScriptedTransport({})
        count, stdout, stderr, _ = self.run_fetcher(transport, state_file=self.state_file)
        self.assertEqual(count, 0)
        self.assertEqual(stdout, "")
        self.assertEqual(transport.requests, [])
        self.assertEqual(self.state_file.read_bytes(), before)
        summary = self.parsed_summary(stderr)
        self.assertEqual(summary["requests"], {"attempted": 0, "succeeded": 0})
        self.assertEqual(summary["metrics"]["days_collected"], 0)

    def test_catchup_is_capped_and_resumes_where_it_stopped(self):
        self.write_state("2026-09-26")
        with mock.patch.object(gleif, "MAX_DAYS_PER_RUN", 2):
            transport = ScriptedTransport({
                "2026-09-27": [page([resource("25490010000000000001", "2026-09-27T00:00:01Z")],
                                    with_next_key=False)],
                "2026-09-28": [page([resource("25490010000000000003", "2026-09-28T05:00:00Z")],
                                    with_next_key=False)],
            })
            count, stdout, stderr, _ = self.run_fetcher(transport, state_file=self.state_file)
        self.assertEqual(count, 2)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_collected_date"], "2026-09-28")
        self.assertTrue(self.parsed_summary(stderr)["metrics"]["truncated"])

        # The next run picks up exactly where the truncated run stopped.
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T01:00:00Z")], with_next_key=False)]})
        count, stdout, stderr, _ = self.run_fetcher(transport, state_file=self.state_file)
        self.assertEqual(count, 1)
        self.assertEqual([json.loads(line)["id"] for line in stdout.splitlines()],
                         ["25490010000000000004"])
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_collected_date"], "2026-09-29")
        self.assertFalse(self.parsed_summary(stderr)["metrics"]["truncated"])

    def test_page_cap_failure_does_not_advance_state(self):
        self.write_state("2026-09-28")
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T01:00:00Z")], next_url=NEXT_URL),
            page([resource("25490010000000000005", "2026-09-29T01:00:01Z")], next_url=None),
        ]})
        with mock.patch.object(gleif, "MAX_PAGES_PER_DAY", 1):
            with self.assertRaisesRegex(ValueError, "exceeds the 1-page cap"):
                self.run_fetcher(transport, state_file=self.state_file)
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_collected_date"], "2026-09-28")

    def test_nonfinal_empty_page_is_rejected(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([], next_url=NEXT_URL)]})
        with self.assertRaisesRegex(ValueError, "non-final GLEIF page"):
            self.run_fetcher(transport, state_file=None, lookback_days=1)

    def test_terminal_empty_page_closes_an_empty_day(self):
        transport = ScriptedTransport({"2026-09-29": [page([], with_next_key=False)]})
        count, stdout, stderr, _ = self.run_fetcher(transport, state_file=self.state_file,
                                                    lookback_days=1)
        self.assertEqual(count, 0)
        self.assertEqual(stdout, "")
        state = json.loads(self.state_file.read_text(encoding="utf-8"))
        self.assertEqual(state["last_collected_date"], "2026-09-29")

    def test_record_outside_collected_day_is_rejected(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-28T23:59:59Z")], with_next_key=False)]})
        with self.assertRaisesRegex(ValueError, "outside collected day"):
            self.run_fetcher(transport, state_file=None, lookback_days=1)

    def test_duplicate_lei_across_pages_is_rejected(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T01:00:00Z")], next_url=NEXT_URL),
            page([resource("25490010000000000004", "2026-09-29T01:00:01Z")], with_next_key=False),
        ]})
        with self.assertRaisesRegex(ValueError, "duplicate LEI"):
            self.run_fetcher(transport, state_file=None, lookback_days=1)

    def test_descending_page_is_rejected(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T02:00:00Z"),
                  resource("25490010000000000005", "2026-09-29T01:00:00Z")], with_next_key=False)]})
        with self.assertRaisesRegex(ValueError, "ascending"):
            self.run_fetcher(transport, state_file=None, lookback_days=1)

    def test_missing_golden_copy_is_rejected(self):
        document = page([resource("25490010000000000004", "2026-09-29T01:00:00Z")],
                        with_next_key=False)
        document["meta"]["goldenCopy"] = {}
        transport = ScriptedTransport({"2026-09-29": [document]})
        with self.assertRaisesRegex(ValueError, "goldenCopy.publishDate"):
            self.run_fetcher(transport, state_file=None, lookback_days=1)

    def test_golden_copy_change_during_pagination_is_rejected(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T01:00:00Z")], next_url=NEXT_URL,
                 golden="2026-09-30T00:00:00Z"),
            page([resource("25490010000000000005", "2026-09-29T01:00:01Z")], with_next_key=False,
                 golden="2026-10-01T00:00:00Z"),
        ]})
        with self.assertRaisesRegex(ValueError, "changed during pagination"):
            self.run_fetcher(transport, state_file=None, lookback_days=1)

    def test_foreign_host_next_is_rejected(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T01:00:00Z")],
                 next_url="https://evil.example/api/v1/lei-records?page%5Bnumber%5D=2")]})
        with self.assertRaisesRegex(ValueError, "unauthenticated GLEIF lei-records URL"):
            self.run_fetcher(transport, state_file=None, lookback_days=1)

    def test_requests_are_rate_limited_to_one_per_second(self):
        transport = ScriptedTransport({"2026-09-29": [
            page([resource("25490010000000000004", "2026-09-29T01:00:00Z")], next_url=NEXT_URL),
            page([resource("25490010000000000005", "2026-09-29T01:00:01Z")], with_next_key=False),
        ]})
        _, _, _, clock = self.run_fetcher(transport, state_file=None, lookback_days=1)
        self.assertEqual(clock.sleeps, [1.0])

    def test_malformed_state_is_rejected(self):
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        self.state_file.write_text(json.dumps({"version": 9, "source": "gleif_lei_records",
                                               "last_collected_date": None}), encoding="utf-8")
        transport = ScriptedTransport({})
        with self.assertRaisesRegex(ValueError, "unexpected version"):
            self.run_fetcher(transport, state_file=self.state_file)

    def test_argument_bounds(self):
        transport = ScriptedTransport({})
        for bad in (dict(lookback_days=0), dict(lookback_days=32),
                    dict(page_size=0), dict(page_size=201), dict(timeout=0)):
            with self.assertRaises(ValueError):
                self.run_fetcher(transport, **bad)

    def test_source_config_references_this_fetcher(self):
        import yaml
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], gleif.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertEqual(len(config["schedule"].split()), 5)
        self.assertTrue(config["enabled"])


if __name__ == "__main__":
    unittest.main()
