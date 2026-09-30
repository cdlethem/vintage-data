#!/usr/bin/env python3
"""Behavioral tests for the Open Library recent-changes extractor."""
import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error
from unittest.mock import patch


payload_url = "https://openlibrary.org/recentchanges.json?limit=1"
SCRIPT = pathlib.Path(__file__).with_name("fetch_open_library.py")
SPEC = importlib.util.spec_from_file_location("fetch_open_library", SCRIPT)
fetch_open_library = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetch_open_library)


class FixtureResponse(io.StringIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def event(author, *, event_id=1, kind="edit-book", changes=...):
    record = {
        "id": event_id,
        "kind": kind,
        "timestamp": "2026-09-17T12:00:00Z",
        "comment": "updated record",
    }
    record["changes"] = [{"key": "/books/OL1M"}, {"key": "/works/OL1W"}] if changes is ... else changes
    if author is not ...:
        record["author"] = author
    return record


class FetchRecentTests(unittest.TestCase):
    def fetch(self, payload, **kwargs):
        requests = []

        def urlopen(request, timeout):
            requests.append((request, timeout))
            return FixtureResponse(__import__("json").dumps(payload))

        with patch.object(fetch_open_library.urllib.request, "urlopen", urlopen):
            records = list(fetch_open_library.fetch_recent(**kwargs))
        return records, requests

    def test_preserves_object_string_missing_and_null_authors(self):
        payload = [
            event({"key": "/people/object_author"}, event_id=1),
            event("string_author", event_id=2),
            event("", event_id=3),
            event(..., event_id=4),
            event(None, event_id=5),
        ]

        records, requests = self.fetch(payload, limit=5)

        self.assertEqual(
            [record["author"] for record in records],
            ["/people/object_author", "string_author", "", None, None],
        )
        self.assertEqual(requests[0][0].full_url, fetch_open_library.BASE + "?limit=5")
        self.assertEqual(requests[0][1], 30)
        self.assertEqual(
            {key: value for key, value in records[0].items() if key not in {"author", "fetched_at"}},
            {
                "source": "open_library",
                "id": 1,
                "kind": "edit-book",
                "timestamp": "2026-09-17T12:00:00Z",
                "comment": "updated record",
                "n_changes": 2,
                "changed_keys": ["/books/OL1M", "/works/OL1W"],
            },
        )
        self.assertTrue(all(record["fetched_at"] == records[0]["fetched_at"] for record in records))

    def test_rejects_non_object_non_string_authors_including_falsey_values(self):
        for author in (False, 0, [], 3.5):
            with self.subTest(author=author):
                with self.assertRaisesRegex(
                    ValueError, "author must be an object, string, or null"
                ):
                    self.fetch([event(author)])

    def test_selects_kind_endpoint_and_preserves_limit(self):
        records, requests = self.fetch([event("editor", kind="add-cover")], limit=7, kind="add-cover")

        self.assertEqual(
            requests[0][0].full_url,
            "https://openlibrary.org/recentchanges/add-cover.json?limit=7",
        )
        self.assertEqual(records[0]["kind"], "add-cover")
        self.assertEqual(records[0]["author"], "editor")

    def test_parses_stringified_changes_from_unfiltered_feed(self):
        stringified = json.dumps([
            {"key": "/people/marco_v3r/usergroup", "revision": 13},
            {"key": "/people/marco_v3r/permission", "revision": 13},
        ])
        records, _ = self.fetch([
            event("/people/marco_v3r", event_id=174163147, kind=None, changes=stringified)
        ])

        self.assertEqual(records[0]["author"], "/people/marco_v3r")
        self.assertEqual(records[0]["n_changes"], 2)
        self.assertEqual(
            records[0]["changed_keys"],
            ["/people/marco_v3r/usergroup", "/people/marco_v3r/permission"],
        )

    def test_tolerates_malformed_non_list_and_null_changes(self):
        cases = [
            ("not a json list", 0, []),
            ('{"key": "/books/OL1M"}', 0, []),
            (None, 0, []),
            ([{"key": "/books/OL1M"}, "stray"], 2, ["/books/OL1M"]),
        ]
        for changes, n_changes, changed_keys in cases:
            with self.subTest(changes=changes):
                records, _ = self.fetch([event("editor", changes=changes)])
                self.assertEqual(records[0]["n_changes"], n_changes)
                self.assertEqual(records[0]["changed_keys"], changed_keys)


class RetriesTests(unittest.TestCase):
    def run_fetch(self, urlopen, **kwargs):
        with patch.object(fetch_open_library.urllib.request, "urlopen", urlopen), \
             patch.object(fetch_open_library.time, "sleep") as sleep:
            records = list(fetch_open_library.fetch_recent(**kwargs))
        return records, sleep

    def test_transient_timeout_retries_then_succeeds(self):
        payload = [event("editor")]
        outcomes = [urllib.error.URLError(TimeoutError("timed out")), payload]

        def urlopen(request, timeout):
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return FixtureResponse(json.dumps(outcome))

        records, sleep = self.run_fetch(urlopen, limit=1)
        self.assertEqual(records[0]["id"], 1)
        self.assertEqual([call.args for call in sleep.call_args_list], [(1.0,)])

    def test_persistent_failure_raises_after_bounded_attempts(self):
        def urlopen(request, timeout):
            raise urllib.error.URLError(TimeoutError("timed out"))

        with patch.object(fetch_open_library.urllib.request, "urlopen", urlopen), \
             patch.object(fetch_open_library.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.URLError):
                list(fetch_open_library.fetch_recent(limit=1))
        self.assertEqual(sleep.call_count, fetch_open_library.MAX_ATTEMPTS - 1)
        self.assertEqual(
            [call.args for call in sleep.call_args_list],
            [(1.0,), (2.0,)])

    def test_transient_5xx_retries_then_succeeds(self):
        payload = [event("editor")]
        outcomes = [urllib.error.HTTPError(payload_url, 503, "unavailable", {}, None),
                    payload]

        def urlopen(request, timeout):
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return FixtureResponse(json.dumps(outcome))

        records, sleep = self.run_fetch(urlopen, limit=1)
        self.assertEqual(records[0]["id"], 1)
        self.assertEqual([call.args for call in sleep.call_args_list], [(1.0,)])

    def test_flapping_404_retries_then_succeeds(self):
        payload = [event("editor")]
        outcomes = [urllib.error.HTTPError(payload_url, 404, "not found", {}, None),
                    payload]

        def urlopen(request, timeout):
            outcome = outcomes.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return FixtureResponse(json.dumps(outcome))

        records, sleep = self.run_fetch(urlopen, limit=1)
        self.assertEqual(records[0]["id"], 1)
        self.assertEqual([call.args for call in sleep.call_args_list], [(1.0,)])

    def test_persistent_404_raises_after_bounded_attempts(self):
        def urlopen(request, timeout):
            raise urllib.error.HTTPError(payload_url, 404, "not found", {}, None)

        with patch.object(fetch_open_library.urllib.request, "urlopen", urlopen), \
             patch.object(fetch_open_library.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                list(fetch_open_library.fetch_recent(limit=1))
        self.assertEqual(sleep.call_count, fetch_open_library.MAX_ATTEMPTS - 1)

    def test_non_retryable_http_status_fails_immediately(self):
        def urlopen(request, timeout):
            raise urllib.error.HTTPError(payload_url, 410, "gone", {}, None)

        with patch.object(fetch_open_library.urllib.request, "urlopen", urlopen), \
             patch.object(fetch_open_library.time, "sleep") as sleep:
            with self.assertRaises(urllib.error.HTTPError):
                list(fetch_open_library.fetch_recent(limit=1))
        sleep.assert_not_called()

if __name__ == "__main__":
    unittest.main()
