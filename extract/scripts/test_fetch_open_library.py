#!/usr/bin/env python3
"""Behavioral tests for the Open Library recent-changes extractor."""
import importlib.util
import io
import pathlib
import unittest
from unittest.mock import patch


SCRIPT = pathlib.Path(__file__).with_name("fetch_open_library.py")
SPEC = importlib.util.spec_from_file_location("fetch_open_library", SCRIPT)
fetch_open_library = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetch_open_library)


class FixtureResponse(io.StringIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def event(author, *, event_id=1, kind="edit-book"):
    record = {
        "id": event_id,
        "kind": kind,
        "timestamp": "2026-09-17T12:00:00Z",
        "comment": "updated record",
        "changes": [{"key": "/books/OL1M"}, {"key": "/works/OL1W"}],
    }
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


if __name__ == "__main__":
    unittest.main()
