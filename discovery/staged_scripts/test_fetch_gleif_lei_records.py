#!/usr/bin/env python3
"""Offline behavioral tests for fetch_gleif_lei_records.py."""

import copy
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone

HERE = pathlib.Path(__file__).resolve().parent
FIXTURES = HERE / "fixtures" / "gleif_lei_records"
SPEC = importlib.util.spec_from_file_location(
    "fetch_gleif_lei_records", HERE / "fetch_gleif_lei_records.py"
)
gleif = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(gleif)

FIXED_NOW = datetime(2026, 9, 17, 3, 0, tzinfo=timezone.utc)
NEXT_URL = "https://api.gleif.org/api/v1/lei-records?page[number]=2&page[size]=2"


def fixture(name):
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def write_state(path, *, watermark=None, boundary=(), continuation=None):
    state = gleif.default_state()
    state.update(
        watermark=watermark,
        boundary_ids=list(boundary),
        continuation_url=continuation,
    )
    path.write_text(json.dumps(state, sort_keys=True) + "\n", encoding="utf-8")
    return path.read_bytes()


class SequenceTransport:
    def __init__(self, pages):
        self.pages = list(pages)
        self.urls = []

    def __call__(self, url, timeout):
        self.urls.append(url)
        if not self.pages:
            raise AssertionError("unexpected request")
        return copy.deepcopy(self.pages.pop(0))


class FakeClock:
    def __init__(self):
        self.value = 20.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    def sleep(self, delay):
        self.sleeps.append(delay)
        self.value += delay


class FailingOutput(io.StringIO):
    def write(self, value):
        raise OSError("simulated output failure")


class GleifExtractorTests(unittest.TestCase):
    def run_extract(self, path, transport, **overrides):
        output = overrides.pop("output", io.StringIO())
        clock = overrides.pop("clock", FakeClock())
        count = gleif.run(
            path=path,
            output=output,
            now=lambda: FIXED_NOW,
            monotonic=clock.monotonic,
            sleep=clock.sleep,
            transport=transport,
            lookback_hours=overrides.pop("lookback_hours", 24),
            page_size=overrides.pop("page_size", 2),
            max_pages=overrides.pop("max_pages", 10),
            timeout=overrides.pop("timeout", 5),
            **overrides,
        )
        return count, output, clock

    def test_follows_returned_link_and_emits_stable_envelopes(self):
        transport = SequenceTransport([fixture("page_1.json"), fixture("page_2.json")])
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            count, output, clock = self.run_extract(state_path, transport)
            state = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(count, 4)
        self.assertEqual(transport.urls[1], NEXT_URL)
        initial = transport.urls[0]
        self.assertTrue(initial.startswith(gleif.ENDPOINT + "?"))
        query = gleif.urllib.parse.parse_qs(gleif.urllib.parse.urlsplit(initial).query)
        self.assertEqual(query["sort"], ["registration.lastUpdateDate"])
        self.assertEqual(query["page[size]"], ["2"])
        self.assertEqual(
            query["filter[registration.lastUpdateDate][gte]"],
            ["2026-09-16T03:00:00Z"],
        )
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([record["id"] for record in records], [
            "AAAAAAAAAAAAAAAAAAAA",
            "BBBBBBBBBBBBBBBBBBBB",
            "CCCCCCCCCCCCCCCCCCCC",
            "DDDDDDDDDDDDDDDDDDDD",
        ])
        self.assertEqual({record["fetched_at"] for record in records}, {"2026-09-17T03:00:00Z"})
        self.assertEqual(
            {record["golden_copy_published_at"] for record in records},
            {"2026-09-17T10:00:00Z"},
        )
        original = fixture("page_1.json")["data"][0]
        self.assertEqual(records[0]["attributes"], original["attributes"])
        self.assertEqual(
            records[0]["relationship_links"],
            {name: value["links"] for name, value in original["relationships"].items()},
        )
        self.assertEqual(state["watermark"], "2026-09-17T02:00:00Z")
        self.assertEqual(state["boundary_ids"], ["DDDDDDDDDDDDDDDDDDDD"])
        self.assertIsNone(state["continuation_url"])
        self.assertEqual(clock.sleeps, [1.0])

    def test_capped_continuation_resumes_and_keeps_timestamp_boundary(self):
        first_transport = SequenceTransport([fixture("page_1.json")])
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            first_count, first_output, _ = self.run_extract(
                state_path, first_transport, max_pages=1
            )
            capped = json.loads(state_path.read_text(encoding="utf-8"))
            self.assertEqual(first_count, 2)
            self.assertEqual(capped["watermark"], "2026-09-17T01:00:00Z")
            self.assertEqual(capped["boundary_ids"], ["BBBBBBBBBBBBBBBBBBBB"])
            self.assertEqual(capped["continuation_url"], NEXT_URL)

            second_transport = SequenceTransport([fixture("page_2.json")])
            second_count, second_output, _ = self.run_extract(state_path, second_transport)
            finished = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(len(first_output.getvalue().splitlines()), 2)
        self.assertEqual(second_transport.urls, [NEXT_URL])
        self.assertEqual(second_count, 2)
        self.assertEqual(
            [json.loads(line)["id"] for line in second_output.getvalue().splitlines()],
            ["CCCCCCCCCCCCCCCCCCCC", "DDDDDDDDDDDDDDDDDDDD"],
        )
        self.assertEqual(finished["watermark"], "2026-09-17T02:00:00Z")
        self.assertEqual(finished["boundary_ids"], ["DDDDDDDDDDDDDDDDDDDD"])
        self.assertIsNone(finished["continuation_url"])

    def test_completed_boundary_is_inclusive_and_deduplicated(self):
        page = fixture("page_2.json")
        page["data"] = [
            {
                "type": "lei-records",
                "id": "DDDDDDDDDDDDDDDDDDDD",
                "attributes": {
                    "lei": "DDDDDDDDDDDDDDDDDDDD",
                    "registration": {"lastUpdateDate": "2026-09-17T02:00:00Z"},
                },
                "relationships": {},
            },
            {
                "type": "lei-records",
                "id": "EEEEEEEEEEEEEEEEEEEE",
                "attributes": {
                    "lei": "EEEEEEEEEEEEEEEEEEEE",
                    "registration": {"lastUpdateDate": "2026-09-17T02:00:00Z"},
                },
                "relationships": {},
            },
        ]
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            write_state(
                state_path,
                watermark="2026-09-17T02:00:00Z",
                boundary=["DDDDDDDDDDDDDDDDDDDD"],
            )
            count, output, _ = self.run_extract(state_path, SequenceTransport([page]))
            state = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(count, 1)
        self.assertEqual(json.loads(output.getvalue())["id"], "EEEEEEEEEEEEEEEEEEEE")
        self.assertEqual(
            state["boundary_ids"],
            ["DDDDDDDDDDDDDDDDDDDD", "EEEEEEEEEEEEEEEEEEEE"],
        )

    def test_malformed_second_page_leaves_state_and_output_untouched(self):
        transport = SequenceTransport([
            fixture("page_1.json"),
            fixture("malformed_page_2.json"),
        ])
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            before = write_state(state_path)
            output = io.StringIO()
            with self.assertRaisesRegex(ValueError, "attributes.lei"):
                self.run_extract(state_path, transport, output=output)
            self.assertEqual(state_path.read_bytes(), before)
            self.assertEqual(output.getvalue(), "")

    def test_output_failure_leaves_prior_state_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            state_path = pathlib.Path(directory) / "state.json"
            before = write_state(state_path)
            with self.assertRaisesRegex(OSError, "simulated output failure"):
                self.run_extract(
                    state_path,
                    SequenceTransport([fixture("page_1.json"), fixture("page_2.json")]),
                    output=FailingOutput(),
                )
            self.assertEqual(state_path.read_bytes(), before)

    def test_invalid_golden_copy_metadata_and_pagination_fail(self):
        cases = [
            ("invalid_metadata.json", "meta.goldenCopy.publishDate"),
            ("invalid_pagination.json", "unauthenticated GLEIF"),
        ]
        for name, message in cases:
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, message):
                    gleif.collect(
                        gleif.default_state(),
                        fetched_at=FIXED_NOW,
                        lookback_hours=24,
                        page_size=2,
                        max_pages=1,
                        timeout=5,
                        transport=SequenceTransport([fixture(name)]),
                        monotonic=lambda: 0.0,
                        sleep=lambda delay: None,
                    )

    def test_rejects_authenticated_or_untrusted_continuations_before_fetch(self):
        urls = [
            "https://user:password@api.gleif.org/api/v1/lei-records?page[number]=2",
            "https://example.invalid/api/v1/lei-records?page[number]=2",
            "http://api.gleif.org/api/v1/lei-records?page[number]=2",
            "https://api.gleif.org/api/v1/lei-records?page[number]=2&access_token=secret",
        ]
        for url in urls:
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, "unauthenticated GLEIF"):
                gleif.validate_continuation(url)
        transport = SequenceTransport([])
        state = gleif.default_state()
        state["continuation_url"] = urls[-1]
        with self.assertRaisesRegex(ValueError, "unauthenticated GLEIF"):
            gleif.collect(
                state,
                fetched_at=FIXED_NOW,
                lookback_hours=24,
                page_size=2,
                max_pages=1,
                timeout=5,
                transport=transport,
                monotonic=lambda: 0.0,
                sleep=lambda delay: None,
            )
        self.assertEqual(transport.urls, [])

    def test_throttle_uses_fake_clock_between_every_request(self):
        page_1 = fixture("page_1.json")
        page_2 = fixture("page_2.json")
        page_2["links"]["next"] = (
            "https://api.gleif.org/api/v1/lei-records?page[number]=3&page[size]=2"
        )
        page_3 = copy.deepcopy(page_2)
        page_3["links"]["next"] = None
        page_3["data"] = []
        clock = FakeClock()
        transport = SequenceTransport([page_1, page_2, page_3])
        count, _, _ = self.run_extract(None, transport, clock=clock)
        self.assertEqual(count, 4)
        self.assertEqual(clock.sleeps, [1.0, 1.0])


if __name__ == "__main__":
    unittest.main(verbosity=2)
