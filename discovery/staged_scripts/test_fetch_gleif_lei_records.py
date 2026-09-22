#!/usr/bin/env python3
"""Offline behavioral tests for fetch_gleif_lei_records.py."""

import copy
import email.message
import importlib.util
import io
import json
import pathlib
import tempfile
import unittest
import urllib.request
import urllib.response
from datetime import datetime, timezone
from unittest import mock

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


class ScriptedHTTPSHandler(urllib.request.HTTPSHandler):
    """Serve scripted responses below urllib's redirect machinery."""

    def __init__(self, responses):
        super().__init__()
        self.responses = list(responses)
        self.urls = []
        self.timeouts = []

    def https_open(self, request):
        self.urls.append(request.full_url)
        self.timeouts.append(request.timeout)
        if not self.responses:
            raise AssertionError("unexpected outbound request")
        status, location, document = self.responses.pop(0)
        headers = email.message.Message()
        if location is not None:
            headers["Location"] = location
        body = b"" if document is None else json.dumps(document).encode("utf-8")
        response = urllib.response.addinfourl(
            io.BytesIO(body), headers, request.full_url, status
        )
        response.msg = "scripted response"
        return response


def scripted_opener(*responses):
    handler = ScriptedHTTPSHandler(responses)
    return gleif.build_http_opener(handler), handler


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

    def test_nonredirect_request_uses_configured_opener(self):
        page = fixture("page_2.json")
        opener, handler = scripted_opener((200, None, page))
        with mock.patch.object(gleif, "HTTP_OPENER", opener):
            result = gleif.request_json(NEXT_URL, 7)

        self.assertEqual(result, page)
        self.assertEqual(handler.urls, [NEXT_URL])
        self.assertEqual(handler.timeouts, [7])

    def test_all_redirect_statuses_fail_closed_on_initial_request(self):
        cases = [
            (301, "https://example.invalid/api/v1/lei-records", "foreign host"),
            (
                302,
                "https://user:redirect-secret@api.gleif.org/api/v1/lei-records",
                "credentials",
            ),
            (303, "http://api.gleif.org/api/v1/lei-records", "insecure URL"),
            (
                307,
                "https://api.gleif.org/api/v1/lei-records?page[number]=99",
                "same host",
            ),
            (308, "https://other.invalid/redirect-secret", "permanent redirect"),
        ]
        for status, destination, label in cases:
            with self.subTest(status=status, destination=label):
                opener, handler = scripted_opener((status, destination, None))
                output = io.StringIO()
                with tempfile.TemporaryDirectory() as directory:
                    path = pathlib.Path(directory) / "state.json"
                    before = write_state(path)
                    with mock.patch.object(gleif, "HTTP_OPENER", opener):
                        with self.assertRaises(ValueError) as raised:
                            self.run_extract(path, gleif.request_json, output=output)
                    self.assertEqual(path.read_bytes(), before)

                diagnostic = str(raised.exception)
                self.assertEqual(
                    diagnostic,
                    f"GLEIF request rejected HTTP redirect (status {status})",
                )
                self.assertNotIn("redirect-secret", diagnostic)
                self.assertNotIn(destination, diagnostic)
                self.assertEqual(len(handler.urls), 1)
                self.assertTrue(handler.urls[0].startswith(gleif.ENDPOINT + "?"))
                self.assertNotIn(destination, handler.urls)
                self.assertEqual(output.getvalue(), "")

    def test_continuation_redirect_rolls_back_without_destination_request(self):
        destination = "https://api.gleif.org/api/v1/lei-records?page[number]=3"
        opener, handler = scripted_opener(
            (200, None, fixture("page_1.json")),
            (302, destination, None),
        )
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "state.json"
            before = write_state(path)
            with mock.patch.object(gleif, "HTTP_OPENER", opener):
                with self.assertRaisesRegex(ValueError, "status 302"):
                    self.run_extract(path, gleif.request_json, output=output)
            self.assertEqual(path.read_bytes(), before)

        self.assertEqual(len(handler.urls), 2)
        self.assertTrue(handler.urls[0].startswith(gleif.ENDPOINT + "?"))
        self.assertEqual(handler.urls[1], NEXT_URL)
        self.assertNotIn(destination, handler.urls)
        self.assertEqual(output.getvalue(), "")

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
