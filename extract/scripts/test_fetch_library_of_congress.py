import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock
import urllib.error
import urllib.parse
import urllib.request

import yaml


SCRIPT = Path(__file__).with_name("fetch_library_of_congress.py")
SPEC = importlib.util.spec_from_file_location("fetch_library_of_congress", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, content_type="application/json; charset=utf-8", url=None):
        body = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
        super().__init__(body)
        self.headers = {"Content-Type": content_type}
        self._url = url or MODULE.SEARCH_URL

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class SequenceOpener:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.requests = []

    def open(self, request, *, timeout):
        self.requests.append((request, timeout))
        response = next(self.responses)
        if isinstance(response, BaseException):
            raise response
        return response


class StubClient:
    def __init__(self, documents):
        self.documents = iter(documents)
        self.urls = []

    def get_page(self, url):
        self.urls.append(url)
        return next(self.documents)


def item(item_id="https://www.loc.gov/item/123/", **changes):
    value = {
        "id": item_id,
        "title": "Robots in industry",
        "date": "2025",
        "dates": ["2025"],
        "contributor": ["Library of Congress"],
        "subject": ["Robotics", "Automation"],
        "digitized": True,
        "online_format": ["image", "pdf"],
        "mime_type": ["image/jpeg", "application/pdf"],
        "url": item_id,
        "resources": [
            {
                "url": "https://tile.loc.gov/example.jpg",
                "files": [{"download": "https://tile.loc.gov/example.pdf"}],
            }
        ],
        "future_metadata": {"retained": True},
    }
    value.update(changes)
    return value


def document(results, next_url=None):
    pagination = {"next": next_url} if next_url is not None else {}
    return {"results": results, "pagination": pagination}


class FetchLibraryOfCongressTests(unittest.TestCase):
    def test_argument_bounds_fail_before_http(self):
        cases = (
            ({"query": ""}, "query"),
            ({"page_size": 0}, "page_size"),
            ({"page_size": MODULE.MAX_PAGE_SIZE + 1}, "page_size"),
            ({"max_pages": 0}, "max_pages"),
            ({"page_size": 1_000, "max_pages": 101}, "100000"),
            ({"timeout": 0}, "timeout"),
            ({"timeout": MODULE.MAX_TIMEOUT + 1}, "timeout"),
        )
        for changes, message in cases:
            with self.subTest(changes=changes):
                client = mock.Mock()
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.fetch_items(client=client, **changes)
                client.get_page.assert_not_called()

    def test_query_is_encoded_and_request_has_timeout_headers_and_no_authentication(self):
        opener = SequenceOpener([JsonResponse(document([]))])
        client = MODULE.LibraryOfCongressClient(
            timeout=17, pacer=MODULE.RequestPacer(interval=0), opener=opener
        )

        self.assertEqual(
            MODULE.fetch_items(
                query="robots & automation/AI",
                page_size=37,
                max_pages=1,
                timeout=17,
                client=client,
            ),
            [],
        )

        request, timeout = opener.requests[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual((parsed.scheme, parsed.hostname, parsed.path), ("https", "www.loc.gov", "/books/"))
        self.assertEqual(
            urllib.parse.parse_qs(parsed.query),
            {"fo": ["json"], "q": ["robots & automation/AI"], "c": ["37"]},
        )
        self.assertEqual(timeout, 17.0)
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertIn("Library-of-Congress-extractor", request.get_header("User-agent"))
        self.assertIsNone(request.get_header("Authorization"))

    def test_source_configuration_is_disabled_bounded_daily_slice(self):
        source_config = SCRIPT.parents[1] / "sources" / "library_of_congress.yml"
        config = yaml.safe_load(source_config.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["sink"], "local")
        self.assertEqual(len(config["schedule"].split()), 5)
        args = MODULE.build_parser().parse_args(config["args"])
        self.assertEqual(args.query, "robotics")
        self.assertEqual(args.page_size, 100)
        self.assertEqual(args.max_pages, 2)
        self.assertLessEqual(args.page_size, MODULE.MAX_PAGE_SIZE)
        self.assertLessEqual(args.page_size * args.max_pages, MODULE.MAX_TOTAL_ITEMS)
        config_text = source_config.read_text(encoding="utf-8")
        self.assertIn("not a complete", config_text)
        self.assertIn("does not download", config_text)
        self.assertIn("external activation/completion gates", config_text)

    def test_pagination_follows_safe_api_links_and_stops_on_empty_terminal_or_budget(self):
        second = "https://www.loc.gov/books/?fo=json&q=robotics&c=2&sp=2"
        client = StubClient([document([item("one"), item("two")], second), document([item("three")])])
        records = MODULE.fetch_items(page_size=2, max_pages=4, client=client)
        self.assertEqual([record["id"] for record in records], ["one", "two", "three"])
        self.assertEqual(client.urls, [mock.ANY, second])
        self.assertEqual(len({record["fetched_at"] for record in records}), 1)

        empty = StubClient([document([], second)])
        self.assertEqual(MODULE.fetch_items(page_size=2, max_pages=3, client=empty), [])
        self.assertEqual(len(empty.urls), 1)

        terminal = StubClient([document([item("one")])])
        self.assertEqual(len(MODULE.fetch_items(page_size=2, max_pages=3, client=terminal)), 1)
        self.assertEqual(len(terminal.urls), 1)

        bounded = StubClient([document([item("one")], second)])
        self.assertEqual(len(MODULE.fetch_items(page_size=1, max_pages=1, client=bounded)), 1)
        self.assertEqual(len(bounded.urls), 1)

    def test_rejects_unsafe_pagination_links_redirects_and_loops(self):
        unsafe = (
            "http://www.loc.gov/books/?sp=2",
            "https://loc.gov.evil.example/books/?sp=2",
            "https://user@www.loc.gov/books/?sp=2",
            "https://www.loc.gov/item/123/",
        )
        for url in unsafe:
            with self.subTest(url=url):
                client = StubClient([document([item()], url)])
                with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "unsafe"):
                    MODULE.fetch_items(page_size=1, max_pages=2, client=client)

                handler = MODULE.SafeRedirectHandler(MODULE.RequestPacer(interval=0))
                with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "unsafe"):
                    handler.redirect_request(mock.Mock(), None, 302, "Found", {}, url)

        initial = MODULE.SEARCH_URL + "?" + urllib.parse.urlencode(
            {"fo": "json", "q": MODULE.DEFAULT_QUERY, "c": 1}
        )
        looping = StubClient([document([item()], initial)])
        with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "loop"):
            MODULE.fetch_items(page_size=1, max_pages=2, client=looping)
        self.assertEqual(len(looping.urls), 1)

    def test_pacer_keeps_requests_strictly_below_twenty_per_minute(self):
        clock = mock.Mock(side_effect=[0.0, 1.0, 3.1])
        sleeps = []
        pacer = MODULE.RequestPacer(clock=clock, sleeper=sleeps.append)
        pacer.wait()
        pacer.wait()
        self.assertEqual(sleeps, [2.1])
        self.assertGreater(MODULE.MIN_REQUEST_INTERVAL, 3.0)

        pacer = mock.Mock()
        handler = MODULE.SafeRedirectHandler(pacer)
        redirected = handler.redirect_request(
            urllib.request.Request(MODULE.SEARCH_URL),
            None,
            302,
            "Found",
            {},
            "https://loc.gov/books/?fo=json&sp=2",
        )
        self.assertEqual(redirected.full_url, "https://loc.gov/books/?fo=json&sp=2")
        pacer.wait.assert_called_once_with()

    def test_http_429_is_clear_and_not_retried(self):
        error = urllib.error.HTTPError(
            MODULE.SEARCH_URL, 429, "Too Many Requests", {"Retry-After": "60"}, io.BytesIO()
        )
        opener = SequenceOpener([error])
        client = MODULE.LibraryOfCongressClient(
            timeout=5, pacer=MODULE.RequestPacer(interval=0), opener=opener
        )
        with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "throttled.*429"):
            client.get_page(MODULE.SEARCH_URL)
        self.assertEqual(len(opener.requests), 1)

    def test_malformed_non_json_and_wrong_shape_responses_fail_clearly(self):
        cases = (
            (JsonResponse(b"not-json"), "malformed JSON"),
            (JsonResponse(b"<html>busy</html>", content_type="text/html"), "non-JSON"),
            (JsonResponse([]), "JSON object"),
            (JsonResponse({"pagination": {}}), "results list"),
            (JsonResponse({"results": [], "pagination": []}), "malformed pagination"),
            (
                JsonResponse(document([]), url="https://evil.example/books/?fo=json"),
                "unsafe",
            ),
            (
                JsonResponse(document([]), url="https://www.loc.gov/item/123/"),
                "unsafe",
            ),
        )
        for response, message in cases:
            with self.subTest(message=message):
                client = MODULE.LibraryOfCongressClient(
                    timeout=5,
                    pacer=MODULE.RequestPacer(interval=0),
                    opener=SequenceOpener([response]),
                )
                with self.assertRaisesRegex(MODULE.LibraryOfCongressError, message):
                    client.get_page(MODULE.SEARCH_URL)

    def test_optional_fields_default_without_losing_complete_metadata(self):
        wire = item()
        record = MODULE.normalize_item(wire, "2026-09-17T12:00:00+00:00")
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["id"], wire["id"])
        self.assertEqual(record["title"], wire["title"])
        self.assertEqual(record["dates"], ["2025"])
        self.assertEqual(record["contributors"], wire["contributor"])
        self.assertEqual(record["subjects"], wire["subject"])
        self.assertIs(record["digitized"], True)
        self.assertEqual(record["online_formats"], wire["online_format"])
        self.assertEqual(record["mime_types"], wire["mime_type"])
        self.assertEqual(record["canonical_url"], wire["url"])
        self.assertEqual(record["resources"], wire["resources"])
        self.assertEqual(
            record["resource_links"],
            ["https://tile.loc.gov/example.jpg", "https://tile.loc.gov/example.pdf"],
        )
        self.assertEqual(record["raw"], wire)
        self.assertIsNot(record["raw"], wire)
        self.assertEqual(record["raw"]["future_metadata"], {"retained": True})

        optional = MODULE.normalize_item({"id": "minimal"}, "now")
        self.assertIsNone(optional["title"])
        self.assertIsNone(optional["date"])
        self.assertIsNone(optional["digitized"])
        for field in ("dates", "contributors", "subjects", "online_formats", "mime_types", "resources", "resource_links"):
            self.assertEqual(optional[field], [])

    def test_invalid_items_oversize_pages_and_duplicates_are_handled(self):
        with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "non-object"):
            MODULE.normalize_item("bad", "now")
        with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "invalid id"):
            MODULE.normalize_item({}, "now")
        with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "invalid digitized"):
            MODULE.normalize_item(item(digitized="yes"), "now")

        too_many = StubClient([document([item("one"), item("two")])])
        with self.assertRaisesRegex(MODULE.LibraryOfCongressError, "above page_size"):
            MODULE.fetch_items(page_size=1, max_pages=1, client=too_many)

        duplicates = StubClient([document([item("one"), item("one")])])
        records = MODULE.fetch_items(page_size=2, max_pages=1, client=duplicates)
        self.assertEqual([record["id"] for record in records], ["one"])

    def test_main_emits_compact_ndjson_envelopes_and_no_linked_content_requests(self):
        records = [MODULE.normalize_item(item("one"), "2026-09-17T12:00:00+00:00")]
        stdout = io.StringIO()
        with mock.patch.object(MODULE, "fetch_items", return_value=records) as fetch:
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--query", "robotics", "--page-size", "1", "--max-pages", "1"])

        fetch.assert_called_once_with(query="robotics", page_size=1, max_pages=1, timeout=30.0)
        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        emitted = json.loads(lines[0])
        self.assertEqual(
            (emitted["source"], emitted["id"], emitted["fetched_at"]),
            (MODULE.SOURCE, "one", "2026-09-17T12:00:00+00:00"),
        )
        self.assertNotIn(": ", lines[0])


if __name__ == "__main__":
    unittest.main()
