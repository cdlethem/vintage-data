import importlib.util
import json
import pathlib
import unittest
import urllib.parse
import urllib.error

SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_openalex_works.py"
spec = importlib.util.spec_from_file_location("fetch_openalex_works", SCRIPT)
openalex = importlib.util.module_from_spec(spec)
spec.loader.exec_module(openalex)


WORK = {
    "id": "https://openalex.org/W123",
    "doi": "https://doi.org/10.1000/example",
    "title": "An example work",
    "publication_date": "2026-09-10",
    "publication_year": 2026,
    "type": "article",
    "language": "en",
    "authorships": [{
        "author": {"id": "https://openalex.org/A1", "display_name": "Ada"},
        "institutions": [{"id": "https://openalex.org/I1", "display_name": "Example University"}],
    }],
    "open_access": {"is_oa": True, "oa_status": "gold"},
    "best_oa_location": {"landing_page_url": "https://example.test/work"},
    "primary_location": {"source": {"id": "https://openalex.org/S1"}},
    "locations": [{"is_oa": True}],
    "topics": [{"id": "https://openalex.org/T13090", "display_name": "Artificial intelligence"}],
    "primary_topic": {"id": "https://openalex.org/T13090"},
    "cited_by_count": 7,
    "referenced_works": ["https://openalex.org/W9"],
    "related_works": ["https://openalex.org/W8"],
}


class JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self, size=-1):
        return json.dumps(self.payload).encode("utf-8")


class FakeOpener:
    def __init__(self, payloads):
        self.payloads = iter(payloads)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        return JsonResponse(next(self.payloads))


class OpenAlexWorksTests(unittest.TestCase):
    def test_normalize_preserves_enrichment_and_stable_work_id(self):
        record = openalex.normalize(WORK, "2026-09-13T00:00:00+00:00")

        self.assertEqual(record["source"], "openalex_works")
        self.assertEqual(record["id"], "https://openalex.org/W123")
        self.assertEqual(record["doi"], "https://doi.org/10.1000/example")
        self.assertEqual(record["authorships"][0]["institutions"][0]["id"], "https://openalex.org/I1")
        self.assertEqual(record["topics"][0]["id"], "https://openalex.org/T13090")
        self.assertEqual(record["referenced_works"], ["https://openalex.org/W9"])
        self.assertEqual(record["related_works"], ["https://openalex.org/W8"])

    def test_empty_response_emits_no_records(self):
        opener = FakeOpener([{ "meta": {"count": 0, "next_cursor": None}, "results": [] }])

        records = list(openalex.fetch_works(
            today=openalex.date(2026, 9, 13), fetched_at="fixed", opener=opener
        ))

        self.assertEqual(records, [])
        self.assertEqual(len(opener.requests), 1)

    def test_cursor_pagination_requests_selected_fields_and_api_key(self):
        second_work = dict(WORK, id="https://openalex.org/W124")
        opener = FakeOpener([
            {"meta": {"count": 2, "next_cursor": "next-page"}, "results": [WORK]},
            {"meta": {"count": 2, "next_cursor": None}, "results": [second_work]},
        ])

        records = list(openalex.fetch_works(
            topic_id="T13090", days_back=3, max_pages=2, max_records=200,
            today=openalex.date(2026, 9, 13), fetched_at="fixed", api_key="key", 
            user_agent="test-agent", opener=opener,
        ))

        self.assertEqual([record["id"] for record in records], [
            "https://openalex.org/W123", "https://openalex.org/W124",
        ])
        first_url = urllib.parse.urlparse(opener.requests[0][0].full_url)
        first_query = urllib.parse.parse_qs(first_url.query)
        self.assertEqual(first_query["cursor"], ["*"])
        self.assertEqual(first_query["per-page"], ["100"])
        self.assertEqual(first_query["api_key"], ["key"])
        self.assertEqual(first_query["select"], [openalex.SELECT_FIELDS])
        self.assertIn("primary_topic.id:T13090", first_query["filter"][0])
        self.assertIn("from_publication_date:2026-09-11", first_query["filter"][0])
        self.assertEqual(opener.requests[0][0].get_header("User-agent"), "test-agent")
        second_query = urllib.parse.parse_qs(urllib.parse.urlparse(opener.requests[1][0].full_url).query)
        self.assertEqual(second_query["cursor"], ["next-page"])

    def test_record_budget_refuses_incomplete_query_before_output(self):
        opener = FakeOpener([{"meta": {"count": 101, "next_cursor": "next"}, "results": [WORK]}])

        with self.assertRaisesRegex(openalex.IncompleteCoverageError, "101 records"):
            list(openalex.fetch_works(max_pages=2, max_records=100, opener=opener))
        self.assertEqual(len(opener.requests), 1)

    def test_page_budget_refuses_following_cursor(self):
        opener = FakeOpener([
            {"meta": {"count": 2, "next_cursor": "next"}, "results": [WORK]},
        ])

        iterator = openalex.fetch_works(max_pages=1, max_records=100, opener=opener)
        self.assertEqual(next(iterator)["id"], "https://openalex.org/W123")
        with self.assertRaisesRegex(openalex.IncompleteCoverageError, "1 pages"):
            next(iterator)
        self.assertEqual(len(opener.requests), 1)

    def test_malformed_response_and_http_error_fail_explicitly(self):
        malformed = FakeOpener([{"meta": {"count": 1, "next_cursor": None}}])
        with self.assertRaisesRegex(openalex.OpenAlexError, "results and meta"):
            list(openalex.fetch_works(opener=malformed))

        def failing_opener(request, timeout):
            raise urllib.error.URLError("offline")

        with self.assertRaisesRegex(openalex.OpenAlexError, "request failed"):
            list(openalex.fetch_works(opener=failing_opener))

    def test_invalid_topic_and_missing_work_id_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "topic_id"):
            openalex.rolling_filter("not-a-topic", 7)
        with self.assertRaisesRegex(openalex.OpenAlexError, "missing its work ID"):
            openalex.normalize({}, "fixed")


if __name__ == "__main__":
    unittest.main()
