import importlib.util
import json
import pathlib
import unittest
import urllib.parse


SCRIPT = pathlib.Path(__file__).parent / "scripts" / "fetch_wikidata_query_service.py"
SPEC = importlib.util.spec_from_file_location("fetch_wikidata_query_service", SCRIPT)
fetcher = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fetcher)

QUERY = "SELECT ?entity ?label WHERE { ?entity wdt:P31 wd:Q5 . } ORDER BY ?entity LIMIT 2"
FIXTURE = {
    "head": {"vars": ["entity", "label"]},
    "results": {"bindings": [
        {"entity": {"type": "uri", "value": "http://www.wikidata.org/entity/Q1"},
         "label": {"type": "literal", "xml:lang": "en", "value": "Universe"}},
        {"entity": {"type": "uri", "value": "http://www.wikidata.org/entity/Q2"}},
    ]},
}


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def read(self, size=-1):
        if not hasattr(self, "_body"):
            self._body = json.dumps(self.payload).encode("utf-8")
        if size < 0:
            result, self._body = self._body, b""
            return result
        result, self._body = self._body[:size], self._body[size:]
        return result


class FetchWikidataQueryServiceTests(unittest.TestCase):
    def opener_with(self, payload):
        calls = []

        def opener(request, *, timeout):
            calls.append((request, timeout))
            return FakeResponse(payload)

        return calls, opener

    def test_normalizes_terms_and_envelopes(self):
        calls, opener = self.opener_with(FIXTURE)
        variables, records = fetcher.fetch(
            QUERY, timeout_seconds=11, opener=opener, fetched_at="2026-09-13T00:00:00+00:00"
        )

        self.assertEqual(["entity", "label"], variables)
        expected = [
            {"source": "wikidata_query_service", "fetched_at": "2026-09-13T00:00:00+00:00",
             "entity": "http://www.wikidata.org/entity/Q1", "label": "Universe"},
            {"source": "wikidata_query_service", "fetched_at": "2026-09-13T00:00:00+00:00",
             "entity": "http://www.wikidata.org/entity/Q2", "label": None},
        ]
        actual = list(records)
        self.assertEqual(expected, [{key: value for key, value in record.items() if key != "id"}
                                    for record in actual])
        self.assertEqual([fetcher.stable_record_id({"entity": "http://www.wikidata.org/entity/Q1", "label": "Universe"}),
                          fetcher.stable_record_id({"entity": "http://www.wikidata.org/entity/Q2", "label": None})],
                         [record["id"] for record in actual])
        self.assertEqual(1, len(calls))

    def test_ids_are_stable_across_binding_key_order(self):
        first = {"entity": "https://www.wikidata.org/entity/Q42", "label": "Douglas Adams"}
        second = {"label": "Douglas Adams", "entity": "https://www.wikidata.org/entity/Q42"}
        self.assertEqual(fetcher.stable_record_id(first), fetcher.stable_record_id(second))

    def test_empty_bindings_emit_no_records(self):
        calls, opener = self.opener_with({"head": {"vars": ["entity"]}, "results": {"bindings": []}})
        _, records = fetcher.fetch(QUERY, opener=opener)
        self.assertEqual([], list(records))
        self.assertEqual(1, len(calls))

    def test_rejects_malformed_responses(self):
        malformed = [
            {},
            {"head": {"vars": []}, "results": {"bindings": []}},
            {"head": {"vars": ["entity"]}, "results": {"bindings": [{"entity": {}}]}},
            {"head": {"vars": ["entity"]}, "results": {"bindings": [{"unknown": {"value": "x"}}]}},
        ]
        for payload in malformed:
            with self.subTest(payload=payload):
                with self.assertRaises(fetcher.ResponseError):
                    fetcher.normalize_response(payload)

    def test_request_is_one_bounded_get_with_json_format_and_timeout(self):
        calls, opener = self.opener_with(FIXTURE)
        _, records = fetcher.fetch(QUERY, timeout_seconds=13, opener=opener)
        list(records)

        self.assertEqual(1, len(calls), "WDQS has no cursor pagination; one request per run")
        request, timeout = calls[0]
        self.assertEqual("GET", request.get_method())
        self.assertEqual(13, timeout)
        self.assertEqual("application/sparql-results+json", request.get_header("Accept"))
        parsed = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
        self.assertEqual([QUERY], parsed["query"])
        self.assertEqual(["json"], parsed["format"])

    def test_rejects_unbounded_queries_before_making_request(self):
        calls, opener = self.opener_with(FIXTURE)
        invalid_queries = [
            "SELECT ?entity WHERE { ?entity wdt:P31 wd:Q5 . }",
            "SELECT ?entity WHERE { ?entity wdt:P31 wd:Q5 . } LIMIT 101",
            "SELECT ?entity WHERE { ?entity wdt:P31 wd:Q5 . } LIMIT 2 LIMIT 3",
        ]
        for query in invalid_queries:
            with self.subTest(query=query):
                with self.assertRaises(ValueError):
                    fetcher.fetch(query, opener=opener)
        self.assertEqual([], calls)

    def test_rejects_timeout_outside_request_bound(self):
        calls, opener = self.opener_with(FIXTURE)
        for timeout in (0, 61):
            with self.subTest(timeout=timeout):
                with self.assertRaises(ValueError):
                    fetcher.fetch(QUERY, timeout_seconds=timeout, opener=opener)
        self.assertEqual([], calls)


if __name__ == "__main__":
    unittest.main()
