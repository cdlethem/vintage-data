import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
import urllib.parse
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_wikidata_query_service.py"
SPEC = importlib.util.spec_from_file_location("fetch_wikidata_query_service", SCRIPT_PATH)
fetcher = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(fetcher)

QUERY = "SELECT ?item ?itemLabel ?mass WHERE { ?item wdt:P31 wd:Q634. } ORDER BY ?item LIMIT 2"
DOCUMENT = {
    "head": {"vars": ["item", "itemLabel", "mass"]},
    "results": {
        "bindings": [
            {
                "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q111"},
                "itemLabel": {"type": "literal", "xml:lang": "en", "value": "Mars"},
                "mass": {
                    "type": "literal",
                    "datatype": "http://www.w3.org/2001/XMLSchema#decimal",
                    "value": "6.4171E23",
                },
            },
            {
                "item": {"type": "uri", "value": "http://www.wikidata.org/entity/Q525"},
                "itemLabel": {"type": "literal", "xml:lang": "en", "value": "Sun"},
            },
        ]
    },
}


class BytesResponse:
    def __init__(self, document=None, body=None):
        self._body = json.dumps(document).encode("utf-8") if body is None else body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self, size=-1):
        return self._body if size < 0 else self._body[:size]


class FetchWikidataQueryServiceTests(unittest.TestCase):
    def test_response_normalization_preserves_values_and_binding_metadata(self):
        records = fetcher.normalize_response(
            DOCUMENT,
            id_variable="item",
            fetched_at="2026-09-17T12:00:00+00:00",
            query_limit=2,
        )

        self.assertEqual(records[0], {
            "source": "wikidata_query_service",
            "fetched_at": "2026-09-17T12:00:00+00:00",
            "id": "http://www.wikidata.org/entity/Q111",
            "item": "http://www.wikidata.org/entity/Q111",
            "itemLabel": "Mars",
            "mass": "6.4171E23",
            "binding_metadata": {
                "item": {"type": "uri"},
                "itemLabel": {"type": "literal", "language": "en"},
                "mass": {
                    "type": "literal",
                    "datatype": "http://www.w3.org/2001/XMLSchema#decimal",
                },
            },
        })
        self.assertIsNone(records[1]["mass"])
        self.assertNotIn("mass", records[1]["binding_metadata"])

    def test_ids_are_stable_across_fetch_times(self):
        first = fetcher.normalize_response(
            DOCUMENT, id_variable="item", fetched_at="2026-09-17T00:00:00+00:00", query_limit=2
        )
        second = fetcher.normalize_response(
            DOCUMENT, id_variable="item", fetched_at="2026-09-18T00:00:00+00:00", query_limit=2
        )

        self.assertEqual([row["id"] for row in first], [row["id"] for row in second])
        self.assertNotEqual(first[0]["fetched_at"], second[0]["fetched_at"])

    def test_empty_bindings_are_a_valid_complete_result(self):
        document = {"head": {"vars": ["item"]}, "results": {"bindings": []}}
        self.assertEqual(
            fetcher.normalize_response(
                document, id_variable="item", fetched_at="2026-09-17T00:00:00+00:00", query_limit=1
            ),
            [],
        )

    def test_malformed_responses_are_rejected(self):
        malformed = [
            None,
            {},
            {"head": {"vars": "item"}, "results": {"bindings": []}},
            {"head": {"vars": ["label"]}, "results": {"bindings": []}},
            {"head": {"vars": ["item"]}, "results": {"bindings": {}}},
            {"head": {"vars": ["item"]}, "results": {"bindings": [{}]}},
            {
                "head": {"vars": ["item"]},
                "results": {"bindings": [{"item": {"type": "uri", "value": 123}}]},
            },
            {
                "head": {"vars": ["item"]},
                "results": {"bindings": [
                    {"item": {"type": "uri", "value": "Q1"}},
                    {"item": {"type": "uri", "value": "Q1"}},
                ]},
            },
        ]
        for document in malformed:
            with self.subTest(document=document):
                with self.assertRaises(ValueError):
                    fetcher.normalize_response(
                        document, id_variable="item", fetched_at="now", query_limit=2
                    )

    def test_get_uses_one_bounded_get_request_with_json_format(self):
        response = BytesResponse(DOCUMENT)
        with patch.object(fetcher.urllib.request, "urlopen", return_value=response) as urlopen:
            records = list(
                fetcher.fetch_query(
                    QUERY,
                    timeout=17,
                    fetched_at="2026-09-17T12:00:00+00:00",
                )
            )

        self.assertEqual(len(records), 2)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        parsed = urllib.parse.urlparse(request.full_url)
        params = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "query.wikidata.org")
        self.assertEqual(parsed.path, "/sparql")
        self.assertEqual(params, {"query": [QUERY], "format": ["json"]})
        self.assertEqual(request.get_method(), "GET")
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 17)
        self.assertEqual(request.get_header("Accept"), "application/sparql-results+json")

    def test_query_and_timeout_bounds_fail_before_http(self):
        invalid_calls = [
            ("SELECT ?item WHERE { ?item ?p ?o }", 30),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT 0", 30),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT 101", 30),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT 1 OFFSET 1", 30),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT 1", 0),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT 1", 121),
        ]
        with patch.object(fetcher.urllib.request, "urlopen") as urlopen:
            for query, timeout in invalid_calls:
                with self.subTest(query=query, timeout=timeout):
                    with self.assertRaises(ValueError):
                        list(fetcher.fetch_query(query, timeout=timeout))
        urlopen.assert_not_called()

    def test_response_cannot_exceed_query_limit(self):
        with self.assertRaisesRegex(ValueError, "more rows than the query LIMIT"):
            fetcher.normalize_response(
                DOCUMENT, id_variable="item", fetched_at="now", query_limit=1
            )

    def test_response_size_is_bounded(self):
        oversized = b"x" * (fetcher.MAX_RESPONSE_BYTES + 1)
        with patch.object(
            fetcher.urllib.request,
            "urlopen",
            return_value=BytesResponse(body=oversized),
        ):
            with self.assertRaisesRegex(ValueError, "response exceeds"):
                list(fetcher.fetch_query("SELECT ?item WHERE {} LIMIT 1"))

    def test_main_emits_ndjson_and_structured_summary(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(fetcher.urllib.request, "urlopen", return_value=BytesResponse(DOCUMENT)):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = fetcher.main(["--query", QUERY, "--timeout", "17"])

        self.assertEqual(status, 0)
        records = [json.loads(line) for line in stdout.getvalue().splitlines()]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["id"], "http://www.wikidata.org/entity/Q111")
        prefix, payload = stderr.getvalue().rstrip().split("\t", 1)
        self.assertEqual(prefix, "VINTAGE_RUN_SUMMARY")
        summary = json.loads(payload)
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 2)
        self.assertEqual(summary["requests"], {"attempted": 1})
        self.assertEqual(summary["metrics"]["query_limit"], 2)
        self.assertEqual(summary["metrics"]["pagination"], "disabled")

    def test_main_reports_malformed_response_without_emitting_records(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with patch.object(
            fetcher.urllib.request,
            "urlopen",
            return_value=BytesResponse({"unexpected": "secret-response"}),
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = fetcher.main(["--query", "secret query LIMIT 1"])

        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        summary = json.loads(stderr.getvalue().split("\t", 1)[1])
        self.assertEqual(summary["health"], "failed")
        self.assertEqual(summary["records"], 0)
        self.assertEqual(summary["error"], "ValueError")
        self.assertNotIn("secret", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
