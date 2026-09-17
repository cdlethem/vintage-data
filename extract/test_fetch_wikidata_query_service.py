import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock
import urllib.parse


SCRIPT = Path(__file__).parent / "scripts" / "fetch_wikidata_query_service.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "wikidata_query_service.yml"
SPEC = importlib.util.spec_from_file_location("fetch_wikidata_query_service", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, headers=None, status=200):
        if isinstance(document, bytes):
            body = document
        else:
            body = json.dumps(document).encode("utf-8")
        super().__init__(body)
        self.headers = headers or {
            "Content-Type": "application/sparql-results+json; charset=utf-8",
            "Content-Length": str(len(body)),
        }
        self.status = status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def sparql_response(*bindings, variables=("item", "itemLabel")):
    return {
        "head": {"vars": list(variables)},
        "results": {"bindings": list(bindings)},
    }


def item_binding(item="Q42", label="Douglas Adams"):
    return {
        "item": {"type": "uri", "value": f"http://www.wikidata.org/entity/{item}"},
        "itemLabel": {"type": "literal", "xml:lang": "en", "value": label},
    }


class FetchWikidataQueryServiceTests(unittest.TestCase):
    def fetch(self, document, *, query="SELECT ?item WHERE { ?item ?p ?o } LIMIT 10", **kwargs):
        response = document if isinstance(document, JsonResponse) else JsonResponse(document)
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ) as urlopen:
            records = list(
                MODULE.fetch_wikidata_query_service(query=query, **kwargs)
            )
        return records, urlopen

    def assert_query_rejected_without_http(self, query, message=None):
        with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
            context = (
                self.assertRaisesRegex((ValueError, TypeError), message)
                if message
                else self.assertRaises((ValueError, TypeError))
            )
            with context:
                list(MODULE.fetch_wikidata_query_service(query=query))
        urlopen.assert_not_called()

    def test_rejects_exact_comment_limit_bypass_before_http(self):
        self.assert_query_rejected_without_http(
            "SELECT ?item WHERE { ?item ?p ?o } # LIMIT 1",
            "outer LIMIT",
        )

    def test_limit_text_in_literals_and_iris_does_not_supply_outer_limit(self):
        queries = (
            'SELECT ?item WHERE { BIND("LIMIT 1" AS ?item) }',
            "SELECT ?item WHERE { BIND('escaped \\'LIMIT 1\\'' AS ?item) }",
            'SELECT ?item WHERE { BIND("""LIMIT 1""" AS ?item) }',
            "SELECT ?item WHERE { BIND(<https://example.test/LIMIT/1#value> AS ?item) }",
        )
        for query in queries:
            with self.subTest(query=query):
                self.assert_query_rejected_without_http(query, "outer LIMIT")

    def test_nested_query_limit_does_not_bound_outer_query(self):
        self.assert_query_rejected_without_http(
            """SELECT ?item WHERE {
              { SELECT ?item WHERE { ?item ?p ?o } LIMIT 1 }
            }""",
            "outer LIMIT",
        )

    def test_rejects_missing_duplicate_or_excessive_outer_limits(self):
        cases = (
            ("SELECT ?item WHERE { ?item ?p ?o }", "exactly one outer LIMIT"),
            (
                "SELECT ?item WHERE { ?item ?p ?o } LIMIT 1 LIMIT 1",
                "exactly one outer LIMIT",
            ),
            (
                f"SELECT ?item WHERE {{ ?item ?p ?o }} LIMIT {MODULE.MAX_ROWS + 1}",
                "between 1 and",
            ),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT 0", "between 1 and"),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT -1", "positive decimal"),
            ("SELECT ?item WHERE { ?item ?p ?o } LIMIT 1 OFFSET 1", "after outer LIMIT"),
        )
        for query, message in cases:
            with self.subTest(query=query):
                self.assert_query_rejected_without_http(query, message)

    def test_rejects_malformed_lexical_input_before_http(self):
        cases = (
            'SELECT ?item WHERE { BIND("unterminated AS ?item) } LIMIT 1',
            "SELECT ?item WHERE { BIND(<https://example.test/bad iri> AS ?item) } LIMIT 1",
            "SELECT ?item WHERE { BIND(\"bad\\q\" AS ?item) } LIMIT 1",
            "SELECT ?item WHERE { ?item ?p ?o LIMIT 1",
            "SELECT ?item WHERE { (?item] ?p ?o } LIMIT 1",
        )
        for query in cases:
            with self.subTest(query=query):
                self.assert_query_rejected_without_http(query)

    def test_valid_bounded_queries_distinguish_nested_and_textual_limits(self):
        queries = (
            "SELECT ?item WHERE { ?item ?p ?o } # LIMIT 999\nLIMIT 7",
            'SELECT ?item WHERE { BIND("LIMIT 999" AS ?item) } LIMIT 8',
            "SELECT ?item WHERE { BIND(<https://example.test/LIMIT/999> AS ?item) } LIMIT 9",
            """SELECT ?item WHERE {
              { SELECT ?item WHERE { ?item ?p ?o } LIMIT 200 }
            }
            LIMIT 10""",
            """PREFIX wd: <http://www.wikidata.org/entity/>
            SELECT ?item WHERE { VALUES ?item { wd:Q42 } } ORDER BY ?item LIMIT 11""",
        )
        expected = (7, 8, 9, 10, 11)
        for query, limit in zip(queries, expected):
            with self.subTest(query=query):
                self.assertEqual(MODULE.validate_query(query), limit)

    def test_configured_representative_query_is_valid_and_requested_once(self):
        records, urlopen = self.fetch(
            sparql_response(variables=("country", "countryLabel", "capital", "capitalLabel")),
            query=MODULE.DEFAULT_QUERY,
            timeout=19,
        )

        self.assertEqual(records, [])
        self.assertEqual(MODULE.validate_query(MODULE.DEFAULT_QUERY), 100)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 19})
        self.assertEqual(request.get_method(), "GET")
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(f"{parsed.scheme}://{parsed.netloc}{parsed.path}", MODULE.ENDPOINT)
        parameters = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(parameters, {"query": [MODULE.DEFAULT_QUERY], "format": ["json"]})
        self.assertEqual(
            request.get_header("Accept"), "application/sparql-results+json"
        )
        self.assertTrue(request.get_header("User-agent"))

    def test_normalizes_bindings_with_stable_envelope_and_id(self):
        document = sparql_response(item_binding())
        first = MODULE.parse_response(
            document,
            query_sha256="query-digest",
            fetched_at="2026-09-17T00:00:00+00:00",
            row_limit=10,
        )[0]
        second = MODULE.parse_response(
            document,
            query_sha256="query-digest",
            fetched_at="2026-09-18T00:00:00+00:00",
            row_limit=10,
        )[0]

        self.assertEqual(first["source"], "wikidata_query_service")
        self.assertEqual(first["fetched_at"], "2026-09-17T00:00:00+00:00")
        self.assertRegex(first["id"], r"^[0-9a-f]{64}$")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(first["query_sha256"], "query-digest")
        self.assertEqual(first["binding"], item_binding())

        changed = sparql_response(item_binding(item="Q1", label="Universe"))
        changed_record = MODULE.parse_response(
            changed,
            query_sha256="query-digest",
            fetched_at="stamp",
            row_limit=10,
        )[0]
        self.assertNotEqual(first["id"], changed_record["id"])

    def test_run_uses_one_utc_timestamp_and_structured_metadata(self):
        document = sparql_response(item_binding(), item_binding("Q1", "Universe"))
        records, urlopen = self.fetch(document)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertRegex(records[0]["fetched_at"], r"\+00:00$")
        urlopen.assert_called_once()
        metadata = MODULE.LAST_RUN_METADATA
        self.assertEqual(metadata["requests"], {"attempted": 1})
        self.assertEqual(metadata["records"], 2)
        self.assertGreater(metadata["response_bytes"], 0)
        self.assertEqual(metadata["row_limit"], 10)
        self.assertRegex(metadata["query_sha256"], r"^[0-9a-f]{64}$")

    def test_empty_valid_response_emits_no_records(self):
        records, _ = self.fetch(sparql_response())
        self.assertEqual(records, [])
        self.assertEqual(MODULE.LAST_RUN_METADATA["records"], 0)

    def test_rejects_malformed_responses(self):
        cases = (
            ([], "not an object"),
            ({}, "missing head or results"),
            ({"head": [], "results": {}}, "missing head or results"),
            ({"head": {"vars": "item"}, "results": {"bindings": []}}, "head.vars"),
            (
                {"head": {"vars": ["item", "item"]}, "results": {"bindings": []}},
                "duplicates",
            ),
            ({"head": {"vars": ["item"]}, "results": {"bindings": {}}}, "bindings"),
            (sparql_response(None), "not an object"),
            (
                sparql_response({"other": {"type": "literal", "value": "x"}}),
                "undeclared variables",
            ),
            (
                sparql_response({"item": {"type": "unknown", "value": "x"}}),
                "invalid type",
            ),
            (
                sparql_response({"item": {"type": "literal", "value": 3}}),
                "invalid value",
            ),
            (
                sparql_response(
                    {"item": {"type": "uri", "value": "x", "xml:lang": "en"}}
                ),
                "invalid annotations",
            ),
        )
        for document, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(document)

    def test_rejects_invalid_json_content_type_and_oversized_response(self):
        responses = (
            (JsonResponse(b"not JSON"), "valid UTF-8 JSON"),
            (
                JsonResponse(
                    sparql_response(), headers={"Content-Type": "text/html"}
                ),
                "unsupported content type",
            ),
            (
                JsonResponse(
                    sparql_response(),
                    headers={
                        "Content-Type": "application/sparql-results+json",
                        "Content-Length": str(MODULE.MAX_RESPONSE_BYTES + 1),
                    },
                ),
                "response-size limit",
            ),
            (
                JsonResponse(b"x" * (MODULE.MAX_RESPONSE_BYTES + 1), headers={}),
                "response-size limit",
            ),
        )
        for response, message in responses:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch(response)

    def test_rejects_response_row_count_above_query_limit(self):
        document = sparql_response(*({} for _ in range(3)), variables=("item",))
        with self.assertRaisesRegex(ValueError, "bounded row count"):
            self.fetch(
                document,
                query="SELECT ?item WHERE { ?item ?p ?o } LIMIT 2",
            )

    def test_request_bounds_fail_before_http(self):
        too_long = "SELECT ?item WHERE { " + ("x" * MODULE.MAX_QUERY_LENGTH) + " } LIMIT 1"
        url_expanding = (
            "SELECT ?item WHERE { BIND(\""
            + ("é" * 6000)
            + "\" AS ?item) } LIMIT 1"
        )
        cases = (
            ({"timeout": 0}, "timeout"),
            ({"timeout": MODULE.MAX_TIMEOUT + 1}, "timeout"),
            ({"query": too_long}, "character limit"),
            ({"query": url_expanding}, "encoded query URL"),
        )
        for kwargs, message in cases:
            with self.subTest(message=message):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        list(MODULE.fetch_wikidata_query_service(**kwargs))
                urlopen.assert_not_called()

    def test_main_emits_ndjson_and_run_summary(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(sparql_response(item_binding())),
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                result = MODULE.main(
                    ["--query", "SELECT ?item ?itemLabel WHERE { ?item ?p ?o } LIMIT 1"]
                )

        self.assertEqual(result, 0)
        output = json.loads(stdout.getvalue())
        self.assertEqual(output["source"], "wikidata_query_service")
        summary_line = stderr.getvalue().strip()
        self.assertTrue(summary_line.startswith(MODULE.SUMMARY_PREFIX))
        summary = json.loads(summary_line.removeprefix(MODULE.SUMMARY_PREFIX))
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["completeness"], "complete")
        self.assertEqual(summary["records"], 1)
        self.assertEqual(summary["requests"], {"attempted": 1})

    def test_source_configuration_is_weekly_documented_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "23 4 \* \* 1"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertIsNone(re.search(r"(?m)^enabled: true$", text))
        self.assertIn("CC0 1.0", text)
        self.assertIn("Wikidata contributors", text)
        self.assertIn("one serial request per run", text)

        lines = text.splitlines()
        scalar_start = lines.index("  - >-") + 1
        query_lines = []
        for line in lines[scalar_start:]:
            if line and not line.startswith("    "):
                break
            query_lines.append(line.strip())
        configured_query = " ".join(part for part in query_lines if part)
        self.assertEqual(MODULE.validate_query(configured_query), 100)
        self.assertEqual(configured_query, " ".join(MODULE.DEFAULT_QUERY.split()))


if __name__ == "__main__":
    unittest.main()
