import contextlib
import importlib.util
import io
import json
import pathlib
import unittest
import urllib.error

MODULE_PATH = pathlib.Path(__file__).parent / "scripts" / "fetch_europe_pmc.py"
SPEC = importlib.util.spec_from_file_location("fetch_europe_pmc", MODULE_PATH)
europe_pmc = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(europe_pmc)


PUBLICATION = {
    "source": "MED",
    "id": "12345",
    "pmid": "12345",
    "pmcid": "PMC12345",
    "doi": "https://doi.org/10.1000/Example.DOI",
    "title": " A   biomedical\npublication ",
    "authorString": "Doe J; Research Group",
    "authorList": {"author": [
        {"firstName": "Jane", "lastName": "Doe", "initials": "J", "authorId": "0000-0001"},
        {"collectiveName": "Research Group"},
    ]},
    "journalTitle": "Test Journal",
    "journalIssn": "1234-5678",
    "journalVolume": "12",
    "issue": "3",
    "pageInfo": "10-19",
    "publisher": "Test Press",
    "firstPublicationDate": "2026-01-15",
    "electronicPublicationDate": "2026-01-14",
    "journalPublicationDate": "2026-01-15",
    "pubYear": "2026",
    "isOpenAccess": "Y",
    "inPMC": "Y",
    "citedByCount": 7,
    "hasReferences": "Y",
    "abstractText": " A useful\nabstract. ",
    "fullTextUrlList": {"fullTextUrl": [{
        "url": "https://example.test/fulltext", "site": "Europe_PMC",
        "documentStyle": "html", "availability": "Free",
    }]},
    "hasTextMinedTerms": "Y",
}


class FakeClient:
    def __init__(self, pages, annotations=None):
        self.pages = list(pages)
        self.annotations = annotations or {}
        self.search_calls = []

    def get_json(self, endpoint, params):
        if endpoint == europe_pmc.SEARCH_ENDPOINT:
            self.search_calls.append(params.copy())
            return self.pages.pop(0), "https://search.test/?cursor=" + params["cursorMark"]
        article_id = params["articleIds"]
        return self.annotations[article_id], "https://annotations.test/?articleIds=" + article_id


class MockResponse:
    def __init__(self, body):
        self.body = io.StringIO(body)

    def __enter__(self):
        return self.body

    def __exit__(self, *unused):
        return False


class EuropePmcTests(unittest.TestCase):
    def test_normalizes_metadata_identifiers_and_annotations(self):
        record = europe_pmc.normalize_publication(
            PUBLICATION, "2026-09-13T00:00:00+00:00", "https://search.test/", [{
                "type": "Gene", "exact": "BRCA1", "prefix": None, "suffix": None,
                "tags": [{"name": "BRCA1", "uri": "urn:test:BRCA1"}],
            }], "https://annotations.test/",
        )
        self.assertEqual(record["id"], "MED:12345")
        self.assertEqual(record["identifiers"]["doi"], "10.1000/example.doi")
        self.assertEqual(record["title"], "A biomedical publication")
        self.assertEqual(record["authors"][0]["name"], "Jane Doe")
        self.assertEqual(record["authors"][1]["name"], "Research Group")
        self.assertTrue(record["open_access"])
        self.assertEqual(record["citations"]["cited_by_count"], 7)
        self.assertEqual(record["fulltext_links"][0]["url"], "https://example.test/fulltext")
        self.assertEqual(record["text_mined_annotations"][0]["exact"], "BRCA1")

    def test_cursor_pagination_saves_each_completed_page_and_reports_partial_coverage(self):
        pages = [
            {"resultList": {"result": [PUBLICATION]}, "nextCursorMark": "cursor-2"},
            {"resultList": {"result": [{**PUBLICATION, "id": "12346", "hasTextMinedTerms": "N"}]}, "nextCursorMark": "cursor-3"},
        ]
        client = FakeClient(pages, {"MED:12345": {"annotations": [{
            "type": "Gene", "exact": "BRCA1", "tags": [{"name": "BRCA1"}],
        }]}})
        saved, emitted = [], []
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            europe_pmc.run("FIRST_PDATE:[2026-01-01 TO 2026-12-31]", 10, 2, 20, {},
                           saved.append, client, emitted.append)
        self.assertEqual([record["id"] for record in emitted], ["MED:12345", "MED:12346"])
        self.assertEqual(saved[0]["cursor"], "cursor-2")
        self.assertEqual(saved[-1]["cursor"], "cursor-3")
        self.assertFalse(saved[-1]["complete"])
        self.assertEqual([call["cursorMark"] for call in client.search_calls], ["*", "cursor-2"])
        report = json.loads(stderr.getvalue())
        self.assertFalse(report["complete"])
        self.assertEqual(report["partial_reason"], "max_pages")

    def test_empty_results_complete_cursor_without_emitting(self):
        client = FakeClient([{"resultList": {"result": []}, "nextCursorMark": "unused"}])
        saved, emitted = [], []
        with contextlib.redirect_stderr(io.StringIO()):
            europe_pmc.run("FIRST_PDATE:[2026-01-01 TO 2026-12-31]", 10, 1, 10, {},
                           saved.append, client, emitted.append)
        self.assertEqual(emitted, [])
        self.assertTrue(saved[-1]["complete"])
        self.assertEqual(saved[-1]["cursor"], "unused")

    def test_malformed_search_response_is_rejected(self):
        client = FakeClient([{"nextCursorMark": "next"}])
        with self.assertRaisesRegex(europe_pmc.ResponseError, "resultList"):
            europe_pmc.run("FIRST_PDATE:[2026-01-01 TO 2026-12-31]", 10, 1, 10, {},
                           lambda state: None, client, lambda record: None)

    def test_http_client_retries_transport_failure_then_parses_json_fixture(self):
        calls = []

        def opener(request, timeout):
            calls.append((request.full_url, timeout))
            if len(calls) == 1:
                raise urllib.error.URLError("offline")
            return MockResponse('{"resultList": {"result": []}}')

        client = europe_pmc.HttpClient(12, 1, 0, opener=opener, sleeper=lambda seconds: None)
        payload, url = client.get_json("https://example.test/search", {"format": "json"})
        self.assertEqual(payload["resultList"]["result"], [])
        self.assertIn("format=json", url)
        self.assertEqual(len(calls), 2)

    def test_http_client_rejects_malformed_json_fixture(self):
        client = europe_pmc.HttpClient(12, 0, 0, opener=lambda request, timeout: MockResponse("not json"))
        with self.assertRaisesRegex(europe_pmc.ResponseError, "malformed JSON"):
            client.get_json("https://example.test/search", {})

    def test_http_client_reports_exhausted_transport_failure(self):
        def opener(request, timeout):
            raise urllib.error.URLError("offline")

        client = europe_pmc.HttpClient(12, 1, 0, opener=opener, sleeper=lambda seconds: None)
        with self.assertRaisesRegex(europe_pmc.FetchError, "after 2 attempts"):
            client.get_json("https://example.test/search", {})

    def test_annotation_payload_supports_article_wrapper(self):
        payload = {"articles": [{"annotations": [{
            "annotationType": "Disease", "text": "cancer", "tags": {"name": "Cancer", "uri": "urn:cancer"},
        }]}]}
        annotations = europe_pmc.annotations_from_payload(payload)
        self.assertEqual(annotations, [{
            "type": "Disease", "exact": "cancer", "prefix": None, "suffix": None,
            "tags": [{"name": "Cancer", "uri": "urn:cancer"}],
        }])

    def test_completed_state_does_not_requery(self):
        client = FakeClient([])
        saved = []
        with contextlib.redirect_stderr(io.StringIO()):
            europe_pmc.run("FIRST_PDATE:[2026-01-01 TO 2026-12-31]", 10, 1, 10,
                           {"query": "FIRST_PDATE:[2026-01-01 TO 2026-12-31]", "cursor": "done", "complete": True},
                           saved.append, client, lambda record: self.fail("must not emit"))
        self.assertEqual(client.search_calls, [])
        self.assertEqual(saved, [])


if __name__ == "__main__":
    unittest.main()
