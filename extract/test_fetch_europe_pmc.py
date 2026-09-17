import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_europe_pmc.py"
SPEC = importlib.util.spec_from_file_location("fetch_europe_pmc", SCRIPT_PATH)
europe_pmc = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(europe_pmc)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class RawResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class QueueOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request.full_url, timeout))
        if not self.responses:
            raise AssertionError(f"unexpected HTTP request: {request.full_url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, bytes):
            return RawResponse(response)
        return JsonResponse(response)


class FailingOutput(io.StringIO):
    def __init__(self, *, fail_on_flush=False):
        super().__init__()
        self.fail_on_flush = fail_on_flush

    def flush(self):
        if self.fail_on_flush:
            raise OSError("staged output flush failed")
        return super().flush()


def search(results, next_cursor=None, hit_count=None):
    document = {"resultList": {"result": results}}
    if next_cursor is not None:
        document["nextCursorMark"] = next_cursor
    if hit_count is not None:
        document["hitCount"] = hit_count
    return document


def result(publication_id, **overrides):
    document = {
        "source": "MED",
        "id": publication_id,
        "pmid": publication_id,
        "title": "  A   normalized\n title  ",
        "abstractText": "  Multi-line\n abstract   text ",
        "authorString": "Ada A, Bob B",
        "authorList": {
            "author": [
                {
                    "fullName": "Ada Alpha",
                    "firstName": "Ada",
                    "lastName": "Alpha",
                    "initials": "AA",
                    "authorId": {"type": "ORCID", "value": "0000-0001"},
                    "authorAffiliationDetailsList": {
                        "authorAffiliation": [{"affiliation": "  Fixture  University "}]
                    },
                }
            ]
        },
        "journalInfo": {
            "printPublicationDate": "2026-09-01",
            "electronicPublicationDate": "2026-08-25",
            "volume": "12",
            "issue": "3",
            "dateOfPublication": "2026 Sep",
            "journalIssue": {},
            "journal": {
                "title": "Journal of Fixtures",
                "medlineAbbreviation": "J Fix",
                "issn": "1234-5678",
                "essn": "8765-4321",
            },
        },
        "pubYear": "2026",
        "firstPublicationDate": "2026-08-25",
        "firstIndexDate": "2026-09-02",
        "publicationStatus": "ppublish",
        "pubTypeList": {"pubType": ["research article"]},
        "language": "eng",
        "keywordList": {"keyword": ["biomedicine"]},
        "meshHeadingList": {"meshHeading": [{"majorTopic_YN": "Y", "descriptorName": "Cells"}]},
        "grantsList": {"grant": [{"grantId": "G-1", "agency": "Fixture Council"}]},
        "citedByCount": 7,
        "isOpenAccess": "Y",
        "inEPMC": "Y",
        "inPMC": "Y",
        "hasPDF": "Y",
        "pmcid": f"PMC{publication_id}",
        "doi": f"10.1234/{publication_id}",
        "fullTextIdList": {"fullTextId": [f"PMC{publication_id}"]},
        "fullTextUrlList": {
            "fullTextUrl": [
                {
                    "url": f"https://example.test/{publication_id}.pdf",
                    "documentStyle": "pdf",
                    "availability": "Open access",
                }
            ]
        },
    }
    document.update(overrides)
    return document


def annotation_article(publication_id, annotations=None):
    return {
        "source": "MED",
        "extId": publication_id,
        "annotations": (
            [
                {
                    "prefix": "the ",
                    "exact": "TP53",
                    "postfix": " gene",
                    "type": "Gene_Proteins",
                    "section": "Abstract",
                    "position": "10.14",
                    "tags": [{"name": "TP53", "uri": "https://identifiers.org/ncbigene:7157"}],
                }
            ]
            if annotations is None
            else annotations
        ),
    }


def read_state(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def records(output):
    return [json.loads(line) for line in output.getvalue().splitlines()]


class EuropePmcTests(unittest.TestCase):
    def run_main(self, state_file, responses, *, extra_args=None, out=None):
        opener = QueueOpener(responses)
        output = io.StringIO() if out is None else out
        args = [
            "--state-file",
            str(state_file),
            "--page-size",
            "1",
            "--max-pages",
            "4",
            "--max-records",
            "4",
            "--request-interval",
            "0",
        ]
        if extra_args:
            args.extend(extra_args)
        with patch.object(europe_pmc.urllib.request, "urlopen", opener):
            europe_pmc.main(args, out=output)
        self.assertEqual([], opener.responses)
        return output, opener

    def test_normalizes_metadata_abstract_links_annotations_and_one_run_timestamp(self):
        client = europe_pmc.HttpClient(
            timeout=3,
            retries=0,
            request_interval=0,
            opener=QueueOpener(
                [
                    search([result("1")], "C2", 2),
                    [annotation_article("1")],
                    search([result("2", title="Second")], None, 2),
                    [annotation_article("2", [])],
                ]
            ),
        )
        fetched_at = "2026-09-17T12:00:00+00:00"
        rows, cursor = europe_pmc.fetch_bounded(
            query=europe_pmc.DEFAULT_QUERY,
            cursor_mark="*",
            page_size=1,
            max_pages=3,
            max_records=3,
            fetched_at=fetched_at,
            client=client,
        )

        self.assertEqual("*", cursor)
        self.assertEqual(["MED:1", "MED:2"], [row["id"] for row in rows])
        self.assertEqual({fetched_at}, {row["fetched_at"] for row in rows})
        first = rows[0]
        self.assertEqual("A normalized title", first["title"])
        self.assertEqual("Multi-line abstract text", first["abstract"])
        self.assertEqual("Ada Alpha", first["authors"][0]["full_name"])
        self.assertEqual(
            {"type": "ORCID", "value": "0000-0001"}, first["authors"][0]["author_id"]
        )
        self.assertEqual(["Fixture University"], first["authors"][0]["affiliations"])
        self.assertEqual("Journal of Fixtures", first["journal"]["title"])
        self.assertEqual("12", first["journal"]["volume"])
        self.assertEqual("3", first["journal"]["issue"])
        self.assertEqual("2026 Sep", first["journal"]["date_of_publication"])
        self.assertEqual(["research article"], first["publication_types"])
        self.assertEqual(["biomedicine"], first["keywords"])
        self.assertEqual("TP53", first["annotations"][0]["exact"])
        self.assertEqual("https://europepmc.org/article/MED/1", first["source_url"])
        self.assertEqual(
            {"https://europepmc.org/articles/PMC1", "https://example.test/1.pdf"},
            {link["url"] for link in first["full_text_links"]},
        )
        search_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(first["retrieval"]["search_url"]).query
        )
        self.assertEqual(["1"], search_query["pageSize"])
        self.assertIs(first["is_open_access"], True)
        self.assertIs(first["in_epmc"], True)
        self.assertIsNotNone(first["retrieval"]["annotations_url"])
        self.assertEqual(1, first["retrieval"]["page"])
        self.assertEqual(2, rows[1]["retrieval"]["page"])
        self.assertEqual([], rows[1]["annotations"])

    def test_production_shaped_annotation_array_is_parsed_through_http_json_path(self):
        opener = QueueOpener([[annotation_article("42"), annotation_article("43", [])]])
        client = europe_pmc.HttpClient(3, 0, 0, opener=opener)
        document = client.get_json("https://example.test/annotations", "annotations")
        parsed = europe_pmc.parse_annotations_response(document, {"MED:42", "MED:43"})

        self.assertEqual("TP53", parsed["MED:42"][0]["exact"])
        self.assertEqual([], parsed["MED:43"])

    def test_empty_annotation_array_is_valid_through_http_json_path(self):
        opener = QueueOpener([[]])
        client = europe_pmc.HttpClient(3, 0, 0, opener=opener)
        document = client.get_json("https://example.test/annotations", "annotations")

        self.assertEqual({"MED:42": []}, europe_pmc.parse_annotations_response(document, {"MED:42"}))

    def test_malformed_annotation_arrays_fail_through_http_json_path(self):
        malformed_documents = [
            {},
            ["not-an-article"],
            [{"source": "MED", "extId": "42", "annotations": {}}],
            [{"source": "MED", "extId": "42", "annotations": ["bad"]}],
            [{"source": "MED", "extId": "unexpected", "annotations": []}],
        ]
        for document in malformed_documents:
            with self.subTest(document=document):
                opener = QueueOpener([document])
                client = europe_pmc.HttpClient(3, 0, 0, opener=opener)
                decoded = client.get_json("https://example.test/annotations", "annotations")
                with self.assertRaises(ValueError):
                    europe_pmc.parse_annotations_response(decoded, {"MED:42"})

    def test_search_response_keeps_endpoint_specific_object_validation(self):
        for document in ([], {}, {"resultList": []}, {"resultList": {"result": {}}}):
            with self.subTest(document=document):
                opener = QueueOpener([document])
                client = europe_pmc.HttpClient(3, 0, 0, opener=opener)
                decoded = client.get_json("https://example.test/search", "search")
                with self.assertRaises(ValueError):
                    europe_pmc.parse_search_response(decoded)

    def test_successful_bounded_runs_resume_then_mark_completion_for_repoll(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            first_output, first_opener = self.run_main(
                state_file,
                [search([result("1")], "C2", 2), [annotation_article("1")]],
                extra_args=["--max-pages", "1"],
            )
            self.assertEqual(["MED:1"], [row["id"] for row in records(first_output)])
            self.assertEqual("C2", read_state(state_file)["cursor_mark"])

            second_output, second_opener = self.run_main(
                state_file,
                [search([result("2")], None, 2), [annotation_article("2")]],
            )
            self.assertEqual(["MED:2"], [row["id"] for row in records(second_output)])
            self.assertEqual("*", read_state(state_file)["cursor_mark"])

            first_query = urllib.parse.parse_qs(urllib.parse.urlsplit(first_opener.requests[0][0]).query)
            second_query = urllib.parse.parse_qs(urllib.parse.urlsplit(second_opener.requests[0][0]).query)
            self.assertEqual(["*"], first_query["cursorMark"])
            self.assertEqual(["C2"], second_query["cursorMark"])

    def test_completed_traversal_repolls_and_emits_new_or_late_indexed_records(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            first_output, _ = self.run_main(
                state_file,
                [search([result("1")], None, 1), [annotation_article("1")]],
            )
            first_id = records(first_output)[0]["id"]
            self.assertEqual("*", read_state(state_file)["cursor_mark"])

            second_output, second_opener = self.run_main(
                state_file,
                [
                    search([result("1")], "NEXT", 2),
                    [annotation_article("1")],
                    search([result("late")], None, 2),
                    [annotation_article("late")],
                ],
            )
            second_rows = records(second_output)
            self.assertEqual([first_id, "MED:late"], [row["id"] for row in second_rows])
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(second_opener.requests[0][0]).query)
            self.assertEqual(["*"], query["cursorMark"])
            self.assertEqual("*", read_state(state_file)["cursor_mark"])

    def test_page_two_failure_preserves_state_and_retry_replays_page_one(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            original = {
                "version": 1,
                "source": "europe_pmc",
                "query": europe_pmc.DEFAULT_QUERY,
                "cursor_mark": "OLD",
            }
            state_file.write_text(json.dumps(original), encoding="utf-8")
            opener = QueueOpener(
                [search([result("1")], "C2"), [annotation_article("1")], []]
            )
            failed_output = io.StringIO()
            with patch.object(europe_pmc.urllib.request, "urlopen", opener):
                with self.assertRaisesRegex(ValueError, "search response must be an object"):
                    europe_pmc.main(
                        [
                            "--state-file", str(state_file), "--page-size", "1",
                            "--max-pages", "2", "--max-records", "2",
                            "--request-interval", "0",
                        ],
                        out=failed_output,
                    )
            self.assertEqual(original, read_state(state_file))
            self.assertEqual("", failed_output.getvalue())

            retry_output, retry_opener = self.run_main(
                state_file,
                [
                    search([result("1")], "C2"),
                    [annotation_article("1")],
                    search([result("2")], None),
                    [annotation_article("2")],
                ],
            )
            self.assertEqual(["MED:1", "MED:2"], [row["id"] for row in records(retry_output)])
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(retry_opener.requests[0][0]).query)
            self.assertEqual(["OLD"], query["cursorMark"])

    def test_later_annotation_failure_preserves_state_and_retry_replays_all_records(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            original = {
                "version": 1,
                "source": "europe_pmc",
                "query": europe_pmc.DEFAULT_QUERY,
                "cursor_mark": "OLD",
            }
            state_file.write_text(json.dumps(original), encoding="utf-8")
            opener = QueueOpener(
                [
                    search([result("1")], "C2"),
                    [annotation_article("1")],
                    search([result("2")], None),
                    {"wrong": "envelope"},
                ]
            )
            failed_output = io.StringIO()
            with patch.object(europe_pmc.urllib.request, "urlopen", opener):
                with self.assertRaisesRegex(ValueError, "annotations response must be an array"): 
                    europe_pmc.main(
                        [
                            "--state-file", str(state_file), "--page-size", "1",
                            "--max-pages", "2", "--max-records", "2",
                            "--request-interval", "0",
                        ],
                        out=failed_output,
                    )
            self.assertEqual(original, read_state(state_file))
            self.assertEqual("", failed_output.getvalue())

            retry_output, _ = self.run_main(
                state_file,
                [
                    search([result("1")], "C2"),
                    [annotation_article("1")],
                    search([result("2")], None),
                    [annotation_article("2")],
                ],
            )
            self.assertEqual(["MED:1", "MED:2"], [row["id"] for row in records(retry_output)])

    def test_output_failure_preserves_state_and_retry_replays_uncommitted_record(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            original = {
                "version": 1,
                "source": "europe_pmc",
                "query": europe_pmc.DEFAULT_QUERY,
                "cursor_mark": "OLD",
            }
            state_file.write_text(json.dumps(original), encoding="utf-8")
            opener = QueueOpener([search([result("1")], None), [annotation_article("1")]])
            with patch.object(europe_pmc.urllib.request, "urlopen", opener):
                with self.assertRaisesRegex(OSError, "flush failed"):
                    europe_pmc.main(
                        [
                            "--state-file", str(state_file), "--page-size", "1",
                            "--max-pages", "1", "--max-records", "1",
                            "--request-interval", "0",
                        ],
                        out=FailingOutput(fail_on_flush=True),
                    )
            self.assertEqual(original, read_state(state_file))

            retry_output, _ = self.run_main(
                state_file,
                [search([result("1")], None), [annotation_article("1")]],
            )
            self.assertEqual(["MED:1"], [row["id"] for row in records(retry_output)])

    def test_serialization_failure_occurs_before_output_or_state_commit(self):
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            original = {"cursor_mark": "OLD"}
            state_file.write_text(json.dumps(original), encoding="utf-8")
            with self.assertRaises(TypeError):
                lines = europe_pmc.serialize_records([{"bad": object()}])
                europe_pmc.publish(lines, output, state_file, {"cursor_mark": "NEW"})
            self.assertEqual("", output.getvalue())
            self.assertEqual(original, read_state(state_file))

    def test_empty_search_is_valid_and_resets_completed_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            state_file.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "source": "europe_pmc",
                        "query": europe_pmc.DEFAULT_QUERY,
                        "cursor_mark": "C2",
                    }
                ),
                encoding="utf-8",
            )
            output, opener = self.run_main(state_file, [search([], None, 0)])
            self.assertEqual("", output.getvalue())
            self.assertEqual("*", read_state(state_file)["cursor_mark"])
            self.assertEqual(1, len(opener.requests))

    def test_changed_query_does_not_reuse_incompatible_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            state_file = Path(directory) / "state.json"
            state_file.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "source": "europe_pmc",
                        "query": "OLD QUERY",
                        "cursor_mark": "OLD-CURSOR",
                    }
                ),
                encoding="utf-8",
            )
            _, opener = self.run_main(
                state_file,
                [search([], None, 0)],
                extra_args=["--query", "NEW QUERY"],
            )
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(opener.requests[0][0]).query)
            self.assertEqual(["*"], query["cursorMark"])
            self.assertEqual("NEW QUERY", read_state(state_file)["query"])

    def test_invalid_json_fails_without_retrying_a_structurally_bad_response(self):
        opener = QueueOpener([b"not json"])
        client = europe_pmc.HttpClient(3, 2, 0, opener=opener)
        with self.assertRaisesRegex(ValueError, "response is not JSON"):
            client.get_json("https://example.test/search", "search")
        self.assertEqual(1, len(opener.requests))

    def test_transient_request_retry_exhaustion_is_bounded(self):
        opener = QueueOpener(
            [
                urllib.error.URLError("offline"),
                urllib.error.URLError("still offline"),
                urllib.error.URLError("offline again"),
            ]
        )
        sleeps = []
        client = europe_pmc.HttpClient(
            3, 2, 0, opener=opener, sleeper=sleeps.append
        )
        with self.assertRaisesRegex(RuntimeError, "failed after 3 attempts"):
            client.get_json("https://example.test/search", "search")
        self.assertEqual(3, len(opener.requests))
        self.assertEqual([1, 2], sleeps)
    def test_annotation_requests_are_bounded_and_flatten_each_batch(self):
        publications = [result(str(number)) for number in range(9)]
        first_batch = [annotation_article(str(number)) for number in range(8)]
        final_batch = [annotation_article("8", [])]
        opener = QueueOpener([search(publications, None, 9), first_batch, final_batch])
        client = europe_pmc.HttpClient(3, 0, 0, opener=opener)

        rows, cursor = europe_pmc.fetch_bounded(
            query="query",
            cursor_mark="*",
            page_size=9,
            max_pages=1,
            max_records=9,
            fetched_at="now",
            client=client,
        )

        self.assertEqual("*", cursor)
        self.assertEqual(9, len(rows))
        self.assertEqual("TP53", rows[7]["annotations"][0]["exact"])
        self.assertEqual([], rows[8]["annotations"])
        first_ids = urllib.parse.parse_qs(
            urllib.parse.urlsplit(opener.requests[1][0]).query
        )["articleIds"][0].split(",")
        final_ids = urllib.parse.parse_qs(
            urllib.parse.urlsplit(opener.requests[2][0]).query
        )["articleIds"][0].split(",")
        self.assertEqual(8, len(first_ids))
        self.assertEqual(["MED:8"], final_ids)


    def test_record_bound_uses_smaller_final_page_and_retains_continuation(self):
        opener = QueueOpener(
            [
                search([result("1"), result("2")], "C2", 10),
                [annotation_article("1"), annotation_article("2")],
                search([result("3")], "C3", 10),
                [annotation_article("3")],
            ]
        )
        client = europe_pmc.HttpClient(3, 0, 0, opener=opener)
        rows, cursor = europe_pmc.fetch_bounded(
            query="query",
            cursor_mark="*",
            page_size=2,
            max_pages=4,
            max_records=3,
            fetched_at="now",
            client=client,
        )
        self.assertEqual(["MED:1", "MED:2", "MED:3"], [row["id"] for row in rows])
        self.assertEqual("C3", cursor)
        second_search_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(opener.requests[2][0]).query
        )
        self.assertEqual(["1"], second_search_query["pageSize"])


if __name__ == "__main__":
    unittest.main()
