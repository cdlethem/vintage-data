import contextlib
from datetime import date
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import unittest
from unittest import mock


SCRIPT = Path(__file__).parent / "scripts" / "fetch_openalex_works.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "openalex_works.yml"
SPEC = importlib.util.spec_from_file_location("fetch_openalex_works", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def work(work_id="W123", **overrides):
    document = {
        "id": f"https://openalex.org/{work_id}",
        "doi": "https://doi.org/10.1000/example",
        "display_name": "Adapting coastal cities to climate change",
        "publication_year": 2026,
        "publication_date": "2026-09-16",
        "type": "article",
        "language": "en",
        "cited_by_count": 4,
        "is_retracted": False,
        "primary_location": {
            "is_oa": True,
            "landing_page_url": "https://example.org/work",
            "source": {"id": "https://openalex.org/S9", "display_name": "Journal"},
        },
        "open_access": {"is_oa": True, "oa_status": "gold"},
        "authorships": [
            {
                "author_position": "first",
                "author": {"id": "https://openalex.org/A7", "display_name": "A. Author"},
            }
        ],
        "topics": [
            {
                "id": "https://openalex.org/T1",
                "display_name": "Climate adaptation",
                "score": 0.99,
            }
        ],
        "updated_date": "2026-09-16T12:00:00.000000",
    }
    document.update(overrides)
    return document


def page(count, results, next_cursor=None):
    return {
        "meta": {"count": count, "next_cursor": next_cursor},
        "results": results,
        "group_by": [],
    }


class FetchOpenAlexWorksTests(unittest.TestCase):
    def fetch(self, documents, **kwargs):
        responses = [JsonResponse(document) for document in documents]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            records = list(
                MODULE.fetch_recent_works(today=date(2026, 9, 17), **kwargs)
            )
        return records, urlopen

    def test_normalizes_selected_metadata_with_stable_openalex_id(self):
        records, _ = self.fetch([page(1, [work()])])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "openalex_works")
        self.assertEqual(record["id"], "W123")
        self.assertEqual(record["openalex_url"], "https://openalex.org/W123")
        self.assertEqual(record["doi"], "https://doi.org/10.1000/example")
        self.assertEqual(record["title"], "Adapting coastal cities to climate change")
        self.assertEqual(record["publication_year"], 2026)
        self.assertEqual(record["publication_date"], "2026-09-16")
        self.assertEqual(record["type"], "article")
        self.assertEqual(record["language"], "en")
        self.assertEqual(record["cited_by_count"], 4)
        self.assertIs(record["is_retracted"], False)
        self.assertTrue(record["open_access"]["is_oa"])
        self.assertEqual(record["authorships"][0]["author_position"], "first")
        self.assertEqual(record["topics"][0]["display_name"], "Climate adaptation")
        self.assertRegex(record["fetched_at"], r"\+00:00$")

        later = MODULE.normalize_work(work(), "later")
        self.assertEqual(later["id"], record["id"])
        self.assertNotEqual(later["fetched_at"], record["fetched_at"])

    def test_request_has_narrow_window_topic_select_page_size_and_headers(self):
        with mock.patch.dict(os.environ, {"OPENALEX_API_KEY": ""}, clear=False):
            records, urlopen = self.fetch([page(0, [])], days_back=2, timeout=19)

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        query = urllib_query(request.full_url)
        self.assertEqual(query["per-page"], ["100"])
        self.assertEqual(query["cursor"], ["*"])
        self.assertEqual(
            query["filter"],
            [
                "from_publication_date:2026-09-16,"
                "to_publication_date:2026-09-17,"
                'default.search:"climate change adaptation"'
            ],
        )
        self.assertEqual(query["select"], [",".join(MODULE.SELECT_FIELDS)])
        self.assertNotIn("api_key", query)
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 19})

    def test_optional_api_key_is_sent_as_query_parameter(self):
        with mock.patch.dict(os.environ, {"OPENALEX_API_KEY": "key value"}):
            _, urlopen = self.fetch([page(0, [])])
        self.assertEqual(urllib_query(urlopen.call_args.args[0].full_url)["api_key"], ["key value"])

    def test_cursor_pagination_is_complete_and_uses_one_fetch_timestamp(self):
        first = page(2, [work("W123")], "next token")
        second = page(2, [work("W456", doi=None)], None)

        records, urlopen = self.fetch([first, second])

        self.assertEqual([record["id"] for record in records], ["W123", "W456"])
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertEqual(urlopen.call_count, 2)
        second_query = urllib_query(urlopen.call_args_list[1].args[0].full_url)
        self.assertEqual(second_query["cursor"], ["next token"])

    def test_terminal_count_shortfall_raises_without_emitting_records(self):
        stdout = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(page(2, [work("W123")], None)),
        ):
            with contextlib.redirect_stdout(stdout):
                with self.assertRaisesRegex(
                    MODULE.IncompleteCoverageError,
                    "1 unique works; meta.count declared 2",
                ):
                    MODULE.main(["--days-back", "2"])
        self.assertEqual(stdout.getvalue(), "")

    def test_duplicate_ids_across_pages_cannot_conceal_terminal_shortfall(self):
        documents = [
            page(2, [work("W123")], "second"),
            page(2, [work("W123")], None),
        ]
        with self.assertRaisesRegex(
            MODULE.IncompleteCoverageError,
            "1 unique works; meta.count declared 2",
        ):
            self.fetch(documents)

    def test_complete_and_empty_terminal_results_succeed(self):
        complete, _ = self.fetch([page(2, [work("W1"), work("W2")])])
        empty, _ = self.fetch([page(0, [])])
        self.assertEqual([record["id"] for record in complete], ["W1", "W2"])
        self.assertEqual(empty, [])

    def test_duplicate_with_complete_unique_coverage_is_emitted_once(self):
        documents = [
            page(2, [work("W1"), work("W1")], "second"),
            page(2, [work("W2")]),
        ]
        records, _ = self.fetch(documents)
        self.assertEqual([record["id"] for record in records], ["W1", "W2"])

    def test_reported_count_above_record_or_page_bound_fails_early(self):
        cases = (
            ({"max_records": 1}, "above max_records=1"),
            ({"max_pages": 1}, "requiring at least 2 pages"),
        )
        for kwargs, message in cases:
            with self.subTest(kwargs=kwargs):
                with self.assertRaisesRegex(MODULE.IncompleteCoverageError, message):
                    self.fetch([page(101 if "max_pages" in kwargs else 2, [])], **kwargs)

    def test_nonterminal_result_at_page_limit_fails(self):
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "max_pages=1"):
            self.fetch([page(2, [work("W1")], "more")], max_pages=1)

    def test_repeated_cursor_and_changing_count_are_rejected(self):
        cases = (
            (
                [page(2, [work("W1")], "*"), page(2, [work("W2")])],
                "repeated cursor",
            ),
            (
                [page(2, [work("W1")], "next"), page(3, [work("W2")])],
                "meta.count changed",
            ),
        )
        for documents, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.OpenAlexError, message):
                    self.fetch(documents)

    def test_invalid_request_bounds_fail_before_http(self):
        invalid = (
            {"days_back": 0},
            {"days_back": MODULE.MAX_DAYS_BACK + 1},
            {"max_pages": 0},
            {"max_pages": MODULE.HARD_MAX_PAGES + 1},
            {"max_records": 0},
            {"max_records": MODULE.HARD_MAX_RECORDS + 1},
            {"timeout": 0},
        )
        for kwargs in invalid:
            with self.subTest(kwargs=kwargs):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaises(ValueError):
                        list(MODULE.fetch_recent_works(**kwargs))
                urlopen.assert_not_called()

    def test_rejects_malformed_responses_and_api_errors(self):
        malformed = (
            ([], "response must be an object"),
            ({"error": "bad-filter", "message": "invalid filter"}, "invalid filter"),
            ({"results": []}, "missing object meta"),
            ({"meta": {"count": -1, "next_cursor": None}, "results": []}, "meta.count"),
            ({"meta": {"count": 0}, "results": []}, "missing next_cursor"),
            ({"meta": {"count": 0, "next_cursor": 7}, "results": []}, "next_cursor"),
            ({"meta": {"count": 0, "next_cursor": None}, "results": {}}, "results must be a list"),
        )
        for document, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.OpenAlexError, message):
                    self.fetch([document])

    def test_rejects_malformed_work_ids_and_selected_fields(self):
        malformed = (
            (work(id="https://example.org/W123"), "invalid id"),
            (work(display_name=7), "display_name"),
            (work(publication_date="September 16"), "publication_date"),
            (work(cited_by_count=-1), "cited_by_count"),
            (work(authorships={}), "authorships"),
        )
        for document, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.OpenAlexError, message):
                    self.fetch([page(1, [document])])

    def test_main_emits_compact_ndjson_after_complete_validation(self):
        stdout = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(page(2, [work("W1"), work("W2")])),
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--days-back", "1", "--max-pages", "1"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual([json.loads(line)["id"] for line in lines], ["W1", "W2"])
        self.assertNotIn(": ", lines[0])

    def test_source_configuration_is_daily_bounded_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r'(?m)^schedule: "[0-9]+ [0-9]+ \* \* \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertRegex(text, r'(?m)^args: .*"--max-pages", "10"')
        self.assertIn("at most 10", text)
        self.assertIn("retries", text)
        self.assertIn("manual", text)
        self.assertIsNone(re.search(r"(?m)^enabled: true$", text))


def urllib_query(url):
    from urllib.parse import parse_qs, urlsplit

    return parse_qs(urlsplit(url).query)


if __name__ == "__main__":
    unittest.main()
