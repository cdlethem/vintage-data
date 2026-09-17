import contextlib
import importlib.util
import io
import json
from pathlib import Path
import re
import unittest
from unittest import mock
import urllib.parse


SCRIPT = Path(__file__).with_name("fetch_world_bank_indicators.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "world_bank_indicators.yml"
SPEC = importlib.util.spec_from_file_location("fetch_world_bank_indicators", SCRIPT)
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


def metadata(*, page=1, pages=1, per_page=1000, total=1, **changes):
    value = {
        "page": page,
        "pages": pages,
        "per_page": per_page,
        "total": total,
        "sourceid": "2",
        "lastupdated": "2026-09-16",
    }
    value.update(changes)
    return value


def observation(
    *,
    indicator_id="SP.POP.TOTL",
    indicator_name="Population, total",
    country_id="US",
    country_name="United States",
    country_iso3code="USA",
    year="2025",
    value=340110988,
    unit="",
    obs_status="",
    decimal=0,
):
    return {
        "indicator": {"id": indicator_id, "value": indicator_name},
        "country": {"id": country_id, "value": country_name},
        "countryiso3code": country_iso3code,
        "date": year,
        "value": value,
        "unit": unit,
        "obs_status": obs_status,
        "decimal": decimal,
    }


def envelope(rows, **metadata_changes):
    return [metadata(**metadata_changes), rows]


class FetchWorldBankIndicatorsTests(unittest.TestCase):
    def fetch(self, documents, **changes):
        responses = [JsonResponse(document) for document in documents]
        arguments = {
            "start_year": 2025,
            "end_year": 2025,
            "indicators": ("SP.POP.TOTL",),
            "country": "all",
            "page_size": 1000,
            "max_pages": 10,
            "max_records": 10_000,
            "timeout": 17,
        }
        arguments.update(changes)
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            records = list(MODULE.fetch_world_bank_indicators(**arguments))
        return records, urlopen

    def test_normalizes_stable_id_null_value_status_units_and_raw_metadata(self):
        wire_metadata = metadata(source_note="International Debt Statistics")
        row = observation(value=None, unit="people", obs_status="E")
        records, _ = self.fetch([[wire_metadata, [row]]])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "world_bank_indicators")
        self.assertEqual(record["id"], "SP.POP.TOTL:US:2025")
        self.assertEqual(record["indicator_name"], "Population, total")
        self.assertEqual(record["country_iso3_code"], "USA")
        self.assertEqual(record["year"], 2025)
        self.assertIsNone(record["value"])
        self.assertEqual(record["unit"], "people")
        self.assertEqual(record["obs_status"], "E")
        self.assertEqual(record["metadata"], wire_metadata)
        self.assertIsNot(record["metadata"], wire_metadata)

        later = MODULE.normalize_observation(
            row,
            "later",
            wire_metadata,
            ("SP.POP.TOTL",),
            2025,
            2025,
        )
        self.assertEqual(later["id"], record["id"])
        self.assertNotEqual(later["fetched_at"], record["fetched_at"])

    def test_fetches_every_declared_page_before_yielding(self):
        first = envelope(
            [observation(country_id="US", country_iso3code="USA")],
            page=1,
            pages=2,
            per_page=1,
            total=2,
        )
        second = envelope(
            [
                observation(
                    country_id="CA",
                    country_name="Canada",
                    country_iso3code="CAN",
                )
            ],
            page=2,
            pages=2,
            per_page=1,
            total=2,
        )
        records, urlopen = self.fetch([first, second], page_size=1)

        self.assertEqual([record["country_id"] for record in records], ["US", "CA"])
        self.assertEqual(urlopen.call_count, 2)
        for number, call in enumerate(urlopen.call_args_list, 1):
            request = call.args[0]
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
            self.assertEqual(query["page"], [str(number)])
            self.assertEqual(query["date"], ["2025:2025"])
            self.assertEqual(query["format"], ["json"])
            self.assertEqual(query["source"], ["2"])
            self.assertEqual(call.kwargs, {"timeout": 17})
            self.assertEqual(request.headers["Accept"], "application/json")
            self.assertTrue(request.headers["User-agent"])

    def test_null_zero_result_is_successful_and_does_not_request_another_page(self):
        zero_result = [metadata(page=1, pages=0, per_page=1000, total=0), None]
        records, urlopen = self.fetch([zero_result])

        self.assertEqual(records, [])
        urlopen.assert_called_once()

    def test_empty_list_zero_result_is_successful_and_does_not_request_another_page(self):
        zero_result = [metadata(page=1, pages=0, per_page=1000, total=0), []]
        records, urlopen = self.fetch([zero_result])

        self.assertEqual(records, [])
        urlopen.assert_called_once()

    def test_rejects_invalid_zero_result_boundaries(self):
        invalid = (
            (
                [metadata(page=1, pages=1, per_page=1000, total=1), None],
                "data may be null only",
            ),
            (
                [metadata(page=1, pages=0, per_page=1000, total=1), []],
                "metadata.pages",
            ),
            (
                [
                    metadata(page=1, pages=0, per_page=1000, total=0),
                    [observation()],
                ],
                "zero-total response contains observations",
            ),
        )
        for document, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.WorldBankError, message):
                    self.fetch([document])

    def test_rejects_malformed_envelopes_and_pagination_metadata(self):
        invalid = (
            ({}, "two-item"),
            ([[], []], "metadata must be an object"),
            ([metadata(page="1"), []], "metadata.page"),
            ([metadata(pages=2, total=1), []], "metadata.pages"),
            ([metadata(page=2), []], "does not match requested page"),
            ([metadata(), {}], "data must be a list"),
        )
        for document, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.WorldBankError, message):
                    self.fetch([document])

    def test_rejects_incomplete_and_changing_pagination_without_yielding(self):
        first = envelope(
            [observation(country_id="US")],
            page=1,
            pages=2,
            per_page=1,
            total=2,
        )
        changing_second = envelope(
            [observation(country_id="CA")],
            page=2,
            pages=3,
            per_page=1,
            total=3,
        )
        with self.assertRaisesRegex(MODULE.WorldBankError, "metadata changed"):
            self.fetch([first, changing_second], page_size=1)

        empty_second = envelope([], page=2, pages=2, per_page=1, total=2)
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "ended with 1"):
            self.fetch([first, empty_second], page_size=1)

    def test_configured_bounds_reject_incomplete_result_before_emitting(self):
        too_many_records = envelope([], page=1, pages=2, per_page=1000, total=1001)
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "max_records=1000"):
            self.fetch([too_many_records], max_records=1000)

        too_many_pages = envelope([], page=1, pages=2, per_page=1, total=2)
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "max_pages=1"):
            self.fetch([too_many_pages], page_size=1, max_pages=1)

    def test_rejects_bad_observation_identity_and_values(self):
        invalid = (
            (observation(indicator_id="NY.GDP.MKTP.CD"), "unrequested indicator"),
            (observation(year="25"), "invalid date"),
            (observation(year="2024"), "outside requested range"),
            (observation(value="340110988"), "invalid value"),
            (observation(decimal=True), "invalid decimal"),
            ({}, "invalid indicator metadata"),
        )
        for row, message in invalid:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.WorldBankError, message):
                    self.fetch([envelope([row])])

    def test_invalid_request_bounds_fail_before_http(self):
        cases = (
            ({"start_year": 2026, "end_year": 2025}, "start_year"),
            ({"indicators": ("BAD.INDICATOR",)}, "not allowlisted"),
            ({"country": "United States"}, "country"),
            ({"page_size": 1001}, "page_size"),
            ({"max_pages": 0}, "max_pages"),
            ({"max_records": 0}, "max_records"),
            ({"timeout": 0}, "timeout"),
        )
        for changes, message in cases:
            with self.subTest(changes=changes):
                arguments = {
                    "start_year": 2025,
                    "end_year": 2025,
                    "indicators": ("SP.POP.TOTL",),
                }
                arguments.update(changes)
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        list(MODULE.fetch_world_bank_indicators(**arguments))
                urlopen.assert_not_called()

    def test_main_emits_standard_ndjson(self):
        stdout = io.StringIO()
        document = envelope([observation(value=None)])
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)
        ):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(
                    [
                        "--start-year",
                        "2025",
                        "--end-year",
                        "2025",
                        "--indicator",
                        "SP.POP.TOTL",
                    ]
                )

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["source"], "world_bank_indicators")
        self.assertEqual(record["id"], "SP.POP.TOTL:US:2025")
        self.assertIsNone(record["value"])

    def test_source_configuration_stays_disabled_with_bounded_backfill(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertRegex(text, r"(?m)^enabled:\s*false\s*$")
        self.assertIsNone(re.search(r"(?m)^enabled:\s*true\s*$", text))
        self.assertRegex(text, r"(?m)^\s*unit:\s*year\s*$")
        self.assertIn('"--start-year", "{yyyy}", "--end-year", "{yyyy}"', text)
        self.assertIn("live-source smoke", text)
        self.assertIn("Airflow import", text)


if __name__ == "__main__":
    unittest.main()
