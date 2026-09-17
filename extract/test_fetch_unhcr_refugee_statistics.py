import contextlib
import importlib.util
import io
import json
import math
from pathlib import Path
import re
import unittest
from unittest import mock
import urllib.parse


SCRIPT = Path(__file__).parent / "scripts" / "fetch_unhcr_refugee_statistics.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "unhcr_refugee_statistics.yml"
SPEC = importlib.util.spec_from_file_location("fetch_unhcr_refugee_statistics", SCRIPT)
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


def observation(number=1, **changes):
    row = {
        "year": 2025,
        "coo_id": number,
        "coo_name": f"Origin {number}",
        "coo": f"O{number}",
        "coo_iso": f"OR{number}",
        "coa_id": 500 + number,
        "coa_name": f"Asylum {number}",
        "coa": f"A{number}",
        "coa_iso": f"AS{number}",
        "refugees": number * 10,
        "asylum_seekers": number,
        "returned_refugees": 0,
        "idps": None,
        "returned_idps": None,
        "stateless": None,
        "ooc": None,
        "oip": None,
        "hst": None,
    }
    row.update(changes)
    return row


def page(*items, max_pages=1):
    return {"items": list(items), "maxPages": max_pages}


class FetchUnhcrRefugeeStatisticsTests(unittest.TestCase):
    def fetch(self, documents, **kwargs):
        responses = [JsonResponse(document) for document in documents]
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                records = list(MODULE.fetch_population(2025, **kwargs))
        return records, urlopen, sleep

    def test_normalizes_stable_source_id_country_fields_and_null_populations(self):
        row = observation(
            coo_id="-",
            coa_id=501,
            refugees=None,
            asylum_seekers=None,
            returned_refugees=None,
        )

        record = MODULE.normalize_observation(row, "2026-09-17T12:00:00+00:00")

        self.assertEqual(record["source"], "unhcr_refugee_statistics")
        self.assertEqual(record["fetched_at"], "2026-09-17T12:00:00+00:00")
        self.assertEqual(record["id"], "2025:-:501")
        self.assertEqual(record["coo_id"], "-")
        self.assertEqual(record["coa_id"], 501)
        self.assertEqual(record["coo_iso"], row["coo_iso"])
        for field in MODULE.POPULATION_FIELDS:
            self.assertIsNone(record[field])

    def test_empty_object_and_invalid_identity_components_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "required year"):
            MODULE.normalize_observation({}, "timestamp")

        for field in ("year", "coo_id", "coa_id"):
            for invalid in (mock.sentinel.missing, None, ""):
                row = observation()
                if invalid is mock.sentinel.missing:
                    del row[field]
                else:
                    row[field] = invalid
                with self.subTest(field=field, invalid=invalid):
                    with self.assertRaisesRegex(ValueError, field):
                        MODULE.normalize_observation(row, "timestamp")

    def test_rejects_non_object_and_non_integer_year(self):
        with self.assertRaisesRegex(ValueError, "not an object"):
            MODULE.normalize_observation([], "timestamp")
        with self.assertRaisesRegex(ValueError, "invalid required year"):
            MODULE.normalize_observation(observation(year="2025"), "timestamp")

    def test_constant_page_size_enforces_limit_without_boundary_gaps(self):
        source_rows = [observation(number) for number in range(1, 221)]
        requested_sizes = []
        requested_pages = []

        def respond(request, *, timeout):
            self.assertEqual(timeout, 17)
            params = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
            requested_page = int(params["page"][0])
            requested_size = int(params["limit"][0])
            requested_pages.append(requested_page)
            requested_sizes.append(requested_size)
            start = (requested_page - 1) * requested_size
            stop = start + requested_size
            return JsonResponse(
                page(
                    *source_rows[start:stop],
                    max_pages=math.ceil(len(source_rows) / requested_size),
                )
            )

        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=respond) as urlopen:
            with mock.patch.object(MODULE.time, "sleep") as sleep:
                records = list(
                    MODULE.fetch_population(
                        2025,
                        page_size=100,
                        limit=150,
                        timeout=17,
                        request_interval=0.5,
                    )
                )

        expected_ids = [f"2025:{number}:{500 + number}" for number in range(1, 151)]
        actual_ids = [record["id"] for record in records]
        self.assertEqual(requested_sizes, [100, 100])
        self.assertEqual(requested_pages, [1, 2])
        self.assertEqual(urlopen.call_count, 2)
        self.assertLessEqual(urlopen.call_count, math.ceil(150 / 100))
        self.assertEqual(actual_ids, expected_ids)
        self.assertEqual(len(actual_ids), len(set(actual_ids)))
        sleep.assert_called_once_with(0.5)

    def test_request_has_year_headers_timeout_and_constant_small_limit(self):
        records, urlopen, sleep = self.fetch(
            [page(observation(), max_pages=1)],
            page_size=100,
            limit=40,
            timeout=23,
        )

        self.assertEqual(len(records), 1)
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        params = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
        self.assertEqual(
            params,
            {"yearFrom": ["2025"], "yearTo": ["2025"], "page": ["1"], "limit": ["40"]},
        )
        self.assertEqual(request.get_header("Accept"), "application/json")
        self.assertTrue(request.get_header("User-agent"))
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 23})
        sleep.assert_not_called()

    def test_empty_page_stops_and_all_records_share_fetch_timestamp(self):
        records, urlopen, sleep = self.fetch(
            [page(observation(1), observation(2), max_pages=3), page(max_pages=3)],
            page_size=2,
            limit=6,
            request_interval=0,
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["fetched_at"], records[1]["fetched_at"])
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)

    def test_max_pages_stops_pagination(self):
        records, urlopen, sleep = self.fetch(
            [page(observation(1), max_pages=1)],
            page_size=1,
            limit=3,
        )

        self.assertEqual(len(records), 1)
        urlopen.assert_called_once()
        sleep.assert_not_called()

    def test_invalid_bounds_fail_before_http(self):
        invalid_arguments = (
            {"year": MODULE.FIRST_REPORTING_YEAR - 1},
            {"year": MODULE.current_reporting_year() + 1},
            {"page_size": 0},
            {"page_size": MODULE.MAX_PAGE_SIZE + 1},
            {"limit": 0},
            {"limit": MODULE.MAX_RECORD_LIMIT + 1},
            {"timeout": 0},
            {"timeout": MODULE.MAX_TIMEOUT_SECONDS + 1},
            {"request_interval": -0.1},
            {"request_interval": MODULE.MAX_REQUEST_INTERVAL_SECONDS + 0.1},
        )
        for arguments in invalid_arguments:
            year = arguments.pop("year", 2025)
            with self.subTest(year=year, arguments=arguments):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaises(ValueError):
                        list(MODULE.fetch_population(year, **arguments))
                urlopen.assert_not_called()

    def test_rejects_malformed_response_envelopes(self):
        malformed = (
            ([], "not an object"),
            ({}, "invalid items"),
            ({"items": {}}, "invalid items"),
            ({"items": [], "maxPages": None}, "invalid maxPages"),
        )
        for document, message in malformed:
            with self.subTest(document=document):
                with self.assertRaisesRegex(ValueError, message):
                    self.fetch([document])

    def test_main_defaults_to_current_reporting_year_and_emits_ndjson(self):
        stdout = io.StringIO()
        records = [MODULE.normalize_observation(observation(), "timestamp")]

        with mock.patch.object(MODULE, "current_reporting_year", return_value=2025):
            with mock.patch.object(MODULE, "fetch_population", return_value=iter(records)) as fetch:
                with contextlib.redirect_stdout(stdout):
                    MODULE.main([])

        fetch.assert_called_once_with(
            2025,
            page_size=MODULE.DEFAULT_PAGE_SIZE,
            limit=MODULE.DEFAULT_LIMIT,
            timeout=MODULE.DEFAULT_TIMEOUT_SECONDS,
        )
        self.assertEqual(json.loads(stdout.getvalue()), records[0])

    def test_source_configuration_is_annual_and_disabled(self):
        text = SOURCE_CONFIG.read_text(encoding="utf-8")

        self.assertRegex(text, r'(?m)^schedule: "43 8 15 1 \*"')
        self.assertRegex(text, r"(?m)^enabled: false$")
        self.assertRegex(
            text,
            r'(?m)^args: \["--page-size", "100", "--limit", "1000"\]$',
        )
        self.assertIsNone(re.search(r"(?m)^enabled: true$", text))


if __name__ == "__main__":
    unittest.main()
