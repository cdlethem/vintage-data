import io
import json
import unittest
from unittest import mock
from urllib.parse import parse_qs, urlparse

import fetch_world_bank_indicators as world_bank


FETCHED_AT = "2026-09-13T00:00:00+00:00"


def observation(period="2024", value=123.5, status=""):
    return {
        "indicator": {"id": "NY.GDP.MKTP.CD", "value": "GDP (current US$)"},
        "country": {"id": "US", "value": "United States"},
        "countryiso3code": "USA",
        "date": period,
        "value": value,
        "unit": "USD",
        "obs_status": status,
        "decimal": 0,
        "footnote": "",
    }


def page(number, pages, rows):
    return [{"page": str(number), "pages": str(pages), "per_page": "1", "total": str(len(rows))}, rows]


class WorldBankIndicatorsTest(unittest.TestCase):
    def test_normalizes_null_value_and_interpretation_metadata(self):
        record = world_bank.normalize_observation(observation(value=None, status="E"), FETCHED_AT)

        self.assertEqual(record["source"], "world_bank_indicators")
        self.assertEqual(record["fetched_at"], FETCHED_AT)
        self.assertEqual(record["indicator"], "NY.GDP.MKTP.CD")
        self.assertEqual(record["indicator_name"], "GDP (current US$)")
        self.assertEqual(record["country"], "United States")
        self.assertEqual(record["country_code"], "US")
        self.assertEqual(record["iso3"], "USA")
        self.assertEqual(record["period"], "2024")
        self.assertIsNone(record["value"])
        self.assertEqual(record["status"], "E")
        self.assertEqual(record["unit"], "USD")
        self.assertEqual(record["indicator_metadata"], observation()["indicator"])

    def test_stable_id_ignores_revised_value_and_fetched_at(self):
        original = world_bank.normalize_observation(observation(value=100), FETCHED_AT)
        revised = world_bank.normalize_observation(observation(value=200), "2026-09-14T00:00:00+00:00")

        self.assertEqual(original["id"], revised["id"])
        self.assertEqual(len(original["id"]), 64)
        self.assertNotEqual(original["value"], revised["value"])

    def test_fetches_all_pages_with_bounded_requests(self):
        requested_urls = []
        responses = [page(1, 2, [observation("2024")]), page(2, 2, [observation("2023")])]

        def get_json(url):
            requested_urls.append(url)
            return responses.pop(0)

        records = list(world_bank.fetch_observations(
            ["NY.GDP.MKTP.CD"], ["usa"], "2023", "2024", page_size=1,
            max_pages=2, get_json=get_json, fetched_at=FETCHED_AT,
        ))

        self.assertEqual([record["period"] for record in records], ["2024", "2023"])
        self.assertEqual(len(requested_urls), 2)
        for number, url in enumerate(requested_urls, start=1):
            parsed = urlparse(url)
            self.assertEqual(parsed.path, "/v2/country/USA/indicator/NY.GDP.MKTP.CD")
            self.assertEqual(parse_qs(parsed.query)["page"], [str(number)])
            self.assertEqual(parse_qs(parsed.query)["per_page"], ["1"])

    def test_empty_data_emits_no_records(self):
        records = list(world_bank.fetch_observations(
            ["SP.POP.TOTL"], ["GBR"], "2024", "2024", get_json=lambda _: page(1, 1, []),
            fetched_at=FETCHED_AT,
        ))

        self.assertEqual(records, [])

    def test_rejects_malformed_response_before_emitting_records(self):
        with self.assertRaisesRegex(ValueError, "two-element JSON array"):
            list(world_bank.fetch_observations(
                ["SP.POP.TOTL"], ["GBR"], "2024", "2024", get_json=lambda _: {"page": 1}
            ))

    def test_rejects_response_exceeding_page_bound_before_following_pages(self):
        calls = []

        def get_json(url):
            calls.append(url)
            return page(1, 2, [observation()])

        with self.assertRaisesRegex(ValueError, "configured maximum is 1"):
            list(world_bank.fetch_observations(
                ["SP.POP.TOTL"], ["GBR"], "2024", "2024", max_pages=1, get_json=get_json
            ))
        self.assertEqual(len(calls), 1)

    def test_rejects_invalid_request_bounds(self):
        with self.assertRaisesRegex(ValueError, "page_size"):
            world_bank.build_observation_url(["SP.POP.TOTL"], ["GBR"], "2024", "2024", 0, 1)
        with self.assertRaisesRegex(ValueError, "max_pages"):
            list(world_bank.fetch_observations(
                ["SP.POP.TOTL"], ["GBR"], "2024", "2024", max_pages=0, get_json=lambda _: page(1, 1, [])
            ))

    def test_get_json_uses_mocked_http(self):
        response = mock.MagicMock()
        response.__enter__.return_value = io.BytesIO(json.dumps(page(1, 1, [])).encode("utf-8"))
        with mock.patch.object(world_bank.urllib.request, "urlopen", return_value=response) as urlopen:
            payload = world_bank._get_json("https://example.test/data")

        self.assertEqual(payload, page(1, 1, []))
        self.assertEqual(urlopen.call_count, 1)
        self.assertEqual(urlopen.call_args.args[0].full_url, "https://example.test/data")


if __name__ == "__main__":
    unittest.main()
