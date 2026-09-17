import io
import json
import pathlib
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(pathlib.Path(__file__).parent / "scripts"))
import fetch_unhcr_refugee_statistics as unhcr  # noqa: E402


ROW = {
    "year": 2025,
    "coo_id": "SYR",
    "coo": "SYR",
    "coo_name": "Syrian Arab Republic",
    "coo_iso": "SYR",
    "coa_id": "DEU",
    "coa": "DEU",
    "coa_name": "Germany",
    "coa_iso": "DEU",
    "refugees": 712000,
    "asylum_seekers": None,
    "returned_refugees": 10,
    "idps": None,
    "returned_idps": 3,
    "stateless": 1,
    "ooc": None,
    "oip": 4,
    "hst": 5,
}


class JsonResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return io.StringIO(json.dumps(self.payload))

    def __exit__(self, exc_type, exc, traceback):
        return False


class UnhcrPopulationTests(unittest.TestCase):
    def test_normalize_preserves_population_nulls_and_country_context(self):
        record = unhcr.normalize(ROW, "2026-01-02T03:04:05+00:00")

        self.assertEqual(record["id"], "unhcr-population:2025:SYR:DEU")
        self.assertEqual(record["country_of_origin"], "Syrian Arab Republic")
        self.assertEqual(record["country_of_asylum_iso3"], "DEU")
        self.assertEqual(record["refugees"], 712000)
        self.assertIsNone(record["asylum_seekers"])
        self.assertIsNone(record["idps"])
        self.assertIsNone(record["other_people_of_concern"])

    def test_observation_id_is_stable_and_source_derived(self):
        same_observation = dict(ROW, refugees=1, asylum_seekers=999)
        self.assertEqual(
            unhcr.observation_id(ROW), unhcr.observation_id(same_observation)
        )
        self.assertEqual(
            unhcr.observation_id(ROW), "unhcr-population:2025:SYR:DEU"
        )

    def test_empty_page_emits_no_records(self):
        with patch.object(unhcr.urllib.request, "urlopen", return_value=JsonResponse({"items": []})) as open_mock:
            self.assertEqual(list(unhcr.fetch_population(2025)), [])
        self.assertEqual(open_mock.call_count, 1)

    def test_malformed_responses_fail_loudly(self):
        with patch.object(unhcr.urllib.request, "urlopen", return_value=JsonResponse({})):
            with self.assertRaisesRegex(ValueError, "items list"):
                list(unhcr.fetch_population(2025))

        with patch.object(unhcr.urllib.request, "urlopen", return_value=JsonResponse({"items": ["not a row"]})):
            with self.assertRaisesRegex(ValueError, "non-object"):
                list(unhcr.fetch_population(2025))

    def test_pagination_uses_api_pages_and_request_limits(self):
        responses = [
            {"items": [ROW], "maxPages": 3},
            {"items": [dict(ROW, coa_id="FRA", coa="FRA", coa_name="France", coa_iso="FRA")], "maxPages": 3},
            {"items": [dict(ROW, coa_id="ESP", coa="ESP", coa_name="Spain", coa_iso="ESP")], "maxPages": 3},
        ]
        requested = []

        def urlopen(request, timeout):
            requested.append((request.full_url, timeout, request.get_header("User-agent")))
            return JsonResponse(responses.pop(0))

        with patch.object(unhcr.urllib.request, "urlopen", side_effect=urlopen), patch.object(unhcr.time, "sleep") as sleep:
            records = list(unhcr.fetch_population(2025, page_size=1, limit=3, max_pages=2))

        self.assertEqual([record["country_of_asylum_id"] for record in records], ["DEU", "FRA"])
        self.assertEqual(len(requested), 2)
        self.assertEqual([parse_qs(urlparse(url).query)["page"] for url, _, _ in requested], [["1"], ["2"]])
        self.assertEqual([parse_qs(urlparse(url).query)["limit"] for url, _, _ in requested], [["1"], ["1"]])
        self.assertTrue(all(timeout == unhcr.TIMEOUT_SECONDS for _, timeout, _ in requested))
        self.assertTrue(all(agent == unhcr.USER_AGENT for _, _, agent in requested))
        sleep.assert_called_once_with(unhcr.REQUEST_DELAY_SECONDS)

    def test_local_request_bounds_reject_unbounded_callers(self):
        with self.assertRaisesRegex(ValueError, "page_size"):
            list(unhcr.fetch_population(2025, page_size=unhcr.MAX_PAGE_SIZE + 1))
        with self.assertRaisesRegex(ValueError, "limit"):
            list(unhcr.fetch_population(2025, limit=unhcr.MAX_RECORDS + 1))
        with self.assertRaisesRegex(ValueError, "max_pages"):
            list(unhcr.fetch_population(2025, max_pages=unhcr.MAX_PAGES + 1))


if __name__ == "__main__":
    unittest.main()
