import importlib.util
import json
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).parent / "scripts" / "fetch_bls_public_api.py"
SPEC = importlib.util.spec_from_file_location("fetch_bls_public_api", SCRIPT)
bls = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(bls)


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return self.payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def successful_payload(series_ids=bls.SERIES_IDS):
    return {
        "status": "REQUEST_SUCCEEDED",
        "Results": {
            "series": [
                {
                    "seriesID": series_id,
                    "data": [
                        {
                            "year": "2026",
                            "period": "M08",
                            "periodName": "August",
                            "value": "4.2",
                            "latest": "true",
                            "footnotes": [{"footnoteCode": "P", "footnoteText": "Preliminary"}],
                        }
                    ],
                }
                for series_id in series_ids
            ]
        },
    }


class NormalizeResponseTests(unittest.TestCase):
    def test_normalizes_records_with_stable_ids_and_footnotes(self):
        records = bls.normalize_response(successful_payload(), "2026-09-13T00:00:00+00:00")

        self.assertEqual(len(records), len(bls.SERIES_IDS))
        self.assertEqual(records[0], {
            "source": "bls_public_api",
            "fetched_at": "2026-09-13T00:00:00+00:00",
            "id": "LNS14000000:2026:M08",
            "series_id": "LNS14000000",
            "year": "2026",
            "period": "M08",
            "period_name": "August",
            "value": "4.2",
            "latest": True,
            "footnotes": [{"code": "P", "text": "Preliminary"}],
        })
        self.assertEqual(bls.natural_id("LNS14000000", "2026", "M08"), records[0]["id"])

    def test_empty_data_is_valid(self):
        payload = successful_payload()
        for series in payload["Results"]["series"]:
            series["data"] = []

        self.assertEqual(bls.normalize_response(payload, "now"), [])

    def test_empty_footnote_placeholder_is_omitted(self):
        payload = successful_payload()
        payload["Results"]["series"][0]["data"][0]["footnotes"] = [{}]
        records = bls.normalize_response(payload, "now")

        self.assertEqual(records[0]["footnotes"], [])

    def test_false_or_missing_latest_is_false(self):
        payload = successful_payload()
        first = payload["Results"]["series"][0]["data"][0]
        first["latest"] = "false"
        payload["Results"]["series"][1]["data"][0].pop("latest")
        records = bls.normalize_response(payload, "now")

        self.assertFalse(records[0]["latest"])
        self.assertFalse(records[1]["latest"])

    def test_unsuccessful_api_response_fails_with_message(self):
        with self.assertRaisesRegex(ValueError, "REQUEST_NOT_PROCESSED: quota exceeded"):
            bls.normalize_response({"status": "REQUEST_NOT_PROCESSED", "message": ["quota exceeded"]}, "now")

    def test_malformed_or_incomplete_series_response_fails(self):
        payload = successful_payload()
        payload["Results"]["series"] = payload["Results"]["series"][:-1]
        with self.assertRaisesRegex(ValueError, "missing series"):
            bls.normalize_response(payload, "now")

        payload = successful_payload()
        payload["Results"]["series"][0]["data"][0]["periodName"] = None
        with self.assertRaisesRegex(ValueError, "invalid periodName"):
            bls.normalize_response(payload, "now")


class RequestTests(unittest.TestCase):
    def test_request_uses_allowlist_and_bounded_history(self):
        request = bls.build_request(bls.SERIES_IDS, 2025, 2026)

        self.assertEqual(request.full_url, bls.API_URL)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(json.loads(request.data), {
            "seriesid": list(bls.SERIES_IDS),
            "startyear": "2025",
            "endyear": "2026",
        })

    def test_request_rejects_unbounded_history(self):
        with self.assertRaisesRegex(ValueError, "between 1 and 20 years"):
            bls.build_request(bls.SERIES_IDS, 2000, 2026)
        with self.assertRaisesRegex(ValueError, "between 1 and 20"):
            list(bls.fetch_bls(years=0, end_year=2026))

    def test_fetch_uses_mocked_http_and_normalizes_response(self):
        payload = json.dumps(successful_payload()).encode("utf-8")
        with patch.object(bls.urllib.request, "urlopen", return_value=FakeResponse(payload)) as urlopen:
            records = list(bls.fetch_bls(years=2, end_year=2026, timeout=7))

        request = urlopen.call_args.args[0]
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 7)
        self.assertEqual(json.loads(request.data)["startyear"], "2025")
        self.assertEqual(json.loads(request.data)["endyear"], "2026")
        self.assertEqual(len(records), len(bls.SERIES_IDS))

    def test_fetch_rejects_invalid_json(self):
        with patch.object(bls.urllib.request, "urlopen", return_value=FakeResponse(b"not json")):
            with self.assertRaisesRegex(ValueError, "invalid JSON"):
                list(bls.fetch_bls(end_year=2026))


if __name__ == "__main__":
    unittest.main()
