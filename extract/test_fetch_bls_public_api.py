import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error


SCRIPT_PATH = Path(__file__).parent / "scripts" / "fetch_bls_public_api.py"
SPEC = importlib.util.spec_from_file_location("fetch_bls_public_api", SCRIPT_PATH)
fetch_bls = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fetch_bls)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

DEFAULT_FOOTNOTES = object()


def observation(
    *,
    year="2026",
    period="M08",
    period_name="August",
    value="4.3",
    latest="true",
    footnotes=DEFAULT_FOOTNOTES,
):
    return {
        "year": year,
        "period": period,
        "periodName": period_name,
        "latest": latest,
        "value": value,
        "footnotes": [{}] if footnotes is DEFAULT_FOOTNOTES else footnotes,
    }


def response(series):
    return {
        "status": "REQUEST_SUCCEEDED",
        "responseTime": 123,
        "message": [],
        "Results": {"series": series},
    }


class FetchBlsPublicApiTests(unittest.TestCase):
    def test_parses_observation_with_stable_id_period_metadata_and_latest_status(self):
        document = response(
            [{"seriesID": "LNS14000000", "data": [observation()]}]
        )

        record = list(
            fetch_bls.parse_response(document, ("LNS14000000",), "2026-09-17T12:00:00+00:00")
        )[0]

        self.assertEqual(record["source"], "bls_public_api")
        self.assertEqual(record["id"], "LNS14000000:2026:M08")
        self.assertEqual(record["series_id"], "LNS14000000")
        self.assertEqual(record["year"], 2026)
        self.assertEqual(record["period"], "M08")
        self.assertEqual(record["period_name"], "August")
        self.assertEqual(record["value"], "4.3")
        self.assertIs(record["latest"], True)
        self.assertEqual(record["footnotes"], [])

        later = list(
            fetch_bls.parse_response(document, ("LNS14000000",), "2026-09-18T12:00:00+00:00")
        )[0]
        self.assertEqual(later["id"], record["id"])
        self.assertNotEqual(later["fetched_at"], record["fetched_at"])

    def test_preserves_non_empty_wire_format_footnotes(self):
        footnotes = [
            {"code": "P", "text": "preliminary"},
            {"code": "R", "text": "revised; see note 1"},
        ]
        document = response(
            [{"seriesID": "LNS14000000", "data": [observation(footnotes=footnotes)]}]
        )

        record = list(
            fetch_bls.parse_response(document, ("LNS14000000",), "stamp")
        )[0]

        self.assertEqual(record["footnotes"], footnotes)
        self.assertIsNot(record["footnotes"], footnotes)

    def test_normalizes_empty_wire_format_footnotes(self):
        for empty_footnotes in ([], [{}]):
            with self.subTest(empty_footnotes=empty_footnotes):
                document = response(
                    [
                        {
                            "seriesID": "LNS14000000",
                            "data": [observation(footnotes=empty_footnotes)],
                        }
                    ]
                )
                record = list(
                    fetch_bls.parse_response(document, ("LNS14000000",), "stamp")
                )[0]
                self.assertEqual(record["footnotes"], [])

    def test_rejects_malformed_wire_format_footnotes(self):
        malformed = (
            None,
            {},
            ["P"],
            [{"code": "P"}],
            [{"text": "preliminary"}],
            [{"code": 1, "text": "preliminary"}],
            [{"code": "P", "text": None}],
        )
        for footnotes in malformed:
            with self.subTest(footnotes=footnotes):
                document = response(
                    [
                        {
                            "seriesID": "LNS14000000",
                            "data": [observation(footnotes=footnotes)],
                        }
                    ]
                )
                with self.assertRaisesRegex(RuntimeError, "footnote"):
                    list(fetch_bls.parse_response(document, ("LNS14000000",), "stamp"))

    def test_empty_series_data_is_successful(self):
        document = response([{"seriesID": "LNS14000000", "data": []}])
        self.assertEqual(
            list(fetch_bls.parse_response(document, ("LNS14000000",), "stamp")),
            [],
        )

    def test_rejects_api_failure_status_and_message(self):
        document = {
            "status": "REQUEST_FAILED",
            "responseTime": 1,
            "message": ["Request could not be serviced"],
            "Results": {},
        }
        with self.assertRaisesRegex(RuntimeError, "Request could not be serviced"):
            list(fetch_bls.parse_response(document, ("LNS14000000",), "stamp"))

    def test_rejects_malformed_responses(self):
        cases = (
            ([], "response must be an object"),
            ({"status": "REQUEST_SUCCEEDED", "message": [], "Results": []}, "object Results"),
            (
                {"status": "REQUEST_SUCCEEDED", "message": [], "Results": {}},
                "Results.series must be a list",
            ),
            (response([{"seriesID": "LNS14000000"}]), "data must be a list"),
            (
                response([{"seriesID": "NOT_ALLOWED", "data": []}]),
                "unexpected seriesID",
            ),
            (response([]), "missing requested series"),
            (
                response(
                    [
                        {"seriesID": "LNS14000000", "data": []},
                        {"seriesID": "LNS14000000", "data": []},
                    ]
                ),
                "duplicate seriesID",
            ),
        )
        for document, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(RuntimeError, message):
                    list(fetch_bls.parse_response(document, ("LNS14000000",), "stamp"))

    def test_rejects_malformed_observations(self):
        cases = (
            (None, "non-object observation"),
            (observation(year="26"), "invalid year"),
            (observation(period="August"), "invalid period"),
            (observation(period_name=""), "invalid periodName"),
            (observation(value=4.3), "invalid value"),
            (observation(latest=True), "invalid latest status"),
        )
        for data, message in cases:
            with self.subTest(message=message):
                document = response([{"seriesID": "LNS14000000", "data": [data]}])
                with self.assertRaisesRegex(RuntimeError, message):
                    list(fetch_bls.parse_response(document, ("LNS14000000",), "stamp"))

    def test_ten_year_request_uses_correct_inclusive_bounds(self):
        document = response([{"seriesID": "LNS14000000", "data": []}])
        with patch.object(
            fetch_bls.urllib.request, "urlopen", return_value=JsonResponse(document)
        ) as urlopen:
            records = list(
                fetch_bls.fetch_bls_public_api(
                    2017, 2026, series_ids=("LNS14000000",), timeout=19
                )
            )

        self.assertEqual(records, [])
        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        self.assertEqual(request.full_url, fetch_bls.URL)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 19})
        self.assertEqual(
            json.loads(request.data),
            {
                "seriesid": ["LNS14000000"],
                "startyear": "2017",
                "endyear": "2026",
            },
        )
        self.assertNotIn("registrationkey", json.loads(request.data))
        self.assertEqual(request.headers["Content-type"], "application/json")
        self.assertEqual(request.headers["Accept"], "application/json")
        self.assertTrue(request.headers["User-agent"])

    def test_eleven_year_request_fails_before_http(self):
        with patch.object(fetch_bls.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(ValueError, "at most 10 inclusive calendar years"):
                list(
                    fetch_bls.fetch_bls_public_api(
                        2016, 2026, series_ids=("LNS14000000",)
                    )
                )
        urlopen.assert_not_called()

    def test_default_request_uses_explicit_allowlist_starting_with_unemployment(self):
        self.assertEqual(fetch_bls.SERIES_IDS[0], "LNS14000000")
        self.assertEqual(set(fetch_bls.SERIES_IDS), fetch_bls.SERIES_ALLOWLIST)
        document = response(
            [{"seriesID": series_id, "data": []} for series_id in fetch_bls.SERIES_IDS]
        )
        with patch.object(
            fetch_bls.urllib.request, "urlopen", return_value=JsonResponse(document)
        ) as urlopen:
            list(fetch_bls.fetch_bls_public_api(2026, 2026))

        payload = json.loads(urlopen.call_args.args[0].data)
        self.assertEqual(payload["seriesid"], list(fetch_bls.SERIES_IDS))

    def test_rejects_non_allowlisted_series_before_http(self):
        with patch.object(fetch_bls.urllib.request, "urlopen") as urlopen:
            with self.assertRaisesRegex(ValueError, "not allowlisted"):
                list(fetch_bls.fetch_bls_public_api(2026, 2026, ("BAD",)))
        urlopen.assert_not_called()

    def test_http_failures_propagate(self):
        error = urllib.error.HTTPError(fetch_bls.URL, 503, "unavailable", {}, None)
        with patch.object(fetch_bls.urllib.request, "urlopen", side_effect=error):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                list(
                    fetch_bls.fetch_bls_public_api(
                        2026, 2026, series_ids=("LNS14000000",)
                    )
                )
        self.assertEqual(caught.exception.code, 503)

    def test_main_emits_contract_ndjson(self):
        document = response(
            [
                {
                    "seriesID": "LNS14000000",
                    "data": [
                        observation(
                            latest="false",
                            footnotes=[{"code": "P", "text": "preliminary"}],
                        )
                    ],
                },
                *[
                    {"seriesID": series_id, "data": []}
                    for series_id in fetch_bls.SERIES_IDS[1:]
                ],
            ]
        )
        stdout = io.StringIO()
        with patch.object(
            fetch_bls.urllib.request, "urlopen", return_value=JsonResponse(document)
        ):
            with contextlib.redirect_stdout(stdout):
                fetch_bls.main(["--start-year", "2026", "--end-year", "2026"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["id"], "LNS14000000:2026:M08")
        self.assertIs(record["latest"], False)
        self.assertEqual(
            record["footnotes"], [{"code": "P", "text": "preliminary"}]
        )


if __name__ == "__main__":
    unittest.main()
