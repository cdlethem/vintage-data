import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import urllib.error
import urllib.parse


SCRIPT_PATH = Path(__file__).with_name("fetch_world_bank_indicators.py")
CONFIG_PATH = Path(__file__).parents[1] / "sources" / "world_bank_indicators.yml"
SPEC = importlib.util.spec_from_file_location("fetch_world_bank_indicators", SCRIPT_PATH)
fetch_world_bank = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fetch_world_bank)


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def observation(
    *,
    indicator_id="SP.POP.TOTL",
    indicator_name="Population, total",
    country_id="US",
    country_iso3="USA",
    country_name="United States",
    year="2025",
    value=340_110_988,
    unit="people",
    status="E",
    decimal=0,
):
    return {
        "indicator": {"id": indicator_id, "value": indicator_name},
        "country": {"id": country_id, "value": country_name},
        "countryiso3code": country_iso3,
        "date": year,
        "value": value,
        "unit": unit,
        "obs_status": status,
        "decimal": decimal,
    }


def response(*observations, page=1, pages=1, per_page=100, total=None):
    return [
        {
            "page": str(page),
            "pages": str(pages),
            "per_page": str(per_page),
            "total": str(len(observations) if total is None else total),
            "sourceid": "2",
            "lastupdated": "2026-07-01",
        },
        list(observations),
    ]


class FetchWorldBankIndicatorsTests(unittest.TestCase):
    def test_normalizes_observation_with_stable_id_and_raw_metadata(self):
        document = response(observation())
        page = fetch_world_bank.parse_response(document, 1, 10)

        record = fetch_world_bank.normalize_observation(
            page.observations[0],
            expected_indicator="SP.POP.TOTL",
            requested_countries=frozenset({"USA"}),
            start_year=2025,
            end_year=2026,
            fetched_at="2026-09-17T12:00:00+00:00",
            metadata=page.metadata,
        )

        self.assertEqual(record["source"], "world_bank_indicators")
        self.assertEqual(record["id"], "SP.POP.TOTL:USA:2025")
        self.assertEqual(record["indicator_name"], "Population, total")
        self.assertEqual(record["country_iso3"], "USA")
        self.assertEqual(record["value"], 340_110_988)
        self.assertEqual(record["unit"], "people")
        self.assertEqual(record["status"], "E")
        self.assertEqual(record["decimal"], 0)
        self.assertEqual(record["raw_metadata"]["lastupdated"], "2026-07-01")
        self.assertEqual(record["raw"], document[1][0])

        later = fetch_world_bank.normalize_observation(
            page.observations[0],
            expected_indicator="SP.POP.TOTL",
            requested_countries=frozenset({"USA"}),
            start_year=2025,
            end_year=2026,
            fetched_at="later",
            metadata=page.metadata,
        )
        self.assertEqual(later["id"], record["id"])
        self.assertNotEqual(later["fetched_at"], record["fetched_at"])

    def test_preserves_null_value_empty_unit_and_status(self):
        item = observation(value=None, unit="", status="")
        page = fetch_world_bank.parse_response(response(item), 1, 10)

        record = fetch_world_bank.normalize_observation(
            item,
            expected_indicator="SP.POP.TOTL",
            requested_countries=frozenset({"USA"}),
            start_year=2025,
            end_year=2025,
            fetched_at="stamp",
            metadata=page.metadata,
        )

        self.assertIsNone(record["value"])
        self.assertEqual(record["unit"], "")
        self.assertEqual(record["status"], "")

    def test_empty_response_is_successful(self):
        with patch.object(
            fetch_world_bank.urllib.request,
            "urlopen",
            return_value=JsonResponse(response(total=0)),
        ):
            records = list(
                fetch_world_bank.fetch_world_bank_indicators(
                    indicators=("SP.POP.TOTL",),
                    countries=("USA",),
                    start_year=2025,
                    end_year=2025,
                )
            )
        self.assertEqual(records, [])

    def test_fetches_every_advertised_page_with_explicit_bounds(self):
        first = response(
            observation(year="2025"), page=1, pages=2, per_page=1, total=2
        )
        second = response(
            observation(year="2026"), page=2, pages=2, per_page=1, total=2
        )
        with patch.object(
            fetch_world_bank.urllib.request,
            "urlopen",
            side_effect=[JsonResponse(first), JsonResponse(second)],
        ) as urlopen:
            records = list(
                fetch_world_bank.fetch_world_bank_indicators(
                    indicators=("SP.POP.TOTL",),
                    countries=("USA",),
                    start_year=2025,
                    end_year=2026,
                    page_size=1,
                    max_pages=2,
                    timeout=19,
                )
            )

        self.assertEqual([item["date"] for item in records], ["2025", "2026"])
        self.assertEqual(urlopen.call_count, 2)
        for index, call in enumerate(urlopen.call_args_list, start=1):
            request = call.args[0]
            parts = urllib.parse.urlsplit(request.full_url)
            query = urllib.parse.parse_qs(parts.query)
            self.assertEqual(
                query,
                {
                    "format": ["json"],
                    "date": ["2025:2026"],
                    "page": [str(index)],
                    "per_page": ["1"],
                },
            )
            self.assertEqual(
                parts.path,
                "/v2/country/USA/indicator/SP.POP.TOTL",
            )
            self.assertEqual(call.kwargs, {"timeout": 19})
            self.assertEqual(request.headers["Accept"], "application/json")
            self.assertTrue(request.headers["User-agent"])

    def test_fetches_each_indicator_separately(self):
        population = response(observation(), total=1)
        gdp = response(
            observation(
                indicator_id="NY.GDP.MKTP.CD",
                indicator_name="GDP (current US$)",
                value=30_000_000_000_000,
                unit="current US$",
            ),
            total=1,
        )
        with patch.object(
            fetch_world_bank.urllib.request,
            "urlopen",
            side_effect=[JsonResponse(population), JsonResponse(gdp)],
        ) as urlopen:
            records = list(
                fetch_world_bank.fetch_world_bank_indicators(
                    indicators=("SP.POP.TOTL", "NY.GDP.MKTP.CD"),
                    countries=("USA",),
                    start_year=2025,
                    end_year=2025,
                )
            )

        self.assertEqual(
            [item["id"] for item in records],
            ["SP.POP.TOTL:USA:2025", "NY.GDP.MKTP.CD:USA:2025"],
        )
        self.assertEqual(urlopen.call_count, 2)

    def test_page_bound_fails_before_followup_request(self):
        document = response(page=1, pages=3, total=0)
        with patch.object(
            fetch_world_bank.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ) as urlopen:
            with self.assertRaisesRegex(RuntimeError, "exceeding max_pages=2"):
                list(
                    fetch_world_bank.fetch_world_bank_indicators(
                        indicators=("SP.POP.TOTL",),
                        countries=("USA",),
                        start_year=2025,
                        end_year=2025,
                        max_pages=2,
                    )
                )
        urlopen.assert_called_once()

    def test_rejects_incomplete_pagination(self):
        document = response(observation(), total=2)
        with patch.object(
            fetch_world_bank.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ):
            with self.assertRaisesRegex(RuntimeError, "advertised 2 observations"):
                list(
                    fetch_world_bank.fetch_world_bank_indicators(
                        indicators=("SP.POP.TOTL",),
                        countries=("USA",),
                        start_year=2025,
                        end_year=2025,
                    )
                )

    def test_rejects_duplicate_observations_across_pages(self):
        first = response(observation(), page=1, pages=2, per_page=1, total=2)
        second = response(observation(), page=2, pages=2, per_page=1, total=2)
        with patch.object(
            fetch_world_bank.urllib.request,
            "urlopen",
            side_effect=[JsonResponse(first), JsonResponse(second)],
        ):
            with self.assertRaisesRegex(RuntimeError, "duplicate observation"):
                list(
                    fetch_world_bank.fetch_world_bank_indicators(
                        indicators=("SP.POP.TOTL",),
                        countries=("USA",),
                        start_year=2025,
                        end_year=2025,
                        page_size=1,
                    )
                )

    def test_rejects_malformed_page_responses(self):
        cases = (
            ({}, "two-element list"),
            ([{}, []], "metadata page"),
            ([{"page": "1", "pages": "1", "per_page": "100", "total": "0"}, {}], "observations must be a list"),
            (response(page=2), "returned page 2"),
            (response(page=1, pages=0), "invalid page bounds"),
            (response(*[observation(year=str(2020 + index)) for index in range(2)], per_page=1), "more observations"),
        )
        for document, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(RuntimeError, message):
                    fetch_world_bank.parse_response(document, 1, 10)

    def test_rejects_malformed_or_out_of_bounds_observations(self):
        cases = (
            (None, "non-object"),
            (observation(indicator_id="BAD"), "unexpected indicator"),
            (observation(country_iso3="CAN"), "unexpected country"),
            (observation(year="25"), "invalid date"),
            (observation(year="2024"), "outside request bounds"),
            (observation(value={}), "invalid value"),
            (observation(unit=None), "invalid unit"),
            (observation(status=None), "invalid status"),
            (observation(decimal="0"), "invalid decimal"),
        )
        metadata = response(total=0)[0]
        for item, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(RuntimeError, message):
                    fetch_world_bank.normalize_observation(
                        item,
                        expected_indicator="SP.POP.TOTL",
                        requested_countries=frozenset({"USA"}),
                        start_year=2025,
                        end_year=2025,
                        fetched_at="stamp",
                        metadata=metadata,
                    )

    def test_rejects_request_bounds_before_http(self):
        cases = (
            ({"indicators": ("BAD",)}, "not allowlisted"),
            ({"countries": ("BAD",)}, "not allowlisted"),
            ({"start_year": 2016, "end_year": 2026}, "at most 10 inclusive years"),
            ({"start_year": 2026, "end_year": 2025}, "must not exceed"),
            ({"page_size": 0}, "page_size"),
            ({"page_size": 1001}, "page_size"),
            ({"max_pages": 101}, "max_pages"),
            ({"timeout": 0}, "timeout"),
        )
        defaults = {
            "indicators": ("SP.POP.TOTL",),
            "countries": ("USA",),
            "start_year": 2025,
            "end_year": 2026,
        }
        for changes, message in cases:
            with self.subTest(message=message):
                with patch.object(fetch_world_bank.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        list(
                            fetch_world_bank.fetch_world_bank_indicators(
                                **{**defaults, **changes}
                            )
                        )
                urlopen.assert_not_called()

    def test_http_failures_propagate(self):
        error = urllib.error.HTTPError(
            fetch_world_bank.API_URL, 503, "unavailable", {}, None
        )
        with patch.object(
            fetch_world_bank.urllib.request, "urlopen", side_effect=error
        ):
            with self.assertRaises(urllib.error.HTTPError) as caught:
                list(
                    fetch_world_bank.fetch_world_bank_indicators(
                        indicators=("SP.POP.TOTL",),
                        countries=("USA",),
                        start_year=2025,
                        end_year=2025,
                    )
                )
        self.assertEqual(caught.exception.code, 503)
        error.close()

    def test_main_emits_contract_ndjson(self):
        document = response(observation(value=None, status="", unit=""))
        stdout = io.StringIO()
        with patch.object(
            fetch_world_bank.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ):
            with contextlib.redirect_stdout(stdout):
                fetch_world_bank.main(
                    [
                        "--indicators",
                        "SP.POP.TOTL",
                        "--countries",
                        "USA",
                        "--start-year",
                        "2025",
                        "--end-year",
                        "2025",
                    ]
                )

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        record = json.loads(lines[0])
        self.assertEqual(record["id"], "SP.POP.TOTL:USA:2025")
        self.assertIsNone(record["value"])
        self.assertIn("fetched_at", record)

    def test_checked_in_source_configuration_remains_disabled(self):
        settings = {}
        for line in CONFIG_PATH.read_text(encoding="utf-8").splitlines():
            if not line or line[0].isspace() or line.lstrip().startswith("#"):
                continue
            key, separator, value = line.partition(":")
            if separator:
                settings[key.strip()] = value.split("#", 1)[0].strip()

        self.assertEqual(settings.get("name"), "world_bank_indicators")
        self.assertEqual(settings.get("script"), "fetch_world_bank_indicators.py")
        self.assertEqual(settings.get("enabled"), "false")


if __name__ == "__main__":
    unittest.main()
