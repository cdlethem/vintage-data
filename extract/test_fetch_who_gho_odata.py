import contextlib
import importlib.util
import io
import json
from pathlib import Path
import unittest
from unittest import mock
import urllib.error
import urllib.parse

import yaml


SCRIPT = Path(__file__).parent / "scripts" / "fetch_who_gho_odata.py"
CONFIG = Path(__file__).parent / "sources" / "who_gho_odata.yml"
SPEC = importlib.util.spec_from_file_location("fetch_who_gho_odata", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

INDICATOR = "WHOSIS_000001"
GEOGRAPHY_TYPE = "COUNTRY"
GEOGRAPHY = "USA"
FETCH_ARGS = {
    "indicators": [INDICATOR],
    "geography_type": GEOGRAPHY_TYPE,
    "geographies": [GEOGRAPHY],
    "start_year": 2020,
    "end_year": 2022,
    "page_size": 2,
    "timeout": 17,
    "retries": 0,
    "retry_delay": 0,
    "max_pages": 5,
    "max_records": 20,
}


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class RawJsonResponse(io.BytesIO):
    def __init__(self, document: str):
        super().__init__(document.encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def observation(
    *,
    upstream_id=1,
    period=2020,
    numeric_value=81.25,
    display_value="81.3",
    dim1_type="SEX",
    dim1="BTSX",
    dim2_type=None,
    dim2=None,
):
    return {
        "Id": upstream_id,
        "IndicatorCode": INDICATOR,
        "SpatialDimType": GEOGRAPHY_TYPE,
        "SpatialDim": GEOGRAPHY,
        "ParentLocationCode": "AMR",
        "ParentLocation": "Americas",
        "TimeDimType": "YEAR",
        "TimeDim": period,
        "Dim1Type": dim1_type,
        "Dim1": dim1,
        "Dim2Type": dim2_type,
        "Dim2": dim2,
        "Dim3Type": None,
        "Dim3": None,
        "DataSourceDimType": None,
        "DataSourceDim": None,
        "NumericValue": numeric_value,
        "Value": display_value,
        "Low": None,
        "High": 82.0,
        "Comments": None,
        "Date": "2022-08-10T16:16:12.68+02:00",
        "TimeDimensionValue": str(period),
        "TimeDimensionBegin": f"{period}-01-01T00:00:00+01:00",
        "TimeDimensionEnd": f"{period}-12-31T00:00:00+01:00",
    }


def envelope(rows, *, next_link=None, count=None):
    value = {"@odata.context": "https://ghoapi.azureedge.net/api/$metadata", "value": rows}
    if next_link is not None:
        value["@odata.nextLink"] = next_link
    if count is not None:
        value["@odata.count"] = count
    return value


def selection_url(*, skip=None):
    filter_expression = MODULE._filter_for(INDICATOR, GEOGRAPHY_TYPE, GEOGRAPHY, 2020, 2022)
    url = MODULE._initial_url(INDICATOR, filter_expression, 2)
    if skip is None:
        return url
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query.append(("$skip", str(skip)))
    return urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(query)))


def run_fetch(documents, **changes):
    kwargs = {**FETCH_ARGS, **changes}
    responses = [JsonResponse(document) for document in documents]
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
        records = MODULE.fetch_who_gho_odata(**kwargs)
    return records, urlopen


class FetchWhoGhoODataTests(unittest.TestCase):
    def test_paginates_with_exact_filters_and_normalizes_values(self):
        records, urlopen = run_fetch(
            [
                envelope(
                    [observation(upstream_id=1, period=2020), observation(upstream_id=2, period=2021)],
                    next_link=selection_url(skip=2),
                    count=3,
                ),
                envelope(
                    [observation(upstream_id=3, period=2022, numeric_value=None, display_value=None)],
                    count=3,
                ),
            ]
        )

        self.assertEqual([record["period"] for record in records], [2020, 2021, 2022])
        self.assertIsNone(records[2]["numeric_value"])
        self.assertIsNone(records[2]["display_value"])
        self.assertEqual(records[0]["dimensions"]["dim1"], {"type": "SEX", "code": "BTSX"})
        self.assertEqual(records[0]["geography_code"], "USA")
        self.assertEqual(records[0]["source"], MODULE.SOURCE)
        self.assertTrue(records[0]["fetched_at"].endswith("+00:00"))
        self.assertEqual(records[0]["source_url"], selection_url())
        self.assertEqual(urlopen.call_count, 2)
        first_request = urlopen.call_args_list[0].args[0]
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(first_request.full_url).query)
        self.assertEqual(query["$filter"], [MODULE._filter_for(INDICATOR, GEOGRAPHY_TYPE, GEOGRAPHY, 2020, 2022)])
        self.assertEqual(query["$top"], ["2"])
        self.assertEqual(query["$count"], ["true"])
        self.assertEqual(urlopen.call_args_list[0].kwargs, {"timeout": 17})

    def test_identity_is_stable_and_dimension_distinct(self):
        base = observation()
        same_identity_changed_measure = {**base, "NumericValue": 99.0, "Value": "99"}
        another_dimension = {**base, "Dim1": "MLE"}
        common = {
            "fetched_at": "2026-09-17T12:00:00+00:00",
            "source_url": selection_url(),
            "indicator": INDICATOR,
            "geography_type": GEOGRAPHY_TYPE,
            "geography": GEOGRAPHY,
            "start_year": 2020,
            "end_year": 2022,
        }
        first = MODULE.normalize_observation(base, **common)
        changed_measure = MODULE.normalize_observation(same_identity_changed_measure, **common)
        changed_dimension = MODULE.normalize_observation(another_dimension, **common)

        self.assertEqual(first["id"], changed_measure["id"])
        self.assertNotEqual(first["id"], changed_dimension["id"])
        self.assertEqual(first["id"], first["observation_identity"])
        self.assertEqual(len(first["id"]), 64)

    def test_retries_retryable_http_errors_with_bounded_backoff(self):
        error = urllib.error.HTTPError(selection_url(), 503, "unavailable", {}, None)
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[error, JsonResponse(envelope([observation()], count=1))],
        ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
            records = MODULE.fetch_who_gho_odata(
                **{**FETCH_ARGS, "retries": 1, "retry_delay": 0.25}
            )
        self.assertEqual(len(records), 1)
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0.25)

    def test_non_retryable_and_exhausted_http_errors_propagate(self):
        for code, retries, expected_calls in ((404, 2, 1), (503, 1, 2)):
            with self.subTest(code=code):
                errors = [
                    urllib.error.HTTPError(selection_url(), code, "failed", {}, None)
                    for _ in range(expected_calls)
                ]
                with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=errors) as urlopen, mock.patch.object(MODULE.time, "sleep"):
                    with self.assertRaises(urllib.error.HTTPError):
                        MODULE.fetch_who_gho_odata(
                            **{**FETCH_ARGS, "retries": retries, "retry_delay": 0}
                        )
                self.assertEqual(urlopen.call_count, expected_calls)

    def test_malformed_envelopes_fail(self):
        cases = (
            ([], "envelope"),
            ({}, "value"),
            ({"value": {}}, "value"),
            ({"value": [], "@odata.nextLink": 3}, "nextLink"),
            ({"value": [], "@odata.count": "1"}, "count"),
        )
        for document, message in cases:
            with self.subTest(document=document):
                with self.assertRaisesRegex(MODULE.WhoGhoError, message):
                    run_fetch([document])

    def test_rows_must_stay_within_every_filter(self):
        changes = (
            ({"IndicatorCode": "OTHER"}, "indicator"),
            ({"SpatialDim": "CAN"}, "geography"),
            ({"TimeDim": 2019}, "outside"),
            ({"TimeDimType": "MONTH"}, "period"),
        )
        for replacement, message in changes:
            with self.subTest(replacement=replacement):
                row = {**observation(), **replacement}
                with self.assertRaisesRegex(MODULE.WhoGhoError, message):
                    run_fetch([envelope([row], count=1)])

    def test_next_link_must_retain_endpoint_filter_page_size_and_cursor(self):
        base = selection_url(skip=1)
        parsed = urllib.parse.urlsplit(base)
        query = urllib.parse.parse_qs(parsed.query)
        candidates = []
        for field, value in (
            ("$filter", "SpatialDim eq 'CAN'"),
            ("$top", "99"),
        ):
            changed = {key: values[0] for key, values in query.items()}
            changed[field] = value
            candidates.append(urllib.parse.urlunsplit(parsed._replace(query=urllib.parse.urlencode(changed))))
        changed_host = urllib.parse.urlunsplit(parsed._replace(netloc="example.invalid"))
        no_cursor = MODULE._initial_url(
            INDICATOR,
            MODULE._filter_for(INDICATOR, GEOGRAPHY_TYPE, GEOGRAPHY, 2020, 2022),
            2,
        )
        candidates.extend((changed_host, no_cursor))
        for candidate in candidates:
            with self.subTest(candidate=candidate):
                with self.assertRaises(MODULE.WhoGhoError):
                    run_fetch([envelope([observation()], next_link=candidate, count=2)])

    def test_page_and_record_budgets_fail_before_partial_results_escape(self):
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "max_records"):
            run_fetch([envelope([observation()], count=3)], max_records=2)
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "max_pages"):
            run_fetch(
                [envelope([observation()], next_link=selection_url(skip=1), count=2)],
                max_pages=1,
            )

    def test_incomplete_and_nonprogressing_pagination_fail(self):
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "ended"):
            run_fetch([envelope([observation()], count=2)])
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "empty intermediate"):
            run_fetch([envelope([], next_link=selection_url(skip=1), count=1)])
        repeated = selection_url(skip=1)
        with self.assertRaisesRegex(MODULE.IncompleteCoverageError, "did not advance"):
            run_fetch(
                [
                    envelope([observation()], next_link=repeated, count=3),
                    envelope(
                        [observation(upstream_id=2, period=2021)],
                        next_link=repeated,
                        count=3,
                    ),
                ]
            )

    def test_duplicate_identity_fails_even_when_upstream_ids_differ(self):
        with self.assertRaisesRegex(MODULE.WhoGhoError, "duplicate observation identity"):
            run_fetch(
                [
                    envelope(
                        [observation(upstream_id=1), observation(upstream_id=2)],
                        count=2,
                    )
                ]
            )

    def test_main_never_emits_partial_records_on_later_page_failure(self):
        documents = [
            JsonResponse(
                envelope([observation()], next_link=selection_url(skip=1), count=2)
            ),
            JsonResponse({"not_value": []}),
        ]
        stdout = io.StringIO()
        argv = [
            "--indicator", INDICATOR,
            "--geography-type", GEOGRAPHY_TYPE,
            "--geography", GEOGRAPHY,
            "--start-year", "2020",
            "--end-year", "2022",
            "--page-size", "2",
            "--retries", "0",
        ]
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=documents), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(SystemExit, "2"):
                MODULE.main(argv)
        self.assertEqual(stdout.getvalue(), "")

    def test_main_rejects_nonfinite_numbers_on_later_page_without_stdout(self):
        argv = [
            "--indicator", INDICATOR,
            "--geography-type", GEOGRAPHY_TYPE,
            "--geography", GEOGRAPHY,
            "--start-year", "2020",
            "--end-year", "2022",
            "--page-size", "2",
            "--retries", "0",
        ]
        malformed_numbers = ("NaN", "Infinity", "-Infinity", "1e400")
        for malformed_number in malformed_numbers:
            with self.subTest(malformed_number=malformed_number):
                later_page = json.dumps(
                    envelope(
                        [observation(upstream_id=2, period=2021, numeric_value="MARKER")],
                        count=2,
                    )
                ).replace('"MARKER"', malformed_number)
                stdout = io.StringIO()
                stderr = io.StringIO()
                responses = [
                    JsonResponse(
                        envelope(
                            [observation()],
                            next_link=selection_url(skip=1),
                            count=2,
                        )
                    ),
                    RawJsonResponse(later_page),
                ]
                with mock.patch.object(
                    MODULE.urllib.request, "urlopen", side_effect=responses
                ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                    with self.assertRaisesRegex(SystemExit, "2"):
                        MODULE.main(argv)
                self.assertEqual(stdout.getvalue(), "")
                self.assertIn("non-finite", stderr.getvalue())

    def test_serialization_refuses_nonfinite_values_before_output(self):
        record = MODULE.normalize_observation(
            observation(),
            fetched_at="2026-09-17T12:00:00+00:00",
            source_url=selection_url(),
            indicator=INDICATOR,
            geography_type=GEOGRAPHY_TYPE,
            geography=GEOGRAPHY,
            start_year=2020,
            end_year=2022,
        )
        record["numeric_value"] = float("nan")
        with self.assertRaises(ValueError):
            MODULE._serialized_lines([record])

    def test_main_serializes_only_after_complete_success(self):
        stdout = io.StringIO()
        with mock.patch.object(
            MODULE,
            "fetch_who_gho_odata",
            return_value=[MODULE.normalize_observation(
                observation(),
                fetched_at="2026-09-17T12:00:00+00:00",
                source_url=selection_url(),
                indicator=INDICATOR,
                geography_type=GEOGRAPHY_TYPE,
                geography=GEOGRAPHY,
                start_year=2020,
                end_year=2022,
            )],
        ), contextlib.redirect_stdout(stdout):
            status = MODULE.main([
                "--indicator", INDICATOR,
                "--geography-type", GEOGRAPHY_TYPE,
                "--geography", GEOGRAPHY,
                "--start-year", "2020",
                "--end-year", "2022",
            ])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(stdout.getvalue())["source"], MODULE.SOURCE)

    def test_smoke_test_is_read_only_and_requires_records(self):
        argv = [
            "--indicator", INDICATOR,
            "--geography-type", GEOGRAPHY_TYPE,
            "--geography", GEOGRAPHY,
            "--start-year", "2020",
            "--end-year", "2022",
            "--smoke-test",
        ]
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(MODULE, "fetch_who_gho_odata", return_value=[{
            "indicator_code": INDICATOR,
            "geography_code": GEOGRAPHY,
            "period": 2020,
        }]), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            self.assertEqual(MODULE.main(argv), 0)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(json.loads(stderr.getvalue())["status"], "ok")

        with mock.patch.object(MODULE, "fetch_who_gho_odata", return_value=[]), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(SystemExit, "2"):
                MODULE.main(argv)

    def test_invalid_scope_and_limits_fail_before_http(self):
        changes = (
            ({"indicators": ["OTHER"]}, "indicators outside"),
            ({"geographies": ["CAN"]}, "geographies outside"),
            ({"start_year": 2019}, "start_year"),
            ({"end_year": 2023}, "end_year"),
            ({"page_size": 0}, "page_size"),
            ({"timeout": 0}, "timeout"),
            ({"retries": 6}, "retries"),
            ({"max_pages": 0}, "max_pages"),
            ({"max_records": 0}, "max_records"),
        )
        for replacement, message in changes:
            with self.subTest(replacement=replacement), mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.fetch_who_gho_odata(**{**FETCH_ARGS, **replacement})
                urlopen.assert_not_called()

    def test_source_configuration_is_explicit_bounded_daily_and_disabled(self):
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["sink"], "local")
        self.assertEqual(len(config["schedule"].split()), 5)
        parsed = MODULE.build_parser().parse_args(config["args"])
        self.assertEqual(parsed.indicators, [INDICATOR])
        self.assertEqual(parsed.geography_type, GEOGRAPHY_TYPE)
        self.assertEqual(parsed.geographies, [GEOGRAPHY])
        self.assertEqual((parsed.start_year, parsed.end_year), (2020, 2022))
        self.assertEqual((parsed.page_size, parsed.timeout, parsed.retries), (100, 30, 2))
        self.assertEqual((parsed.max_pages, parsed.max_records), (10, 1000))


if __name__ == "__main__":
    unittest.main()
