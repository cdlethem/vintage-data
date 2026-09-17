import importlib.util
import io
import json
import pathlib
import re
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_noaa_coops_observations.py")
SPEC = importlib.util.spec_from_file_location("fetch_noaa_coops_observations", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "noaa_coops_observations.yml"


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def water_level_document(rows=None):
    return {
        "metadata": {
            "id": "9414290",
            "name": "San Francisco",
            "lat": "37.8063",
            "lon": "-122.4659",
        },
        "data": rows
        if rows is not None
        else [
            {
                "t": "2026-09-16 12:00",
                "v": "1.234",
                "s": "0.041",
                "f": "0,0,0,0",
                "q": "v",
            }
        ],
    }


def fetch(**overrides):
    arguments = {
        "station": "9414290",
        "product": "water_level",
        "begin_date": "2026-09-16T00:00:00Z",
        "end_date": "2026-09-17T00:00:00Z",
        "datum": "MLLW",
        "units": "metric",
        "time_zone": "gmt",
        "timeout": 17,
        "retries": 0,
        "retry_delay": 0,
    }
    arguments.update(overrides)
    return list(MODULE.fetch_observations(**arguments))


def test_normalizes_water_level_and_preserves_metadata_quality_fields():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(water_level_document())) as urlopen:
        records = fetch()

    assert len(records) == 1
    record = records[0]
    assert record["source"] == "noaa_coops_observations"
    assert record["station_id"] == "9414290"
    assert record["station"] == {
        "id": "9414290",
        "name": "San Francisco",
        "lat": 37.8063,
        "lon": -122.4659,
    }
    assert record["station_name"] == "San Francisco"
    assert record["latitude"] == 37.8063
    assert record["longitude"] == -122.4659
    assert record["timestamp"] == "2026-09-16T12:00:00+00:00"
    assert record["value"] == 1.234
    assert record["standard_error"] == 0.041
    assert record["flags"] == "0,0,0,0"
    assert record["quality_code"] == "v"
    assert record["units"] == "metric"
    assert record["datum"] == "MLLW"
    assert record["product"] == "water_level"
    assert re.fullmatch(r"[0-9a-f]{64}", record["id"])
    assert datetime.fromisoformat(record["fetched_at"]).tzinfo == timezone.utc

    request = urlopen.call_args.args[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert query == {
        "application": ["vintage_data"],
        "begin_date": ["20260916 00:00"],
        "datum": ["MLLW"],
        "end_date": ["20260917 00:00"],
        "format": ["json"],
        "product": ["water_level"],
        "station": ["9414290"],
        "time_zone": ["gmt"],
        "units": ["metric"],
    }
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert request.get_header("Accept") == "application/json"
    assert urlopen.call_args.kwargs == {"timeout": 17}


def test_normalizes_predictions_and_missing_optional_values():
    document = {
        "predictions": [
            {"t": "2026-09-16 12:00", "v": "2.50"},
            {"t": "2026-09-16 12:06", "v": ""},
        ]
    }
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        records = fetch(product="predictions")

    assert [record["value"] for record in records] == [2.5, None]
    assert records[0]["station"] == {"id": "9414290"}
    assert records[0]["standard_error"] is None
    assert records[0]["flags"] is None
    assert records[0]["quality_code"] is None
    assert records[0]["product"] == "predictions"


def test_empty_data_is_a_successful_empty_result():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(water_level_document([]))):
        assert fetch() == []


@pytest.mark.parametrize("datum", ["MHHW", "MLW"])
def test_accepts_documented_datums(datum):
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(water_level_document([]))) as urlopen:
        assert fetch(datum=datum) == []

    query = urllib.parse.parse_qs(urllib.parse.urlsplit(urlopen.call_args.args[0].full_url).query)
    assert query["datum"] == [datum]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"product": "air_temperature"}, "unsupported product"),
        ({"datum": "BAD"}, "datum 'BAD' is not valid for product 'water_level'"),
        ({"product": "predictions", "datum": "BAD"}, "datum 'BAD' is not valid for product 'predictions'"),
    ],
)
def test_rejects_invalid_product_datum_combinations_before_http(overrides, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match=re.escape(message)):
            fetch(**overrides)
    urlopen.assert_not_called()


@pytest.mark.parametrize("time_zone", ["lst", "lst_ldt", "LST"])
def test_rejects_local_time_zones_before_http(time_zone):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match="only 'gmt' is supported"):
            fetch(time_zone=time_zone)
    urlopen.assert_not_called()


def test_utc_normalization_and_distinct_ids_across_fall_back_interval():
    document = water_level_document(
        [
            {"t": "2025-11-02 08:30", "v": "1", "s": "0.1", "f": "0", "q": "v"},
            {"t": "2025-11-02 09:30", "v": "2", "s": "0.1", "f": "0", "q": "v"},
        ]
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        records = fetch(
            begin_date="2025-11-02T08:00:00+00:00",
            end_date="2025-11-02T10:00:00+00:00",
        )

    assert [record["timestamp"] for record in records] == [
        "2025-11-02T08:30:00+00:00",
        "2025-11-02T09:30:00+00:00",
    ]
    assert records[0]["id"] != records[1]["id"]


def test_stable_ids_ignore_fetch_time_and_mutable_measurement_fields():
    first = water_level_document()
    second = water_level_document(
        [{"t": "2026-09-16 12:00", "v": "9.999", "s": "", "f": "1,0", "q": "p"}]
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=[response(first), response(second)]):
        first_record = fetch()[0]
        second_record = fetch()[0]

    assert first_record["id"] == second_record["id"]


def test_stable_ids_separate_datums_for_the_same_station_product_and_time():
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=[response(water_level_document()), response(water_level_document())],
    ):
        mllw_record = fetch(datum="MLLW")[0]
        mhhw_record = fetch(datum="MHHW")[0]

    assert mllw_record["id"] != mhhw_record["id"]


@pytest.mark.parametrize(
    ("document", "exception", "message"),
    [
        ([], TypeError, "response must be a JSON object"),
        ({}, TypeError, "missing 'data' list"),
        ({"data": {}}, TypeError, "missing 'data' list"),
        ({"data": ["bad"]}, TypeError, "row 0 must be an object"),
        ({"data": [{"v": "1.0"}]}, ValueError, "missing timestamp"),
        ({"data": [{"t": "not-a-date", "v": "1.0"}]}, ValueError, "not a valid date/time"),
        ({"data": [{"t": "2026-09-16 12:00", "v": "bad"}]}, ValueError, "is not numeric"),
        ({"metadata": [], "data": []}, TypeError, "metadata must be an object"),
        ({"metadata": {"id": "123"}, "data": []}, ValueError, "does not match requested station"),
    ],
)
def test_rejects_malformed_responses(document, exception, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(exception, match=re.escape(message)):
            fetch()


def test_reports_station_specific_api_error():
    document = {"error": {"message": "No data was found for this station and datum."}}
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(
            MODULE.NOAAAPIError,
            match="station 9414290 product water_level datum MHHW: No data was found",
        ):
            fetch(datum="MHHW")


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"begin_date": "2026-09-17", "end_date": "2026-09-17"}, "begin_date must be earlier"),
        ({"begin_date": "2026-08-01", "end_date": "2026-09-17"}, "must not exceed 31 days"),
        ({"timeout": 0}, "timeout must be positive"),
        ({"retries": -1}, "retries must be a non-negative integer"),
        ({"retry_delay": -1}, "retry_delay must be non-negative"),
    ],
)
def test_rejects_invalid_bounds_before_http(overrides, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match=message):
            fetch(**overrides)
    urlopen.assert_not_called()


def test_retries_transient_http_failure_with_bounded_backoff():
    error = urllib.error.HTTPError(MODULE.API_URL, 503, "unavailable", {}, None)
    with (
        mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[error, response(water_level_document([]))],
        ) as urlopen,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        assert fetch(retries=1, retry_delay=0.25) == []

    assert urlopen.call_count == 2
    sleep.assert_called_once_with(0.25)


def test_does_not_retry_permanent_http_failure():
    error = urllib.error.HTTPError(MODULE.API_URL, 400, "bad request", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen:
        with pytest.raises(urllib.error.HTTPError):
            fetch(retries=2)
    urlopen.assert_called_once()


def test_main_paces_products_and_emits_ndjson(capsys):
    documents = [water_level_document(), {"predictions": [{"t": "2026-09-16 12:00", "v": "1.8"}]}]
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=map(response, documents)),
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        MODULE.main(
            [
                "--station",
                "9414290",
                "--begin-date",
                "2026-09-16T00:00:00Z",
                "--end-date",
                "2026-09-17T00:00:00Z",
                "--request-delay",
                "0.5",
            ]
        )

    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [record["product"] for record in output] == ["water_level", "predictions"]
    sleep.assert_called_once_with(0.5)


def test_source_configuration_stays_disabled():
    config = CONFIG.read_text(encoding="utf-8")
    assert re.search(r"(?m)^enabled:\s*false\s*$", config)
