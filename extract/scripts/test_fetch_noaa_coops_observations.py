import importlib.util
import json
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest


MODULE_PATH = Path(__file__).with_name("fetch_noaa_coops_observations.py")
SPEC = importlib.util.spec_from_file_location("fetch_noaa_coops_observations", MODULE_PATH)
coops = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(coops)


WATER_LEVEL_RESPONSE = {
    "metadata": {
        "id": "9414290",
        "name": "San Francisco",
        "lat": "37.8063",
        "lon": "-122.4659",
    },
    "data": [
        {"t": "2026-09-01 00:00", "v": "1.234", "s": "0.012", "f": "0,0,0,0", "q": "p"},
        {"t": "2026-09-01 00:06", "v": "", "s": "", "f": "1,0,0,0", "q": "v"},
    ],
}


def test_normalize_response_preserves_datum_units_quality_and_missing_values():
    records = coops.normalize_response(
        WATER_LEVEL_RESPONSE,
        station="9414290",
        product="water_level",
        datum="MLLW",
        units="metric",
        time_zone="gmt",
    )

    assert records == [
        {
            "id": "684486ab9812c8afb69f3c772c8bc3f431b15c6f03c35b9424607dab0155af20",
            "source": "noaa_coops_observations",
            "station": "9414290",
            "station_metadata": WATER_LEVEL_RESPONSE["metadata"],
            "timestamp": "2026-09-01T00:00:00Z",
            "product": "water_level",
            "datum": "MLLW",
            "units": "metric",
            "time_zone": "gmt",
            "value": 1.234,
            "standard_error": 0.012,
            "flags": "0,0,0,0",
            "quality_code": "p",
        },
        {
            "id": "1a8ada5a29499e822ca4506c4ac21f016b85349abc06bb8a698283737e2acb90",
            "source": "noaa_coops_observations",
            "station": "9414290",
            "station_metadata": WATER_LEVEL_RESPONSE["metadata"],
            "timestamp": "2026-09-01T00:06:00Z",
            "product": "water_level",
            "datum": "MLLW",
            "units": "metric",
            "time_zone": "gmt",
            "value": None,
            "standard_error": None,
            "flags": "1,0,0,0",
            "quality_code": "v",
        },
    ]


def test_stable_ids_change_when_measurement_interpretation_changes():
    record = coops.normalize_response(
        WATER_LEVEL_RESPONSE, "9414290", "water_level", "MLLW", "metric", "gmt"
    )[0]
    same_record = coops.normalize_response(
        WATER_LEVEL_RESPONSE, "9414290", "water_level", "MLLW", "metric", "gmt"
    )[0]
    other_datum = coops.normalize_response(
        WATER_LEVEL_RESPONSE, "9414290", "water_level", "MSL", "metric", "gmt"
    )[0]

    assert record["id"] == same_record["id"]
    assert record["id"] != other_datum["id"]


def test_predictions_normalize_type_and_all_rows_from_bounded_response():
    payload = {
        "metadata": {"id": "9414290", "name": "San Francisco"},
        "predictions": [
            {"t": "2026-09-01 00:00", "v": "0.5", "type": "H"},
            {"t": "2026-09-01 06:00", "v": "-0.2", "type": "L"},
        ],
    }

    records = coops.normalize_response(payload, "9414290", "predictions", "MLLW", "english", "lst")

    assert [record["timestamp"] for record in records] == ["2026-09-01T00:00:00", "2026-09-01T06:00:00"]
    assert [record["prediction_type"] for record in records] == ["H", "L"]
    assert [record["value"] for record in records] == [0.5, -0.2]


def test_empty_data_is_a_valid_bounded_result():
    payload = {"metadata": {"id": "9414290", "name": "San Francisco"}, "data": []}
    assert coops.normalize_response(payload, "9414290", "water_level", "MLLW", "metric", "gmt") == []


@pytest.mark.parametrize(
    "payload, message",
    [
        ({"data": []}, "missing station metadata"),
        ({"metadata": {"id": "9414290"}, "data": {}}, "missing data array"),
        ({"metadata": {"id": "9414290"}, "data": [{"v": "1"}]}, "missing timestamp"),
        ({"metadata": {"id": "9414290"}, "data": [{"t": "bad", "v": "1"}]}, "invalid response timestamp"),
        ({"metadata": {"id": "9414290"}, "data": [{"t": "2026-09-01 00:00", "v": "bad"}]}, "invalid water level"),
        ({"error": {"message": "No data was found"}}, "CO-OPS API error"),
    ],
)
def test_malformed_responses_fail_clearly(payload, message):
    with pytest.raises(coops.NoaaCoopsError, match=message):
        coops.normalize_response(payload, "9414290", "water_level", "MLLW", "metric", "gmt")


def test_request_construction_encodes_all_interpretation_parameters():
    url = coops.build_request_url(
        station="9414290",
        begin_date="2026-09-01",
        end_date="2026-09-03",
        product="water_level",
        datum="navd",
        units="english",
        time_zone="lst_ldt",
    )
    query = parse_qs(urlparse(url).query)

    assert urlparse(url).scheme == "https"
    assert query == {
        "application": ["noaa_coops_observations"],
        "begin_date": ["20260901"],
        "datum": ["NAVD"],
        "end_date": ["20260903"],
        "format": ["json"],
        "product": ["water_level"],
        "station": ["9414290"],
        "time_zone": ["lst_ldt"],
        "units": ["english"],
    }


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"station": "941429"}, "seven-digit"),
        ({"station": "9414290", "product": "hourly_height"}, "unsupported product"),
        ({"station": "9414290", "datum": "bogus"}, "not valid"),
        ({"station": "9414290", "observation_date": "2026-09-01", "begin_date": "2026-09-01"}, "cannot be combined"),
        ({"station": "9414290", "begin_date": "2026-09-01"}, "supplied together"),
        ({"station": "9414290", "begin_date": "2026-09-02", "end_date": "2026-09-01"}, "must not precede"),
        ({"station": "9414290", "begin_date": "2026-01-01", "end_date": "2026-02-01"}, "limited to 31 days"),
    ],
)
def test_request_bounds_and_invalid_combinations_are_rejected(kwargs, message):
    with pytest.raises(coops.NoaaCoopsError, match=message):
        coops.build_request_url(**kwargs)


def test_default_request_uses_completed_day():
    begin, end = coops.resolve_date_range(today=date(2026, 9, 2))
    assert (begin.isoformat(), end.isoformat()) == ("2026-09-01", "2026-09-01")


def test_fetch_uses_mocked_http_and_normalizes_response():
    captured = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps(WATER_LEVEL_RESPONSE).encode("utf-8")

    def opener(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        return Response()

    records = coops.fetch_observations(
        station="9414290",
        observation_date="2026-09-01",
        timeout=12,
        opener=opener,
    )

    assert len(records) == 2
    assert captured["timeout"] == 12
    assert parse_qs(urlparse(captured["url"]).query)["begin_date"] == ["20260901"]
