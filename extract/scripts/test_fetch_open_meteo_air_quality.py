import importlib.util
import io
import json
import pathlib
import re
import urllib.error
import urllib.parse
from datetime import date
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_open_meteo_air_quality.py")
SPEC = importlib.util.spec_from_file_location("fetch_open_meteo_air_quality", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "open_meteo_air_quality.yml"

FETCHED_AT = "2026-11-08T08:35:00+00:00"
TODAY = date(2026, 11, 8)
VARIABLES = ("pm2_5", "ozone")
LOCATIONS = (
    MODULE.Location("alpha", "Alpha", 40.0, -75.0),
    MODULE.Location("beta", "Beta", 51.5, -0.1),
)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def air_quality_document(
    *,
    latitude=40.01,
    longitude=-75.02,
    times=None,
    pm2_5=None,
    ozone=None,
    units=None,
):
    return {
        "latitude": latitude,
        "longitude": longitude,
        "generationtime_ms": 0.04,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "elevation": 120.0,
        "hourly_units": units
        if units is not None
        else {"time": "iso8601", "pm2_5": "μg/m³", "ozone": "μg/m³"},
        "hourly": {
            "time": times if times is not None else ["2026-11-08T07:00", "2026-11-08T08:00"],
            "pm2_5": pm2_5 if pm2_5 is not None else [18.5, 19.0],
            "ozone": ozone if ozone is not None else [0.0, None],
        },
    }


def fetch(*, locations=(LOCATIONS[0],), variables=VARIABLES, **overrides):
    arguments = {
        "locations": locations,
        "variables": variables,
        "history_days": 3,
        "batch_size": 10,
        "timeout": 17,
        "request_delay": 0,
        "fetched_at": FETCHED_AT,
        "today": TODAY,
    }
    arguments.update(overrides)
    return list(MODULE.fetch_air_quality(**arguments))


def test_normalizes_metadata_units_values_and_request():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(air_quality_document())
    ) as urlopen:
        records = fetch()

    assert len(records) == 2
    first = records[0]
    assert first == {
        "id": first["id"],
        "source": "open_meteo_air_quality",
        "fetched_at": FETCHED_AT,
        "forecast_issued_at": None,
        "forecast_valid_at": "2026-11-08T07:00:00+00:00",
        "location_id": "alpha",
        "location_name": "Alpha",
        "requested_latitude": 40.0,
        "requested_longitude": -75.0,
        "latitude": 40.01,
        "longitude": -75.02,
        "elevation": 120.0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "utc_offset_seconds": 0,
        "provider": {
            "name": "Open-Meteo",
            "url": "https://open-meteo.com/",
            "upstream": "Copernicus Atmosphere Monitoring Service (CAMS)",
        },
        "units": {"pm2_5": "μg/m³", "ozone": "μg/m³"},
        "air_quality": {"pm2_5": 18.5, "ozone": 0.0},
    }
    assert re.fullmatch(r"[0-9a-f]{64}", first["id"])
    assert records[1]["air_quality"] == {"pm2_5": 19.0, "ozone": None}
    assert records[1]["forecast_valid_at"] == "2026-11-08T08:00:00+00:00"
    assert records[1]["id"] != first["id"]

    request = urlopen.call_args.args[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert query == {
        "latitude": ["40.0"],
        "longitude": ["-75.0"],
        "hourly": ["pm2_5,ozone"],
        "start_date": ["2026-11-06"],
        "end_date": ["2026-11-08"],
        "timezone": ["UTC"],
    }
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert urlopen.call_args.kwargs == {"timeout": 17.0}


def test_single_day_window_uses_same_start_and_end_date():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(air_quality_document())
    ) as urlopen:
        fetch(history_days=1)

    request = urlopen.call_args.args[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert query["start_date"] == ["2026-11-08"]
    assert query["end_date"] == ["2026-11-08"]


def test_collection_time_is_not_fabricated_as_provider_issuance_time():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(air_quality_document())
    ):
        records = fetch()

    assert {record["fetched_at"] for record in records} == {FETCHED_AT}
    assert {record["forecast_issued_at"] for record in records} == {None}


def test_ids_are_stable_within_a_revision_and_distinct_across_collections():
    document = air_quality_document(times=["2026-11-08T07:00"], pm2_5=[18.5], ozone=[0.0])
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document)
    ):
        baseline = fetch()
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document)
    ):
        replay = fetch()
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document)
    ):
        revised = fetch(fetched_at="2026-11-09T08:35:00+00:00")

    assert [record["id"] for record in baseline] == [record["id"] for record in replay]
    assert revised[0]["id"] != baseline[0]["id"]
    assert revised[0]["fetched_at"] == "2026-11-09T08:35:00+00:00"
    assert revised[0]["forecast_issued_at"] is None


def test_one_collection_timestamp_is_generated_for_all_batches():
    documents = [
        air_quality_document(latitude=40.01, longitude=-75.02),
        air_quality_document(latitude=51.51, longitude=-0.12),
    ]
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=[response(documents[0]), response(documents[1])]
    ), mock.patch.object(MODULE.time, "sleep") as sleep:
        records = fetch(locations=LOCATIONS, batch_size=1, request_delay=0.25)

    assert len(records) == 4
    assert {record["fetched_at"] for record in records} == {FETCHED_AT}
    sleep.assert_called_once_with(0.25)


def test_multiple_locations_share_one_batched_request_and_preserve_response_order():
    documents = [
        air_quality_document(latitude=40.01, longitude=-75.02),
        air_quality_document(latitude=51.51, longitude=-0.12),
    ]
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(documents)
    ) as urlopen:
        records = fetch(locations=LOCATIONS)

    assert urlopen.call_count == 1
    assert [record["location_id"] for record in records] == ["alpha", "alpha", "beta", "beta"]
    request = urlopen.call_args.args[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert query["latitude"] == ["40.0,51.5"]
    assert query["longitude"] == ["-75.0,-0.1"]


def test_empty_hourly_response_is_successful():
    document = air_quality_document(times=[], pm2_5=[], ozone=[])
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        assert fetch() == []


@pytest.mark.parametrize("unit", [None, "", "   ", 3])
def test_rejects_missing_null_empty_or_non_string_unit_for_every_configured_variable(unit):
    units = {"time": "iso8601", "pm2_5": "μg/m³", "ozone": "μg/m³"}
    units["pm2_5"] = unit
    document = air_quality_document(units=units)
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(ValueError, match="hourly_units\\['pm2_5'\\]"):
            fetch()


def test_accepts_complete_non_empty_units_for_all_configured_variables():
    document = air_quality_document(
        units={"time": "iso8601", "pm2_5": "μg/m³", "ozone": "μg/m³"}
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        records = fetch()

    assert records[0]["units"] == {"pm2_5": "μg/m³", "ozone": "μg/m³"}


@pytest.mark.parametrize(
    ("mutate", "exception", "message"),
    [
        (
            lambda document: document["hourly"].__setitem__("pm2_5", [18.5]),
            ValueError,
            "hourly.pm2_5 has 1 values",
        ),
        (
            lambda document: document["hourly"].__setitem__("pm2_5", [18.5, "high"]),
            TypeError,
            "hourly.pm2_5\\[1\\] must be a finite number",
        ),
        (
            lambda document: document["hourly"].__setitem__("ozone", None),
            TypeError,
            "hourly.ozone must be a list",
        ),
        (
            lambda document: document["hourly"].__setitem__("time", ["2026-11-08T07:00", "2026-11-08T07:00"]),
            ValueError,
            "duplicate forecast-valid times",
        ),
        (
            lambda document: document["hourly"].__setitem__("time", ["2026-11-08T07:00", "not-a-time"]),
            ValueError,
            "hourly.time\\[1\\] is not a valid ISO date/time",
        ),
        (
            lambda document: document.update({"utc_offset_seconds": 3600}),
            ValueError,
            "not UTC despite timezone=UTC request",
        ),
        (
            lambda document: document.update({"timezone": "Europe/London"}),
            ValueError,
            "timezone must be GMT",
        ),
        (
            lambda document: document.update({"utc_offset_seconds": None}),
            TypeError,
            "utc_offset_seconds must be an integer",
        ),
        (
            lambda document: document.update({"latitude": 91.0}),
            ValueError,
            "latitude is outside valid bounds",
        ),
        (
            lambda document: document.update({"longitude": -181.0}),
            ValueError,
            "longitude is outside valid bounds",
        ),
        (
            lambda document: document.update({"elevation": "high"}),
            TypeError,
            "elevation must be a finite number",
        ),
        (
            lambda document: document.update({"hourly": None}),
            TypeError,
            "missing hourly object",
        ),
        (
            lambda document: document.update({"hourly_units": None}),
            TypeError,
            "missing hourly_units object",
        ),
    ],
)
def test_rejects_malformed_series_and_metadata_without_emitting(mutate, exception, message):
    document = air_quality_document()
    mutate(document)
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(exception, match=message):
            fetch()


@pytest.mark.parametrize(
    ("document", "locations", "exception", "message"),
    [
        ({"error": True, "reason": "no location"}, (LOCATIONS[0],), MODULE.OpenMeteoAPIError, "no location"),
        ({"error": True}, (LOCATIONS[0],), MODULE.OpenMeteoAPIError, "unknown error"),
        (["not-a-dict", {}], LOCATIONS, TypeError, "location response 0 must be an object"),
        (
            [air_quality_document(), air_quality_document(latitude=51.51, longitude=-0.12)],
            (LOCATIONS[0],),
            ValueError,
            "expected 1",
        ),
        (
            [
                air_quality_document(),
                {"error": True, "reason": "bad coordinates"},
            ],
            LOCATIONS,
            MODULE.OpenMeteoAPIError,
            "bad coordinates",
        ),
        (
            air_quality_document(),
            LOCATIONS,
            TypeError,
            "batch response must be a JSON list",
        ),
    ],
)
def test_rejects_malformed_batch_and_api_error_documents(document, locations, exception, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(exception, match=message):
            fetch(locations=locations)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"history_days": 0}, "history_days must be between 1 and 7"),
        ({"history_days": 8}, "history_days must be between 1 and 7"),
        ({"history_days": True}, "history_days must be an integer"),
        ({"history_days": 2.5}, "history_days must be an integer"),
        ({"batch_size": 0}, "batch_size must be between 1 and 10"),
        ({"batch_size": 11}, "batch_size must be between 1 and 10"),
        ({"batch_size": True}, "batch_size must be an integer"),
        ({"timeout": 0}, "timeout must be positive"),
        ({"request_delay": -0.1}, "request_delay must be a non-negative number"),
        ({"request_delay": float("inf")}, "request_delay must be a non-negative number"),
        ({"locations": ()}, "at least one location is required"),
        ({"locations": "alpha"}, "locations must be a sequence of Location objects"),
        ({"variables": ()}, "at least one hourly variable is required"),
        (
            {
                "locations": (
                    LOCATIONS[0],
                    MODULE.Location("alpha", "Duplicate", 1.0, 1.0),
                )
            },
            "duplicate location id 'alpha'",
        ),
        (
            {
                "locations": (
                    MODULE.Location("Upper", "Bad Id", 1.0, 1.0),
                )
            },
            "invalid id 'Upper'",
        ),
        (
            {
                "locations": (
                    MODULE.Location("pole", "Pole", 91.0, 1.0),
                )
            },
            "latitude must be between -90 and 90",
        ),
        ({"variables": ["pm2_5", "pm2_5"]}, "must not contain duplicates"),
        ({"variables": ["pm2_5", "visibility"]}, "not allowlisted: visibility"),
    ],
)
def test_rejects_invalid_bounds_before_http(overrides, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises((TypeError, ValueError), match=message):
            fetch(**overrides)

    urlopen.assert_not_called()


def test_http_errors_propagate():
    error = urllib.error.HTTPError(MODULE.API_URL, 503, "unavailable", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError) as caught:
            fetch()

    assert caught.value.code == 503


def test_main_emits_ndjson_for_explicit_location_and_variable(capsys):
    document = air_quality_document(
        times=["2026-11-08T07:00"], pm2_5=[18.5], ozone=[0.0]
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        MODULE.main(
            [
                "--location",
                "alpha,Alpha,40.0,-75.0",
                "--variable",
                "pm2_5",
                "--history-days",
                "1",
                "--batch-size",
                "10",
                "--timeout",
                "17",
                "--request-delay",
                "0",
            ]
        )

    emitted = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(emitted) == 1
    assert emitted[0]["source"] == "open_meteo_air_quality"
    assert emitted[0]["location_id"] == "alpha"
    assert emitted[0]["air_quality"] == {"pm2_5": 18.5}
    assert emitted[0]["forecast_valid_at"] == "2026-11-08T07:00:00+00:00"


def test_source_configuration_documents_daily_cadence_licence_and_attribution():
    config = CONFIG.read_text(encoding="utf-8")
    assert re.search(r'(?m)^schedule:\s*"35 8 \* \* \*"\s*# UTC', config)
    assert re.search(r"(?m)^enabled:\s*true\s*$", config)
    assert "CC BY 4.0" in config
    assert re.search(r"(?m)^attribution_required:\s*true\s*$", config)
    assert "10,000 calls/day" in config
