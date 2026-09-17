import importlib.util
import io
import json
import pathlib
import re
import urllib.error
import urllib.parse
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_open_meteo_weather.py")
SPEC = importlib.util.spec_from_file_location("fetch_open_meteo_weather", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "open_meteo_weather.yml"

FETCHED_AT = "2026-09-17T12:34:56+00:00"
VARIABLES = ("temperature_2m", "precipitation")
LOCATIONS = (
    MODULE.Location("alpha", "Alpha", 40.0, -75.0),
    MODULE.Location("beta", "Beta", 51.5, -0.1),
)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def weather_document(
    *,
    latitude=40.01,
    longitude=-75.02,
    times=None,
    temperatures=None,
    precipitation=None,
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
        else {"time": "iso8601", "temperature_2m": "°C", "precipitation": "mm"},
        "hourly": {
            "time": times
            if times is not None
            else ["2026-09-17T13:00", "2026-09-17T14:00"],
            "temperature_2m": temperatures if temperatures is not None else [18.5, 19.0],
            "precipitation": precipitation if precipitation is not None else [0.0, None],
        },
    }


def fetch(*, locations=(LOCATIONS[0],), variables=VARIABLES, **overrides):
    arguments = {
        "locations": locations,
        "variables": variables,
        "forecast_days": 3,
        "batch_size": 10,
        "timeout": 17,
        "request_delay": 0,
        "fetched_at": FETCHED_AT,
    }
    arguments.update(overrides)
    return list(MODULE.fetch_forecasts(**arguments))


def test_normalizes_forecast_metadata_units_values_and_request():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(weather_document())
    ) as urlopen:
        records = fetch()

    assert len(records) == 2
    first = records[0]
    assert first == {
        "id": first["id"],
        "source": "open_meteo_weather",
        "fetched_at": FETCHED_AT,
        "forecast_issued_at": None,
        "forecast_valid_at": "2026-09-17T13:00:00+00:00",
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
            "model": "best_match",
        },
        "units": {"temperature_2m": "°C", "precipitation": "mm"},
        "weather": {"temperature_2m": 18.5, "precipitation": 0.0},
    }
    assert re.fullmatch(r"[0-9a-f]{64}", first["id"])
    assert records[1]["weather"] == {"temperature_2m": 19.0, "precipitation": None}
    assert records[1]["forecast_valid_at"] == "2026-09-17T14:00:00+00:00"
    assert records[1]["id"] != first["id"]

    request = urlopen.call_args.args[0]
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
    assert query == {
        "latitude": ["40.0"],
        "longitude": ["-75.0"],
        "hourly": ["temperature_2m,precipitation"],
        "forecast_days": ["3"],
        "models": ["best_match"],
        "timezone": ["UTC"],
    }
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert urlopen.call_args.kwargs == {"timeout": 17.0}


def test_collection_time_is_not_fabricated_as_provider_issuance_time():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(weather_document())
    ):
        records = fetch()

    assert {record["fetched_at"] for record in records} == {FETCHED_AT}
    assert {record["forecast_issued_at"] for record in records} == {None}
    assert {record["forecast_valid_at"] for record in records} == {
        "2026-09-17T13:00:00+00:00",
        "2026-09-17T14:00:00+00:00",
    }


def test_ids_are_stable_within_a_revision_and_distinct_across_collections():
    document = weather_document(times=["2026-09-17T13:00"], temperatures=[18.5], precipitation=[0.0])
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        side_effect=[response(document), response(document), response(document)],
    ):
        first = fetch()[0]
        repeated = fetch()[0]
        revised = fetch(fetched_at="2026-09-17T18:34:56Z")[0]

    assert first["id"] == repeated["id"]
    assert first["id"] != revised["id"]
    assert first["forecast_valid_at"] == revised["forecast_valid_at"]
    assert revised["fetched_at"] == "2026-09-17T18:34:56+00:00"
    assert revised["forecast_issued_at"] is None


def test_one_collection_timestamp_is_generated_for_all_batches():
    documents = [
        weather_document(times=["2026-09-17T13:00"], temperatures=[18], precipitation=[0]),
        weather_document(
            latitude=51.49,
            longitude=-0.11,
            times=["2026-09-17T13:00"],
            temperatures=[14],
            precipitation=[0.2],
        ),
    ]
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=map(response, documents)),
        mock.patch.object(MODULE, "_collection_timestamp", return_value=FETCHED_AT) as timestamp,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        records = fetch(locations=LOCATIONS, batch_size=1, request_delay=0.25, fetched_at=None)

    timestamp.assert_called_once_with(None)
    assert {record["fetched_at"] for record in records} == {FETCHED_AT}
    assert [record["location_id"] for record in records] == ["alpha", "beta"]
    sleep.assert_called_once_with(0.25)


def test_multiple_locations_share_one_batched_request_and_preserve_response_order():
    documents = [
        weather_document(times=["2026-09-17T13:00"], temperatures=[18], precipitation=[0]),
        weather_document(
            latitude=51.49,
            longitude=-0.11,
            times=["2026-09-17T13:00"],
            temperatures=[14],
            precipitation=[0.2],
        ),
    ]
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(documents)
    ) as urlopen:
        records = fetch(locations=LOCATIONS, batch_size=2)

    assert [record["location_id"] for record in records] == ["alpha", "beta"]
    query = urllib.parse.parse_qs(
        urllib.parse.urlsplit(urlopen.call_args.args[0].full_url).query
    )
    assert query["latitude"] == ["40.0,51.5"]
    assert query["longitude"] == ["-75.0,-0.1"]


def test_empty_hourly_response_is_successful():
    document = weather_document(times=[], temperatures=[], precipitation=[])
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        assert fetch() == []


@pytest.mark.parametrize(
    ("unit", "missing"),
    [(None, False), (None, True), ("", False), ("   ", False), (1, False), ([], False), ({}, False)],
)
def test_rejects_missing_null_empty_or_non_string_unit_for_every_configured_variable(unit, missing):
    units = {"time": "iso8601", "temperature_2m": "°C", "precipitation": "mm"}
    if missing:
        del units["precipitation"]
    else:
        units["precipitation"] = unit
    document = weather_document(units=units)
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(
            ValueError,
            match=r"hourly_units\['precipitation'\] must be a non-empty string",
        ):
            fetch()


def test_accepts_complete_non_empty_units_for_all_configured_variables():
    document = weather_document(
        units={"time": "iso8601", "temperature_2m": "  °C  ", "precipitation": " mm "}
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        records = fetch()

    assert records[0]["units"] == {"temperature_2m": "°C", "precipitation": "mm"}


@pytest.mark.parametrize(
    ("mutate", "exception", "message"),
    [
        (lambda document: document.update(hourly=[]), TypeError, "missing hourly object"),
        (lambda document: document.update(hourly_units=[]), TypeError, "missing hourly_units object"),
        (lambda document: document["hourly"].update(time="bad"), TypeError, "hourly.time must be a list"),
        (
            lambda document: document["hourly"].update(temperature_2m=[1]),
            ValueError,
            "temperature_2m has 1 values; expected 2",
        ),
        (
            lambda document: document["hourly"].update(precipitation="bad"),
            TypeError,
            "hourly.precipitation must be a list",
        ),
        (
            lambda document: document["hourly"].update(temperature_2m=[1, "hot"]),
            TypeError,
            r"hourly.temperature_2m\[1\] must be a finite number",
        ),
        (
            lambda document: document["hourly"].update(time=["bad", "2026-09-17T14:00"]),
            ValueError,
            "not a valid ISO date/time",
        ),
        (
            lambda document: document["hourly"].update(
                time=["2026-09-17T13:00", "2026-09-17T13:00"]
            ),
            ValueError,
            "duplicate forecast-valid times",
        ),
        (lambda document: document.pop("latitude"), ValueError, "missing latitude"),
        (lambda document: document.update(longitude=181), ValueError, "outside valid bounds"),
        (lambda document: document.update(utc_offset_seconds=3600), ValueError, "not UTC"),
        (lambda document: document.update(timezone="UTC"), ValueError, "timezone must be GMT"),
    ],
)
def test_rejects_malformed_series_and_metadata_without_emitting(mutate, exception, message):
    document = weather_document()
    mutate(document)
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(exception, match=message):
            fetch()


@pytest.mark.parametrize(
    ("document", "exception", "message"),
    [
        ([], ValueError, "returned 0 location responses; expected 2"),
        ([weather_document()], ValueError, "returned 1 location responses; expected 2"),
        ([weather_document(), "bad"], TypeError, "location response 1 must be an object"),
        ({"error": True, "reason": "invalid latitude"}, MODULE.OpenMeteoAPIError, "invalid latitude"),
    ],
)
def test_rejects_malformed_batch_and_api_error_documents(document, exception, message):
    locations = LOCATIONS if isinstance(document, list) else (LOCATIONS[0],)
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(exception, match=message):
            fetch(locations=locations)


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"locations": ()}, "at least one location"),
        (
            {"locations": tuple(MODULE.Location(f"p{i}", f"Point {i}", 0, 0) for i in range(21))},
            "at most 20 locations",
        ),
        ({"locations": (MODULE.Location("alpha", "Alpha", 91, 0),)}, "latitude must be between"),
        ({"locations": (LOCATIONS[0], LOCATIONS[0])}, "duplicate location id"),
        ({"variables": ()}, "at least one hourly variable"),
        ({"variables": ("temperature_2m", "not_real")}, "not allowlisted"),
        ({"forecast_days": 0}, "forecast_days must be between"),
        ({"forecast_days": 17}, "forecast_days must be between"),
        ({"batch_size": 0}, "batch_size must be between"),
        ({"batch_size": 11}, "batch_size must be between"),
        ({"timeout": 0}, "timeout must be positive"),
        ({"request_delay": -1}, "request_delay must be a non-negative number"),
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
    document = weather_document(
        times=["2026-09-17T13:00"],
        temperatures=[18.5],
        precipitation=[0.0],
        units={"time": "iso8601", "temperature_2m": "°C"},
    )
    document["hourly"].pop("precipitation")
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        MODULE.main(
            [
                "--location",
                "alpha,Alpha,40,-75",
                "--variable",
                "temperature_2m",
                "--forecast-days",
                "1",
                "--request-delay",
                "0",
            ]
        )

    emitted = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(emitted) == 1
    assert emitted[0]["location_id"] == "alpha"
    assert emitted[0]["forecast_issued_at"] is None
    assert emitted[0]["weather"] == {"temperature_2m": 18.5}


def test_source_configuration_documents_conservative_disabled_cadence_and_attribution():
    config = CONFIG.read_text(encoding="utf-8")
    assert re.search(r'(?m)^schedule:\s*"[^\n]*\*/6[^\n]*"', config)
    assert re.search(r"(?m)^enabled:\s*false\s*$", config)
    assert "CC BY 4.0" in config
    assert re.search(r"(?m)^attribution_required:\s*true\s*$", config)
    assert "10,000 calls/day" in config
