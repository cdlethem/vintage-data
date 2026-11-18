import importlib.util
import io
import json
import pathlib
import urllib.error
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_met_no_weather.py")
SPEC = importlib.util.spec_from_file_location("fetch_met_no_weather", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
CONFIG = SCRIPT.parents[1] / "sources" / "met_no_weather.yml"

FETCHED_AT = "2026-11-18T05:30:00+00:00"
TIMES = (
    "2026-11-18T05:00:00Z",
    "2026-11-18T06:00:00Z",
)
VARIABLES = dict(
    air_pressure_at_sea_level=983.0,
    air_temperature=8.9,
    cloud_area_fraction=60.2,
    relative_humidity=89.1,
    wind_from_direction=247.9,
    wind_speed=5.4,
)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def weather_document(times=TIMES, variables=VARIABLES, location=("59.9139", "10.7522", 25)):
    latitude, longitude, altitude = location
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [float(longitude), float(latitude), int(altitude)]},
        "properties": {
            "timeseries": [
                {
                    "time": time,
                    "data": {
                        "instant": {"details": dict(variables)},
                        "next_1_hours": {
                            "summary": {"symbol_code": "partlycloudy_night"},
                            "details": {"precipitation_amount": 0.0},
                        },
                        "next_6_hours": {
                            "summary": {"symbol_code": "partlycloudy_day"},
                            "details": {"precipitation_amount": 0.2},
                        },
                    },
                }
                for time in times
            ]
        },
    }


def locations(*indices):
    return [MODULE.DEFAULT_LOCATIONS[i] for i in indices]


def fetch(*, locations=locations(0), **overrides):
    arguments = {
        "timeout": 17,
        "request_delay": 0,
        "fetched_at": FETCHED_AT,
    }
    arguments.update(overrides)
    return list(MODULE.fetch_forecasts(locations=locations, **arguments))


def test_normalizes_forecast_metadata_units_values_and_request():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen",
        return_value=response(weather_document(location=("59.9139", "10.7522", 25))),
    ) as urlopen:
        records = fetch()
    assert urlopen.call_count == 1
    request = urlopen.call_args.args[0]
    assert request.full_url == (
        MODULE.API_URL
        + "?lat=59.9139&lon=10.7522&altitude=25"
    )
    assert request.headers["User-agent"] == MODULE.USER_AGENT
    assert urlopen.call_args.kwargs == {"timeout": 17}

    first = records[0]
    normalized_times = (
        "2026-11-18T05:00:00+00:00",
        "2026-11-18T06:00:00+00:00",
    )
    assert [record["id"] for record in records] == [
        MODULE._stable_revision_id("oslo", time, FETCHED_AT) for time in normalized_times
    ]
    assert first == {
        "id": first["id"],
        "source": "met_no_weather",
        "fetched_at": FETCHED_AT,
        "forecast_issued_at": None,
        "forecast_valid_at": "2026-11-18T05:00:00+00:00",
        "location_id": "oslo",
        "location_name": "Oslo",
        "requested_latitude": 59.9139,
        "requested_longitude": 10.7522,
        "requested_altitude": 25,
        "latitude": 59.9139,
        "longitude": 10.7522,
        "altitude": 25,
        "provider": {
            "name": "MET Norway",
            "url": MODULE.PROVIDER_URL,
            "license": "NLOD 2.0 / CC BY 4.0",
        },
        "units": {
            "air_pressure_at_sea_level": "Pa",
            "air_temperature": "C",
            "cloud_area_fraction": "%",
            "relative_humidity": "%",
            "wind_from_direction": "degrees",
            "wind_speed": "m/s",
        },
        "symbol_code": "partlycloudy_night",
        "weather": {
            "air_pressure_at_sea_level": 983.0,
            "air_temperature": 8.9,
            "cloud_area_fraction": 60.2,
            "relative_humidity": 89.1,
            "wind_from_direction": 247.9,
            "wind_speed": 5.4,
            "precipitation_amount_next_1_hours": 0.0,
            "precipitation_amount_next_6_hours": 0.2,
        },
    }


def test_collection_time_is_not_fabricated_as_provider_issuance_time():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(weather_document())):
        records = fetch()
    assert all(record["forecast_issued_at"] is None for record in records)
    assert all(record["fetched_at"] == FETCHED_AT for record in records)


def test_period_blocks_taper_to_null_near_the_horizon():
    document = weather_document()
    last = document["properties"]["timeseries"][-1]
    last["data"].pop("next_1_hours")
    last["data"].pop("next_6_hours")
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        records = fetch()
    final = records[-1]
    assert final["symbol_code"] is None
    assert final["weather"]["precipitation_amount_next_1_hours"] is None
    assert final["weather"]["precipitation_amount_next_6_hours"] is None
    assert records[0]["symbol_code"] == "partlycloudy_night"


def test_ids_are_stable_within_a_revision_and_distinct_across_collections():
    document = weather_document()
    fresh = lambda *args, **kwargs: response(document)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fresh):
        first = fetch()
        second = fetch()
    assert [record["id"] for record in first] == [record["id"] for record in second]
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fresh):
        revised = fetch(fetched_at="2026-11-18T11:30:00+00:00")
    assert {record["id"] for record in first}.isdisjoint({record["id"] for record in revised})
    assert all(record["fetched_at"] == "2026-11-18T11:30:00+00:00" for record in revised)
    assert all(record["forecast_issued_at"] is None for record in revised)


def test_one_collection_timestamp_is_generated_for_all_locations():
    documents = [
        weather_document(
            location=(str(loc.latitude), str(loc.longitude), loc.altitude)
        )
        for loc in MODULE.DEFAULT_LOCATIONS[:3]
    ]
    with mock.patch.object(MODULE.time, "sleep") as sleep, mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=[response(d) for d in documents]
    ):
        records = fetch(locations=locations(0, 1, 2), request_delay=0.25)
    assert sleep.call_args_list == [mock.call(0.25), mock.call(0.25)]
    assert len(records) == len(TIMES) * 3
    assert {record["fetched_at"] for record in records} == {FETCHED_AT}


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda doc: doc.update(type="FeatureX"), "type must be Feature"),
        (lambda doc: doc.update(geometry={"type": "Point", "coordinates": [10.7522, 59.9139]}), "coordinates must have longitude, latitude, altitude"),
        (lambda doc: doc.update(geometry={"type": "Point", "coordinates": [10.7522, 59.9139, "high"]}), "must be a number"),
        (lambda doc: doc["properties"].update(timeseries=[]), "timeseries must be a non-empty list"),
        (lambda doc: doc["properties"]["timeseries"][0].update(time="not-a-time"), "is not ISO 8601"),
        (lambda doc: doc["properties"]["timeseries"][1].update(time=TIMES[0]), "duplicate forecast-valid times"),
        (lambda doc: doc["properties"]["timeseries"][0]["data"].pop("instant"), "missing the instant block"),
        (lambda doc: doc["properties"]["timeseries"][0]["data"]["instant"]["details"].pop("wind_speed"), "missing wind_speed"),
        (lambda doc: doc["properties"]["timeseries"][0]["data"]["instant"]["details"].update(wind_speed="fast"), "must be a number"),
        (lambda doc: doc["properties"]["timeseries"][0]["data"]["next_1_hours"]["summary"].update(symbol_code=""), "symbol_code must be a non-empty string"),
        (lambda doc: doc["properties"]["timeseries"][0]["data"]["next_6_hours"]["details"].update(precipitation_amount=None), "must be a number"),
        (lambda doc: doc.update(properties=None), "missing properties"),
    ],
)
def test_rejects_malformed_documents_without_emitting(mutate, message):
    document = weather_document()
    mutate(document)
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises((TypeError, ValueError), match=message):
            fetch()


def test_rejects_too_many_timesteps():
    document = weather_document(times=[f"2026-11-{18 + i // 24:02d}T{i % 24:02d}:00:00Z" for i in range(217)])
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(ValueError, match="at most 216"):
            fetch()


def test_http_errors_propagate_with_detail():
    error = urllib.error.HTTPError(MODULE.API_URL, 403, "Forbidden", {}, io.BytesIO(b"User-Agent missing"))
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(MODULE.MetNoAPIError, match="HTTP 403.*User-Agent missing"):
            fetch()


@pytest.mark.parametrize(
    "overrides, message",
    [
        (dict(locations=()), "at least one location is required"),
        (dict(locations=locations(0, 0)), "duplicated"),
        (dict(locations=[MODULE.Location("a", "A", 91, 0, 0)]), "latitude is outside valid bounds"),
        (dict(locations=[MODULE.Location("a", "A", 0, 0, 12000)]), "altitude is outside valid bounds"),
        (dict(locations=[MODULE.Location("a", "A", 0, 0, 0.5)]), "altitude must be an integer"),
        (dict(timeout=0), "timeout must be positive"),
        (dict(request_delay=-1), "request_delay must be a non-negative number"),
    ],
)
def test_rejects_invalid_arguments_before_http(overrides, message):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises((TypeError, ValueError), match=message):
            fetch(**overrides)
    urlopen.assert_not_called()


def test_main_emits_ndjson_for_explicit_location(capsys):
    document = weather_document(location=("40.7128", "-74.0060", 10))
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        MODULE.main(
            ["--location", "new_york,New York,40.7128,-74.0060,10", "--request-delay", "0"]
        )
    emitted = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert len(emitted) == len(TIMES)
    assert emitted[0]["location_id"] == "new_york"
    assert emitted[0]["weather"]["air_temperature"] == 8.9
    assert emitted[0]["provider"]["license"] == "NLOD 2.0 / CC BY 4.0"


def test_source_configuration_documents_conservative_cadence_and_attribution():
    config = CONFIG.read_text(encoding="utf-8")
    assert "schedule: \"29 */6 * * *\"" in config
    assert "NLOD 2.0 / CC BY 4.0" in config
    assert "MET Norway" in config
    assert "attribution_required: true" in config
    assert "20 requests/second" in config
