import importlib.util
import io
import json
import pathlib
import urllib.error
from datetime import datetime, timezone
from unittest import mock

import pytest

SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_ipma_forecasts.py"
SPEC = importlib.util.spec_from_file_location("fetch_ipma_forecasts", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


class FixedDateTime:
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 11, 2, 10, 30, 0, tzinfo=timezone.utc)


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def district(location_id, name):
    return {
        "idRegiao": 1,
        "idAreaAviso": f"{location_id % 100:04d}",
        "idConcelho": location_id,
        "globalIdLocal": location_id,
        "latitude": "40.6413",
        "idDistrito": 3,
        "local": name,
        "longitude": "-8.6535",
    }


def districts_document(*locations):
    return {"owner": "IPMA", "country": "PT", "data": list(locations)}


WEATHER_LOOKUP = {
    "owner": "IPMA",
    "country": "PT",
    "data": [
        {"descWeatherTypeEN": "Clear sky", "descWeatherTypePT": "Céu limpo", "idWeatherType": 1},
        {"descWeatherTypeEN": "Partly cloudy", "descWeatherTypePT": "Parcialmente nublado", "idWeatherType": 2},
        {"descWeatherTypeEN": "--", "descWeatherTypePT": "---", "idWeatherType": -99},
    ],
}

WIND_LOOKUP = {
    "owner": "IPMA",
    "country": "PT",
    "data": [
        {"descClassWindSpeedDailyEN": "Weak", "descClassWindSpeedDailyPT": "Fraco", "classWindSpeed": "1"},
        {"descClassWindSpeedDailyEN": "Moderate", "descClassWindSpeedDailyPT": "Moderado", "classWindSpeed": "2"},
    ],
}


def city_document(*rows, data_update="2026-11-02T04:31:03"):
    document = {"owner": "IPMA", "country": "PT", "dataUpdate": data_update, "data": []}
    for i, row in enumerate(rows):
        row = dict(row)
        row.setdefault("forecastDate", f"2026-11-0{2 + i}")
        document["data"].append(row)
    return document


def row(weather=2, wind=1, **overrides):
    base = {
        "precipitaProb": "0.0",
        "tMin": "10.4",
        "tMax": "20.4",
        "predWindDir": "NW",
        "idWeatherType": weather,
        "classWindSpeed": wind,
        "longitude": "-8.6535",
        "latitude": "40.6413",
    }
    base.update(overrides)
    return base


def urlopen_side_effect(by_url):
    calls = []

    def fake(request, timeout=None):
        url = request.full_url
        calls.append((url, timeout))
        value = by_url.get(url)
        if value is None:
            raise urllib.error.HTTPError(url, 404, "not found", {}, None)
        if isinstance(value, Exception):
            raise value
        return response(value)

    return fake, calls


def test_emits_one_record_per_forecast_day_with_decoded_labels():
    location = district(1010500, "Porto")
    document = city_document(*[row() for _ in range(5)])
    fake, calls = urlopen_side_effect(
        {
            MODULE.DISTRICTS_URL: districts_document(location),
            MODULE.WEATHER_TYPES_URL: WEATHER_LOOKUP,
            MODULE.WIND_CLASSES_URL: WIND_LOOKUP,
            MODULE.CITY_FORECAST_URL.format(id=1010500): document,
        }
    )
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), \
            mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
        exit_code = MODULE.run(["--retries", "0"])
    assert exit_code == 0
    records = [json.loads(line) for line in out.getvalue().splitlines()]
    assert len(records) == 5
    first = records[0]
    assert first["source"] == "ipma_city_forecasts"
    assert first["id"] == "1010500:2026-11-02"
    assert first["fetched_at"] == "2026-11-02T10:30:00+00:00"
    assert first["local"] == "Porto"
    assert first["id_distrito"] == 3
    assert first["data_update"] == "2026-11-02T04:31:03"
    assert first["weather_type_en"] == "Partly cloudy"
    assert first["wind_speed_class_en"] == "Weak"
    # Source values pass through unchanged.
    assert first["tMin"] == "10.4"
    assert first["precipitaProb"] == "0.0"
    assert [r["id"] for r in records] == [f"1010500:2026-11-0{2 + i}" for i in range(5)]
    summary = json.loads(err.getvalue().splitlines()[-1].split("VINTAGE_RUN_SUMMARY\t", 1)[1])
    assert summary["health"] == "healthy"
    assert summary["completeness"] == "complete"
    assert summary["partitions"] == {"attempted": 1, "succeeded": 1, "failed": 0, "failures": []}
    assert summary["requests"]["succeeded"] == 4


def test_single_timestamp_across_districts():
    locations = [district(1010500, "Porto"), district(1020500, "Lisboa")]
    document = city_document(*[row() for _ in range(2)])
    by_url = {
        MODULE.DISTRICTS_URL: districts_document(*locations),
        MODULE.WEATHER_TYPES_URL: WEATHER_LOOKUP,
        MODULE.WIND_CLASSES_URL: WIND_LOOKUP,
    }
    for location in locations:
        by_url[MODULE.CITY_FORECAST_URL.format(id=location["globalIdLocal"])] = document
    fake, _ = urlopen_side_effect(by_url)
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), \
            mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
        exit_code = MODULE.run(["--retries", "0"])
    assert exit_code == 0
    records = [json.loads(line) for line in out.getvalue().splitlines()]
    assert len(records) == 4
    assert len({r["fetched_at"] for r in records}) == 1


def test_no_data_and_unknown_codes_decode_to_null():
    location = district(1010500, "Porto")
    document = city_document(row(weather=-99, wind=7))
    fake, _ = urlopen_side_effect(
        {
            MODULE.DISTRICTS_URL: districts_document(location),
            MODULE.WEATHER_TYPES_URL: WEATHER_LOOKUP,
            MODULE.WIND_CLASSES_URL: WIND_LOOKUP,
            MODULE.CITY_FORECAST_URL.format(id=1010500): document,
        }
    )
    out, _ = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), \
            mock.patch("sys.stdout", out), mock.patch("sys.stderr", io.StringIO()):
        exit_code = MODULE.run(["--retries", "0"])
    assert exit_code == 0
    record = json.loads(out.getvalue().splitlines()[0])
    assert record["weather_type_en"] is None
    assert record["wind_speed_class_en"] is None


def test_transient_http_error_is_retried_and_succeeds():
    location = district(1010500, "Porto")
    document = city_document(row())
    payloads = {
        MODULE.DISTRICTS_URL: districts_document(location),
        MODULE.WEATHER_TYPES_URL: WEATHER_LOOKUP,
        MODULE.WIND_CLASSES_URL: WIND_LOOKUP,
        MODULE.CITY_FORECAST_URL.format(id=1010500): document,
    }
    state = {"districts_calls": 0}

    def fake(request, timeout=None):
        url = request.full_url
        if url == MODULE.DISTRICTS_URL:
            state["districts_calls"] += 1
            if state["districts_calls"] == 1:
                raise urllib.error.HTTPError(url, 503, "unavailable", {}, None)
        return response(payloads[url])

    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), \
            mock.patch.object(MODULE.time, "sleep") as sleep, \
            mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
        exit_code = MODULE.run(["--retries", "2", "--retry-delay", "5"])
    assert exit_code == 0
    assert len([json.loads(l) for l in out.getvalue().splitlines()]) == 1
    sleep.assert_called_once_with(5)
    summary = json.loads(err.getvalue().splitlines()[-1].split("VINTAGE_RUN_SUMMARY\t", 1)[1])
    assert summary["requests"]["attempted"] == 5
    assert summary["requests"]["retries"] == 1


def test_district_failure_degrades_run_but_keeps_other_districts():
    locations = [district(1010500, "Porto"), district(1020500, "Lisboa")]
    document = city_document(row())
    fake, _ = urlopen_side_effect(
        {
            MODULE.DISTRICTS_URL: districts_document(*locations),
            MODULE.WEATHER_TYPES_URL: WEATHER_LOOKUP,
            MODULE.WIND_CLASSES_URL: WIND_LOOKUP,
            MODULE.CITY_FORECAST_URL.format(id=1010500): document,
            MODULE.CITY_FORECAST_URL.format(id=1020500): urllib.error.HTTPError(
                "forecast", 503, "unavailable", {}, None
            ),
        }
    )
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), \
            mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
        exit_code = MODULE.run(["--retries", "0"])
    assert exit_code == 0
    records = [json.loads(line) for line in out.getvalue().splitlines()]
    assert [r["local"] for r in records] == ["Porto"]
    summary = json.loads(err.getvalue().splitlines()[-1].split("VINTAGE_RUN_SUMMARY\t", 1)[1])
    assert summary["health"] == "degraded"
    assert summary["completeness"] == "partial"
    assert summary["partitions"] == {
        "attempted": 2,
        "succeeded": 1,
        "failed": 1,
        "failures": [{"name": "1020500", "error": mock.ANY}],
    }


def test_total_failure_emits_no_records_and_fails():
    location = district(1010500, "Porto")
    fake, _ = urlopen_side_effect(
        {
            MODULE.DISTRICTS_URL: urllib.error.HTTPError(
                MODULE.DISTRICTS_URL, 404, "not found", {}, None
            ),
        }
    )
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), \
            mock.patch("sys.stdout", out), mock.patch("sys.stderr", err):
        exit_code = MODULE.run(["--retries", "0"])
    assert exit_code == 1
    assert out.getvalue() == ""
    summary = json.loads(err.getvalue().splitlines()[-1].split("VINTAGE_RUN_SUMMARY\t", 1)[1])
    assert summary["health"] == "failed"
    assert summary["completeness"] == "failed"


def test_empty_district_list_fails_the_run():
    fake, _ = urlopen_side_effect(
        {
            MODULE.DISTRICTS_URL: {"owner": "IPMA", "country": "PT", "data": []},
        }
    )
    out, _ = io.StringIO(), io.StringIO()
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake), \
            mock.patch.object(MODULE, "datetime", FixedDateTime), \
            mock.patch("sys.stdout", out), mock.patch("sys.stderr", io.StringIO()):
        with pytest.raises(MODULE.FetchError):
            MODULE.fetch_districts(30, 0, 0)
    assert out.getvalue() == ""


@pytest.mark.parametrize(
    ("row_", "message"),
    [
        ({"tMin": "1", "tMax": "2"}, "forecastDate"),
        ({"forecastDate": "2026-11-02", "tMin": "1"}, "tMax"),
    ],
)
def test_rejects_malformed_forecast_rows(row_, message):
    with pytest.raises(MODULE.FetchError, match=message):
        MODULE.normalize_row(
            row_, district(1010500, "Porto"), {}, {}, "2026-11-02T10:30:00+00:00", None
        )


@pytest.mark.parametrize("bad", [0, -1])
def test_rejects_non_positive_timeout_without_a_request(bad):
    with pytest.raises(SystemExit):
        MODULE.run(["--timeout", str(bad)])


@pytest.mark.parametrize("bad", [6, -1])
def test_rejects_out_of_range_retries_without_a_request(bad):
    with pytest.raises(SystemExit):
        MODULE.run(["--retries", str(bad)])
