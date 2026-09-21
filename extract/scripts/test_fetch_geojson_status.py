"""Offline, deterministic tests for fetch_geojson_status.py.

Fixture provenance: every response document below is a hand-built, synthetic
GeoJSON FeatureCollection shaped to match the public schema documented by the
FEMA OpenShelters, NSW Beachwatch, and ND DOT alert endpoints (feature
`type`/`geometry`/`properties` envelope, id fields named in FEEDS). None of
these fixtures were captured from a live response. They exercise the request
construction and response-parsing boundary only; they are NOT representative
live-source evidence and must not be read as confirming current upstream
behavior (see extract/sources/geojson_status.yml history for the open HTTP
500 diagnostic gate this test file does not resolve).
"""

import importlib.util
import io
import json
import pathlib
import urllib.error
import urllib.request
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_geojson_status.py")
SPEC = importlib.util.spec_from_file_location("fetch_geojson_status", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def feature_collection(features):
    return {"type": "FeatureCollection", "features": features}


def shelter_feature(shelter_id="SH-1", name="Synthetic Community Center"):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [-93.1, 44.9]},
        "properties": {"shelter_id": shelter_id, "shelter_name": name},
    }


def fetch(**overrides):
    arguments = {"feed": "fema_shelters", "limit": 2000, "timeout": 60}
    arguments.update(overrides)
    return list(MODULE.fetch_features(**arguments))


def test_request_uses_configured_url_user_agent_and_timeout():
    for feed, (url, _source, _property_id) in MODULE.FEEDS.items():
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=response(feature_collection([])),
        ) as urlopen:
            fetch(feed=feed, timeout=23)

        urlopen.assert_called_once()
        request = urlopen.call_args.args[0]
        assert isinstance(request, urllib.request.Request)
        assert request.full_url == url
        assert request.get_header("User-agent") == MODULE.USER_AGENT
        assert urlopen.call_args.kwargs == {"timeout": 23}


def test_valid_geojson_produces_expected_ndjson_output():
    document = feature_collection([shelter_feature("SH-42", "Synthetic Shelter")])
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        records = fetch(feed="fema_shelters")

    assert len(records) == 1
    record = records[0]
    assert record["source"] == "fema_open_shelters"
    assert record["id"] == "SH-42"
    assert record["properties"]["shelter_name"] == "Synthetic Shelter"
    assert record["type"] == "Feature"
    assert "fetched_at" in record and record["fetched_at"]


def test_valid_empty_collection_is_a_successful_empty_result():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(feature_collection([]))
    ):
        assert fetch() == []


def test_http_500_raises_explicit_failure_with_no_successful_empty_output():
    error = urllib.error.HTTPError(MODULE.FEEDS["fema_shelters"][0], 500, "Internal Server Error", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            fetch()

    assert excinfo.value.code == 500


@pytest.mark.parametrize("status", [400, 403, 404, 503])
def test_other_http_error_statuses_raise(status):
    error = urllib.error.HTTPError(MODULE.FEEDS["fema_shelters"][0], status, "error", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            fetch()

    assert excinfo.value.code == status


def test_transport_unavailability_raises_url_error():
    error = urllib.error.URLError("Name or service not known")
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.URLError):
            fetch()


def test_malformed_json_raises_json_decode_error():
    body = Response(b"{not valid json")
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=body):
        with pytest.raises(json.JSONDecodeError):
            fetch()


@pytest.mark.parametrize(
    "document",
    [
        {"type": "Feature", "features": []},
        {"type": "FeatureCollection"},
        {"type": "FeatureCollection", "features": "not-a-list"},
        [],
    ],
)
def test_invalid_payload_structure_raises_value_error(document):
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises((ValueError, AttributeError)):
            fetch()


def test_missing_configured_property_id_raises_for_property_keyed_feed():
    document = feature_collection([{"type": "Feature", "properties": {"shelter_name": "No ID Here"}}])
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(ValueError, match="missing its configured ID"):
            fetch(feed="fema_shelters")


def test_missing_id_raises_for_top_level_id_feed():
    document = feature_collection([{"type": "Feature", "properties": {}}])
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        with pytest.raises(ValueError, match="missing its configured ID"):
            fetch(feed="nddot_alerts")


def test_nddot_alerts_uses_top_level_feature_id():
    document = feature_collection(
        [{"type": "Feature", "id": "alert-7", "properties": {"headline": "Synthetic closure"}}]
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)):
        records = fetch(feed="nddot_alerts")

    assert records[0]["id"] == "alert-7"
    assert records[0]["source"] == "north_dakota_road_alerts"


def test_main_emits_ndjson_for_configured_feed(capsys):
    feature = shelter_feature("SH-9", "Synthetic Site")
    document = feature_collection([feature])
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document)),
        mock.patch("sys.argv", ["fetch_geojson_status.py", "--feed", "fema_shelters"]),
    ):
        MODULE.main()

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == 1
    emitted = json.loads(lines[0])
    assert emitted["id"] == "SH-9"
    assert emitted["source"] == "fema_open_shelters"
    assert emitted["geometry"] == feature["geometry"]
