import importlib.util
import io
import json
import pathlib
import urllib.error
from datetime import datetime, timezone
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).parents[1] / "scripts" / "fetch_nws_active_alerts.py"
SPEC = importlib.util.spec_from_file_location("fetch_nws_active_alerts", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FixedDateTime:
    @classmethod
    def now(cls, tz=None):
        return datetime(2026, 9, 17, 12, 34, 56, tzinfo=timezone.utc)


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def feature(alert_id, *, geometry=None, event="Flood Warning"):
    return {
        "id": alert_id,
        "type": "Feature",
        "geometry": geometry,
        "properties": {
            "event": event,
            "headline": f"{event} headline",
            "severity": "Severe",
        },
    }


def collection(features, next_url=None):
    document = {"type": "FeatureCollection", "features": features}
    if next_url is not None:
        document["pagination"] = {"next": next_url}
    return document


def test_normalizes_features_without_losing_original_payload():
    original = feature(
        "https://api.weather.gov/alerts/urn:oid:2.49.0.1.840.0.example",
        geometry={"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [0, 0]]]},
    )

    record = MODULE.normalize_feature(original, "2026-09-17T12:34:56+00:00")

    assert record["id"] == original["id"]
    assert record["properties"] is original["properties"]
    assert record["geometry"] is original["geometry"]
    assert record["feature"] is original
    assert record["source"] == "nws_active_alerts"


def test_fetches_normal_collection_with_one_timestamp_and_configured_headers():
    features = [feature("alert-one"), feature("alert-two", event="Tornado Warning")]
    observed = []

    def urlopen(request, timeout):
        observed.append((request, timeout))
        return response(collection(features))

    with mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=urlopen
    ):
        records = list(MODULE.fetch_alerts(timeout=23))

    assert [record["id"] for record in records] == ["alert-one", "alert-two"]
    assert {record["fetched_at"] for record in records} == {
        "2026-09-17T12:34:56+00:00"
    }
    assert len(observed) == 1
    request, timeout = observed[0]
    assert request.full_url == MODULE.URL
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert request.get_header("Accept") == "application/geo+json"
    assert timeout == 23


def test_empty_collection_is_a_successful_empty_snapshot():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(collection([]))
    ):
        assert list(MODULE.fetch_alerts()) == []


def test_follows_documented_pagination_and_keeps_run_timestamp():
    next_url = "https://api.weather.gov/alerts/active?cursor=next-page"
    pages = [
        response(collection([feature("first")], next_url)),
        response(collection([feature("second")]))
    ]
    requested_urls = []

    def urlopen(request, timeout):
        requested_urls.append(request.full_url)
        return pages.pop(0)

    with mock.patch.object(MODULE, "datetime", FixedDateTime), mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=urlopen
    ):
        records = list(MODULE.fetch_alerts())

    assert requested_urls == [MODULE.URL, next_url]
    assert [record["id"] for record in records] == ["first", "second"]
    assert records[0]["fetched_at"] == records[1]["fetched_at"]


@pytest.mark.parametrize(
    "document, message",
    [
        ([], "not an object"),
        ({"type": "Feature", "features": []}, "not a GeoJSON FeatureCollection"),
        ({"type": "FeatureCollection"}, "missing features"),
        ({"type": "FeatureCollection", "features": {}}, "missing features"),
    ],
)
def test_rejects_malformed_collections(document, message):
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document)
    ):
        with pytest.raises(ValueError, match=message):
            list(MODULE.fetch_alerts())


@pytest.mark.parametrize(
    "bad_feature, message",
    [
        (None, "not an object"),
        ({"id": "", "properties": {}}, "missing its source id"),
        ({"id": "alert", "properties": []}, "invalid properties"),
        ({"id": "alert", "properties": {}, "geometry": []}, "invalid geometry"),
    ],
)
def test_rejects_malformed_features(bad_feature, message):
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(collection([bad_feature])),
    ):
        with pytest.raises(ValueError, match=message):
            list(MODULE.fetch_alerts())


def test_http_failures_propagate():
    error = urllib.error.HTTPError(MODULE.URL, 503, "unavailable", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error):
        with pytest.raises(urllib.error.HTTPError) as caught:
            list(MODULE.fetch_alerts())
    assert caught.value.code == 503


@pytest.mark.parametrize("timeout", [0, -1])
def test_rejects_non_positive_timeout_without_a_request(timeout):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match="timeout must be positive"):
            list(MODULE.fetch_alerts(timeout=timeout))
    urlopen.assert_not_called()


@pytest.mark.parametrize("max_pages", [0, -1])
def test_rejects_non_positive_page_bound_without_a_request(max_pages):
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match="max_pages must be positive"):
            list(MODULE.fetch_alerts(max_pages=max_pages))
    urlopen.assert_not_called()


def test_stops_at_page_bound_instead_of_silently_truncating():
    next_url = "https://api.weather.gov/alerts/active?cursor=more"
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(collection([], next_url)),
    ) as urlopen:
        with pytest.raises(RuntimeError, match="exceeded max_pages=1"):
            list(MODULE.fetch_alerts(max_pages=1))
    assert urlopen.call_count == 1


def test_rejects_pagination_cycles():
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(collection([], MODULE.URL)),
    ) as urlopen:
        with pytest.raises(ValueError, match="pagination contains a cycle"):
            list(MODULE.fetch_alerts(max_pages=2))
    assert urlopen.call_count == 1


@pytest.mark.parametrize(
    "next_url",
    [
        "http://api.weather.gov/alerts/active?cursor=x",
        "https://example.test/alerts/active?cursor=x",
        "https://user@api.weather.gov/alerts/active?cursor=x",
    ],
)
def test_rejects_untrusted_pagination_urls(next_url):
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=response(collection([], next_url)),
    ) as urlopen:
        with pytest.raises(ValueError, match="outside api.weather.gov"):
            list(MODULE.fetch_alerts())
    assert urlopen.call_count == 1
