import importlib.util
import io
from pathlib import Path
from unittest.mock import patch

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "fetch_nws_active_alerts.py"
SPEC = importlib.util.spec_from_file_location("fetch_nws_active_alerts", SCRIPT)
NWS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(NWS)


class JsonResponse(io.StringIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def feature(alert_id, event="Tornado Warning", geometry=None):
    return {
        "type": "Feature",
        "id": alert_id,
        "properties": {"event": event},
        "geometry": geometry or {"type": "Point", "coordinates": [-97.0, 35.0]},
    }


def collection(features, next_url=None):
    document = {"type": "FeatureCollection", "features": features}
    if next_url is not None:
        document["pagination"] = {"next": next_url}
    return document


def responses(documents, requests):
    def fake_urlopen(request, timeout):
        requests.append((request.full_url, request.headers, timeout))
        return JsonResponse(documents.pop(0))

    return fake_urlopen


def test_normal_collection_preserves_id_geometry_properties_and_feature():
    source_feature = feature("urn:oid:2.49.0.1.840.0.1", geometry={"type": "Point", "coordinates": [1, 2]})
    documents = [__import__("json").dumps(collection([source_feature]))]
    requests = []

    with patch.object(NWS.urllib.request, "urlopen", responses(documents, requests)):
        records = list(NWS.fetch_alerts(timeout=17))

    assert records == [
        {
            "source": "nws_active_alerts",
            "fetched_at": records[0]["fetched_at"],
            "id": source_feature["id"],
            "properties": source_feature["properties"],
            "geometry": source_feature["geometry"],
            "feature": source_feature,
        }
    ]
    assert records[0]["fetched_at"].endswith("+00:00")
    assert requests == [(NWS.URL, {"User-agent": NWS.USER_AGENT}, 17)]


def test_empty_collection_is_a_successful_empty_snapshot():
    documents = [__import__("json").dumps(collection([]))]
    requests = []

    with patch.object(NWS.urllib.request, "urlopen", responses(documents, requests)):
        assert list(NWS.fetch_alerts()) == []

    assert len(requests) == 1


def test_pagination_uses_one_timestamp_and_stops_at_limit():
    next_url = "https://api.weather.gov/alerts/active?cursor=second"
    documents = [
        __import__("json").dumps(collection([feature("first")], next_url)),
        __import__("json").dumps(collection([feature("second"), feature("third")])),
    ]
    requests = []

    with patch.object(NWS.urllib.request, "urlopen", responses(documents, requests)):
        records = list(NWS.fetch_alerts(limit=2))

    assert [record["id"] for record in records] == ["first", "second"]
    assert len({record["fetched_at"] for record in records}) == 1
    assert [request[0] for request in requests] == [NWS.URL, next_url]
    assert len(documents) == 0


def test_limit_bounds_requests_before_following_next_page():
    documents = [
        __import__("json").dumps(collection([feature("first")], "https://api.weather.gov/alerts/active?cursor=second")),
    ]
    requests = []

    with patch.object(NWS.urllib.request, "urlopen", responses(documents, requests)):
        records = list(NWS.fetch_alerts(limit=1))

    assert [record["id"] for record in records] == ["first"]
    assert len(requests) == 1


@pytest.mark.parametrize(
    "document, error",
    [
        ({"type": "FeatureCollection", "features": {}}, ValueError),
        ({"type": "FeatureCollection", "features": [{}]}, ValueError),
        ({"type": "FeatureCollection", "features": [], "pagination": {"next": 3}}, TypeError),
        ({"type": "FeatureCollection", "features": [], "pagination": {"next": "http://example.test/page"}}, ValueError),
    ],
)
def test_malformed_responses_fail_without_silent_data_loss(document, error):
    documents = [__import__("json").dumps(document)]
    requests = []

    with patch.object(NWS.urllib.request, "urlopen", responses(documents, requests)):
        with pytest.raises(error):
            list(NWS.fetch_alerts())


def test_http_failures_propagate():
    with patch.object(NWS.urllib.request, "urlopen", side_effect=OSError("service unavailable")):
        with pytest.raises(OSError, match="service unavailable"):
            list(NWS.fetch_alerts())
