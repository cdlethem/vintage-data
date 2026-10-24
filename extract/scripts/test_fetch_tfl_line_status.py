"""Offline, deterministic tests for fetch_tfl_line_status.py.

No network: ``urllib.request.urlopen`` is mocked; assertions cover request
construction, normalization, validity-period math, and the retry policy.
"""
import importlib.util
import io
import json
import pathlib
import urllib.error
from unittest import mock

import pytest

SCRIPT = pathlib.Path(__file__).with_name("fetch_tfl_line_status.py")
SPEC = importlib.util.spec_from_file_location("fetch_tfl_line_status", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()


def response(document):
    return Response(json.dumps(document).encode("utf-8"))


def period(from_date, to_date, is_now):
    return {
        "$type": "Tfl.Api.Presentation.Entities.ValidityPeriod, Tfl.Api.Presentation.Entities",
        "fromDate": from_date,
        "toDate": to_date,
        "isNow": is_now,
    }


def line_status(severity=10, description="Good Service", is_now=True,
                from_date="2026-10-24T05:34:52Z", to_date="2026-10-25T01:29:00Z",
                reason=None, periods=None):
    if periods is None:
        periods = [period(from_date, to_date, is_now)]
    return {
        "$type": "Tfl.Api.Presentation.Entities.LineStatus, Tfl.Api.Presentation.Entities",
        "id": 0,
        "statusSeverity": severity,
        "statusSeverityDescription": description,
        "reason": reason,
        "created": "0001-01-01T00:00:00",
        "validityPeriods": periods,
    }


def line(mode_id="central", mode_name="tube", statuses=None, disruptions=None, name=None):
    return {
        "$type": "Tfl.Api.Presentation.Entities.Line, Tfl.Api.Presentation.Entities",
        "id": mode_id,
        "name": name if name is not None else "Central",
        "modeName": mode_name,
        "created": "2026-09-22T09:53:41.037Z",
        "modified": "2026-09-22T09:53:41.037Z",
        "lineStatuses": [line_status()] if statuses is None else statuses,
        "disruptions": [] if disruptions is None else disruptions,
        "crowding": {"$type": "Tfl.Api.Presentation.Entities.Crowding, Tfl.Api.Presentation.Entities"},
        "routeSections": [],
    }


def disruption(**overrides):
    doc = {
        "$type": "Tfl.Api.Presentation.Entities.Disruption, Tfl.Api.Presentation.Entities",
        "category": "RealTimeDisruption",
        "reason": "Minor delays due to an earlier signal fault.",
        "description": "Delays between A and B.",
        "created": "2026-10-24T05:34:52Z",
        "validFrom": None,
        "validTo": None,
        "isNow": True,
    }
    doc.update(overrides)
    return doc


def fetch(**overrides):
    arguments = {"mode": "tube", "timeout": 30}
    arguments.update(overrides)
    return list(MODULE.fetch_lines(**arguments))


def test_valid_payload_produces_normalized_ndjson():
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line()])), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        records = fetch()
    assert len(records) == 1
    record = records[0]
    assert record["source"] == MODULE.SOURCE
    assert record["fetched_at"] == "2026-10-24T05:40:00+00:00"
    assert record["id"] == "tube:central"
    assert record["mode"] == "tube"
    assert record["line_id"] == "central"
    assert record["status"] == "Good Service"
    assert record["status_severity"] == 10
    assert record["statuses"][0]["is_now"] is True
    assert record["disruptions"] == []
    assert record["crowding"] is None
    assert "$type" not in json.dumps(record)


def test_request_uses_configured_url_user_agent_and_timeout():
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line()])) as urlopen, \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        fetch()
        (request,), kwargs = urlopen.call_args
        assert request.get_full_url() == "https://api.tfl.gov.uk/Line/Mode/tube/Status"
        assert request.get_header("User-agent") == MODULE.USER_AGENT
        assert kwargs == {"timeout": 30}


def test_current_status_is_the_one_with_live_validity_period():
    statuses = [
        line_status(severity=7, description="Partial Service",
                    from_date="2026-11-02T00:00:00Z", to_date="2026-11-03T00:00:00Z",
                    periods=[period("2026-11-02T00:00:00Z", "2026-11-03T00:00:00Z", False)]),
        line_status(severity=9, description="Minor Delays",
                    reason="Earlier engineering work.",
                    from_date="2026-10-24T05:34:52Z", to_date="2026-10-25T01:29:00Z"),
    ]
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line(statuses=statuses)])), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        records = fetch()
    assert records[0]["status"] == "Minor Delays"
    assert records[0]["status_severity"] == 9
    assert len(records[0]["statuses"]) == 2
    assert records[0]["statuses"][0]["is_now"] is False
    assert records[0]["statuses"][1]["reason"] == "Earlier engineering work."


def test_validity_periods_collapse_to_min_from_max_to():
    status = line_status(periods=[
        period("2026-10-24T05:34:52Z", "2026-10-25T01:29:00Z", True),
        period("2026-10-24T00:00:00Z", "2026-10-26T00:00:00Z", True),
    ])
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line(statuses=[status])])), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        records = fetch()
    assert records[0]["statuses"][0]["valid_from"] == "2026-10-24T00:00:00Z"
    assert records[0]["statuses"][0]["valid_to"] == "2026-10-26T00:00:00Z"
    assert records[0]["statuses"][0]["is_now"] is True


def test_empty_validity_period_means_standing_current_status():
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line(statuses=[line_status(periods=[])])])), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        records = fetch()
    assert records[0]["status"] == "Good Service"
    assert records[0]["statuses"][0]["is_now"] is True
    assert records[0]["statuses"][0]["valid_from"] is None
    assert records[0]["statuses"][0]["valid_to"] is None


def test_disruptions_are_normalized_compactly():
    doc = disruption(validFrom="2026-10-24T05:34:52Z", validTo="2026-10-25T01:29:00Z")
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line(disruptions=[doc])])), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        records = fetch()
    d = records[0]["disruptions"][0]
    assert d == {
        "category": "RealTimeDisruption",
        "reason": "Minor delays due to an earlier signal fault.",
        "description": "Delays between A and B.",
        "created": "2026-10-24T05:34:52Z",
        "valid_from": "2026-10-24T05:34:52Z",
        "valid_to": "2026-10-25T01:29:00Z",
        "is_now": True,
    }


def test_populated_crowding_is_kept():
    line_doc = line()
    line_doc["crowding"] = {"$type": "Tfl.Api.Presentation.Entities.Crowding, Tfl.Api.Presentation.Entities",
                            "description": "Busy"}
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line_doc])), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        records = fetch()
    assert records[0]["crowding"] == {"description": "Busy"}


def test_empty_list_is_a_successful_empty_result():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([])):
        assert fetch() == []


def test_non_list_payload_raises_type_error():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"a": 1})):
        with pytest.raises(TypeError):
            fetch()


def test_missing_line_id_raises_value_error():
    doc = line()
    del doc["id"]
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response([doc])):
        with pytest.raises(ValueError):
            fetch()


def test_line_without_line_statuses_raises_value_error():
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           return_value=response([line(statuses=[])])):
        with pytest.raises(ValueError):
            fetch()


def test_transient_503_is_retried_then_succeeds():
    error = urllib.error.HTTPError("https://api.tfl.gov.uk/Line/Mode/tube/Status",
                                   503, "Service Unavailable", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen",
                           side_effect=[error, response([line()])]), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"), \
         mock.patch.object(MODULE.time, "sleep") as sleep:
        assert len(fetch()) == 1
    assert [call.args[0] for call in sleep.call_args_list] == [2]


def test_400_is_a_config_bug_and_not_retried():
    error = urllib.error.HTTPError("https://api.tfl.gov.uk/Line/Mode/tube/Status",
                                   400, "Bad Request", {}, None)
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, \
         mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            fetch()
        assert excinfo.value.code == 400
        assert urlopen.call_count == 1
        sleep.assert_not_called()


def test_persistent_transport_failure_raises_after_final_attempt():
    error = urllib.error.URLError("Name or service not known")
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen, \
         mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(urllib.error.URLError):
            fetch()
        assert urlopen.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [2, 4]


def test_main_emits_ndjson_for_all_modes(capsys):
    documents = {mode: [line(mode_id=f"{mode}-1", mode_name=mode,
                             name=f"{mode.title()} One")] for mode in MODULE.MODES}
    def fake_urlopen(request, timeout=None):
        mode = request.full_url.rsplit("/", 2)[-2]
        return response(documents[mode])
    with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen), \
         mock.patch.object(MODULE, "datetime_now", return_value="2026-10-24T05:40:00+00:00"):
        assert MODULE.main([]) == 0
    out = capsys.readouterr()
    lines = [json.loads(l) for l in out.out.splitlines()]
    assert [l["id"] for l in lines] == [f"{m}:{m}-1" for m in MODULE.MODES]
    assert all(l["source"] == MODULE.SOURCE for l in lines)
    assert len({l["fetched_at"] for l in lines}) == 1
    assert out.err == "mode=tube lines=1\nmode=bus lines=1\nmode=tram lines=1\nmode=dlr lines=1\nmode=overground lines=1\n"
