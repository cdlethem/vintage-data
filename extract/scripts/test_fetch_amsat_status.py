"""Offline, deterministic tests for fetch_amsat_status.py.

Fixture provenance: every response document below is a hand-built, synthetic
AMSAT v1 response shaped to match the live public schema (top-level `data`
list plus `meta`; rows carry `id`, `satellite_display_name`, `report`,
`reported_time`, `grid_square`, `callsign`). None of these fixtures were
captured from a live response. They exercise the request construction and
response-parsing boundary, plus the bounded retry behavior for transient
transport failures: the upstream host intermittently stalls for tens of
seconds, so a stalled response must be retried within a bounded budget
instead of losing the whole run, while HTTP error statuses and malformed
payloads still fail immediately.

Stdlib only; no network access.
"""

import importlib.util
import io
import json
import pathlib
import sys
import urllib.error
import urllib.parse
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_amsat_status.py")
SPEC = importlib.util.spec_from_file_location("fetch_amsat_status", SCRIPT)
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


def report_row(report_id=1367269, satellite="NOAA 19", callsign="K1ABC"):
    return {
        "id": report_id,
        "name": "AMSAT-1 status",
        "satellite_display_name": satellite,
        "report": "telemetry OK",
        "reported_time": "2026-10-19T03:15:00Z",
        "grid_square": "FN20",
        "callsign": callsign,
    }


def document(rows):
    return {"data": rows, "meta": {"limit": len(rows)}}


def fetch(**overrides):
    arguments = {"hours": 24, "limit": 100, "timeout": 30}
    arguments.update(overrides)
    return list(MODULE.fetch_reports(**arguments))


def test_request_uses_configured_url_query_user_agent_and_timeout():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document([report_row()]))
    ) as urlopen:
        fetch(hours=6, limit=50, timeout=17)

    (request,), kwargs = urlopen.call_args
    parsed = urllib.parse.urlparse(str(request.full_url))
    assert parsed.netloc == "www.amsat.org"
    assert parsed.path == "/status/api/v1/reports.php"
    assert urllib.parse.parse_qs(parsed.query) == {"hours": ["6"], "limit": ["50"]}
    assert request.headers["User-agent"] == MODULE.USER_AGENT
    assert kwargs == {"timeout": 17}


def test_limit_above_ceiling_is_clamped_in_query():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document([report_row()]))
    ) as urlopen:
        fetch(limit=5000)

    (request,), _ = urlopen.call_args
    parsed = urllib.parse.urlparse(str(request.full_url))
    assert urllib.parse.parse_qs(parsed.query)["limit"] == ["1000"]


def test_valid_payload_produces_expected_ndjson_output():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=response(document([report_row()]))
    ):
        records = fetch()

    assert len(records) == 1
    record = records[0]
    assert record["source"] == "amsat_status_reports"
    assert record["id"] == "1367269"
    assert record["fetched_at"]
    assert record["satellite_display_name"] == "NOAA 19"
    assert record["callsign"] == "K1ABC"


def test_valid_empty_data_is_a_successful_empty_result():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document([]))):
        assert fetch() == []


def test_missing_data_key_raises_type_error():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response({"meta": {}})):
        with pytest.raises(TypeError):
            fetch()


def test_row_without_id_raises_value_error():
    row = report_row()
    del row["id"]
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(document([row]))):
        with pytest.raises(ValueError):
            fetch()


def test_transient_timeout_is_retried_then_succeeds():
    with (
        mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[
                TimeoutError("read timed out"),
                TimeoutError("read timed out"),
                response(document([report_row()]))
            ],
        ) as urlopen,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        records = fetch()

    assert len(records) == 1
    assert urlopen.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [2, 4]


@pytest.mark.parametrize(
    "failure",
    [
        TimeoutError("read timed out"),
        urllib.error.URLError("Connection reset by peer"),
        OSError("Network is unreachable"),
    ],
    ids=["timeout", "url-error", "os-error"],
)
def test_persistent_transport_failure_raises_after_final_attempt(failure):
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=failure) as urlopen,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        with pytest.raises(type(failure)):
            fetch()

    assert urlopen.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [2, 4]


@pytest.mark.parametrize("status", [400, 404, 500, 503])
def test_http_error_statuses_are_not_retried(status):
    error = urllib.error.HTTPError(MODULE.URL, status, "error", {}, None)
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        with pytest.raises(urllib.error.HTTPError):
            fetch()

    urlopen.assert_called_once()
    sleep.assert_not_called()


def test_malformed_json_is_not_retried():
    body = Response(b"{not valid json")
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=body) as urlopen:
        with pytest.raises(json.JSONDecodeError):
            fetch()

    urlopen.assert_called_once()


def test_main_emits_ndjson(capsys):
    with (
        mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response(document([report_row()]))
        ),
        mock.patch.object(sys, "argv", ["fetch_amsat_status.py"]),
    ):
        MODULE.main()

    emitted = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line]
    assert len(emitted) == 1
    assert emitted[0]["id"] == "1367269"
