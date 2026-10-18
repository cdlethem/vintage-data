"""Offline, deterministic tests for fetch_tatoeba.py.

The endpoint is a slow nonprofit service; the interesting behavior is the
bounded retry of transient transport failures (timeouts, connection resets)
while HTTP error statuses and malformed payloads fail immediately. All
network access is mocked.
"""

import importlib.util
import io
import json
import pathlib
import urllib.error
from unittest import mock

import pytest


SCRIPT = pathlib.Path(__file__).with_name("fetch_tatoeba.py")
SPEC = importlib.util.spec_from_file_location("fetch_tatoeba", SCRIPT)
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


def sentence_document(sentence_id=14037517, text="We all have to pull our weight."):
    return {
        "results": [
            {
                "id": sentence_id,
                "text": text,
                "lang": "eng",
                "license": "CC BY 2.0 FR",
                "translations": [
                    [{"id": 99001, "text": "Nous devons tous faire notre part.", "lang": "fra"}]
                ],
            }
        ],
    }


def fetch(**overrides):
    arguments = {"from_lang": "eng", "to_lang": "fra"}
    arguments.update(overrides)
    return list(MODULE.fetch_recent(**arguments))


def test_request_uses_configured_url_user_agent_and_timeout():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(sentence_document())) as urlopen:
        assert len(fetch()) == 1

    args, kwargs = urlopen.call_args
    request = args[0]
    assert request.full_url.startswith(MODULE.BASE + "?")
    assert "from=eng" in request.full_url and "to=fra" in request.full_url
    assert "sort=created" in request.full_url and "direction=desc" in request.full_url
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert kwargs == {"timeout": 30}


def test_valid_document_produces_expected_ndjson_output():
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response(sentence_document())):
        records = fetch()

    assert len(records) == 1
    record = records[0]
    assert record["source"] == "tatoeba"
    assert record["id"] == 14037517
    assert record["text"] == "We all have to pull our weight."
    assert record["translations"] == [
        {"id": 99001, "text": "Nous devons tous faire notre part.", "lang": "fra"}
    ]
    assert "fetched_at" in record and record["fetched_at"]


def test_transient_timeout_is_retried_then_succeeds():
    with (
        mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[
                TimeoutError("The read operation timed out"),
                TimeoutError("The read operation timed out"),
                response(sentence_document()),
            ],
        ) as urlopen,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        records = fetch()

    assert len(records) == 1
    assert urlopen.call_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [2, 4]


def test_transient_connection_error_is_retried_then_succeeds():
    with (
        mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[urllib.error.URLError("Connection reset by peer"), response(sentence_document())],
        ),
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        records = fetch()

    assert len(records) == 1
    sleep.assert_called_once_with(2)


def test_persistent_transport_failure_raises_after_final_attempt():
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=TimeoutError("The read operation timed out")) as urlopen,
        mock.patch.object(MODULE.time, "sleep"),
    ):
        with pytest.raises(TimeoutError):
            fetch()

    assert urlopen.call_count == 3


def test_http_error_statuses_are_not_retried():
    error = urllib.error.HTTPError(MODULE.BASE, 503, "Service Unavailable", {}, None)
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        with pytest.raises(urllib.error.HTTPError):
            fetch()

    urlopen.assert_called_once()
    sleep.assert_not_called()


def test_malformed_json_is_not_retried():
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=Response(b"{not valid json")) as urlopen,
        mock.patch.object(MODULE.time, "sleep") as sleep,
    ):
        with pytest.raises(json.JSONDecodeError):
            fetch()

    urlopen.assert_called_once()
    sleep.assert_not_called()
