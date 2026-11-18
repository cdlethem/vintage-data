import importlib.util
import io
import json
from pathlib import Path
import urllib.error
from unittest import mock

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "fetch_ripe_ris_live.py"
SPEC = importlib.util.spec_from_file_location("fetch_ripe_ris_live", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def update(message_id, **changes):
    value = {
        "type": "UPDATE",
        "id": message_id,
        "timestamp": 1763481600.123,
        "host": "rrc21",
        "peer": "198.32.176.1",
        "peer_asn": 6447,
        "path": [6447, 131072],
        "origin": 131072,
        "announcements": [{"prefix": "198.51.100.0/24"}],
        "withdrawals": [],
    }
    value.update(changes)
    return value


def stream_line(message, **changes):
    value = {"data": message}
    value.update(changes)
    return (json.dumps(value) + "\n").encode("utf-8")


class Stream(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def stream(lines):
    return Stream(b"".join(lines))


def http_error(code, retry_after=None):
    headers = {} if retry_after is None else {"Retry-After": retry_after}
    return urllib.error.HTTPError(MODULE.BASE, code, "error", headers, None)


def drain(max_messages=2, return_value=None, side_effect=None):
    """Consume stream_messages against mocked urlopen; return (records, urlopen, sleep)."""
    kwargs = (
        {"return_value": return_value} if return_value is not None else {"side_effect": side_effect}
    )
    with mock.patch.object(MODULE.urllib.request, "urlopen", **kwargs) as urlopen, \
            mock.patch.object(MODULE.time, "sleep") as sleep:
        records = list(MODULE.stream_messages(max_messages=max_messages))
    return records, urlopen, sleep


def test_stream_yields_update_records_with_envelope_and_skips_keepalives():
    lines = [
        stream_line(update("a-1")),
        stream_line({"type": "KEEPALIVE", "id": "k-1"}),
        stream_line(update("a-2")),
        b"",
    ]
    records, urlopen, sleep = drain(return_value=stream(lines))

    assert [record["id"] for record in records] == ["a-1", "a-2"]
    assert all(record["source"] == "ripe_ris_live" for record in records)
    assert all(record["type"] == "UPDATE" for record in records)
    assert records[0]["announcements"] == [{"prefix": "198.51.100.0/24"}]
    assert urlopen.call_count == 1
    sleep.assert_not_called()


def test_transient_502_is_retried_then_succeeds():
    effects = [
        http_error(502),
        stream([stream_line(update("a-1")), stream_line(update("a-2"))]),
    ]
    records, urlopen, sleep = drain(side_effect=effects)

    assert [record["id"] for record in records] == ["a-1", "a-2"]
    assert urlopen.call_count == 2
    assert [call.args[0] for call in sleep.call_args_list] == [MODULE.RETRY_BASE_DELAY]


def test_persistent_502_exhausts_attempts_and_raises():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=http_error(502)
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(urllib.error.HTTPError):
            list(MODULE.stream_messages(max_messages=2))
    assert urlopen.call_count == MODULE.MAX_ATTEMPTS
    assert [call.args[0] for call in sleep.call_args_list] == [
        MODULE.RETRY_BASE_DELAY,
        MODULE.RETRY_BASE_DELAY * 2,
        MODULE.RETRY_BASE_DELAY * 4,
    ]


def test_nonretryable_http_error_fails_immediately():
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", side_effect=http_error(403)
    ) as urlopen, mock.patch.object(MODULE.time, "sleep") as sleep:
        with pytest.raises(urllib.error.HTTPError):
            list(MODULE.stream_messages(max_messages=2))
    assert urlopen.call_count == 1
    sleep.assert_not_called()


def test_transport_error_is_retried_then_succeeds():
    effects = [
        urllib.error.URLError("connection reset by peer"),
        stream([stream_line(update("a-1")), stream_line(update("a-2"))]),
    ]
    records, urlopen, sleep = drain(side_effect=effects)

    assert [record["id"] for record in records] == ["a-1", "a-2"]
    assert urlopen.call_count == 2
    assert [call.args[0] for call in sleep.call_args_list] == [MODULE.RETRY_BASE_DELAY]


@pytest.mark.parametrize(
    "retry_after, delay",
    [
        ("2", 2.0),
        ("45", MODULE.MAX_RETRY_DELAY),
    ],
)
def test_retry_after_header_is_honored_within_the_bound(retry_after, delay):
    effects = [
        http_error(502, retry_after),
        stream([stream_line(update("a-1")), stream_line(update("a-2"))]),
    ]
    records, urlopen, sleep = drain(side_effect=effects)

    assert [record["id"] for record in records] == ["a-1", "a-2"]
    assert [call.args[0] for call in sleep.call_args_list] == [delay]
