from __future__ import annotations

import contextlib
import http.client
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import socket
import sys
import tempfile
import subprocess
import threading
import time
from types import SimpleNamespace
from unittest import mock
import urllib.error
import urllib.parse

import pytest
import yaml


SCRIPT = Path(__file__).with_name("fetch_common_crawl_index.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "common_crawl_index.yml"
REPO_ROOT = SCRIPT.parents[2]
BASE_MODEL = REPO_ROOT / "transform" / "models" / "base" / "base_common_crawl_index.sql"
MART_DIR = REPO_ROOT / "transform" / "models" / "marts" / "common_crawl_index"
MART_MODEL = MART_DIR / "fct_common_crawl_index_observation.sql"
MART_METADATA = MART_DIR / "_common_crawl_index_models.yml"
RAW_GENERATOR = REPO_ROOT / "transform" / "scripts" / "sync_raw_sources.py"
LOADER_SCHEMA = REPO_ROOT / "load" / "loader" / "schema.py"
EXTRACT_RUNNER = REPO_ROOT / "orchestration" / "include" / "extract_runner.py"

SPEC = importlib.util.spec_from_file_location("fetch_common_crawl_index", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
try:
    SPEC.loader.exec_module(MODULE)
finally:
    sys.modules.pop(SPEC.name, None)

COLLECTION = "CC-MAIN-2026-30"
PATTERN = "https://www.commoncrawl.org/blog/*"
INDEX_URL = f"https://{MODULE.OFFICIAL_HOST}/{COLLECTION}-index"


class Response(io.BytesIO):
    def __init__(self, payload, *, url, headers=None, status=200, after_read=None):
        if not isinstance(payload, bytes):
            payload = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        super().__init__(payload)
        self.headers = headers or {}
        self.status = status
        self._url = url
        self._after_read = after_read

    def geturl(self):
        return self._url

    def read(self, size=-1):
        value = super().read(size)
        if self._after_read is not None:
            self._after_read()
        return value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class QueueOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, request, timeout):
        self.calls.append((request, timeout))
        if not self.responses:
            raise AssertionError(f"unexpected request: {request.full_url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

class DelayedAbortSocket:
    """Socket proxy whose discovered abort path would exceed the test guard."""

    def __init__(self, transport):
        self.transport = transport
        self.shutdown_calls = 0

    def shutdown(self, how):
        self.shutdown_calls += 1
        threading.Event().wait(TEST_JOIN_TIMEOUT * 2)
        self.transport.shutdown(how)

    def __getattr__(self, name):
        return getattr(self.transport, name)


class GuardedHTTPResponse(http.client.HTTPResponse):
    """Expose any attempt to close while its buffered read is active."""

    def __init__(self, sock):
        super().__init__(sock)
        self.read_active = threading.Event()
        self.close_during_read = False

    def read(self, amount=None):
        self.read_active.set()
        try:
            return super().read(amount)
        finally:
            self.read_active.clear()

    def close(self):
        active = self.read_active.is_set()
        self.close_during_read |= active
        if active:
            threading.Event().wait(TEST_JOIN_TIMEOUT * 2)
        super().close()


class SocketResponse:
    """A real socket-backed HTTPResponse with bounded test-side teardown."""

    def __init__(
        self, *, url, content_length, response_class=http.client.HTTPResponse
    ):
        self.client, self.server = socket.socketpair()
        self.server.sendall(
            (
                "HTTP/1.1 200 OK\r\n"
                f"Content-Length: {content_length}\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
        )
        self.response = response_class(self.client)
        self.response.begin()
        self.response.geturl = lambda: url
        self.stop = threading.Event()
        self.feeder = None
        self.sent = 0

    def feed(self, payload, *, interval):
        def send() -> None:
            for byte in payload:
                if self.stop.wait(interval):
                    return
                try:
                    self.server.sendall(bytes((byte,)))
                except OSError:
                    return
                self.sent += 1

        self.feeder = threading.Thread(target=send, daemon=True, name="common-crawl-test-feeder")
        self.feeder.start()

    def cleanup(self):
        self.stop.set()
        try:
            self.server.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.server.close()
        if self.feeder is not None:
            self.feeder.join(0.1)
        self.response.close()
        self.client.close()


class StalledHeaderOpener:
    """Own a socket-backed HTTPResponse whose header terminator never arrives."""

    def __init__(self):
        self.client, self.server = socket.socketpair()
        self.server.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n")
        self.calls = []
        self.owner = None
        self.response = None

    def bind_owner(self, owner):
        self.owner = owner
        owner.replace(self.client)

    def __call__(self, request, timeout):
        self.calls.append((request, timeout))
        self.response = http.client.HTTPResponse(self.client)
        try:
            self.response.begin()
        except BaseException:
            self.response.close()
            raise
        self.response.geturl = lambda: request.full_url
        return self.response

    def cleanup(self):
        if self.response is not None:
            self.response.close()
        for transport in (self.server, self.client):
            try:
                transport.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            transport.close()


DEADLINE_TOLERANCE = 0.15
TEST_JOIN_TIMEOUT = 0.5


def run_with_timeout(operation, cleanup):
    outcome = []

    def run():
        try:
            outcome.append((True, operation()))
        except BaseException as exc:
            outcome.append((False, exc))

    worker = threading.Thread(target=run, daemon=True, name="common-crawl-test-call")
    worker.start()
    worker.join(TEST_JOIN_TIMEOUT)
    hung = worker.is_alive()
    if hung:
        cleanup()
        worker.join(0.1)
    assert not hung, "deadline regression exceeded its bounded test timeout"
    assert outcome
    succeeded, value = outcome[0]
    if succeeded:
        return value
    raise value



def discovery(endpoint=INDEX_URL, collection=COLLECTION):
    return [
        {"id": "CC-MAIN-2026-26", "cdx-api": f"https://{MODULE.OFFICIAL_HOST}/CC-MAIN-2026-26-index"},
        {"id": collection, "name": "fixture", "cdx-api": endpoint},
    ]


def cdx_row(number=1, **changes):
    row = {
        "urlkey": f"org,commoncrawl)/blog/post-{number}",
        "timestamp": f"202609{number:02d}123456",
        "url": f"https://www.commoncrawl.org/blog/post-{number}",
        "mime": "text/html",
        "status": "200",
        "digest": f"sha1:FIXTURE{number}",
        "length": str(1000 + number),
        "offset": "10",
        "filename": "crawl-data/fixture.warc.gz",
    }
    row.update(changes)
    return row


def ndjson(*rows):
    return b"".join(
        (row if isinstance(row, bytes) else json.dumps(row).encode("utf-8")) + b"\n"
        for row in rows
    )


def index_url(parameters, endpoint=INDEX_URL):
    return endpoint + "?" + urllib.parse.urlencode(parameters)


def responses_for(
    *pages, endpoint=INDEX_URL, collection=COLLECTION, page_size=2, available_pages=None
):
    page_count = len(pages) if available_pages is None else available_pages
    values = [
        Response(discovery(endpoint, collection), url=MODULE.DISCOVERY_URL),
        Response(
            {"pages": page_count},
            url=index_url(
                {
                    "url": PATTERN,
                    "output": "json",
                    "pageSize": page_size,
                    "showNumPages": "true",
                },
                endpoint,
            ),
        ),
    ]
    for page_number, payload in enumerate(pages):
        values.append(
            Response(
                payload,
                url=index_url(
                    {"url": PATTERN, "output": "json", "pageSize": page_size, "page": page_number},
                    endpoint,
                ),
            )
        )
    return values


def fetch(responses, **changes):
    arguments = {
        "collection": COLLECTION,
        "url_pattern": PATTERN,
        "page_size": 2,
        "max_pages": 3,
        "max_records": 10,
        "max_bytes": 1024 * 1024,
        "max_duration": 60,
        "timeout": 5,
        "retries": 0,
    }
    arguments.update(changes)
    opener = QueueOpener(responses)
    records = MODULE.fetch_common_crawl_index(opener=opener, sleeper=lambda _: None, **arguments)
    return records, opener


def request_query(call):
    return urllib.parse.parse_qs(urllib.parse.urlsplit(call[0].full_url).query)


def test_discovers_one_collection_and_paginates_exact_narrow_query_metadata_only():
    records, opener = fetch(responses_for(ndjson(cdx_row(1), cdx_row(2)), ndjson(cdx_row(3))))

    assert [record["url"] for record in records] == [
        "https://www.commoncrawl.org/blog/post-1",
        "https://www.commoncrawl.org/blog/post-2",
        "https://www.commoncrawl.org/blog/post-3",
    ]
    assert {record["collection_id"] for record in records} == {COLLECTION}
    assert {record["query_url_pattern"] for record in records} == {PATTERN}
    assert [record["query_page"] for record in records] == [0, 0, 1]
    assert records[0]["content_length"] == 1001
    assert records[0]["crawl_timestamp"] == "20260901123456"
    assert records[0]["mime_type"] == "text/html"
    assert records[0]["status_code"] == "200"
    assert len(opener.calls) == 4
    assert opener.calls[0][0].full_url == MODULE.DISCOVERY_URL
    assert request_query(opener.calls[1]) == {
        "url": [PATTERN],
        "output": ["json"],
        "pageSize": ["2"],
        "showNumPages": ["true"],
    }
    for page, call in enumerate(opener.calls[2:]):
        parsed = urllib.parse.urlsplit(call[0].full_url)
        assert parsed.scheme == "https"
        assert parsed.hostname == MODULE.OFFICIAL_HOST
        assert parsed.path == f"/{COLLECTION}-index"
        assert request_query(call) == {
            "url": [PATTERN],
            "output": ["json"],
            "pageSize": ["2"],
            "page": [str(page)],
        }
        assert "filename" not in parsed.path
        assert ".warc" not in call[0].full_url
        assert call[0].get_header("Accept") == "application/json"
        assert call[0].get_header("User-agent") == MODULE.USER_AGENT
        assert 0 < call[1] <= 5
    assert MODULE.LAST_RUN_METADATA["pages"] == 2
    assert MODULE.LAST_RUN_METADATA["truncated"] is False
    assert MODULE.LAST_RUN_METADATA["requests"]["succeeded"] == 4


def test_empty_page_is_successful_complete_empty_result():
    records, opener = fetch(responses_for(), page_size=2)
    assert records == []
    assert len(opener.calls) == 2
    assert MODULE.LAST_RUN_METADATA["records"] == 0
    assert MODULE.LAST_RUN_METADATA["truncated"] is False


@pytest.mark.parametrize(
    ("endpoint", "message"),
    [
        ("http://index.commoncrawl.org/CC-MAIN-2026-30-index", "official HTTPS"),
        ("https://evil.example/CC-MAIN-2026-30-index", "official HTTPS"),
        ("https://index.commoncrawl.org/other-index", "unexpected"),
        ("https://index.commoncrawl.org/CC-MAIN-2026-30-index?url=*", "unexpected"),
    ],
)
def test_rejects_discovered_nonofficial_or_changed_endpoint(endpoint, message):
    opener = QueueOpener([Response(discovery(endpoint), url=MODULE.DISCOVERY_URL)])
    with pytest.raises(MODULE.CommonCrawlError, match=message):
        MODULE.fetch_common_crawl_index(
            collection=COLLECTION, url_pattern=PATTERN, opener=opener, retries=0
        )
    assert len(opener.calls) == 1


def test_rejects_redirect_that_changes_endpoint_or_host():
    changed_path = Response(discovery(), url=f"https://{MODULE.OFFICIAL_HOST}/elsewhere")
    with pytest.raises(MODULE.CommonCrawlError, match="changed the requested endpoint"):
        fetch([changed_path])

    changed_host = Response(discovery(), url="https://evil.example/collinfo.json")
    with pytest.raises(MODULE.CommonCrawlError, match="official HTTPS"):
        fetch([changed_host])


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"collection": ""}, "collection"),
        ({"collection": "CC-MAIN-*"}, "collection"),
        ({"url_pattern": "*"}, "http or https"),
        ({"url_pattern": "https://*.example.org/path/*"}, "host"),
        ({"url_pattern": "https://example.org/*"}, "named path prefix"),
        ({"url_pattern": "https://example.org/a/*/b"}, "trailing path wildcard"),
        ({"url_pattern": "https://user@example.org/a/*"}, "credentials"),
        ({"url_pattern": "https://example.org:443/a/*"}, "port"),
        ({"page_size": 0}, "page_size"),
        ({"max_pages": MODULE.HARD_MAX_PAGES + 1}, "max_pages"),
        ({"max_records": 0}, "max_records"),
        ({"max_bytes": 0}, "max_bytes"),
        ({"max_duration": 0}, "max_duration"),
        ({"timeout": MODULE.MAX_TIMEOUT + 1}, "timeout"),
        ({"retries": MODULE.MAX_RETRIES + 1}, "retries"),
    ],
)
def test_invalid_configuration_fails_before_discovery(changes, message):
    arguments = {"collection": COLLECTION, "url_pattern": PATTERN}
    arguments.update(changes)
    opener = mock.Mock()
    with pytest.raises(ValueError, match=message):
        MODULE.fetch_common_crawl_index(opener=opener, **arguments)
    opener.assert_not_called()


def test_duplicate_identity_is_collection_aware_and_deduplicated_within_collection():
    duplicate = cdx_row(1)
    records, _ = fetch(responses_for(ndjson(duplicate, duplicate), page_size=3), page_size=3)
    assert len(records) == 1
    assert MODULE.LAST_RUN_METADATA["duplicate_records"] == 1
    assert MODULE.LAST_RUN_METADATA["skipped_records"] == 1

    second_collection = "CC-MAIN-2026-34"
    second_endpoint = f"https://{MODULE.OFFICIAL_HOST}/{second_collection}-index"
    second_responses = responses_for(
        ndjson(duplicate), endpoint=second_endpoint, collection=second_collection, page_size=2
    )
    second, _ = fetch(second_responses, collection=second_collection)
    assert second[0]["id"] != records[0]["id"]


def test_malformed_missing_and_invalid_rows_are_skipped_observably():
    missing = cdx_row(2)
    missing.pop("digest")
    invalid = cdx_row(3, length="not-an-integer")
    payload = ndjson(cdx_row(1), b"not-json", missing, invalid)
    records, _ = fetch(responses_for(payload, page_size=5), page_size=5)

    assert len(records) == 1
    assert MODULE.LAST_RUN_METADATA["malformed_lines"] == 1
    assert MODULE.LAST_RUN_METADATA["missing_field_records"] == 2
    assert MODULE.LAST_RUN_METADATA["skipped_records"] == 3
    summary = MODULE._summary()
    assert summary["health"] == "degraded"
    assert summary["completeness"] == "partial"


def test_record_and_page_ceilings_report_truncation():
    records, _ = fetch(
        responses_for(ndjson(cdx_row(1), cdx_row(2))),
        max_records=1,
    )
    assert len(records) == 1
    assert MODULE.LAST_RUN_METADATA["truncation_reason"] == "max_records"

    records, opener = fetch(
        responses_for(
            ndjson(cdx_row(1), cdx_row(2)), available_pages=2
        ),
        max_pages=1,
    )
    assert len(records) == 2
    assert len(opener.calls) == 3
    assert MODULE.LAST_RUN_METADATA["truncation_reason"] == "max_pages"


def test_streamed_byte_ceiling_includes_discovery_and_truncates_query_observably():
    discovery_payload = json.dumps(discovery(), separators=(",", ":")).encode("utf-8")
    count_payload = b'{"pages":1}'
    page = ndjson(cdx_row(1), cdx_row(2))
    responses = [
        Response(discovery_payload, url=MODULE.DISCOVERY_URL),
        Response(
            count_payload,
            url=index_url(
                {"url": PATTERN, "output": "json", "pageSize": 2, "showNumPages": "true"}
            ),
        ),
        Response(
            page,
            url=index_url({"url": PATTERN, "output": "json", "pageSize": 2, "page": 0}),
        ),
    ]
    maximum = len(discovery_payload) + len(count_payload) + len(page) - 5
    records, _ = fetch(responses, max_bytes=maximum)
    assert len(records) == 1
    assert MODULE.LAST_RUN_METADATA["bytes_streamed"] == maximum
    assert MODULE.LAST_RUN_METADATA["truncation_reason"] == "max_bytes"
    assert MODULE._summary()["completeness"] == "partial"


def test_discovery_cannot_exceed_byte_ceiling_as_an_empty_success():
    body = json.dumps(discovery()).encode("utf-8")
    opener = QueueOpener([Response(body, url=MODULE.DISCOVERY_URL)])
    with pytest.raises(MODULE.BudgetExceeded, match="discovery"):
        MODULE.fetch_common_crawl_index(
            collection=COLLECTION,
            url_pattern=PATTERN,
            max_bytes=len(body) - 1,
            retries=0,
            opener=opener,
        )


def test_duration_ceiling_truncates_page_and_is_observable():
    class Clock:
        value = 0.0

        def __call__(self):
            return self.value

    clock = Clock()
    responses = responses_for(ndjson(cdx_row(1), cdx_row(2)))
    responses[-1]._after_read = lambda: setattr(clock, "value", 2.0)
    records, _ = fetch(
        responses,
        clock=clock,
        max_duration=1,
    )
    assert records == []
    assert MODULE.LAST_RUN_METADATA["truncation_reason"] == "max_duration"
    assert MODULE.LAST_RUN_METADATA["duration_seconds"] >= 2

def test_stalled_read_uses_remaining_absolute_budget_after_earlier_work():
    payload = json.dumps(discovery(), separators=(",", ":")).encode("utf-8")
    first = Response(
        payload,
        url=MODULE.DISCOVERY_URL,
        headers={"Content-Length": str(len(payload))},
        after_read=lambda: time.sleep(0.05),
    )
    stalled = SocketResponse(url=index_url(
        {"url": PATTERN, "output": "json", "pageSize": 2, "showNumPages": "true"}
    ), content_length=11)
    opener = QueueOpener([first, stalled.response])
    maximum = 0.08
    started = time.monotonic()

    try:
        with pytest.raises(MODULE.BudgetExceeded, match="duration ceiling"):
            run_with_timeout(
                lambda: MODULE.fetch_common_crawl_index(
                    collection=COLLECTION,
                    url_pattern=PATTERN,
                    page_size=2,
                    max_duration=maximum,
                    timeout=1,
                    retries=0,
                    opener=opener,
                ),
                stalled.cleanup,
            )
        elapsed = time.monotonic() - started
        assert maximum - 0.015 <= elapsed <= maximum + DEADLINE_TOLERANCE
        assert len(opener.calls) == 2
        assert MODULE.LAST_RUN_METADATA["requests"]["failed"] == 1
        assert MODULE.LAST_RUN_METADATA["requests"]["payload_bytes"] == len(payload)
        assert stalled.response.isclosed()
        assert stalled.client.fileno() == -1
        assert not any(
            thread.name == "common-crawl-watchdog" and thread.is_alive()
            for thread in threading.enumerate()
        )
    finally:
        stalled.cleanup()


def test_drip_fed_socket_read_cannot_refresh_absolute_deadline():
    payload = json.dumps(discovery(), separators=(",", ":")).encode("utf-8")
    dripping = SocketResponse(url=MODULE.DISCOVERY_URL, content_length=len(payload))
    dripping.feed(payload, interval=0.01)
    opener = QueueOpener([dripping.response])
    maximum = 0.06
    started = time.monotonic()

    try:
        with pytest.raises(MODULE.BudgetExceeded, match="duration ceiling"):
            run_with_timeout(
                lambda: MODULE.fetch_common_crawl_index(
                    collection=COLLECTION,
                    url_pattern=PATTERN,
                    max_duration=maximum,
                    timeout=1,
                    retries=0,
                    opener=opener,
                ),
                dripping.cleanup,
            )
        elapsed = time.monotonic() - started
        dripping.cleanup()
        assert maximum - 0.015 <= elapsed <= maximum + DEADLINE_TOLERANCE
        assert 0 < dripping.sent < len(payload)
        assert len(opener.calls) == 1
        assert MODULE.LAST_RUN_METADATA["requests"]["payload_bytes"] == 0
        assert MODULE.LAST_RUN_METADATA["requests"]["failed"] == 1
        assert dripping.response.isclosed()
        assert dripping.client.fileno() == -1
        assert dripping.feeder is not None and not dripping.feeder.is_alive()
    finally:
        dripping.cleanup()


def test_repeated_stalled_response_headers_cancel_owned_transport_at_deadline():
    maximum = 0.025
    for _ in range(3):
        opener = StalledHeaderOpener()
        started = time.monotonic()
        try:
            with pytest.raises(MODULE.BudgetExceeded, match="duration ceiling"):
                run_with_timeout(
                    lambda: MODULE.fetch_common_crawl_index(
                        collection=COLLECTION,
                        url_pattern=PATTERN,
                        max_duration=maximum,
                        timeout=1,
                        retries=0,
                        opener=opener,
                    ),
                    opener.cleanup,
                )
            elapsed = time.monotonic() - started
            assert maximum - 0.01 <= elapsed <= maximum + DEADLINE_TOLERANCE
            assert len(opener.calls) == 1
            assert MODULE.LAST_RUN_METADATA["requests"]["failed"] == 1
            assert MODULE.LAST_RUN_METADATA["requests"]["payload_bytes"] == 0
            opener.cleanup()
            assert opener.client.fileno() == -1
            assert not any(
                thread.name == "common-crawl-watchdog" and thread.is_alive()
                for thread in threading.enumerate()
            )
        finally:
            opener.cleanup()


def test_cancellation_does_not_close_response_while_buffered_read_holds_lock():
    stalled = SocketResponse(
        url=MODULE.DISCOVERY_URL,
        content_length=1,
        response_class=GuardedHTTPResponse,
    )
    delayed_abort = DelayedAbortSocket(stalled.response.fp.raw._sock)
    stalled.response.fp.raw._sock = delayed_abort
    opener = QueueOpener([stalled.response])
    maximum = 0.05
    started = time.monotonic()

    try:
        with pytest.raises(MODULE.BudgetExceeded, match="duration ceiling"):
            run_with_timeout(
                lambda: MODULE.fetch_common_crawl_index(
                    collection=COLLECTION,
                    url_pattern=PATTERN,
                    max_duration=maximum,
                    timeout=1,
                    retries=0,
                    opener=opener,
                ),
                stalled.cleanup,
            )
        elapsed = time.monotonic() - started
        assert maximum - 0.015 <= elapsed <= maximum + DEADLINE_TOLERANCE
        assert delayed_abort.shutdown_calls == 0
        assert stalled.response.isclosed()
        assert stalled.client.fileno() == -1
        assert not stalled.response.close_during_read
        assert len(opener.calls) == 1
        assert MODULE.LAST_RUN_METADATA["requests"]["failed"] == 1
        assert not any(
            thread.name == "common-crawl-watchdog" and thread.is_alive()
            for thread in threading.enumerate()
        )
    finally:
        stalled.cleanup()

def http_error(code, headers=None):
    return urllib.error.HTTPError(
        MODULE.DISCOVERY_URL, code, "fixture", headers or {}, io.BytesIO(b"provider error")
    )


@pytest.mark.parametrize("status", [429, 500, 503])
def test_throttling_and_5xx_retry_are_bounded_and_counted(status):
    count_url = index_url(
        {"url": PATTERN, "output": "json", "pageSize": 2, "showNumPages": "true"}
    )
    opener = QueueOpener(
        [
            http_error(status, {"Retry-After": "0"}),
            Response(discovery(), url=MODULE.DISCOVERY_URL),
            Response({"pages": 0}, url=count_url),
        ]
    )
    records = MODULE.fetch_common_crawl_index(
        collection=COLLECTION,
        url_pattern=PATTERN,
        page_size=2,
        retries=1,
        opener=opener,
        sleeper=lambda _: None,
    )
    assert records == []
    assert MODULE.LAST_RUN_METADATA["requests"] == {
        "attempted": 3,
        "succeeded": 2,
        "failed": 1,
        "retries": 1,
        "payload_bytes": MODULE.LAST_RUN_METADATA["bytes_streamed"],
        "backoff_s": 0.0,
    }


def test_timeout_after_bounded_retry_is_failed_not_empty_success():
    opener = QueueOpener([TimeoutError("fixture timeout"), TimeoutError("fixture timeout")])
    with pytest.raises(MODULE.CommonCrawlError, match="failed after 2 attempts"):
        MODULE.fetch_common_crawl_index(
            collection=COLLECTION,
            url_pattern=PATTERN,
            retries=1,
            opener=opener,
            sleeper=lambda _: None,
        )
    assert len(opener.calls) == 2

    stdout = io.StringIO()
    stderr = io.StringIO()
    with mock.patch.object(MODULE, "_open_official", side_effect=TimeoutError("fixture timeout")):
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = MODULE.main(
                [
                    "--collection", COLLECTION,
                    "--url-pattern", PATTERN,
                    "--retries", "0",
                ]
            )
    assert result == 1
    assert stdout.getvalue() == ""
    summary_line = stderr.getvalue().strip()
    assert summary_line.startswith(MODULE.SUMMARY_PREFIX)
    summary = json.loads(summary_line.removeprefix(MODULE.SUMMARY_PREFIX))
    assert summary["health"] == "failed"
    assert summary["completeness"] == "failed"
    assert "timeout" in summary["error"]


def test_actual_cli_and_configured_argv_emit_ndjson_and_required_summary():
    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
    args = MODULE.build_parser().parse_args(config["args"])
    assert config["name"] == MODULE.SOURCE
    assert config["script"] == SCRIPT.name
    assert config["enabled"] is False
    assert config["sink"] == "local"
    assert config["run_summary"]["required"] is True
    assert config["cadence"]["auto"] is False
    assert args.collection == COLLECTION
    assert MODULE.validate_url_pattern(args.url_pattern) == PATTERN
    assert args.max_pages <= MODULE.HARD_MAX_PAGES
    assert args.max_records <= MODULE.HARD_MAX_RECORDS
    assert args.max_bytes <= MODULE.HARD_MAX_BYTES
    assert args.max_duration <= MODULE.HARD_MAX_DURATION
    assert args.max_duration <= config["timeout_minutes"] * 60

    opener = QueueOpener(
        responses_for(ndjson(cdx_row(1)), page_size=args.page_size)
    )
    stdout = io.StringIO()
    stderr = io.StringIO()
    with mock.patch.object(MODULE, "_open_official", side_effect=opener):
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            result = MODULE.main(config["args"])
    assert result == 0
    output_lines = stdout.getvalue().splitlines()
    assert len(output_lines) == 1
    record = json.loads(output_lines[0])
    assert {
        "urlkey", "crawl_timestamp", "url", "mime_type", "status_code", "digest", "content_length"
    } <= set(record)
    assert record["collection_id"] == COLLECTION
    assert record["query_url_pattern"] == PATTERN
    assert "filename" not in record
    summary = json.loads(stderr.getvalue().strip().removeprefix(MODULE.SUMMARY_PREFIX))
    assert summary["health"] == "healthy"
    assert summary["completeness"] == "complete"
    assert summary["records"] == 1
    assert summary["metrics"]["truncated"] is False


def test_ndjson_satisfies_existing_extractor_envelope_contract():
    record = MODULE._normalize_record(
        cdx_row(1), collection=COLLECTION, pattern=PATTERN,
        index_url=INDEX_URL, page=0, fetched_at="2026-09-17T12:00:00+00:00"
    )
    line = json.dumps(record, separators=(",", ":"))
    include_dir = EXTRACT_RUNNER.parent
    sys.path.insert(0, str(include_dir))
    try:
        spec = importlib.util.spec_from_file_location("extract_runner", EXTRACT_RUNNER)
        runner = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(runner)
    finally:
        sys.path.remove(str(include_dir))
    runner._check_envelope(line)


def _load_module(path, name, mocked_modules=None):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    with mock.patch.dict(sys.modules, mocked_modules or {}):
        sys.modules[name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            sys.modules.pop(name, None)
    return module


def test_fixture_integrates_with_raw_schema_generator_base_and_mart_execution():
    duckdb = pytest.importorskip("duckdb")
    record = MODULE._normalize_record(
        cdx_row(1), collection=COLLECTION, pattern=PATTERN,
        index_url=INDEX_URL, page=0, fetched_at="2026-09-17T12:00:00+00:00"
    )
    loader = _load_module(LOADER_SCHEMA, "common_crawl_loader_schema")
    settings = SimpleNamespace(
        name=MODULE.SOURCE,
        column_types={"source": "string", "id": "string", "fetched_at": "timestamp"},
        exclude_keys=[],
        schema_detection="auto",
        detect_temporal=True,
        keep_payload=True,
    )
    inferred = loader.infer_columns([record], settings)
    columns = loader.table_columns(inferred, settings)
    types = {column.name: column.type for column in columns}
    assert types["source"] == "string"
    assert types["fetched_at"] == "timestamp"
    assert types["query_page"] == "integer"
    assert types["content_length"] == "integer"
    assert types["collection_id"] == "string"
    assert types["url"] == "string"

    generator = _load_module(
        RAW_GENERATOR, "common_crawl_raw_generator", {"duckdb": mock.MagicMock()}
    )
    native_types = {
        "string": "VARCHAR", "integer": "BIGINT", "double": "DOUBLE",
        "boolean": "BOOLEAN", "date": "DATE", "timestamp": "TIMESTAMP WITH TIME ZONE",
        "json": "JSON",
    }
    raw_source = generator.RawSource(
        MODULE.SOURCE,
        tuple(generator.Column(column.name, native_types[column.type]) for column in columns),
        2,
        "2026-09-17T12:01:00+00:00",
    )
    with tempfile.TemporaryDirectory() as directory:
        project_dir = Path(directory)
        changed = generator.write_all(project_dir, [raw_source])
        assert changed == [
            "models/base/_raw_sources.yml",
            f"models/base/base_{MODULE.SOURCE}.sql",
        ]
        generated = yaml.safe_load((project_dir / generator.SOURCE_YAML).read_text())
        generated_sql = (
            project_dir / "models" / "base" / f"base_{MODULE.SOURCE}.sql"
        ).read_text()
    table = generated["sources"][0]["tables"][0]
    assert table["name"] == MODULE.SOURCE
    declared = {column["name"]: column["data_type"] for column in table["columns"]}
    assert declared["content_length"] == "BIGINT"
    assert declared["fetched_at"] == "TIMESTAMP WITH TIME ZONE"
    assert f"source('raw', '{MODULE.SOURCE}')" in generated_sql
    checked_schema = yaml.safe_load(
        (REPO_ROOT / "transform" / "models" / "base" / "_raw_sources.yml").read_text()
    )
    checked_table = next(
        table
        for table in checked_schema["sources"][0]["tables"]
        if table["name"] == MODULE.SOURCE
    )
    assert checked_table == table
    assert [item["name"] for item in checked_schema["sources"][0]["tables"]] == sorted(
        item["name"] for item in checked_schema["sources"][0]["tables"]
    )
    checked_base_sql = re.sub(
        r"\{\{\s*config\(.*?\)\s*\}\}\s*", "", BASE_MODEL.read_text(), count=1, flags=re.S
    )
    assert checked_base_sql == generated_sql


    connection = duckdb.connect(":memory:")
    connection.execute(
        """create table raw_common_crawl_index (
        _row_id varchar, _source varchar, _batch_id varchar, _source_file varchar,
        _file_row_num bigint, _dt date, _extract_started_at timestamptz,
        _load_id varchar, _loaded_at timestamptz, _content_hash varchar, _payload json,
        source varchar, fetched_at timestamptz, id varchar, collection_id varchar,
        query_url_pattern varchar, index_url varchar, query_page bigint, urlkey varchar,
        url varchar, crawl_timestamp varchar, mime_type varchar, status_code varchar,
        digest varchar, content_length bigint)"""
    )
    values = (
        "row-1", MODULE.SOURCE, "batch-1", "fixture.ndjson", 1, "2026-09-17",
        "2026-09-17T12:00:00+00:00", "load-1", "2026-09-17T12:01:00+00:00",
        "hash-old", json.dumps(record), record["source"], record["fetched_at"], record["id"],
        record["collection_id"], record["query_url_pattern"], record["index_url"],
        record["query_page"], record["urlkey"], record["url"], record["crawl_timestamp"],
        record["mime_type"], record["status_code"], record["digest"], record["content_length"],
    )
    connection.execute("insert into raw_common_crawl_index values (" + ",".join(["?"] * len(values)) + ")", values)
    duplicate = list(values)
    duplicate[0] = "row-2"
    duplicate[7] = "load-2"
    duplicate[8] = "2026-09-17T12:02:00+00:00"
    duplicate[9] = "hash-new"
    connection.execute(
        "insert into raw_common_crawl_index values (" + ",".join(["?"] * len(duplicate)) + ")",
        duplicate,
    )

    base_sql = re.sub(
        r"\{\{\s*config\(.*?\)\s*\}\}\s*", "", BASE_MODEL.read_text(), count=1, flags=re.S
    ).replace("{{ source('raw', 'common_crawl_index') }}", "raw_common_crawl_index")
    connection.execute("create view base_common_crawl_index as " + base_sql)
    mart_sql = MART_MODEL.read_text()
    mart_sql = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}\s*", "", mart_sql, count=1, flags=re.S)
    mart_sql = mart_sql.replace("{{ ref('base_common_crawl_index') }}", "base_common_crawl_index")
    connection.execute("create table fct_common_crawl_index_observation as " + mart_sql)
    row = connection.execute(
        """select collection_id, capture_id, crawled_at, content_length,
                  query_url_pattern, index_url, _batch_id, _load_id,
                  _source_file, _file_row_num, source_loaded_at, _content_hash
           from fct_common_crawl_index_observation"""
    ).fetchone()
    assert row is not None
    assert connection.execute("select count(*) from fct_common_crawl_index_observation").fetchone()[0] == 1
    assert row[0] == COLLECTION
    assert row[1] == record["id"]
    assert str(row[2]).startswith("2026-09-01 12:34:56")
    assert row[3:6] == (1001, PATTERN, INDEX_URL)
    assert row[6:10] == ("batch-1", "load-2", "fixture.ndjson", 1)
    assert str(row[10]).startswith("2026-09-17 12:02:00")
    assert row[11] == "hash-new"


def test_mart_contract_metadata_matches_sql_fields_and_lineage():
    metadata = yaml.safe_load(MART_METADATA.read_text())
    model = metadata["models"][0]
    assert model["name"] == "fct_common_crawl_index_observation"
    assert model["config"]["contract"]["enforced"] is True
    assert model["config"]["meta"]["grain"] == (
        "source_name, collection_id, capture_id, observed_at"
    )
    names = [column["name"] for column in model["columns"]]
    assert len(names) == len(set(names))
    for field in (
        "collection_id", "capture_id", "observed_at", "crawled_at", "url",
        "mime_type", "status_code", "digest", "content_length", "query_url_pattern",
        "index_url", "query_page", "_batch_id", "_load_id", "_source_file",
        "_file_row_num", "source_loaded_at", "_content_hash",
    ):
        assert field in names
    sql = MART_MODEL.read_text()
    assert "partition by common_crawl_index_observation_key" in sql
    assert "order by source_loaded_at desc" in sql
    assert "ref('base_common_crawl_index')" in sql


def _dbt_parse(project_dir, profiles_dir, target_path, *, enabled):
    arguments = [
        "dbt", "parse",
        "--project-dir", str(project_dir),
        "--profiles-dir", str(profiles_dir),
        "--target-path", str(target_path),
        "--no-use-colors",
        "--quiet",
    ]
    if enabled:
        arguments.extend(["--vars", json.dumps({"common_crawl_index_enabled": True})])
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(profiles_dir),
        "DBT_LOG_PATH": str(profiles_dir / "logs"),
        "RAYON_NUM_THREADS": "1",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    completed = subprocess.run(
        arguments,
        cwd=project_dir,
        env=environment,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if completed.returncode:
        output = completed.stderr + "\n" + completed.stdout
        redacted = re.sub(
            r"(?i)(password|token|secret)(\s*[:=]\s*)\S+",
            r"\1\2<redacted>",
            output,
        )
        pytest.fail(
            f"dbt parse exited {completed.returncode}: {redacted[-4000:]}",
            pytrace=False,
        )
    return json.loads((target_path / "manifest.json").read_text())


def test_actual_dbt_parse_resolves_gated_common_crawl_lineage():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        profiles = root / "profiles"
        profiles.mkdir()
        (profiles / "profiles.yml").write_text(
            """vintage_data:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ':memory:'
      schema: transform
      threads: 1
""",
            encoding="utf-8",
        )
        default_manifest = _dbt_parse(
            REPO_ROOT / "transform", profiles, root / "target-default", enabled=False
        )
        enabled_manifest = _dbt_parse(
            REPO_ROOT / "transform", profiles, root / "target-enabled", enabled=True
        )

    source_id = "source.vintage_data.raw.common_crawl_index"
    base_id = "model.vintage_data.base_common_crawl_index"
    mart_id = "model.vintage_data.fct_common_crawl_index_observation"
    assert source_id in default_manifest["sources"]
    disabled_ids = {
        node["unique_id"]
        for group in default_manifest["disabled"].values()
        for node in group
    }
    assert {base_id, mart_id} <= disabled_ids
    assert base_id not in default_manifest["nodes"]
    assert mart_id not in default_manifest["nodes"]

    assert source_id in enabled_manifest["sources"]
    assert enabled_manifest["nodes"][base_id]["depends_on"]["nodes"] == [source_id]
    assert base_id in enabled_manifest["nodes"][mart_id]["depends_on"]["nodes"]
