import contextlib
import importlib.util
import io
import json
from pathlib import Path
from unittest import mock
import urllib.error

import pytest
import yaml


SCRIPT = Path(__file__).parent / "scripts" / "fetch_common_crawl_index.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "common_crawl_index.yml"
SPEC = importlib.util.spec_from_file_location("fetch_common_crawl_index", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
FETCHED_AT = "2026-09-21T12:34:56+00:00"


def index_entry(collection_id, name="Example crawl"):
    base = f"https://index.commoncrawl.org/{collection_id}"
    return {
        "id": collection_id,
        "name": name,
        "timegate": f"{base}/",
        "cdx-api": f"{base}-index",
        "cdx-toolkit": f"{base}-index",
    }


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, status=200, headers=None):
        if isinstance(document, bytes):
            body = document
        else:
            body = json.dumps(document).encode("utf-8")
        super().__init__(body)
        self.status = status
        self.headers = {} if headers is None else headers
        self.read_sizes = []

    def read(self, size=-1):
        self.read_sizes.append(size)
        return super().read(size)

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def test_successful_fixture_is_one_bounded_request_and_deterministic_ndjson():
    document = [
        index_entry("CC-MAIN-2026-38", "September 2026 Index"),
        index_entry("CC-MAIN-2026-30", "July 2026 Index"),
    ]
    response = JsonResponse(document)
    stdout = io.StringIO()
    stderr = io.StringIO()

    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response) as urlopen,
        mock.patch.object(MODULE, "_utc_now", return_value=FETCHED_AT),
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        status = MODULE.main(["--timeout", "17"])

    assert status == 0
    assert stderr.getvalue() == ""
    lines = stdout.getvalue().splitlines()
    records = [json.loads(line) for line in lines]
    assert [record["id"] for record in records] == [
        "CC-MAIN-2026-30",
        "CC-MAIN-2026-38",
    ]
    assert all(record["source"] == MODULE.SOURCE for record in records)
    assert all(record["fetched_at"] == FETCHED_AT for record in records)
    assert records[0]["cdx_api"].endswith("CC-MAIN-2026-30-index")
    assert records[0]["raw"] == document[1]
    assert lines == [
        json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
        for record in records
    ]

    urlopen.assert_called_once()
    request = urlopen.call_args.args[0]
    assert request.full_url == MODULE.URL
    assert request.get_method() == "GET"
    assert request.get_header("Accept") == "application/json"
    assert request.get_header("User-agent") == MODULE.USER_AGENT
    assert urlopen.call_args.kwargs == {"timeout": 17}
    assert response.read_sizes == [MODULE.MAX_RESPONSE_BYTES + 1]


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"indexes": []}, "expected an array"),
        ([], "index list is empty"),
        ([{"name": "missing id"}], "id must be a nonempty string"),
        ([index_entry("duplicate"), index_entry("duplicate")], "duplicate collection id"),
        ([{**index_entry("bad-url"), "cdx-api": "http://example.test/index"}], "public HTTPS URL"),
    ],
)
def test_malformed_responses_fail_without_partial_stdout(document, message):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)),
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        status = MODULE.main([])

    assert status == 1
    assert stdout.getvalue() == ""
    assert message in stderr.getvalue()
    assert "Traceback" not in stderr.getvalue()


def test_malformed_json_diagnostic_is_bounded_redacted_and_nonzero():
    secret = "do-not-print-this-secret"
    body = (
        b'{"password":"do-not-print-this-secret","url":"https://user:pass@example.test/",'
        + (b"x" * 5000)
    )
    stdout = io.StringIO()
    stderr = io.StringIO()
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=JsonResponse(body)),
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        status = MODULE.main([])

    diagnostic = stderr.getvalue()
    assert status == 1
    assert stdout.getvalue() == ""
    assert "malformed JSON" in diagnostic
    assert secret not in diagnostic
    assert "user" not in diagnostic
    assert len(diagnostic.splitlines()) == 1
    assert len(diagnostic) <= len(f"{MODULE.SOURCE}: \n") + MODULE.MAX_DIAGNOSTIC_CHARS


def test_deeply_nested_json_fails_without_traceback_or_output():
    secret = b"password=deep-secret"
    body = (b"[" * 100_000) + b'"' + secret + b'"' + (b"]" * 100_000)
    assert len(body) <= MODULE.MAX_RESPONSE_BYTES
    stdout = io.StringIO()
    stderr = io.StringIO()

    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=JsonResponse(body)),
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        status = MODULE.main([])

    diagnostic = stderr.getvalue()
    assert status == 1
    assert stdout.getvalue() == ""
    assert "malformed JSON" in diagnostic
    assert secret.decode() not in diagnostic
    assert "Traceback" not in diagnostic
    assert len(diagnostic.splitlines()) == 1
    assert len(diagnostic) <= len(f"{MODULE.SOURCE}: \n") + MODULE.MAX_DIAGNOSTIC_CHARS


def test_later_record_lone_surrogate_fails_before_any_ndjson_output():
    secret = "later-record-secret"
    document = [
        index_entry("CC-MAIN-2026-30"),
        {**index_entry("CC-MAIN-2026-38"), "name": "\ud800", "token": secret},
    ]
    body = json.dumps(document).encode("utf-8")
    assert b"\\ud800" in body
    assert len(body) <= MODULE.MAX_RESPONSE_BYTES
    stdout = io.StringIO()
    stderr = io.StringIO()

    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", return_value=JsonResponse(body)),
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        status = MODULE.main([])

    diagnostic = stderr.getvalue()
    assert status == 1
    assert stdout.getvalue() == ""
    assert "non-Unicode-scalar string" in diagnostic
    assert secret not in diagnostic
    assert "Traceback" not in diagnostic
    assert len(diagnostic.splitlines()) == 1
    assert len(diagnostic) <= len(f"{MODULE.SOURCE}: \n") + MODULE.MAX_DIAGNOSTIC_CHARS


def test_http_429_is_nonzero_bounded_and_does_not_emit_records():
    error = urllib.error.HTTPError(
        MODULE.URL,
        429,
        "Too Many Requests password=upstream-secret",
        {"Retry-After": "60"},
        io.BytesIO(b"token=response-secret"),
    )
    stdout = io.StringIO()
    stderr = io.StringIO()
    with (
        mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=error) as urlopen,
        contextlib.redirect_stdout(stdout),
        contextlib.redirect_stderr(stderr),
    ):
        status = MODULE.main([])

    assert status == 1
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == f"{MODULE.SOURCE}: Common Crawl returned HTTP 429\n"
    assert urlopen.call_count == 1


def test_response_byte_and_entry_caps_are_enforced():
    oversized = JsonResponse(b"x" * (MODULE.MAX_RESPONSE_BYTES + 1))
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=oversized):
        with pytest.raises(MODULE.CommonCrawlIndexError, match="byte safety limit"):
            MODULE.fetch_index_metadata()

    document = [index_entry(f"CC-MAIN-{index:04d}") for index in range(MODULE.MAX_INDEXES + 1)]
    with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)):
        with pytest.raises(MODULE.CommonCrawlIndexError, match="entry safety limit"):
            MODULE.fetch_index_metadata()


def test_invalid_timeout_fails_before_http():
    with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
        with pytest.raises(ValueError, match="timeout must be"):
            MODULE.fetch_index_metadata(timeout=0)
    urlopen.assert_not_called()


def test_source_config_uses_enabled_generic_weekly_schedule_and_bounds():
    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))

    assert config["name"] == MODULE.SOURCE
    assert config["script"] == SCRIPT.name
    assert config["enabled"] is True
    assert config["schedule"] == "43 7 * * 2"
    minute, hour, day, month, weekday = config["schedule"].split()
    assert 1 <= int(minute) <= 59
    assert 0 <= int(hour) <= 23
    assert (day, month, weekday) == ("*", "*", "2")
    assert config["retries"] == 1
    assert config["timeout_minutes"] == 5
    assert config["sink"] == "local"
    assert config["cadence"] == {"auto": False}

    args = MODULE.build_parser().parse_args(config["args"])
    assert args.timeout == 30
