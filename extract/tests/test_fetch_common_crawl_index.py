import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
from pathlib import Path
import threading
import urllib.error
import urllib.parse

import pytest
import yaml


SCRIPT = Path(__file__).parents[1] / "scripts" / "fetch_common_crawl_index.py"
CONFIG = SCRIPT.parents[1] / "sources" / "common_crawl_index.yml"
SPEC = importlib.util.spec_from_file_location("fetch_common_crawl_index", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)
FETCHED_AT = "2026-09-21T12:34:56+00:00"


class LocalAPI:
    def __init__(self):
        self.requests = []
        self.responder = None

    def __enter__(self):
        api = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                api.requests.append(
                    {
                        "path": self.path,
                        "headers": {key.lower(): value for key, value in self.headers.items()},
                    }
                )
                status, headers, body = api.responder(self.path)
                if not isinstance(body, bytes):
                    body = json.dumps(body).encode("utf-8")
                self.send_response(status)
                for key, value in headers.items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def crawl(crawl_id, index_url):
    return {"id": crawl_id, "cdx-api": index_url, "name": crawl_id}


def cdx_row(number=1, **changes):
    row = {
        "urlkey": f"com,example)/page-{number}",
        "timestamp": f"2026092{number}123456",
        "url": f"https://example.com/page-{number}",
        "mime": "text/html",
        "mime-detected": "text/html",
        "status": "200",
        "digest": f"DIGEST{number}",
        "length": str(1000 + number),
        "offset": str(2000 + number),
        "filename": f"crawl-data/CC-MAIN-2026-39/segments/file-{number}.warc.gz",
        "languages": "eng",
        "encoding": "UTF-8",
    }
    row.update(changes)
    return row


def ndjson(*rows):
    return b"".join(json.dumps(row).encode("utf-8") + b"\n" for row in rows)


def query(path):
    return urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)


def test_discovers_valid_crawls_and_selects_newest_independent_of_metadata_order():
    with LocalAPI() as api:
        document = [
            crawl("CC-MAIN-2026-31", f"{api.base_url}/index/31"),
            crawl("CC-MAIN-2026-39", f"{api.base_url}/index/39"),
            crawl("CC-MAIN-2025-51", f"{api.base_url}/index/51"),
        ]
        api.responder = lambda path: (200, {"Content-Type": "application/json"}, document)

        crawls = MODULE.discover_crawls(f"{api.base_url}/collinfo.json", timeout=2)
        selected = MODULE.select_crawl(crawls)

    assert selected == {"id": "CC-MAIN-2026-39", "cdx-api": f"{api.base_url}/index/39"}
    assert len(api.requests) == 1
    assert api.requests[0]["path"] == "/collinfo.json"
    assert api.requests[0]["headers"]["accept"] == "application/json"
    assert api.requests[0]["headers"]["user-agent"] == MODULE.USER_AGENT


def test_explicit_crawl_must_be_present_in_collection_metadata():
    crawls = [{"id": "CC-MAIN-2026-39", "cdx-api": "https://index.invalid/39"}]
    assert MODULE.select_crawl(crawls, "CC-MAIN-2026-39") == crawls[0]
    with pytest.raises(MODULE.CommonCrawlError, match="not present"):
        MODULE.select_crawl(crawls, "CC-MAIN-2026-31")


def test_fetch_domain_paginates_and_normalizes_flat_stable_records():
    with LocalAPI() as api:
        def respond(path):
            values = query(path)
            if values.get("showNumPages") == ["true"]:
                return 200, {}, {"pages": 3, "pageSize": 1, "blocks": 3}
            page = int(values["page"][0])
            return 200, {"Content-Type": "application/x-ndjson"}, ndjson(cdx_row(page + 1))

        api.responder = respond
        records = list(
            MODULE.fetch_domain(
                "Example.COM.",
                crawl("CC-MAIN-2026-39", f"{api.base_url}/index"),
                page_size=1,
                max_pages=2,
                timeout=2,
                fetched_at=FETCHED_AT,
            )
        )

    assert len(records) == 2
    assert len(api.requests) == 3
    assert [query(item["path"]).get("page") for item in api.requests] == [None, ["0"], ["1"]]
    for request in api.requests:
        assert query(request["path"])["url"] == ["example.com/*"]
        assert query(request["path"])["matchType"] == ["domain"]
        assert query(request["path"])["output"] == ["json"]
        assert query(request["path"])["pageSize"] == ["1"]

    first = records[0]
    assert first == {
        "source": "common_crawl_index",
        "id": (
            "CC-MAIN-2026-39:com%2Cexample%29%2Fpage-1:20260921123456:"
            "crawl-data%2FCC-MAIN-2026-39%2Fsegments%2Ffile-1.warc.gz:2001"
        ),
        "fetched_at": FETCHED_AT,
        "crawl_id": "CC-MAIN-2026-39",
        "watched_domain": "example.com",
        "url_key": "com,example)/page-1",
        "timestamp": "20260921123456",
        "captured_at": "2026-09-21T12:34:56+00:00",
        "url": "https://example.com/page-1",
        "mime_type": "text/html",
        "detected_mime_type": "text/html",
        "status_code": "200",
        "digest": "DIGEST1",
        "content_length": 1001,
        "warc_offset": 2001,
        "warc_filename": "crawl-data/CC-MAIN-2026-39/segments/file-1.warc.gz",
        "languages": "eng",
        "encoding": "UTF-8",
    }


def test_capture_id_distinguishes_each_warc_coordinate():
    common = cdx_row(
        urlkey="com,example)/same",
        timestamp="20260921123456",
        url="https://example.com/same",
    )
    rows = [
        common,
        {**common, "filename": "crawl-data/other.warc.gz"},
        {**common, "offset": "9999"},
    ]

    records = [
        MODULE.normalize_record(
            row,
            crawl_id="CC-MAIN-2026-39",
            domain="example.com",
            fetched_at=FETCHED_AT,
        )
        for row in rows
    ]

    assert len({record["id"] for record in records}) == 3
    assert records[0]["id"].endswith("file-1.warc.gz:2001")


@pytest.mark.parametrize(
    "row_changes, crawl_id, message",
    [
        ({}, "not-a-crawl", "invalid crawl id"),
        ({"urlkey": ""}, "CC-MAIN-2026-39", "invalid urlkey"),
        ({"timestamp": ""}, "CC-MAIN-2026-39", "invalid timestamp"),
        ({"filename": None}, "CC-MAIN-2026-39", "invalid filename"),
        ({"offset": 1.5}, "CC-MAIN-2026-39", "must be an integer"),
        ({"offset": None}, "CC-MAIN-2026-39", "missing required field 'offset'"),
        ({"offset": "unknown"}, "CC-MAIN-2026-39", "must be an integer"),
    ],
)
def test_capture_id_requires_valid_stable_coordinates(row_changes, crawl_id, message):
    with pytest.raises(MODULE.CommonCrawlError, match=message):
        MODULE.normalize_record(
            cdx_row(**row_changes),
            crawl_id=crawl_id,
            domain="example.com",
            fetched_at=FETCHED_AT,
        )


def test_empty_page_metadata_returns_no_records_without_requesting_data_pages():
    with LocalAPI() as api:
        api.responder = lambda path: (200, {}, {"pages": 0, "pageSize": 1, "blocks": 0})
        records = list(
            MODULE.fetch_domain(
                "example.com",
                crawl("CC-MAIN-2026-39", f"{api.base_url}/index"),
                timeout=2,
            )
        )

    assert records == []
    assert len(api.requests) == 1
    assert query(api.requests[0]["path"])["showNumPages"] == ["true"]


@pytest.mark.parametrize(
    "body, message",
    [
        ({"id": "CC-MAIN-2026-39"}, "must be a JSON list"),
        ([{"id": "bad", "cdx-api": "https://index.invalid"}], "invalid id"),
        ([{"id": "CC-MAIN-2026-39", "cdx-api": 3}], "invalid cdx-api"),
        (b"not-json", "malformed JSON"),
    ],
)
def test_rejects_malformed_collection_metadata(body, message):
    with LocalAPI() as api:
        api.responder = lambda path: (200, {}, body)
        with pytest.raises(MODULE.CommonCrawlError, match=message):
            MODULE.discover_crawls(f"{api.base_url}/collinfo", timeout=2)


@pytest.mark.parametrize(
    "metadata, page, message",
    [
        ({"pages": "one"}, None, "invalid pages"),
        ({"pages": 1}, b"not-json\n", "malformed JSON"),
        ({"pages": 1}, b"[]\n", "must be an object"),
        ({"pages": 1}, ndjson(cdx_row(timestamp="yesterday")), "invalid timestamp"),
        ({"pages": 1}, ndjson(cdx_row(length="large")), "must be an integer"),
    ],
)
def test_rejects_malformed_index_responses(metadata, page, message):
    with LocalAPI() as api:
        def respond(path):
            if query(path).get("showNumPages") == ["true"]:
                return 200, {}, metadata
            return 200, {}, page

        api.responder = respond
        with pytest.raises(MODULE.CommonCrawlError, match=message):
            list(
                MODULE.fetch_domain(
                    "example.com",
                    crawl("CC-MAIN-2026-39", f"{api.base_url}/index"),
                    timeout=2,
                )
            )


def test_fetch_domain_propagates_http_errors():
    with LocalAPI() as api:
        api.responder = lambda path: (503, {}, b"unavailable")
        with pytest.raises(urllib.error.HTTPError) as error:
            list(
                MODULE.fetch_domain(
                    "example.com",
                    crawl("CC-MAIN-2026-39", f"{api.base_url}/index"),
                    timeout=2,
                )
            )
    assert error.value.code == 503


def test_watchlist_resolves_explicit_crawl_once_through_injected_collinfo_url():
    with LocalAPI() as api:
        metadata_requests = []

        def respond(path):
            parsed = urllib.parse.urlsplit(path)
            if parsed.path == "/fixtures/collinfo.json":
                metadata_requests.append(path)
                return 200, {}, [
                    crawl("CC-MAIN-2026-40", f"{api.base_url}/index/newer"),
                    crawl("CC-MAIN-2026-39", f"{api.base_url}/index/selected"),
                ]
            values = query(path)
            assert parsed.path == "/index/selected"
            if values.get("showNumPages") == ["true"]:
                return 200, {}, {"pages": 1}
            domain = values["url"][0].removesuffix("/*")
            return 200, {}, ndjson(
                cdx_row(1, urlkey=f"com,{domain})/", url=f"https://{domain}/")
            )

        api.responder = respond
        records = list(
            MODULE.fetch_watchlist(
                ["alpha.com", "beta.com"],
                crawl_id="CC-MAIN-2026-39",
                collinfo_url=f"{api.base_url}/fixtures/collinfo.json",
                max_pages=1,
                timeout=2,
                fetched_at=FETCHED_AT,
            )
        )

    assert len(metadata_requests) == 1
    assert [record["watched_domain"] for record in records] == ["alpha.com", "beta.com"]
    assert {record["crawl_id"] for record in records} == {"CC-MAIN-2026-39"}
    assert all(request["path"].startswith("/index/selected?") for request in api.requests[1:])


def test_watchlist_isolates_domain_http_failure_and_continues(capsys):
    with LocalAPI() as api:
        def respond(path):
            parsed = urllib.parse.urlsplit(path)
            if parsed.path == "/collinfo":
                return 200, {}, [crawl("CC-MAIN-2026-39", f"{api.base_url}/index")]
            values = query(path)
            domain = values["url"][0]
            if domain == "broken.example/*":
                return 500, {}, b"failed"
            if values.get("showNumPages") == ["true"]:
                return 200, {}, {"pages": 1}
            return 200, {}, ndjson(cdx_row())

        api.responder = respond
        records = list(
            MODULE.fetch_watchlist(
                ["broken.example", "working.example"],
                collinfo_url=f"{api.base_url}/collinfo",
                timeout=2,
                fetched_at=FETCHED_AT,
            )
        )

    assert len(records) == 1
    assert records[0]["watched_domain"] == "working.example"
    error = capsys.readouterr().err
    assert "skipping 'broken.example'" in error
    assert "HTTPError" in error


def test_watchlist_isolates_invalid_domain_and_fetches_following_domain(capsys):
    with LocalAPI() as api:
        def respond(path):
            parsed = urllib.parse.urlsplit(path)
            if parsed.path == "/collinfo":
                return 200, {}, [crawl("CC-MAIN-2026-39", f"{api.base_url}/index")]
            if query(path).get("showNumPages") == ["true"]:
                return 200, {}, {"pages": 1}
            return 200, {}, ndjson(cdx_row())

        api.responder = respond
        records = list(
            MODULE.fetch_watchlist(
                ["https://invalid.example", "Working.Example."],
                collinfo_url=f"{api.base_url}/collinfo",
                timeout=2,
                fetched_at=FETCHED_AT,
            )
        )

    assert [record["watched_domain"] for record in records] == ["working.example"]
    assert len(api.requests) == 3
    assert "skipping 'https://invalid.example'" in capsys.readouterr().err


def test_main_emits_compact_ndjson_using_injected_collection_endpoint(capsys):
    with LocalAPI() as api:
        def respond(path):
            parsed = urllib.parse.urlsplit(path)
            if parsed.path == "/collinfo":
                return 200, {}, [crawl("CC-MAIN-2026-39", f"{api.base_url}/index")]
            if query(path).get("showNumPages") == ["true"]:
                return 200, {}, {"pages": 1}
            return 200, {}, ndjson(cdx_row())

        api.responder = respond
        MODULE.main(
            [
                "--collinfo-url",
                f"{api.base_url}/collinfo",
                "--max-pages",
                "1",
                "--timeout",
                "2",
                "example.com",
            ]
        )

    output = capsys.readouterr().out
    assert output.count("\n") == 1
    assert json.loads(output)["source"] == MODULE.SOURCE
    assert ": " not in output


def test_source_configuration_matches_bounded_disabled_auto_schema_contract():
    config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

    assert config["name"] == MODULE.SOURCE
    assert config["script"] == SCRIPT.name
    assert config["enabled"] is False
    assert config["sink"] == "local"
    assert config["format"] == "ndjson"
    assert config["schema_detection"] == "auto"
    assert config["keep_payload"] is True
    assert config["cadence"] == {
        "auto": True,
        "floor_minutes": 240,
        "min_minutes": 240,
        "max_minutes": 1440,
        "volatile_keys": ["fetched_at"],
    }
    assert tuple(config["curated_domains"]) == MODULE.CURATED_DOMAINS
    assert config["resource_keys"] == [
        "common_crawl_index",
        "common_crawl_api",
        "extract_sink_resource",
    ]
    args = MODULE.build_parser().parse_args(config["args"])
    assert args.page_size == 1
    assert args.max_pages == 5
    assert args.timeout == 30
