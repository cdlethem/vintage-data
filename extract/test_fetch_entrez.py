from __future__ import annotations

import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from types import SimpleNamespace
import tempfile
import unittest
from unittest import mock
import urllib.error
import urllib.parse

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = Path(__file__).parent / "scripts" / "fetch_entrez.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "entrez.yml"
TRANSFORM = REPO_ROOT / "transform"
SPEC = importlib.util.spec_from_file_location("fetch_entrez", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class RawResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class QueueOpener:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request, timeout):
        self.requests.append((request, timeout))
        if not self.responses:
            raise AssertionError(f"unexpected HTTP request: {request.full_url}")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        if isinstance(response, bytes):
            return RawResponse(response)
        return RawResponse(json.dumps(response).encode("utf-8"))


def search(count, identifiers):
    return {"header": {"type": "esearch"}, "esearchresult": {"count": str(count), "idlist": identifiers}}


def article(pmid: str, *, title: str = "A climate study") -> bytes:
    return f'''<?xml version="1.0" encoding="UTF-8"?>
<PubmedArticleSet>
<PubmedArticle><MedlineCitation><PMID Version="1">{pmid}</PMID><Article>
<Journal><JournalIssue><PubDate><Year>2026</Year><Month>Sep</Month></PubDate></JournalIssue><Title>Journal of Fixtures</Title></Journal>
<ArticleTitle>{title}</ArticleTitle>
<Abstract><AbstractText Label="BACKGROUND">First  section.</AbstractText><AbstractText>Second section.</AbstractText></Abstract>
<AuthorList><Author><LastName>Alpha</LastName><ForeName>Ada</ForeName><Initials>AA</Initials></Author></AuthorList>
<PublicationTypeList><PublicationType>Journal Article</PublicationType></PublicationTypeList>
</Article></MedlineCitation><PubmedData><ArticleIdList><ArticleId IdType="pubmed">{pmid}</ArticleId><ArticleId IdType="doi">10.1234/{pmid}</ArticleId></ArticleIdList></PubmedData></PubmedArticle>
</PubmedArticleSet>
'''.encode("utf-8")


def query(request):
    return urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)


class EntrezTests(unittest.TestCase):
    def fetch(self, responses, **changes):
        opener = QueueOpener(responses)
        client = MODULE.HttpClient(
            timeout=7,
            retries=0,
            request_interval=0,
            opener=opener,
        )
        arguments = {
            "db": "pubmed",
            "term": "climate change",
            "email": "operator@example.test",
            "tool": "fixture-tool",
            "retmax": 3,
            "page_size": 2,
            "max_pages": 2,
            "client": client,
            "now": lambda: datetime(2026, 9, 17, 12, 30, tzinfo=timezone.utc),
        }
        arguments.update(changes)
        return MODULE.fetch_entrez(**arguments), opener

    def test_discovery_to_exact_record_output_paginates_with_identification(self):
        payloads = [article("101"), article("102", title="Second"), article("103")]
        records, opener = self.fetch(
            [search(3, ["101", "102"]), search(3, ["103"]), *payloads]
        )

        self.assertEqual(["101", "102", "103"], [record["id"] for record in records])
        self.assertEqual({"2026-09-17T12:30:00+00:00"}, {record["fetched_at"] for record in records})
        self.assertEqual(payloads, [record["raw_payload"].encode("utf-8") for record in records])
        self.assertEqual("BACKGROUND: First section.\nSecond section.", records[0]["abstract"])
        self.assertEqual("10.1234/101", records[0]["doi"])
        self.assertEqual("Alpha", records[0]["authors"][0]["last_name"])

        search_requests = opener.requests[:2]
        self.assertEqual([["0"], ["2"]], [query(item[0])["retstart"] for item in search_requests])
        self.assertEqual([["2"], ["1"]], [query(item[0])["retmax"] for item in search_requests])
        for request, timeout in opener.requests:
            parameters = query(request)
            self.assertEqual(["operator@example.test"], parameters["email"])
            self.assertEqual(["fixture-tool"], parameters["tool"])
            self.assertEqual(["pubmed"], parameters["db"])
            self.assertEqual(7, timeout)
        for request, _ in opener.requests[2:]:
            parameters = query(request)
            self.assertEqual(["abstract"], parameters["rettype"])
            self.assertEqual(["xml"], parameters["retmode"])

    def test_stops_at_page_and_record_bounds(self):
        records, opener = self.fetch(
            [search(50, ["1", "2"]), search(50, ["3", "4"]), *(article(str(i)) for i in range(1, 5))],
            retmax=10,
            max_pages=2,
        )
        self.assertEqual(["1", "2", "3", "4"], [record["id"] for record in records])
        self.assertEqual(2, sum("esearch.fcgi" in item[0].full_url for item in opener.requests))
        self.assertEqual(6, len(opener.requests))

        capped, capped_opener = self.fetch(
            [search(50, ["1", "2"]), article("1")], retmax=1
        )
        self.assertEqual(["1"], [record["id"] for record in capped])
        self.assertEqual(2, len(capped_opener.requests))
        self.assertEqual(["1"], query(capped_opener.requests[0][0])["retmax"])

    def test_empty_results_are_deterministic_and_do_not_fetch_records(self):
        first, first_opener = self.fetch([search(0, [])])
        second, second_opener = self.fetch([search(0, [])])
        self.assertEqual([], first)
        self.assertEqual([], second)
        self.assertEqual(1, len(first_opener.requests))
        self.assertEqual(1, len(second_opener.requests))

    def test_repeated_runs_preserve_identical_payload_bytes_and_stable_identifier(self):
        raw = article("987654", title="Stable &amp; exact")
        first, _ = self.fetch([search(1, ["987654"]), raw])
        second, _ = self.fetch(
            [search(1, ["987654"]), raw],
            now=lambda: datetime(2026, 9, 17, 13, 30, tzinfo=timezone.utc),
        )
        self.assertEqual("987654", first[0]["id"])
        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertEqual(raw.decode("utf-8"), first[0]["raw_payload"])
        self.assertEqual(first[0]["raw_payload"], second[0]["raw_payload"])
        self.assertNotEqual(first[0]["fetched_at"], second[0]["fetched_at"])

    def test_unsupported_database_and_missing_identification_fail_before_http(self):
        invalid = (
            {"db": "protein"},
            {"email": ""},
            {"tool": " "},
            {"retmax": MODULE.HARD_MAX_RECORDS + 1},
            {"page_size": 0},
            {"max_pages": MODULE.HARD_MAX_PAGES + 1},
        )
        for changes in invalid:
            with self.subTest(changes=changes):
                opener = QueueOpener([])
                client = MODULE.HttpClient(opener=opener, request_interval=0)
                arguments = {
                    "db": "pubmed",
                    "term": "query",
                    "email": "operator@example.test",
                    "client": client,
                }
                arguments.update(changes)
                with self.assertRaises(ValueError):
                    MODULE.fetch_entrez(**arguments)
                self.assertEqual([], opener.requests)

    def test_malformed_search_fetch_and_api_errors_fail_without_main_output(self):
        bad_search = (
            (b"not-json", "not JSON"),
            ({}, "missing esearchresult"),
            ({"error": "bad query"}, "bad query"),
            ({"esearchresult": {"count": "x", "idlist": []}}, "count"),
            ({"esearchresult": {"count": "1", "idlist": [7]}}, "idlist"),
        )
        for response, message in bad_search:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.EntrezError, message):
                    self.fetch([response])

        bad_fetch = (
            (b"not xml", "not XML"),
            (b"<eFetchResult><ERROR>Invalid uid</ERROR></eFetchResult>", "Invalid uid"),
            (article("2"), "PMID mismatch"),
            (b"<PubmedArticleSet/>", "expected one PubmedArticle"),
        )
        for response, message in bad_fetch:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.EntrezError, message):
                    self.fetch([search(1, ["1"]), response])

        stdout = io.StringIO()
        opener = QueueOpener([search(2, ["1", "2"]), article("1"), b"bad xml"])
        with mock.patch.object(MODULE.urllib.request, "urlopen", opener), contextlib.redirect_stdout(stdout):
            with self.assertRaisesRegex(MODULE.EntrezError, "not XML"):
                MODULE.main([
                    "--email", "operator@example.test", "--retmax", "2",
                    "--page-size", "2", "--max-pages", "1", "--request-interval", "0",
                    "--retries", "0",
                ])
        self.assertEqual("", stdout.getvalue())

    def test_http_errors_retries_throttling_and_pacing_are_bounded(self):
        headers = {"Retry-After": "2"}
        throttled = urllib.error.HTTPError(
            "https://example.test", 429, "too many requests", headers, io.BytesIO(b"slow down")
        )
        opener = QueueOpener([throttled, b"ok", b"again"])
        sleeps = []
        client = MODULE.HttpClient(
            retries=1,
            request_interval=0.34,
            opener=opener,
            clock=lambda: 0.0,
            sleeper=sleeps.append,
        )
        self.assertEqual(b"ok", client.get("https://example.test/one", "ESearch"))
        self.assertEqual(b"again", client.get("https://example.test/two", "EFetch"))
        self.assertEqual(3, len(opener.requests))
        self.assertIn(2.0, sleeps)
        self.assertGreaterEqual(sleeps.count(0.34), 2)

        forbidden = urllib.error.HTTPError(
            "https://example.test", 400, "bad", {}, io.BytesIO(b"invalid database")
        )
        with self.assertRaisesRegex(MODULE.EntrezError, "status=400"):
            MODULE.HttpClient(retries=2, request_interval=0, opener=QueueOpener([forbidden])).get(
                "https://example.test", "ESearch"
            )

    def test_source_configuration_is_bounded_disabled_and_parses_as_argv(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual("entrez", config["name"])
        self.assertEqual("fetch_entrez.py", config["script"])
        self.assertIs(config["enabled"], False)
        parsed = MODULE.build_parser().parse_args(config["args"])
        self.assertEqual("pubmed", parsed.db)
        self.assertLessEqual(parsed.retmax, MODULE.HARD_MAX_RECORDS)
        self.assertLessEqual(parsed.page_size, MODULE.HARD_MAX_PAGE_SIZE)
        self.assertLessEqual(parsed.max_pages, MODULE.HARD_MAX_PAGES)
        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertIn("NCBI_EMAIL is mandatory", text)
        self.assertIn("database-backed", text)
        self.assertIn("external activation gates", text.lower())

    def test_models_parse_offline_with_generated_temporary_source_metadata(self):
        sync_path = TRANSFORM / "scripts" / "sync_raw_sources.py"
        sync_spec = importlib.util.spec_from_file_location("entrez_sync_raw_sources", sync_path)
        sync = importlib.util.module_from_spec(sync_spec)
        assert sync_spec.loader is not None
        sys.modules[sync_spec.name] = sync
        try:
            sync_spec.loader.exec_module(sync)
        finally:
            sys.modules.pop(sync_spec.name, None)

        loader_path = REPO_ROOT / "load" / "loader" / "schema.py"
        loader_spec = importlib.util.spec_from_file_location("entrez_loader_schema", loader_path)
        loader = importlib.util.module_from_spec(loader_spec)
        assert loader_spec.loader is not None
        sys.modules[loader_spec.name] = loader
        try:
            loader_spec.loader.exec_module(loader)
        finally:
            sys.modules.pop(loader_spec.name, None)

        fixture_record = self.fetch(
            [search(1, ["101"]), article("101")], retmax=1
        )[0][0]
        settings = SimpleNamespace(
            name="entrez",
            column_types={"source": "string", "id": "string", "fetched_at": "timestamp"},
            exclude_keys=[],
            schema_detection="auto",
            detect_temporal=True,
            keep_payload=True,
        )
        inferred = loader.table_columns(loader.infer_columns([fixture_record], settings), settings)
        native_types = {
            "string": "VARCHAR", "integer": "BIGINT", "double": "DOUBLE",
            "boolean": "BOOLEAN", "date": "DATE",
            "timestamp": "TIMESTAMP WITH TIME ZONE", "json": "JSON",
        }
        raw_source = sync.RawSource(
            "entrez",
            tuple(sync.Column(column.name, native_types[column.type]) for column in inferred),
            1,
            "2026-09-17T12:30:00+00:00",
        )

        inventory = yaml.safe_load(
            (TRANSFORM / "models" / "base" / "_raw_sources.yml").read_text(encoding="utf-8")
        )
        synchronized_sources = [
            sync.RawSource(
                table["name"],
                tuple(
                    sync.Column(column["name"], column["data_type"])
                    for column in table["columns"]
                ),
                0,
                None,
            )
            for table in inventory["sources"][0]["tables"]
        ]
        synchronized_sources.append(raw_source)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project = root / "project"
            profiles = root / "profiles"
            project.mkdir()
            profiles.mkdir()
            shutil.copy2(TRANSFORM / "dbt_project.yml", project)
            shutil.copytree(TRANSFORM / "models", project / "models")
            (profiles / "profiles.yml").write_text(
                "vintage_data:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n"
                f"      path: '{(root / 'dev.duckdb').as_posix()}'\n      schema: transform\n      threads: 1\n",
                encoding="utf-8",
            )
            changed = sync.write_all(project, synchronized_sources)
            self.assertEqual(["models/base/_raw_sources.yml"], changed)
            generated = yaml.safe_load(
                (project / sync.SOURCE_YAML).read_text(encoding="utf-8")
            )
            tables = {table["name"]: table for table in generated["sources"][0]["tables"]}
            self.assertIn("entrez", tables)
            self.assertEqual(
                "VARCHAR",
                next(
                    column["data_type"]
                    for column in tables["entrez"]["columns"]
                    if column["name"] == "raw_payload"
                ),
            )

            command = [
                sys.executable, "-m", "dbt.cli.main", "parse", "--no-version-check",
                "--project-dir", str(project), "--profiles-dir", str(profiles),
                "--target-path", str(root / "target"), "--vars", "{enable_entrez_models: true}",
            ]
            environment = {
                "PATH": os.environ.get("PATH", ""),
                "HOME": str(root),
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
                "PYTHONUTF8": "1",
            }
            result = subprocess.run(
                command, cwd=project, env=environment, capture_output=True, text=True,
                timeout=60, check=False,
            )
            diagnostic = (result.stdout + result.stderr).replace(str(root), "<tmp>")[-4000:]
            self.assertEqual(0, result.returncode, diagnostic)

            manifest_path = root / "target" / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertIn("model.vintage_data.base_entrez", manifest["nodes"])
            fact = manifest["nodes"]["model.vintage_data.fct_entrez_pubmed_observation"]
            self.assertEqual("table", fact["config"]["materialized"])
            self.assertTrue(fact["config"]["contract"]["enforced"])
            self.assertIn("source.vintage_data.raw.entrez", manifest["sources"])

            validator_path = TRANSFORM / "scripts" / "validate_project.py"
            validator_spec = importlib.util.spec_from_file_location("entrez_validate_project", validator_path)
            validator = importlib.util.module_from_spec(validator_spec)
            assert validator_spec.loader is not None
            validator_spec.loader.exec_module(validator)
            self.assertEqual([], validator.validate_manifest(manifest))


if __name__ == "__main__":
    unittest.main()
