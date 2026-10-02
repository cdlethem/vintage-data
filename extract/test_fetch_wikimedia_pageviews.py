"""Behavioral tests for fetch_wikimedia_pageviews.py (offline, scripted transport)."""

import contextlib
import importlib.util
import io
import json
from datetime import datetime, timezone
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock
import urllib.error

import yaml


SCRIPT = Path(__file__).parent / "scripts" / "fetch_wikimedia_pageviews.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "wikimedia_pageviews.yml"
REPO_ROOT = SCRIPT.parents[2]
RAW_GENERATOR = REPO_ROOT / "transform" / "scripts" / "sync_raw_sources.py"
LOADER_SCHEMA = REPO_ROOT / "load" / "loader" / "schema.py"
EXTRACT_RUNNER = REPO_ROOT / "orchestration" / "include" / "extract_runner.py"
SPEC = importlib.util.spec_from_file_location("fetch_wikimedia_pageviews", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
# NOW is 2026-10-02 05:27 UTC; the per-article endpoint publishes completed UTC
# days only, so the most recent completed day is 2026-10-01.
NOW = datetime(2026, 10, 2, 5, 27, tzinfo=timezone.utc)
START, END = "20260930", "20261001"


class JsonResponse(io.BytesIO):
    def __init__(self, document, *, url=None):
        payload = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
        super().__init__(payload)
        self._url = url

    def geturl(self):
        return self._url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def item(
    *,
    language="en",
    project="wikipedia",
    title="Albert_Einstein",
    access="all-access",
    agent="user",
    granularity="daily",
    timestamp="2026093000",
    views=9737,
):
    return {
        "project": f"{language}.{project}",
        "article": title,
        "granularity": granularity,
        "timestamp": timestamp,
        "access": access,
        "agent": agent,
        "views": views,
    }


def document(*items):
    return {"items": list(items)}


def request_url(title="Albert_Einstein", access="all-access", agent="user", start=START, end=END,
                language="en", project="wikipedia"):
    return (
        f"{MODULE.BASE}/{language}.{project}/{access}/{agent}/{title}/"
        f"{MODULE.GRANULARITY}/{start}/{end}"
    )


def http_error(code):
    return urllib.error.HTTPError(request_url(), code, "error", {}, io.BytesIO(b"error"))


class FetchWikimediaPageviewsTests(unittest.TestCase):
    def fetch(self, articles, responses, **changes):
        arguments = {
            "timeout": 7,
            "retries": 0,
            "retry_budget": 30,
            "now": lambda: NOW,
            "access": "all-access",
            "agent": "user",
            "days": 2,
        }
        arguments.update(changes)

        def scripted_opener(request, timeout=None):
            response = responses.pop(0)
            if response.geturl() is None:
                response._url = request.full_url
            return response

        opener = mock.Mock(side_effect=scripted_opener)
        client = MODULE.WikimediaClient(
            timeout=arguments.pop("timeout"),
            retries=arguments.pop("retries"),
            retry_budget=arguments.pop("retry_budget"),
            opener=opener,
        )
        records = MODULE.fetch_pageviews(articles, client=client, **arguments)
        return records, opener

    def test_normalizes_response_and_preserves_envelope(self):
        records, opener = self.fetch(
            [["en", "wikipedia", "  Albert Einstein  "]],
            [
                JsonResponse(
                    document(
                        item(timestamp="2026093000", views=10596),
                        item(timestamp="2026100100", views=9698),
                    ),
                    url=request_url(),
                )
            ],
        )

        self.assertEqual(len(records), 2)
        record = records[0]
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertEqual(record["fetched_at"], "2026-10-02T05:27:00+00:00")
        self.assertEqual(record["language"], "en")
        self.assertEqual(record["project"], "wikipedia")
        self.assertEqual(record["article"], "Albert_Einstein")
        self.assertEqual(record["access"], "all-access")
        self.assertEqual(record["agent"], "user")
        self.assertEqual(record["granularity"], "daily")
        self.assertEqual(record["timestamp"], "2026093000")
        self.assertEqual(record["date"], "2026-09-30")
        self.assertEqual(record["views"], 10596)
        self.assertEqual(record["url"], request_url())
        self.assertRegex(record["id"], r"^[0-9a-f]{64}$")
        self.assertEqual(records[1]["timestamp"], "2026100100")
        self.assertEqual(records[1]["views"], 9698)

        call = opener.call_args
        self.assertEqual(call.kwargs, {"timeout": 7})
        self.assertEqual(call.args[0].headers["Accept"], "application/json")
        self.assertTrue(call.args[0].headers["User-agent"])

    def test_window_uses_completed_days_only(self):
        _, opener = self.fetch([["en", "wikipedia", "Albert_Einstein"]], [JsonResponse(document())])
        self.assertEqual(opener.call_args.args[0].full_url, request_url(start=START, end=END))

        _, opener = self.fetch(
            [["en", "wikipedia", "Albert_Einstein"]], [JsonResponse(document())], days=3
        )
        self.assertEqual(
            opener.call_args.args[0].full_url,
            request_url(start="20260929", end="20261001"),
        )

        # The current, incomplete day must never be requested.
        self.assertNotIn(NOW.strftime("%Y%m%d"), opener.call_args.args[0].full_url)

    def test_record_identity_is_stable_and_separates_dimensions(self):
        first, _ = self.fetch(
            [["en", "wikipedia", "  Albert Einstein  "]],
            [JsonResponse(document(item()))],
        )
        second, _ = self.fetch([["en", "wikipedia", "Albert_Einstein"]], [JsonResponse(document(item()))])
        self.assertEqual(first[0]["id"], second[0]["id"])

        de, _ = self.fetch([["de", "wikipedia", "Albert_Einstein"]], [JsonResponse(document(item(language="de")))])
        other_access, _ = self.fetch(
            [["en", "wikipedia", "Albert_Einstein"]],
            [JsonResponse(document(item(access="desktop")))],
            access="desktop",
        )
        other_agent, _ = self.fetch(
            [["en", "wikipedia", "Albert_Einstein"]],
            [JsonResponse(document(item(agent="automated")))],
            agent="automated",
        )
        self.assertEqual(
            len(
                {
                    first[0]["id"],
                    de[0]["id"],
                    other_access[0]["id"],
                    other_agent[0]["id"],
                }
            ),
            4,
        )

    def test_input_bounds_and_allowlists_fail_before_http(self):
        invalid = (
            ([], "at least one"),
            ([["EN.WIKI", "wikipedia", "x"]], "language"),
            ([["en", "commons", "x"]], "project"),
            ([["en", "wikipedia", ""]], "title"),
            ([["en", "wikipedia", "x\nmalicious"]], "control"),
            ([["en", "wikipedia", "x" * (MODULE.MAX_TITLE_BYTES + 1)]], "at most"),
            ([["en", "wikipedia", ".."]], "dot segment"),
            ([["en", "wikipedia", "same"], ["EN", "Wikipedia", "same"]], "duplicate"),
            ([["en", "wikipedia", str(i)] for i in range(MODULE.MAX_ARTICLES + 1)], "at most"),
        )
        for articles, message in invalid:
            with self.subTest(message=message):
                client = mock.Mock()
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.fetch_pageviews(articles, client=client)
                client.get.assert_not_called()

        for bad in (
            dict(days=0),
            dict(days=MODULE.MAX_DAYS + 1),
            dict(days=True),
            dict(access="desktop-rotated"),
            dict(agent="robot"),
            dict(timeout=0),
            dict(retries=MODULE.MAX_RETRIES + 1),
            dict(retry_budget=MODULE.MAX_RETRY_BUDGET + 1),
        ):
            with self.subTest(message=str(bad)):
                client = mock.Mock()
                with self.assertRaises(ValueError):
                    MODULE.fetch_pageviews([["en", "wikipedia", "x"]], client=client, **bad)
                client.get.assert_not_called()

    def test_retryable_failures_exhaust_budget_and_non_retryable_fail_fast(self):
        for failure in (http_error(500), http_error(429), urllib.error.URLError("reset")):
            with self.subTest(failure=type(failure).__name__):
                sleeps = []
                client = MODULE.WikimediaClient(
                    timeout=7,
                    retries=2,
                    retry_budget=30,
                    opener=mock.Mock(side_effect=failure),
                    clock=lambda: 0,
                    sleeper=sleeps.append,
                )
                with self.assertRaisesRegex(MODULE.WikimediaError, "after 3 attempts"):
                    client.get(request_url())
                self.assertEqual(sleeps, [1.0, 2.0])

        client = MODULE.WikimediaClient(
            timeout=7,
            retries=2,
            retry_budget=30,
            opener=mock.Mock(side_effect=http_error(404)),
        )
        with self.assertRaisesRegex(MODULE.WikimediaError, "HTTP 404"):
            client.get(request_url())
        self.assertEqual(client.opener.call_count, 1)

    def test_response_contract_is_enforced(self):
        base = {"fetch": lambda responses: self.fetch([["en", "wikipedia", "Albert_Einstein"]], responses)}
        cases = (
            (document(item(language="de")), "project"),
            (document(item(title="Other_Article")), "article"),
            (document(item(granularity="monthly")), "granularity"),
            (document(item(access="desktop")), "access"),
            (document(item(agent="spider")), "agent"),
            (document(item(timestamp="2026093012")), "daily marker"),
            (document(item(timestamp="2026100200")), "outside the requested window"),
            (document(item(views=-1)), "views"),
            (document(item(views=1.5)), "views"),
            (document(item(views=True)), "views"),
            (document(item(), item()), "repeated a day"),
            ({"detail": "no items"}, "items"),
            (["items"], "object"),
        )
        for payload, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.WikimediaError, message):
                    base["fetch"]([JsonResponse(payload)])

    def test_main_fixture_output_is_valid_ndjson_and_passes_envelope(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        articles = list(MODULE.build_parser().parse_args(config["args"]).article)

        def scripted_open(request, timeout=None):
            segments = request.full_url.split("/")
            project_segment = segments[segments.index("per-article") + 1]
            title = segments[segments.index("user") + 1]
            language, project = project_segment.split(".", 1)
            return JsonResponse(
                document(
                    item(
                        title=title,
                        timestamp="2026093000",
                        views=1,
                        language=language,
                        project=project,
                    ),
                    item(
                        title=title,
                        timestamp="2026100100",
                        views=2,
                        language=language,
                        project=project,
                    ),
                ),
                url=request.full_url,
            )

        include_dir = EXTRACT_RUNNER.parent
        sys.path.insert(0, str(include_dir))
        try:
            runner_spec = importlib.util.spec_from_file_location("extract_runner", EXTRACT_RUNNER)
            runner = importlib.util.module_from_spec(runner_spec)
            assert runner_spec.loader is not None
            runner_spec.loader.exec_module(runner)
        finally:
            sys.path.remove(str(include_dir))

        stdout = io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "build_opener",
                return_value=mock.Mock(open=scripted_open),
            ),
            contextlib.redirect_stdout(stdout),
        ):
            MODULE.main(config["args"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), len(articles) * 2)
        seen_ids = set()
        for line in lines:
            record = json.loads(line)
            self.assertTrue(record["source"])
            self.assertTrue(record["fetched_at"])
            self.assertTrue(record["id"])
            runner._check_envelope(line)
            seen_ids.add(record["id"])
        self.assertEqual(len(seen_ids), len(lines))
        self.assertNotIn(": ", lines[0])

    def test_main_failure_exits_nonzero_without_stdout(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "build_opener",
                return_value=mock.Mock(open=mock.Mock(side_effect=http_error(404))),
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            with self.assertRaises(SystemExit) as raised:
                MODULE.main(
                    [
                        "--article", "en", "wikipedia", "Albert_Einstein",
                        "--days", "2", "--retries", "0",
                    ]
                )
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("HTTP 404", stderr.getvalue())

    def test_source_yaml_parses_and_configured_argv_matches_extractor_bounds(self):
        import re

        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertTrue(config["enabled"])
        self.assertEqual(config["sink"], "local")
        self.assertEqual(config["retries"], 1)
        self.assertTrue(re.fullmatch(r"\d+ \d+ \* \* \*", config["schedule"]))

        args = MODULE.build_parser().parse_args(config["args"])
        self.assertGreaterEqual(len(args.article), 1)
        self.assertLessEqual(len(args.article), MODULE.MAX_ARTICLES)
        self.assertLessEqual(args.days, MODULE.MAX_DAYS)
        self.assertLessEqual(args.timeout, MODULE.MAX_TIMEOUT)
        self.assertLessEqual(args.retries, MODULE.MAX_RETRIES)
        self.assertLessEqual(args.retry_budget, MODULE.MAX_RETRY_BUDGET)
        worst_case_request_time = len(args.article) * (args.retries + 1) * args.timeout
        self.assertLessEqual(
            min(worst_case_request_time, args.retry_budget), config["timeout_minutes"] * 60
        )
        for field in ("licence", "attribution", "rate_limit", "cadence_note"):
            self.assertIn(field, config)


class PageviewsWarehouseIntegrationTests(unittest.TestCase):
    def test_offline_loader_naming_and_generator_output_are_consistent(self):
        loader_spec = importlib.util.spec_from_file_location("loader_schema", LOADER_SCHEMA)
        loader = importlib.util.module_from_spec(loader_spec)
        assert loader_spec.loader is not None
        sys.modules[loader_spec.name] = loader
        try:
            loader_spec.loader.exec_module(loader)
        finally:
            sys.modules.pop(loader_spec.name, None)

        article = MODULE.make_article_request("en", "wikipedia", "Albert Einstein", "all-access", "user")
        record = MODULE.normalize_view(
            item(),
            article,
            fetched_at="2026-10-02T05:27:00+00:00",
            url=request_url(),
            start=START,
            end=END,
        )
        settings = SimpleNamespace(
            name=MODULE.SOURCE,
            column_types={
                "source": "string",
                "id": "string",
                "fetched_at": "timestamp",
            },
            exclude_keys=[],
            schema_detection="auto",
            detect_temporal=True,
            keep_payload=True,
        )
        inferred = loader.infer_columns([record], settings)
        columns = loader.table_columns(inferred, settings)
        native_types = {
            "string": "VARCHAR",
            "integer": "BIGINT",
            "double": "DOUBLE",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "timestamp": "TIMESTAMP WITH TIME ZONE",
            "json": "JSON",
        }

        generator_spec = importlib.util.spec_from_file_location("sync_raw_sources", RAW_GENERATOR)
        generator = importlib.util.module_from_spec(generator_spec)
        assert generator_spec.loader is not None
        with mock.patch.dict(sys.modules, {"duckdb": mock.MagicMock()}):
            sys.modules[generator_spec.name] = generator
            try:
                generator_spec.loader.exec_module(generator)
            finally:
                sys.modules.pop(generator_spec.name, None)

        raw_source = generator.RawSource(
            MODULE.SOURCE,
            tuple(generator.Column(column.name, native_types[column.type]) for column in columns),
            2,
            "2026-10-02T05:27:00+00:00",
        )
        with tempfile.TemporaryDirectory() as directory:
            project_dir = Path(directory)
            changed = generator.write_all(project_dir, [raw_source])
            self.assertEqual(
                changed,
                [
                    "models/base/_raw_sources.yml",
                    f"models/base/base_{MODULE.SOURCE}.sql",
                ],
            )
            generated = yaml.safe_load(
                (project_dir / generator.SOURCE_YAML).read_text(encoding="utf-8")
            )
            base_sql = (
                project_dir / "models" / "base" / f"base_{MODULE.SOURCE}.sql"
            ).read_text(encoding="utf-8")
        table = generated["sources"][0]["tables"][0]
        model = generated["models"][0]
        self.assertEqual(table["name"], MODULE.SOURCE)
        self.assertEqual(model["name"], f"base_{MODULE.SOURCE}")
        declared = {column["name"]: column["data_type"] for column in table["columns"]}
        self.assertEqual(declared["source"], "VARCHAR")
        self.assertEqual(declared["id"], "VARCHAR")
        self.assertEqual(declared["fetched_at"], "TIMESTAMP WITH TIME ZONE")
        self.assertEqual(declared["views"], "BIGINT")
        self.assertEqual(declared["date"], "DATE")
        self.assertEqual(declared["timestamp"], "VARCHAR")
        self.assertIn(f"source('raw', '{MODULE.SOURCE}')", base_sql)

        inventory = yaml.safe_load(
            (REPO_ROOT / "transform" / "models" / "base" / "_raw_sources.yml").read_text(
                encoding="utf-8"
            )
        )
        raw_tables = inventory["sources"][0]["tables"]
        self.assertNotIn(MODULE.SOURCE, {table["name"] for table in raw_tables})


if __name__ == "__main__":
    unittest.main()
