import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
import tempfile
from unittest import mock
import urllib.error
import urllib.parse

import yaml


SCRIPT = Path(__file__).with_name("fetch_gitlab_public_projects.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "gitlab_public_projects.yml"
REPO_ROOT = SCRIPT.parents[2]
RAW_GENERATOR = REPO_ROOT / "transform" / "scripts" / "sync_raw_sources.py"
LOADER_SCHEMA = REPO_ROOT / "load" / "loader" / "schema.py"
EXTRACT_RUNNER = REPO_ROOT / "orchestration" / "include" / "extract_runner.py"
SPEC = importlib.util.spec_from_file_location("fetch_gitlab_public_projects", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class JsonResponse(io.BytesIO):
    def __init__(self, document, headers=None):
        payload = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
        super().__init__(payload)
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def project(project_id=101, **changes):
    value = {
        "id": project_id,
        "description": "Complete object is retained",
        "name": "  Example Project  ",
        "name_with_namespace": "Example Group / Example Project",
        "path": " example-project ",
        "path_with_namespace": " example-group/example-project ",
        "created_at": "2026-09-01T10:20:30.000Z",
        "default_branch": "main",
        "topics": [" metadata ", "open-source"],
        "ssh_url_to_repo": "git@gitlab.com:example-group/example-project.git",
        "http_url_to_repo": "https://gitlab.com/example-group/example-project.git",
        "web_url": "https://gitlab.com/example-group/example-project",
        "star_count": 12,
        "forks_count": 3,
        "last_activity_at": "2026-09-17T11:22:33.000+00:00",
        "namespace": {
            "id": 7,
            "name": "Example Group",
            "path": "example-group",
            "kind": "group",
            "full_path": " example-group ",
        },
        "visibility": "public",
        "extra_future_field": {"kept": True},
    }
    value.update(changes)
    return value


def http_error(code, headers=None):
    return urllib.error.HTTPError(
        MODULE.API_URL,
        code,
        "error",
        headers or {},
        io.BytesIO(b"error"),
    )


class FetchGitLabPublicProjectsTests(unittest.TestCase):
    def fetch(self, responses, **changes):
        arguments = {
            "per_page": 2,
            "max_pages": 3,
            "order_by": "last_activity_at",
            "sort": "desc",
            "timeout": 17,
            "retries": 0,
            "retry_budget": 60,
        }
        arguments.update(changes)
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            records = MODULE.fetch_public_projects(**arguments)
        return records, urlopen

    def test_normalizes_project_and_preserves_complete_raw_object(self):
        wire = project()
        records, _ = self.fetch([JsonResponse([wire], {"X-Next-Page": ""})])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["source"], "gitlab_public_projects")
        self.assertRegex(record["fetched_at"], r"\+00:00$")
        self.assertEqual(record["id"], "101")
        self.assertEqual(record["name"], "Example Project")
        self.assertEqual(record["path"], "example-project")
        self.assertEqual(record["path_with_namespace"], "example-group/example-project")
        self.assertEqual(record["namespace_path"], "example-group")
        self.assertEqual(record["web_url"], wire["web_url"])
        self.assertEqual(record["http_url_to_repo"], wire["http_url_to_repo"])
        self.assertEqual(record["ssh_url_to_repo"], wire["ssh_url_to_repo"])
        self.assertEqual(record["visibility"], "public")
        self.assertEqual(record["created_at"], "2026-09-01T10:20:30+00:00")
        self.assertEqual(record["last_activity_at"], "2026-09-17T11:22:33+00:00")
        self.assertEqual(record["star_count"], 12)
        self.assertEqual(record["forks_count"], 3)
        self.assertEqual(record["topics"], ["metadata", "open-source"])
        self.assertEqual(record["raw"], wire)
        self.assertIsNot(record["raw"], wire)
        self.assertEqual(record["raw"]["extra_future_field"], {"kept": True})

        later = MODULE.normalize_project(wire, "later")
        self.assertEqual(later["id"], record["id"])
        self.assertNotEqual(later["fetched_at"], record["fetched_at"])

    def test_multi_page_progression_uses_public_sort_parameters_and_no_authentication(self):
        responses = [
            JsonResponse([project(1), project(2)], {"X-Next-Page": "2"}),
            JsonResponse([project(3)], {"X-Next-Page": ""}),
        ]
        records, urlopen = self.fetch(responses, order_by="star_count", sort="asc")

        self.assertEqual([record["id"] for record in records], ["1", "2", "3"])
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(len({record["fetched_at"] for record in records}), 1)
        for expected_page, call in enumerate(urlopen.call_args_list, 1):
            request = call.args[0]
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(request.full_url).query)
            self.assertEqual(
                query,
                {
                    "visibility": ["public"],
                    "simple": ["false"],
                    "order_by": ["star_count"],
                    "sort": ["asc"],
                    "per_page": ["2"],
                    "page": [str(expected_page)],
                },
            )
            self.assertEqual(request.get_header("Accept"), "application/json")
            self.assertTrue(request.get_header("User-agent"))
            for header in ("Private-token", "Authorization", "Job-token"):
                self.assertIsNone(request.get_header(header))
            self.assertEqual(call.kwargs, {"timeout": 17.0})

    def test_max_pages_is_an_enforced_sample_boundary(self):
        responses = [
            JsonResponse([project(1), project(2)], {"X-Next-Page": "2"}),
            JsonResponse([project(3), project(4)], {"X-Next-Page": "3"}),
        ]
        records, urlopen = self.fetch(responses, max_pages=2)

        self.assertEqual([record["id"] for record in records], ["1", "2", "3", "4"])
        self.assertEqual(urlopen.call_count, 2)

    def test_empty_short_and_explicit_terminal_pages_stop(self):
        cases = (
            ([JsonResponse([], {"X-Next-Page": "2"})], []),
            ([JsonResponse([project(1)], {"X-Next-Page": "2"})], ["1"]),
            ([JsonResponse([project(1), project(2)], {"X-Next-Page": ""})], ["1", "2"]),
        )
        for responses, expected in cases:
            with self.subTest(expected=expected):
                records, urlopen = self.fetch(responses)
                self.assertEqual([record["id"] for record in records], expected)
                urlopen.assert_called_once()

    def test_duplicate_ids_within_and_across_pages_are_emitted_once(self):
        responses = [
            JsonResponse([project(1), project(1, name="Duplicate")], {"X-Next-Page": "2"}),
            JsonResponse([project(1), project(2)], {"X-Next-Page": ""}),
        ]
        records, _ = self.fetch(responses)
        self.assertEqual([record["id"] for record in records], ["1", "2"])
        self.assertEqual(records[0]["name"], "Example Project")

    def test_invalid_limits_and_sorting_fail_before_http(self):
        cases = (
            ({"per_page": 0}, "per_page"),
            ({"per_page": MODULE.MAX_PER_PAGE + 1}, "per_page"),
            ({"max_pages": 0}, "max_pages"),
            ({"max_pages": MODULE.HARD_MAX_PAGES + 1}, "max_pages"),
            ({"timeout": 0}, "timeout"),
            ({"retries": -1}, "retries"),
            ({"retry_budget": 0}, "retry_budget"),
            ({"order_by": "popularity"}, "order_by"),
            ({"sort": "sideways"}, "sort"),
        )
        for changes, message in cases:
            with self.subTest(changes=changes):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.fetch_public_projects(**changes)
                urlopen.assert_not_called()

    def test_rejects_private_projects_and_malformed_project_fields(self):
        malformed = (
            (project(visibility="private"), "not public"),
            (project(id="101"), "invalid id"),
            (project(namespace=None), "namespace"),
            (project(topics="metadata"), "topics"),
            (project(star_count=-1), "star_count"),
            (project(created_at="yesterday"), "created_at"),
        )
        for document, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.GitLabError, message):
                    self.fetch([JsonResponse([document], {"X-Next-Page": ""})])

    def test_rejects_malformed_payload_pagination_and_oversize_page(self):
        cases = (
            (JsonResponse({"projects": []}), "JSON list"),
            (JsonResponse(b"not-json"), "malformed JSON"),
            (JsonResponse([project(1), project(2), project(3)]), "above per_page"),
            (JsonResponse([project(1), project(2)], {"X-Next-Page": "bad"}), "X-Next-Page"),
            (JsonResponse([project(1), project(2)], {"X-Next-Page": "4"}), "does not follow"),
        )
        for response, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.GitLabError, message):
                    self.fetch([response])

    def test_429_respects_retry_after_then_succeeds(self):
        sleeps = []
        client = MODULE.GitLabClient(
            timeout=17,
            retries=1,
            retry_budget=30,
            clock=lambda: 0,
            sleeper=sleeps.append,
        )
        responses = [
            http_error(429, {"Retry-After": "3"}),
            JsonResponse([project(1)], {"X-Next-Page": ""}),
        ]
        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses) as urlopen:
            records = MODULE.fetch_public_projects(
                per_page=2,
                max_pages=1,
                timeout=17,
                retries=1,
                retry_budget=30,
                client=client,
            )

        self.assertEqual([record["id"] for record in records], ["1"])
        self.assertEqual(sleeps, [3.0])
        self.assertEqual(urlopen.call_count, 2)

    def test_rate_limit_delay_must_fit_local_cap_and_budget(self):
        cases = (
            (121, 180, "local 120s cap"),
            (10, 5, "remaining retry budget"),
        )
        for delay, budget, message in cases:
            with self.subTest(delay=delay, budget=budget):
                client = MODULE.GitLabClient(
                    timeout=2,
                    retries=1,
                    retry_budget=budget,
                    clock=lambda: 0,
                    sleeper=lambda _: None,
                )
                with mock.patch.object(
                    MODULE.urllib.request,
                    "urlopen",
                    side_effect=http_error(429, {"Retry-After": str(delay)}),
                ):
                    with self.assertRaisesRegex(MODULE.RetryBudgetError, message):
                        client.get_page({"page": 1})

    def test_timeout_and_server_error_exhaust_retries(self):
        failures = (
            TimeoutError("timed out"),
            http_error(503),
        )
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                sleeps = []
                client = MODULE.GitLabClient(
                    timeout=5,
                    retries=2,
                    retry_budget=30,
                    clock=lambda: 0,
                    sleeper=sleeps.append,
                )
                with mock.patch.object(
                    MODULE.urllib.request,
                    "urlopen",
                    side_effect=[failure, failure, failure],
                ) as urlopen:
                    with self.assertRaisesRegex(MODULE.GitLabError, "after 3 attempts"):
                        client.get_page({"page": 1})
                self.assertEqual(urlopen.call_count, 3)
                self.assertEqual(sleeps, [1.0, 2.0])

    def test_failed_later_page_exits_nonzero_without_stdout(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        responses = [
            JsonResponse([project(1), project(2)], {"X-Next-Page": "2"}),
            http_error(503),
            http_error(503),
            http_error(503),
        ]
        with (
            mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=responses),
            mock.patch.object(MODULE.time, "sleep"),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            with self.assertRaises(SystemExit) as raised:
                MODULE.main(
                    [
                        "--per-page",
                        "2",
                        "--max-pages",
                        "2",
                        "--timeout",
                        "1",
                        "--retries",
                        "2",
                        "--retry-budget",
                        "30",
                    ]
                )
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("after 3 attempts", stderr.getvalue())

    def test_main_fixture_output_is_valid_ndjson_and_passes_existing_contract(self):
        stdout = io.StringIO()
        response = JsonResponse([project(1), project(2)], {"X-Next-Page": ""})
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response):
            with contextlib.redirect_stdout(stdout):
                MODULE.main(["--per-page", "2", "--max-pages", "1", "--retries", "0"])

        include_dir = EXTRACT_RUNNER.parent
        sys.path.insert(0, str(include_dir))
        try:
            runner_spec = importlib.util.spec_from_file_location("extract_runner", EXTRACT_RUNNER)
            runner = importlib.util.module_from_spec(runner_spec)
            assert runner_spec.loader is not None
            runner_spec.loader.exec_module(runner)
        finally:
            sys.path.remove(str(include_dir))

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 2)
        for line in lines:
            record = json.loads(line)
            self.assertTrue(record["source"])
            self.assertTrue(record["fetched_at"])
            self.assertTrue(record["id"])
            runner._check_envelope(line)
        self.assertNotIn(": ", lines[0])

    def test_source_yaml_parses_and_configured_argv_matches_extractor_bounds(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["sink"], "local")
        self.assertEqual(config["retries"], 1)

        args = MODULE.build_parser().parse_args(config["args"])
        self.assertLessEqual(args.per_page, MODULE.MAX_PER_PAGE)
        self.assertLessEqual(args.max_pages, MODULE.HARD_MAX_PAGES)
        self.assertLessEqual(args.timeout, MODULE.MAX_TIMEOUT)
        self.assertLessEqual(args.retries, MODULE.MAX_RETRIES)
        self.assertLessEqual(args.retry_budget, MODULE.MAX_RETRY_BUDGET)
        worst_case_request_time = args.max_pages * (args.retries + 1) * args.timeout
        self.assertLessEqual(min(worst_case_request_time, args.retry_budget), config["timeout_minutes"] * 60)
        empty_client = mock.Mock()
        empty_client.get_page.return_value = ([], {})
        self.assertEqual(
            MODULE.fetch_public_projects(
                per_page=args.per_page,
                max_pages=args.max_pages,
                order_by=args.order_by,
                sort=args.sort,
                timeout=args.timeout,
                retries=args.retries,
                retry_budget=args.retry_budget,
                client=empty_client,
            ),
            [],
        )

        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertIn("bounded sample", text)
        self.assertIn("not a consistent or exhaustive snapshot", " ".join(text.split()))
        self.assertIn("Database-backed raw-source synchronization", text)
        self.assertIn("external gate", text)

    def test_offline_loader_naming_and_generator_output_are_consistent(self):
        loader_spec = importlib.util.spec_from_file_location("loader_schema", LOADER_SCHEMA)
        loader = importlib.util.module_from_spec(loader_spec)
        assert loader_spec.loader is not None
        sys.modules[loader_spec.name] = loader
        try:
            loader_spec.loader.exec_module(loader)
        finally:
            sys.modules.pop(loader_spec.name, None)

        record = MODULE.normalize_project(project(), "2026-09-17T12:00:00+00:00")
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
            "2026-09-17T12:00:00+00:00",
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
        self.assertEqual(declared["topics"], "JSON")
        self.assertEqual(declared["raw"], "JSON")
        self.assertEqual(declared["created_at"], "TIMESTAMP WITH TIME ZONE")
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
