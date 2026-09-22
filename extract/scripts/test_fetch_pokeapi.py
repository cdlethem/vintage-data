from __future__ import annotations

import contextlib
from datetime import datetime
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).with_name("fetch_pokeapi.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "pokeapi_pokemon_data.yml"
REPO_ROOT = SCRIPT.parents[2]
SPEC = importlib.util.spec_from_file_location("fetch_pokeapi", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


def pokemon(identifier: int, name: str | None = None) -> dict[str, object]:
    return {
        "name": name or f"pokemon-{identifier}",
        "url": f"https://pokeapi.co/api/v2/pokemon/{identifier}/",
    }


def page(results, next_offset: int | None, *, limit: int = 2):
    next_url = None
    if next_offset is not None:
        next_url = (
            "https://pokeapi.co/api/v2/pokemon/"
            f"?offset={next_offset}&limit={limit}"
        )
    return {"count": 100, "previous": None, "next": next_url, "results": results}


class Response(io.BytesIO):
    def __init__(self, document, url: str):
        if isinstance(document, bytes):
            body = document
        else:
            body = json.dumps(document).encode("utf-8")
        super().__init__(body)
        self.url = url

    def geturl(self):
        return self.url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FixtureOpener:
    def __init__(self, documents, *, final_urls=None):
        self.documents = list(documents)
        self.final_urls = list(final_urls) if final_urls is not None else None
        self.calls = []

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        if not self.documents:
            raise AssertionError("unexpected HTTP request")
        document = self.documents.pop(0)
        url = request.full_url
        if self.final_urls is not None:
            url = self.final_urls.pop(0)
        return Response(document, url)


class FetchPokeAPITests(unittest.TestCase):
    def fetch(self, documents, **changes):
        opener = FixtureOpener(documents)
        arguments = {
            "resource": "pokemon",
            "page_size": 2,
            "max_pages": 20,
            "opener": opener,
            "sleeper": mock.Mock(),
        }
        arguments.update(changes)
        result = MODULE.fetch_pokemon(**arguments)
        return result, opener, arguments["sleeper"]

    def test_multiple_pages_emit_valid_records_and_stop_at_null(self):
        result, opener, sleeper = self.fetch(
            [
                page([pokemon(1, "bulbasaur"), pokemon(2, "ivysaur")], 2),
                page([pokemon(3, "venusaur")], None),
            ]
        )

        self.assertFalse(result.truncated)
        self.assertEqual([record["id"] for record in result.records], [1, 2, 3])
        self.assertEqual([record["name"] for record in result.records], [
            "bulbasaur", "ivysaur", "venusaur"
        ])
        self.assertEqual(len(opener.calls), 2)
        first_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(opener.calls[0][0].full_url).query
        )
        second_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(opener.calls[1][0].full_url).query
        )
        self.assertEqual(first_query, {"limit": ["2"], "offset": ["0"]})
        self.assertEqual(second_query, {"limit": ["2"], "offset": ["2"]})
        self.assertEqual([call[1] for call in opener.calls], [MODULE.REQUEST_TIMEOUT] * 2)
        sleeper.assert_called_once_with(1.0)

        fetched_values = {record["fetched_at"] for record in result.records}
        self.assertEqual(len(fetched_values), 1)
        fetched_at = datetime.fromisoformat(fetched_values.pop())
        self.assertIsNotNone(fetched_at.tzinfo)
        for record in result.records:
            self.assertEqual(record["source"], "pokeapi_pokemon_data")
            self.assertIs(type(record["id"]), int)
            self.assertGreater(record["id"], 0)
            self.assertEqual(
                record["url"],
                f"https://pokeapi.co/api/v2/pokemon/{record['id']}/",
            )

    def test_empty_catalog_is_a_success_and_makes_one_request(self):
        result, opener, sleeper = self.fetch([page([], None)])
        self.assertEqual(result.records, [])
        self.assertFalse(result.truncated)
        self.assertEqual(len(opener.calls), 1)
        sleeper.assert_not_called()

    def test_hard_page_bound_stops_without_detail_requests_and_reports_truncation(self):
        opener = FixtureOpener([page([pokemon(25), pokemon(26)], 2)])
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(MODULE.urllib.request, "build_opener", return_value=opener),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            MODULE.main(["pokemon", "2", "--max-pages", "1"])

        self.assertEqual(len(opener.calls), 1)
        self.assertEqual(len(stdout.getvalue().splitlines()), 2)
        self.assertIn("truncated after max-pages=1", stderr.getvalue())
        self.assertNotIn("/pokemon/25/", opener.calls[0][0].full_url)

    def test_paces_every_following_request_by_at_least_one_second(self):
        result, opener, sleeper = self.fetch(
            [
                page([pokemon(1), pokemon(2)], 2),
                page([pokemon(3), pokemon(4)], 4),
                page([pokemon(5)], None),
            ]
        )
        self.assertEqual(len(result.records), 5)
        self.assertEqual(len(opener.calls), 3)
        self.assertEqual(sleeper.call_args_list, [mock.call(1.0), mock.call(1.0)])
        self.assertGreaterEqual(MODULE.REQUEST_INTERVAL, 1.0)

    def test_uses_a_finite_timeout_on_every_request(self):
        result, opener, _ = self.fetch([page([pokemon(1)], None)], timeout=7.5)
        self.assertEqual(len(result.records), 1)
        self.assertEqual(opener.calls[0][1], 7.5)
        self.assertGreater(opener.calls[0][1], 0)

    def test_rejects_malformed_pages_and_records(self):
        malformed = (
            ([], "JSON object"),
            ({"results": []}, "results and next"),
            ({"results": {}, "next": None}, "results must be a list"),
            (page(["not-an-object"], None), "non-object"),
            (page([{"name": "", "url": pokemon(1)["url"]}], None), "invalid name"),
            (page([{"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/bulbasaur/"}], None), "positive integer id"),
            (page([pokemon(1), pokemon(1)], None), "duplicate"),
            (page([], 2), "empty page"),
        )
        for document, message in malformed:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.PokeAPIError, message):
                    self.fetch([document])

    def test_rejects_duplicate_ids_across_pages_without_emitting(self):
        with self.assertRaisesRegex(MODULE.PokeAPIError, "duplicate Pokémon id 2"):
            self.fetch(
                [
                    page([pokemon(1), pokemon(2)], 2),
                    page([pokemon(2)], None),
                ]
            )

    def test_rejects_off_origin_and_non_catalog_next_urls(self):
        bad_urls = (
            "https://example.com/api/v2/pokemon/?limit=2&offset=2",
            "http://pokeapi.co/api/v2/pokemon/?limit=2&offset=2",
            "https://pokeapi.co/api/v2/ability/?limit=2&offset=2",
            "https://pokeapi.co/api/v2/pokemon/1/",
            "https://pokeapi.co/api/v2/pokemon/?limit=2&offset=4",
        )
        for next_url in bad_urls:
            with self.subTest(next_url=next_url):
                document = page([pokemon(1), pokemon(2)], None)
                document["next"] = next_url
                with self.assertRaisesRegex(MODULE.PokeAPIError, "catalog|pagination"):
                    self.fetch([document])


    def test_redirect_handler_and_final_response_reject_off_origin_urls(self):
        handler = MODULE.CatalogRedirectHandler()
        request = MODULE.urllib.request.Request(
            "https://pokeapi.co/api/v2/pokemon/?limit=2&offset=0"
        )
        with self.assertRaisesRegex(MODULE.PokeAPIError, "catalog endpoint"):
            handler.redirect_request(
                request, None, 302, "Found", {},
                "https://example.com/api/v2/pokemon/?limit=2&offset=0",
            )
        with self.assertRaisesRegex(MODULE.PokeAPIError, "changed the requested"):
            handler.redirect_request(
                request, None, 302, "Found", {},
                "https://pokeapi.co/api/v2/pokemon/?limit=2&offset=2",
            )

        opener = FixtureOpener(
            [page([pokemon(1)], None)],
            final_urls=["https://example.com/api/v2/pokemon/?limit=2&offset=0"],
        )
        with self.assertRaisesRegex(MODULE.PokeAPIError, "catalog endpoint"):
            MODULE.fetch_pokemon("pokemon", 2, opener=opener, sleeper=mock.Mock())

    def test_http_and_json_failures_are_useful_nonzero_cli_errors(self):
        failures = (
            (
                urllib.error.HTTPError(
                    "https://pokeapi.co/api/v2/pokemon/?limit=2&offset=0",
                    503,
                    "Unavailable",
                    {},
                    None,
                ),
                "HTTP 503",
            ),
            (Response(b"not-json", "https://pokeapi.co/api/v2/pokemon/?limit=2&offset=0"), "malformed JSON"),
        )
        for failure, diagnostic in failures:
            with self.subTest(diagnostic=diagnostic):
                opener = mock.Mock()
                if isinstance(failure, BaseException):
                    opener.open.side_effect = failure
                else:
                    opener.open.return_value = failure
                stderr = io.StringIO()
                with (
                    mock.patch.object(MODULE.urllib.request, "build_opener", return_value=opener),
                    contextlib.redirect_stderr(stderr),
                    self.assertRaises(SystemExit) as raised,
                ):
                    MODULE.main(["pokemon", "2", "--max-pages", "1"])
                self.assertEqual(raised.exception.code, 1)
                self.assertIn("pokeapi_pokemon_data", stderr.getvalue())
                self.assertIn(diagnostic, stderr.getvalue())

    def test_main_emits_only_parseable_ndjson_to_stdout(self):
        opener = FixtureOpener([page([pokemon(7, "squirtle")], None)])
        stdout = io.StringIO()
        with (
            mock.patch.object(MODULE.urllib.request, "build_opener", return_value=opener),
            contextlib.redirect_stdout(stdout),
        ):
            MODULE.main(["pokemon", "100", "--max-pages", "20"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(json.loads(lines[0])["id"], 7)
        self.assertNotIn(" ", lines[0])

    def test_invalid_arguments_exit_nonzero_without_http(self):
        cases = (
            ["ability", "2"],
            ["pokemon", "0"],
            ["pokemon", "1001"],
            ["pokemon", "2", "--max-pages", "0"],
            ["pokemon", "2", "--max-pages", "21"],
            ["pokemon"],
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                completed = subprocess.run(
                    [sys.executable, str(SCRIPT), *arguments],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=10,
                    check=False,
                )
                self.assertNotEqual(completed.returncode, 0)
                self.assertEqual(completed.stdout, "")
                self.assertIn("usage:", completed.stderr)


class SourceFactoryIntegrationTests(unittest.TestCase):
    def test_real_factory_discovers_enabled_daily_source_with_configured_arguments(self):
        dags_root = REPO_ROOT / "orchestration" / "dags"
        factory_path = dags_root / "extract_dags.py"

        created_dags = []
        active_dags = []

        class FakeDAG:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                self.task = None
                created_dags.append(self)

            def __enter__(self):
                active_dags.append(self)
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                active_dags.pop()

        class FakePythonOperator:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
                active_dags[-1].task = self

        airflow = ModuleType("airflow")
        airflow.DAG = FakeDAG
        airflow_providers = ModuleType("airflow.providers")
        airflow_standard = ModuleType("airflow.providers.standard")
        airflow_operators = ModuleType("airflow.providers.standard.operators")
        airflow_python = ModuleType("airflow.providers.standard.operators.python")
        airflow_python.PythonOperator = FakePythonOperator

        pendulum = ModuleType("pendulum")
        pendulum.datetime = lambda *args, **kwargs: (args, kwargs)

        cadence_plan = ModuleType("cadence_plan")
        cadence_plan.plan_sources = lambda: {}
        cadence_plan.effective_schedule = lambda cfg, plan: (
            cfg["schedule"], "declared (cadence detection off)"
        )

        extract_runner = ModuleType("extract_runner")
        extract_runner.SCRIPTS_DIR = SCRIPT.parent
        extract_runner.run = lambda cfg: cfg

        replacements = {
            "airflow": airflow,
            "airflow.providers": airflow_providers,
            "airflow.providers.standard": airflow_standard,
            "airflow.providers.standard.operators": airflow_operators,
            "airflow.providers.standard.operators.python": airflow_python,
            "pendulum": pendulum,
            "cadence_plan": cadence_plan,
            "extract_runner": extract_runner,
        }
        factory_spec = importlib.util.spec_from_file_location(
            "extract_dags_pokeapi_test", factory_path
        )
        factory = importlib.util.module_from_spec(factory_spec)
        assert factory_spec.loader is not None
        with mock.patch.dict(sys.modules, replacements):
            factory_spec.loader.exec_module(factory)

        dag = getattr(factory, "extract__pokeapi_pokemon_data")
        self.assertIn(dag, created_dags)
        self.assertEqual(dag.dag_id, "extract__pokeapi_pokemon_data")
        self.assertEqual(dag.schedule, "23 11 * * *")
        self.assertFalse(dag.is_paused_upon_creation)
        self.assertEqual(dag.tags, ["extract"])
        self.assertEqual(dag.task.task_id, "run")
        config = dag.task.op_kwargs["cfg"]
        self.assertEqual(config["name"], "pokeapi_pokemon_data")
        self.assertEqual(config["script"], "fetch_pokeapi.py")
        self.assertEqual(
            config["args"], ["pokemon", "100", "--max-pages", "20"]
        )
        self.assertTrue(config["enabled"])


if __name__ == "__main__":
    unittest.main()
