"""Tests for fetch_oecd_cpi.py."""

import importlib.util
import io
import json
import sys
import unittest
import unittest.mock
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
from urllib.request import Request

import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "extract" / "scripts" / "fetch_oecd_cpi.py"
SOURCE_CONFIG = ROOT / "extract" / "sources" / "oecd_cpi.yml"
DAG_FACTORY = ROOT / "orchestration" / "dags" / "extract_dags.py"
SPEC = importlib.util.spec_from_file_location("fetch_oecd_cpi", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

FIXTURE_KEY = ".M.N.CPI.IX._T.N._Z"


def fixture_document() -> dict[str, Any]:
    """A minimal, well-formed SDMX-JSON response for two areas and two periods."""
    return {
        "prepared": "2026-10-26T00:00:00Z",
        "data": {
            "structure": {
                "dimensions": {
                    "observation": [
                        {
                            "id": "REF_AREA",
                            "values": [
                                {"id": "USA", "name": "United States of America"},
                                {"id": "DEU", "name": "Germany"},
                            ],
                        },
                        {"id": "FREQ", "values": [{"id": "M", "name": "Monthly"}]},
                        {"id": "METHODOLOGY", "values": [{"id": "N", "name": "National"}]},
                        {
                            "id": "MEASURE",
                            "values": [{"id": "CPI", "name": "Consumer Price Index"}],
                        },
                        {"id": "UNIT_MEASURE", "values": [{"id": "IX", "name": "Index"}]},
                        {"id": "EXPENDITURE", "values": [{"id": "_T", "name": "Total"}]},
                        {"id": "ADJUSTMENT", "values": [{"id": "N", "name": "Unadjusted"}]},
                        {
                            "id": "TRANSFORMATION",
                            "values": [{"id": "_Z", "name": "As Published"}],
                        },
                        {
                            "id": "TIME_PERIOD",
                            "values": [
                                {"id": "2026-07", "name": "2026-07"},
                                {"id": "2026-08", "name": "2026-08"},
                            ],
                        },
                    ]
                },
                "attributes": {
                    "observation": [
                        {
                            "id": "OBS_STATUS",
                            "values": [
                                {"id": "A", "name": "Normal value"},
                                {"id": "E", "name": "Estimated value"},
                            ],
                        },
                        {"id": "BASE_PER", "values": [{"id": "2015", "name": "2015"}]},
                        {"id": "DURABILITY", "values": []},
                        {"id": "DECIMALS", "values": [{"id": "2", "name": "Two"}]},
                    ]
                },
            },
            "dataSets": [
                {
                    "action": "Information",
                    "observations": {
                        "0:0:0:0:0:0:0:0:0": [138.9, 0, 0, None, 0],
                        "0:0:0:0:0:0:0:0:1": [139.4, 0, 0, None, 0],
                        "1:0:0:0:0:0:0:0:0": [117.2, 1, 0, None, 0],
                        "1:0:0:0:0:0:0:0:1": [117.9, 1, 0, None, 0],
                    },
                }
            ],
        },
    }


def document_with(value: Any, obs_key: str = "0:0:0:0:0:0:0:0:0") -> dict[str, Any]:
    document = fixture_document()
    document["data"]["dataSets"][0]["observations"][obs_key][0] = value
    return document

class JsonResponse(io.BytesIO):
    """A readable response body with the headers the fetcher inspects."""

    def __init__(self, body: bytes) -> None:
        super().__init__(body)
        self.headers = {"Content-Length": str(len(body))}


class ParseResponseTest(unittest.TestCase):
    def test_fixture_parses_in_area_period_order_with_stable_ids(self) -> None:
        records = MODULE.parse_response(
            fixture_document(), FIXTURE_KEY, "2026-07", "2026-08", "2026-10-26T00:00:00Z"
        )
        self.assertEqual(len(records), 4)
        self.assertEqual(
            [(record["ref_area"], record["period"]) for record in records],
            [("DEU", "2026-07"), ("DEU", "2026-08"), ("USA", "2026-07"), ("USA", "2026-08")],
        )
        first = records[0]
        self.assertEqual(first["source"], "oecd_cpi")
        self.assertEqual(first["ref_area_label"], "Germany")
        self.assertEqual(first["index"], 117.2)
        self.assertIn(first["attributes"]["OBS_STATUS"]["id"], {"A", "E"})
        self.assertIsNone(first["attributes"]["DURABILITY"])
        self.assertEqual(first["dataflow"], MODULE.DATAFLOW_ID)
        self.assertEqual(first["source_metadata"]["provider"], "OECD")
        # Stable ids: a value revision must not change observation identity.
        again = MODULE.parse_response(
            document_with(139.0), FIXTURE_KEY, "2026-07", "2026-08", "2026-10-26T00:00:01Z"
        )
        self.assertEqual([r["id"] for r in records], [r["id"] for r in again])

    def test_rejects_out_of_window_period(self) -> None:
        document = fixture_document()
        document["data"]["structure"]["dimensions"]["observation"][8]["values"].append(
            {"id": "2026-09", "name": "2026-09"}
        )
        document["data"]["dataSets"][0]["observations"]["1:0:0:0:0:0:0:0:2"] = [1.0, 0, 0, None, 0]
        with self.assertRaisesRegex(ValueError, "outside the requested window"):
            MODULE.parse_response(
                document, FIXTURE_KEY, "2026-07", "2026-08", "2026-10-26T00:00:00Z"
            )

    def test_rejects_non_finite_observation_value(self) -> None:
        with self.assertRaisesRegex(ValueError, "finite number"):
            MODULE.parse_response(
                document_with(float("nan")), FIXTURE_KEY, "2026-07", "2026-08",
                "2026-10-26T00:00:00Z",
            )

    def test_rejects_wrong_fixed_dimension_code(self) -> None:
        document = fixture_document()
        document["data"]["structure"]["dimensions"]["observation"][1]["values"][0] = {
            "id": "Q",
            "name": "Quarterly",
        }
        with self.assertRaisesRegex(ValueError, "FREQ"):
            MODULE.parse_response(
                document, FIXTURE_KEY, "2026-07", "2026-08", "2026-10-26T00:00:00Z"
            )

    def test_rejects_missing_structure(self) -> None:
        document = fixture_document()
        del document["data"]["structure"]
        with self.assertRaisesRegex(ValueError, "structure"):
            MODULE.parse_response(
                document, FIXTURE_KEY, "2026-07", "2026-08", "2026-10-26T00:00:00Z"
            )

    def test_window_and_key_validation(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not be after"):
            MODULE.validate_request(FIXTURE_KEY, "2026-08", "2026-07", 60, 2, 15)
        with self.assertRaisesRegex(ValueError, "must not exceed 36 months"):
            MODULE.validate_request(FIXTURE_KEY, "2020-01", "2026-01", 60, 2, 15)
        with self.assertRaisesRegex(ValueError, "YYYY-MM period"):
            MODULE.validate_request(FIXTURE_KEY, "2026-1", "2026-08", 60, 2, 15)
        with self.assertRaisesRegex(ValueError, "eight dot-separated"):
            MODULE.validate_request("M.N.CPI.IX._T.N._Z", "2026-07", "2026-08", 60, 2, 15)


class FetchWithRetriesTest(unittest.TestCase):
    def _urlopen(self, body: bytes, fail_times: int):
        state = {"calls": 0}

        def fake_urlopen(request: Request, timeout: float):
            state["calls"] += 1
            if state["calls"] <= fail_times:
                raise HTTPError(request.full_url, 503, "Service Unavailable", None, None)
            return JsonResponse(body)

        return fake_urlopen, state

    def test_transient_503_then_success(self) -> None:
        body = json.dumps(fixture_document()).encode("utf-8")
        fake, state = self._urlopen(body, fail_times=1)
        with unittest.mock.patch.object(MODULE.urllib.request, "urlopen", fake):
            records = MODULE.fetch_observations(
                FIXTURE_KEY, "2026-07", "2026-08", timeout=5, retries=2, retry_delay=0
            )
        self.assertEqual(len(records), 4)
        self.assertEqual(state["calls"], 2)
        self.assertEqual(MODULE.LAST_RUN_STATS["requests"]["attempted"], 2)
        self.assertEqual(MODULE.LAST_RUN_STATS["requests"]["retried"], 1)

    def test_retries_exhausted_raises(self) -> None:
        body = b"{}"
        fake, state = self._urlopen(body, fail_times=99)
        with unittest.mock.patch.object(MODULE.urllib.request, "urlopen", fake):
            with self.assertRaisesRegex(Exception, "503"):
                MODULE.fetch_observations(
                    FIXTURE_KEY, "2026-07", "2026-08", timeout=5, retries=1, retry_delay=0
                )
        self.assertEqual(state["calls"], 2)
        self.assertEqual(MODULE.LAST_RUN_STATS["requests"]["attempted"], 2)


class BuildUrlTest(unittest.TestCase):
    def test_builds_expected_url(self) -> None:
        url = MODULE.build_url(FIXTURE_KEY, "2026-07", "2026-08")
        parts = urlparse(url)
        self.assertEqual(parts.netloc, "sdmx.oecd.org")
        self.assertTrue(parts.path.startswith("/public/rest/data/OECD.SDD.TPS"))
        query = parse_qs(parts.query)
        self.assertEqual(query["startPeriod"], ["2026-07"])
        self.assertEqual(query["endPeriod"], ["2026-08"])
        self.assertIn(FIXTURE_KEY, parts.path)


class CliTest(unittest.TestCase):
    def test_emits_ndjson_and_summary(self) -> None:
        body = json.dumps(fixture_document()).encode("utf-8")

        def fake(request: Request, timeout: float):
            return JsonResponse(body)
        argv = [
            FIXTURE_KEY,
            "2026-07",
            "2026-08",
            "--timeout", "5",
            "--retries", "0",
            "--retry-delay", "0",
        ]
        with unittest.mock.patch.object(MODULE.urllib.request, "urlopen", fake), \
                unittest.mock.patch.object(sys, "stdout", new_callable=io.StringIO) as out, \
                unittest.mock.patch.object(sys, "stderr", new_callable=io.StringIO) as err:
            code = MODULE.main(argv)
        self.assertEqual(code, 0)
        lines = out.getvalue().splitlines()
        self.assertEqual(len(lines), 4)
        self.assertEqual(json.loads(lines[0])["source"], "oecd_cpi")
        summary_lines = [
            line for line in err.getvalue().splitlines()
            if line.startswith(MODULE.SUMMARY_PREFIX)
        ]
        self.assertEqual(len(summary_lines), 1)
        summary = json.loads(summary_lines[0].split("\t", 1)[1])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["records"], 4)


class SourceConfigTest(unittest.TestCase):
    def test_yaml_is_parsable(self) -> None:
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], "oecd_cpi")
        self.assertEqual(config["script"], "fetch_oecd_cpi.py")
        self.assertTrue(config["enabled"])
        self.assertEqual(config["retries"], 1)
        self.assertTrue(config["schedule"].strip())


class DagStub:
    active: "DagStub | None" = None

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)
        self.tasks: list[Any] = []

    def __enter__(self) -> "DagStub":
        type(self).active = self
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool:
        type(self).active = None
        return False


class DagFactoryTest(unittest.TestCase):
    def test_factory_discovers_source_with_runtime_stubs(self) -> None:
        from types import SimpleNamespace

        airflow = ModuleType("airflow")
        airflow.DAG = DagStub
        providers = ModuleType("airflow.providers")
        standard = ModuleType("airflow.providers.standard")
        operators = ModuleType("airflow.providers.standard.operators")
        python_operator = ModuleType("airflow.providers.standard.operators.python")

        def make_operator(**kwargs: Any) -> Any:
            dag = DagStub.active
            if dag is None:
                raise RuntimeError("operator created outside a DAG context")
            task = SimpleNamespace(**{**dag.default_args, **kwargs})
            dag.tasks.append(task)
            return task

        python_operator.PythonOperator = make_operator
        pendulum = ModuleType("pendulum")
        pendulum.datetime = lambda *args, **kwargs: (args, kwargs)
        cadence_plan = ModuleType("cadence_plan")
        cadence_plan.plan_sources = lambda: {}
        cadence_plan.effective_schedule = lambda config, plan: (
            config["schedule"],
            "fixture schedule",
        )
        extract_runner = ModuleType("extract_runner")
        extract_runner.SCRIPTS_DIR = SCRIPT.parent
        extract_runner.run = lambda cfg: cfg
        stubs = {
            "airflow": airflow,
            "airflow.providers": providers,
            "airflow.providers.standard": standard,
            "airflow.providers.standard.operators": operators,
            "airflow.providers.standard.operators.python": python_operator,
            "pendulum": pendulum,
            "cadence_plan": cadence_plan,
            "extract_runner": extract_runner,
        }
        dag_spec = importlib.util.spec_from_file_location(
            "fixture_extract_dags_oecd", DAG_FACTORY
        )
        dag_module = importlib.util.module_from_spec(dag_spec)
        assert dag_spec.loader is not None
        with unittest.mock.patch.dict(sys.modules, stubs):
            dag_spec.loader.exec_module(dag_module)

        dag = getattr(dag_module, "extract__oecd_cpi")
        self.assertEqual(dag.schedule, "47 12 * * *")
        self.assertFalse(dag.is_paused_upon_creation)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertIn("fetch_oecd_cpi.py", dag.doc_md)
        self.assertEqual(len(dag.tasks), 1)
        self.assertEqual(dag.tasks[0].task_id, "run")
        self.assertEqual(dag.tasks[0].retries, 1)


if __name__ == "__main__":
    unittest.main()
