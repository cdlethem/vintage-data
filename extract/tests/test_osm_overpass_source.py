import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest import mock

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "extract" / "sources" / "osm_overpass.yml"
SCRIPT = REPO_ROOT / "extract" / "scripts" / "fetch_osm_overpass.py"
FACTORY = REPO_ROOT / "orchestration" / "dags" / "extract_dags.py"


class FakeDAG:
    def __init__(self, **kwargs):
        self.dag_id = kwargs["dag_id"]
        self.schedule = kwargs["schedule"]
        self.catchup = kwargs["catchup"]
        self.max_active_runs = kwargs["max_active_runs"]
        self.is_paused_upon_creation = kwargs["is_paused_upon_creation"]
        self.default_args = kwargs["default_args"]
        self.doc_md = kwargs["doc_md"]
        self.tasks = []

    def __enter__(self):
        FakePythonOperator.current_dag = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        FakePythonOperator.current_dag = None


class FakePythonOperator:
    current_dag = None

    def __init__(self, **kwargs):
        self.task_id = kwargs["task_id"]
        self.python_callable = kwargs["python_callable"]
        self.op_kwargs = kwargs["op_kwargs"]
        self.execution_timeout = kwargs["execution_timeout"]
        if self.current_dag is not None:
            self.current_dag.tasks.append(self)


def load_candidate_dag():
    """Execute the real DAG factory against only the checked-in candidate config."""
    airflow = types.ModuleType("airflow")
    airflow.DAG = FakeDAG
    providers = types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    python_operator = types.ModuleType("airflow.providers.standard.operators.python")
    python_operator.PythonOperator = FakePythonOperator

    pendulum = types.ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: (args, kwargs)

    cadence_plan = types.ModuleType("cadence_plan")
    cadence_plan.plan_sources = lambda: {}
    cadence_plan.effective_schedule = lambda cfg, plan: (
        cfg["schedule"], "declared (cadence detection off)"
    )

    extract_runner = types.ModuleType("extract_runner")
    extract_runner.SCRIPTS_DIR = SCRIPT.parent
    extract_runner.run = lambda cfg: cfg

    modules = {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.python": python_operator,
        "pendulum": pendulum,
        "cadence_plan": cadence_plan,
        "extract_runner": extract_runner,
    }
    spec = importlib.util.spec_from_file_location("candidate_extract_dags", FACTORY)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    original_glob = Path.glob

    def candidate_only(path, pattern):
        if path == REPO_ROOT / "extract" / "sources" and pattern == "*.yml":
            return iter((CONFIG,))
        return original_glob(path, pattern)

    with mock.patch.dict(sys.modules, modules), mock.patch.object(
        Path, "glob", candidate_only
    ):
        spec.loader.exec_module(module)
    return module


class OsmOverpassSourceIntegrationTests(unittest.TestCase):
    def test_checked_in_config_is_bounded_conservative_and_effectively_disabled(self):
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

        self.assertEqual(config["name"], "osm_overpass")
        self.assertEqual(config["script"], "fetch_osm_overpass.py")
        self.assertTrue(SCRIPT.is_file())
        self.assertFalse(config["enabled"])
        self.assertEqual(config["retries"], 0)
        self.assertLessEqual(config["timeout_minutes"], 3)
        self.assertEqual(config["schedule"], "41 6 15 * *")
        self.assertEqual(
            config["args"],
            [
                "--bbox", "51.50,-0.13,51.51,-0.12",
                "--tag", "amenity=drinking_water",
                "--http-timeout", "45",
                "--query-timeout", "25",
                "--retries", "2",
                "--retry-delay", "5",
            ],
        )
        self.assertTrue(config["attribution_required"])
        self.assertIn("OpenStreetMap contributors", config["attribution"])
        self.assertIn("one serial POST", config["rate_limit"])

    def test_real_factory_generates_isolated_paused_candidate_dag(self):
        module = load_candidate_dag()

        dag = getattr(module, "extract__osm_overpass")
        self.assertEqual(dag.dag_id, "extract__osm_overpass")
        self.assertEqual(dag.schedule, "41 6 15 * *")
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.default_args["retries"], 0)
        self.assertEqual(len(dag.tasks), 1)
        task = dag.tasks[0]
        config = task.op_kwargs["cfg"]
        self.assertEqual(config, yaml.safe_load(CONFIG.read_text(encoding="utf-8")))
        self.assertFalse(config["enabled"])
        self.assertEqual(task.python_callable.__module__, __name__)
        self.assertEqual(task.execution_timeout.total_seconds(), 180)
        self.assertIn("fetch_osm_overpass.py --bbox", dag.doc_md)


if __name__ == "__main__":
    unittest.main()
