import importlib.util
import io
import json
import pathlib
import sys
import types
import unittest
import urllib.parse
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import yaml

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "extract" / "sources" / "open_meteo_air_quality.yml"
SCRIPT = REPO_ROOT / "extract" / "scripts" / "fetch_open_meteo_air_quality.py"
FACTORY = REPO_ROOT / "orchestration" / "dags" / "extract_dags.py"

SPEC = importlib.util.spec_from_file_location("fetch_open_meteo_air_quality", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

FETCHED_AT = "2026-10-05T19:13:00+00:00"
TWO_HOURS = ("2026-10-04T00:00", "2026-10-04T01:00")


class Response:
    def __init__(self, document):
        self.document = document

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return json.dumps(self.document).encode("utf-8")


def location_document(times=TWO_HOURS, **overrides):
    document = {
        "latitude": 40.7,
        "longitude": -74.0,
        "elevation": 32.0,
        "utc_offset_seconds": 0,
        "timezone": "GMT",
        "timezone_abbreviation": "GMT",
        "generationtime_ms": 0.5,
        "hourly_units": {
            "pm2_5": "\u00b5g/m\u00b3",
            "pm10": "\u00b5g/m\u00b3",
            "nitrogen_dioxide": "\u00b5g/m\u00b3",
            "sulphur_dioxide": "\u00b5g/m\u00b3",
            "carbon_monoxide": "\u00b5g/m\u00b3",
            "ozone": "\u00b5g/m\u00b3",
            "dust": "\u00b5g/m\u00b3",
            "european_aqi": "EAQI",
            "us_aqi": "USAQI",
        },
        "hourly": {
            "time": list(times),
            "pm2_5": [8.8, 9.1],
            "pm10": [11.5, 11.9],
            "nitrogen_dioxide": [29.2, 28.7],
            "sulphur_dioxide": [1.0, 1.2],
            "carbon_monoxide": [0.9, 0.8],
            "ozone": [27.0, 26.4],
            "dust": [0.0, 0.1],
            "european_aqi": [42, 44],
            "us_aqi": [57, 59],
        },
    }
    document.update(overrides)
    return document


def batched_document(count=2):
    return [location_document() for _ in range(count)]


def fetch_with(document, **kwargs):
    with mock.patch.object(
        MODULE.urllib.request, "urlopen", return_value=Response(document)
    ) as urlopen:
        records = list(MODULE.fetch_air_quality(fetched_at=FETCHED_AT, **kwargs))
    return records, urlopen


class FetchAirQualityTests(unittest.TestCase):
    def test_batched_response_yields_one_row_per_location_hour(self):
        locations = MODULE.DEFAULT_LOCATIONS[:2]
        records, urlopen = fetch_with(batched_document(2), locations=locations)

        self.assertEqual(len(records), 4)
        urlopen.assert_called_once()
        first = records[0]
        for key in ("source", "fetched_at", "id", "valid_at", "location_id"):
            self.assertIn(key, first)
        self.assertEqual(first["source"], MODULE.SOURCE)
        self.assertEqual(first["fetched_at"], FETCHED_AT)
        self.assertEqual(first["valid_at"], "2026-10-04T00:00:00+00:00")
        self.assertEqual(first["location_id"], "new_york")
        self.assertEqual(first["air_quality"]["pm2_5"], 8.8)
        self.assertEqual(first["air_quality"]["us_aqi"], 57)
        self.assertEqual(first["units"]["pm2_5"], "\u00b5g/m\u00b3")
        self.assertEqual(first["longitude"], -74.0)
        self.assertEqual(len(first["id"]), 64)
        # second hour of the second location
        self.assertEqual(records[3]["valid_at"], "2026-10-04T01:00:00+00:00")
        self.assertEqual(records[3]["location_id"], "london")
        self.assertEqual(records[3]["air_quality"]["pm2_5"], 9.1)

    def test_single_location_response_is_treated_as_batch_of_one(self):
        records, _ = fetch_with(location_document(), locations=[MODULE.DEFAULT_LOCATIONS[0]])
        self.assertEqual(len(records), 2)
        self.assertEqual({record["location_id"] for record in records}, {"new_york"})

    def test_request_url_binds_locations_variables_and_utc_window(self):
        locations = MODULE.DEFAULT_LOCATIONS[:2]
        _, urlopen = fetch_with(
            batched_document(2), locations=locations, variables=["pm2_5", "us_aqi"]
        )
        request = urlopen.call_args[0][0]
        query = dict(urllib.parse.parse_qsl(request.full_url.split("?", 1)[1]))
        self.assertEqual(query["latitude"], "40.7128,51.5072")
        self.assertEqual(query["longitude"], "-74.006,-0.1276")
        self.assertEqual(query["hourly"], "pm2_5,us_aqi")
        self.assertEqual(query["timezone"], "UTC")
        self.assertIn("start_date=", request.full_url)
        self.assertIn("end_date=", request.full_url)

    def test_ids_are_stable_within_a_run_and_distinct_across_collections(self):
        locations = MODULE.DEFAULT_LOCATIONS[:2]
        records_a, _ = fetch_with(batched_document(2), locations=locations)
        self.assertEqual(len({record["id"] for record in records_a}), len(records_a))
        records_a_again, _ = fetch_with(batched_document(2), locations=locations)
        self.assertEqual(
            [record["id"] for record in records_a],
            [record["id"] for record in records_a_again],
        )
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=Response(batched_document(2))
        ):
            records_next = list(
                MODULE.fetch_air_quality(
                    locations=locations, fetched_at="2026-10-06T19:13:00+00:00"
                )
            )
        self.assertNotEqual(
            [record["id"] for record in records_a],
            [record["id"] for record in records_next],
        )
        # same location-hour with a different collection time is a distinct revision
        self.assertEqual(
            (records_a[0]["location_id"], records_a[0]["valid_at"]),
            (records_next[0]["location_id"], records_next[0]["valid_at"]),
        )

    def test_error_document_raises(self):
        with self.assertRaises(MODULE.OpenMeteoAPIError):
            fetch_with({"error": True, "reason": "too many requests"}, locations=[MODULE.DEFAULT_LOCATIONS[0]])
        with self.assertRaises(MODULE.OpenMeteoAPIError):
            fetch_with([location_document(), {"error": True, "reason": "bad location"}], locations=MODULE.DEFAULT_LOCATIONS[:2])

    def test_batch_length_mismatch_raises(self):
        with self.assertRaises(ValueError):
            fetch_with(batched_document(3), locations=MODULE.DEFAULT_LOCATIONS[:2])

    def test_non_utc_response_is_rejected(self):
        with self.assertRaises(ValueError):
            fetch_with(location_document(utc_offset_seconds=3600), locations=[MODULE.DEFAULT_LOCATIONS[0]])
        with self.assertRaises(ValueError):
            fetch_with(location_document(timezone="America/New_York"), locations=[MODULE.DEFAULT_LOCATIONS[0]])

    def test_non_finite_value_is_rejected(self):
        document = location_document()
        document["hourly"]["pm2_5"][0] = float("inf")
        with self.assertRaises(ValueError):
            fetch_with(document, locations=[MODULE.DEFAULT_LOCATIONS[0]])

    def test_series_length_mismatch_is_rejected(self):
        document = location_document()
        document["hourly"]["ozone"].append(26.0)
        with self.assertRaises(ValueError):
            fetch_with(document, locations=[MODULE.DEFAULT_LOCATIONS[0]])

    def test_duplicate_valid_times_are_rejected(self):
        with self.assertRaises(ValueError):
            fetch_with(
                location_document(times=("2026-10-04T00:00", "2026-10-04T00:00")),
                locations=[MODULE.DEFAULT_LOCATIONS[0]],
            )

    def test_missing_unit_is_rejected(self):
        document = location_document()
        del document["hourly_units"]["dust"]
        with self.assertRaises(ValueError):
            fetch_with(document, locations=[MODULE.DEFAULT_LOCATIONS[0]])

    def test_out_of_range_location_is_rejected(self):
        bad = MODULE.Location("nowhere", "Nowhere", 95.0, 10.0)
        with self.assertRaises(ValueError):
            fetch_with(location_document(), locations=[bad])

    def test_duplicate_variable_is_rejected(self):
        with self.assertRaises(ValueError):
            fetch_with(location_document(), locations=[MODULE.DEFAULT_LOCATIONS[0]], variables=["pm2_5", "pm2_5"])

    def test_empty_window_is_rejected(self):
        with self.assertRaises(ValueError):
            fetch_with(
                location_document(),
                locations=[MODULE.DEFAULT_LOCATIONS[0]],
                past_days=0,
                forecast_days=0,
            )


class MainTests(unittest.TestCase):
    def test_main_emits_ndjson_and_run_summary(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=Response(batched_document(5))
        ), redirect_stdout(stdout), redirect_stderr(stderr):
            MODULE.main(["--past-days", "1", "--forecast-days", "2"])

        lines = stdout.getvalue().splitlines()
        self.assertEqual(len(lines), 10)
        for line in lines:
            record = json.loads(line)
            self.assertEqual(record["source"], MODULE.SOURCE)
            self.assertIn("fetched_at", record)
            self.assertIn("id", record)
        summary_lines = [
            line for line in stderr.getvalue().splitlines()
            if line.startswith("VINTAGE_RUN_SUMMARY\t")
        ]
        self.assertEqual(len(summary_lines), 1)
        payload = json.loads(summary_lines[0].split("\t", 1)[1])
        self.assertEqual(payload["records"], 10)
        self.assertEqual(payload["locations"], 5)
        self.assertEqual(payload["requests"], 1)

    def test_main_rejects_malformed_location(self):
        with self.assertRaises(SystemExit):
            MODULE.main(["--location", "not,enough,parts"])

    def test_main_rejects_unsupported_variable(self):
        with self.assertRaises(SystemExit):
            MODULE.main(["--variable", "alder_pollen"])


class FakeDAG:
    def __init__(self, **kwargs):
        self.dag_id = kwargs["dag_id"]
        self.schedule = kwargs["schedule"]
        self.start_date = kwargs["start_date"]
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
        if FakePythonOperator.current_dag is not None:
            FakePythonOperator.current_dag.tasks.append(self)


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
    original_glob = pathlib.Path.glob

    def candidate_only(path, pattern):
        if path == REPO_ROOT / "extract" / "sources" and pattern == "*.yml":
            return iter((CONFIG,))
        return original_glob(path, pattern)

    with mock.patch.dict(sys.modules, modules), mock.patch.object(
        pathlib.Path, "glob", candidate_only
    ):
        spec.loader.exec_module(module)
    return module


class AirQualitySourceIntegrationTests(unittest.TestCase):
    def test_checked_in_config_is_bounded_conservative_and_enabled(self):
        config = yaml.safe_load(CONFIG.read_text(encoding="utf-8"))

        self.assertEqual(config["name"], "open_meteo_air_quality")
        self.assertEqual(config["script"], "fetch_open_meteo_air_quality.py")
        self.assertTrue(SCRIPT.is_file())
        self.assertTrue(config["enabled"])
        self.assertEqual(config["retries"], 1)
        self.assertEqual(config["timeout_minutes"], 5)
        self.assertEqual(config["schedule"], "13 19 * * *")
        self.assertEqual(
            config["args"], ["--past-days", "1", "--forecast-days", "2", "--timeout", "30"]
        )
        self.assertEqual(config["cadence"], {"auto": False})
        self.assertTrue(config["attribution_required"])
        self.assertIn("CC BY 4.0", config["licence"])
        self.assertIn("Open-Meteo", config["licence"])
        self.assertIn("10,000 calls/day", config["rate_limit"])

    def test_real_factory_generates_enabled_candidate_dag(self):
        module = load_candidate_dag()

        dag = getattr(module, "extract__open_meteo_air_quality")
        self.assertEqual(dag.dag_id, "extract__open_meteo_air_quality")
        self.assertEqual(dag.schedule, "13 19 * * *")
        self.assertFalse(dag.is_paused_upon_creation)
        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertEqual(dag.default_args["retries"], 1)
        self.assertEqual(len(dag.tasks), 1)
        task = dag.tasks[0]
        config = task.op_kwargs["cfg"]
        self.assertEqual(config, yaml.safe_load(CONFIG.read_text(encoding="utf-8")))
        self.assertEqual(task.execution_timeout.total_seconds(), 300)
        self.assertIn("fetch_open_meteo_air_quality.py --past-days", dag.doc_md)


if __name__ == "__main__":
    unittest.main()
