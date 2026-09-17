import contextlib
from datetime import datetime
import importlib.util
import io
import math
import json
from pathlib import Path
import subprocess
import sys
import types
import unittest
from unittest import mock
import urllib.error


SCRIPT = Path(__file__).with_name("fetch_imf_datamapper.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "imf_datamapper.yml"
DAG_FACTORY = SCRIPT.parents[2] / "orchestration" / "dags" / "extract_dags.py"
SPEC = importlib.util.spec_from_file_location("fetch_imf_datamapper", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def fixture(value=2.5, future_value=-0.25):
    return {
        "api": {"version": "1"},
        "values": {
            "NGDP_RPCH": {
                "CAN": {"2024": 0},
                "USA": {"2024": value, "2031": future_value},
                "WEOWORLD": {"2024": 3.2},
            }
        },
    }


class JsonResponse(io.BytesIO):
    def __init__(self, document, status=200):
        if isinstance(document, bytes):
            payload = document
        else:
            payload = json.dumps(document).encode("utf-8")
        super().__init__(payload)
        self.status = status

    def getcode(self):
        return self.status

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


class FetchIMFDataMapperTests(unittest.TestCase):
    def fetch(self, document=None, **kwargs):
        response = JsonResponse(fixture() if document is None else document)
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response) as urlopen:
            records = list(MODULE.fetch_imf_datamapper(**kwargs))
        return records, urlopen

    def test_default_and_explicit_indicator_invocations_emit_valid_ndjson(self):
        for argv in ([], ["NGDP_RPCH"]):
            with self.subTest(argv=argv):
                stdout = io.StringIO()
                with mock.patch.object(
                    MODULE.urllib.request, "urlopen", return_value=JsonResponse(fixture())
                ) as urlopen:
                    with contextlib.redirect_stdout(stdout):
                        MODULE.main(argv)

                records = [json.loads(line) for line in stdout.getvalue().splitlines()]
                self.assertEqual(len(records), 3)
                self.assertEqual(len({record["id"] for record in records}), 3)
                for record in records:
                    self.assertEqual(record["source"], "imf_datamapper")
                    self.assertEqual(record["indicator"], "NGDP_RPCH")
                    self.assertTrue(record["fetched_at"])
                    self.assertEqual(len(record["id"]), 64)
                request = urlopen.call_args.args[0]
                self.assertEqual(request.full_url, MODULE.API_URL)
                self.assertEqual(urlopen.call_args.kwargs, {"timeout": 60})
                self.assertEqual(request.headers["Accept"], "application/json")
                self.assertTrue(request.headers["User-agent"])

    def test_preserves_values_years_and_future_year_without_projection_labels(self):
        records, _ = self.fetch()
        keyed = {(row["country"], row["year"]): row for row in records}

        self.assertEqual(keyed[("USA", "2024")]["value"], 2.5)
        self.assertEqual(keyed[("USA", "2031")]["value"], -0.25)
        self.assertEqual(keyed[("CAN", "2024")]["value"], 0)
        self.assertIs(type(keyed[("CAN", "2024")]["value"]), int)
        self.assertNotIn("actual", keyed[("USA", "2031")])
        self.assertNotIn("projected", keyed[("USA", "2031")])
        self.assertNotIn("status", keyed[("USA", "2031")])

    def test_stable_ids_ignore_fetch_time_and_revised_values(self):
        first = MODULE.parse_response(fixture(value=2.5), "NGDP_RPCH", "first")
        revised = MODULE.parse_response(fixture(value=9.75), "NGDP_RPCH", "later")

        first_ids = {(row["country"], row["year"]): row["id"] for row in first}
        revised_ids = {(row["country"], row["year"]): row["id"] for row in revised}
        self.assertEqual(first_ids, revised_ids)
        self.assertEqual(len(first_ids.values()), len(set(first_ids.values())))
        self.assertNotEqual(first[0]["fetched_at"], revised[0]["fetched_at"])
        first_usa = next(row for row in first if row["country"] == "USA" and row["year"] == "2024")
        revised_usa = next(row for row in revised if row["country"] == "USA" and row["year"] == "2024")
        self.assertNotEqual(first_usa["value"], revised_usa["value"])
        self.assertEqual(first_usa["id"], revised_usa["id"])

    def test_omits_regional_aggregates_instead_of_mislabeling_them(self):
        records, _ = self.fetch()

        self.assertEqual({row["country"] for row in records}, {"CAN", "USA"})
        self.assertNotIn("WEOWORLD", {row["country"] for row in records})

    def test_rejects_malformed_and_empty_payloads(self):
        cases = (
            (None, "payload must be an object"),
            ({}, "invalid values object"),
            ({"values": {}}, "invalid NGDP_RPCH indicator object"),
            ({"values": {"NGDP_RPCH": {}}}, "payload is empty"),
            ({"values": {"NGDP_RPCH": {"USA": {}}}}, "has no observations"),
            ({"values": {"NGDP_RPCH": {"USA": {"FY24": 1}}}}, "invalid year"),
            ({"values": {"NGDP_RPCH": {"USA": {"2024": None}}}}, "invalid observation value"),
            ({"values": {"NGDP_RPCH": {"USA": {"2024": "1.0"}}}}, "invalid observation value"),
            ({"values": {"NGDP_RPCH": {"WEOWORLD": {"2024": 3.2}}}}, "no country observations"),
        )
        for document, message in cases:
            with self.subTest(document=document):
                with self.assertRaisesRegex(MODULE.IMFDataMapperError, message):
                    MODULE.parse_response(document, "NGDP_RPCH", "now")

    def test_http_invalid_json_and_network_failures_are_clear(self):
        failures = (
            (urllib.error.HTTPError(MODULE.API_URL, 503, "unavailable", {}, None), "HTTP 503"),
            (urllib.error.URLError("offline"), "network failure: offline"),
            (TimeoutError("timed out"), "network failure: timed out"),
        )
        for failure, message in failures:
            with self.subTest(message=message):
                with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=failure):
                    with self.assertRaisesRegex(MODULE.IMFDataMapperError, message):
                        list(MODULE.fetch_imf_datamapper())

        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(b"not-json")
        ):
            with self.assertRaisesRegex(MODULE.IMFDataMapperError, "invalid JSON"):
                list(MODULE.fetch_imf_datamapper())

    def test_rejects_non_ngdp_rpch_and_unbounded_timeout_before_http(self):
        for kwargs, message in (
            ({"indicator": "NGDP"}, "indicator must be NGDP_RPCH"),
            ({"timeout": 0}, "timeout must be"),
            ({"timeout": 121}, "timeout must be"),
        ):
            with self.subTest(kwargs=kwargs):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        list(MODULE.fetch_imf_datamapper(**kwargs))
                urlopen.assert_not_called()

    def test_actual_dag_factory_recognizes_enabled_weekly_fetch_command(self):
        operators = []

        class FakeDAG:
            current = None

            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.dag_id = kwargs["dag_id"]
                self.tasks = []

            def __enter__(self):
                FakeDAG.current = self
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                FakeDAG.current = None

        class FakePythonOperator:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                operators.append(self)
                if FakeDAG.current is not None:
                    FakeDAG.current.tasks.append(self)

        airflow = types.ModuleType("airflow")
        airflow.DAG = FakeDAG
        providers = types.ModuleType("airflow.providers")
        standard = types.ModuleType("airflow.providers.standard")
        operators_module = types.ModuleType("airflow.providers.standard.operators")
        python_module = types.ModuleType("airflow.providers.standard.operators.python")
        python_module.PythonOperator = FakePythonOperator
        pendulum = types.ModuleType("pendulum")
        pendulum.datetime = lambda *args, **kwargs: (args, kwargs)
        cadence_plan = types.ModuleType("cadence_plan")
        cadence_plan.plan_sources = lambda: {}
        cadence_plan.effective_schedule = lambda cfg, plan: (cfg["schedule"], "configured")
        extract_runner = types.ModuleType("extract_runner")
        extract_runner.SCRIPTS_DIR = SCRIPT.parent
        extract_runner.run = object()
        stubs = {
            "airflow": airflow,
            "airflow.providers": providers,
            "airflow.providers.standard": standard,
            "airflow.providers.standard.operators": operators_module,
            "airflow.providers.standard.operators.python": python_module,
            "pendulum": pendulum,
            "cadence_plan": cadence_plan,
            "extract_runner": extract_runner,
        }
        factory_spec = importlib.util.spec_from_file_location(
            "test_imf_extract_dags", DAG_FACTORY
        )
        factory = importlib.util.module_from_spec(factory_spec)
        assert factory_spec.loader is not None
        with mock.patch.dict(sys.modules, stubs):
            factory_spec.loader.exec_module(factory)

        dag = factory.extract__imf_datamapper
        self.assertEqual(dag.kwargs["schedule"], "41 11 * * 3")
        self.assertFalse(dag.kwargs["is_paused_upon_creation"])
        self.assertEqual(len(dag.tasks), 1)
        cfg = dag.tasks[0].kwargs["op_kwargs"]["cfg"]
        self.assertEqual(cfg["script"], "fetch_imf_datamapper.py")
        self.assertEqual(cfg["args"], ["NGDP_RPCH", "--timeout", "60"])
        self.assertTrue(cfg["enabled"])


def run_live() -> int:
    command = [sys.executable, str(SCRIPT), "NGDP_RPCH", "--timeout", "60"]
    try:
        completed = subprocess.run(
            command, capture_output=True, text=True, timeout=90, check=False
        )
    except subprocess.TimeoutExpired as exc:
        print(f"live validation failed: fetch exceeded {exc.timeout} seconds", file=sys.stderr)
        return 1
    if completed.returncode != 0:
        detail = completed.stderr.strip() or "no stderr"
        print(
            f"live validation failed: fetch exited {completed.returncode}: {detail}",
            file=sys.stderr,
        )
        return 1

    lines = completed.stdout.splitlines()
    if not lines:
        print("live validation failed: fetch emitted no observations", file=sys.stderr)
        return 1
    ids = set()
    countries = 0
    try:
        for number, line in enumerate(lines, 1):
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError(f"line {number} is not a JSON object")
            for field in ("source", "fetched_at", "id", "indicator", "country", "year"):
                if not record.get(field):
                    raise ValueError(f"line {number} has empty {field}")
            try:
                datetime.fromisoformat(record["fetched_at"])
            except (TypeError, ValueError) as exc:
                raise ValueError(f"line {number} has invalid fetched_at") from exc
            if record["id"] != MODULE.stable_observation_id(
                record["indicator"], record["country"], record["year"]
            ):
                raise ValueError(f"line {number} has invalid stable id")
            if MODULE.YEAR_RE.fullmatch(record["year"]) is None:
                raise ValueError(f"line {number} has invalid year")
            value = record.get("value")
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError(f"line {number} has invalid value")
            if record["source"] != "imf_datamapper":
                raise ValueError(f"line {number} has wrong source")
            if record["indicator"] != "NGDP_RPCH":
                raise ValueError(f"line {number} has non-NGDP_RPCH output")
            if record["country"] not in MODULE.COUNTRY_CODES:
                raise ValueError(f"line {number} mislabels an aggregate as a country")
            countries += 1
            if record["id"] in ids:
                raise ValueError(f"line {number} has duplicate id")
            ids.add(record["id"])
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        print(f"live validation failed: {exc}", file=sys.stderr)
        return 1
    if countries == 0:
        print("live validation failed: fetch emitted no country observations", file=sys.stderr)
        return 1
    print(
        f"live validation passed: {len(lines)} NGDP_RPCH country observations, "
        f"{len(ids)} unique IDs"
    )
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--live"]:
        raise SystemExit(run_live())
    unittest.main()
