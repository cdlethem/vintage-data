import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import sys
import threading
from types import ModuleType, SimpleNamespace
import urllib.error
import urllib.parse
import unittest
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = Path(__file__).parent / "scripts" / "fetch_ecb_sdmx_statistics.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "ecb_sdmx_statistics.yml"
DAG_FACTORY = ROOT / "orchestration" / "dags" / "extract_dags.py"
SPEC = importlib.util.spec_from_file_location("fetch_ecb_sdmx_statistics", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def definition(identifier, values, name=None):
    return {
        "id": identifier,
        "name": name or identifier,
        "values": [
            {"id": value_id, "name": value_name}
            for value_id, value_name in values
        ],
    }


def fixture_document():
    return {
        "header": {
            "id": "fixture-response-1",
            "prepared": "2026-09-03T09:30:00Z",
            "sender": {"id": "ECB", "name": "European Central Bank"},
        },
        "dataSets": [
            {
                "action": "Replace",
                "validFrom": "2026-09-03T09:00:00Z",
                "attributes": [0],
                "series": {
                    "0:0:0:0:0": {
                        "attributes": [0, 0],
                        "observations": {
                            "2": [1.1701, 0],
                            "0": [1.1684, 1],
                            "1": [1.1692, 0],
                        },
                    }
                },
            }
        ],
        "structure": {
            "dimensions": {
                "dataSet": [],
                "series": [
                    definition("FREQ", [("D", "Daily")]),
                    definition("CURRENCY", [("USD", "US dollar")]),
                    definition("CURRENCY_DENOM", [("EUR", "Euro")]),
                    definition("EXR_TYPE", [("SP00", "Spot")]),
                    definition("EXR_SUFFIX", [("A", "Average")]),
                ],
                "observation": [
                    definition(
                        "TIME_PERIOD",
                        [
                            ("2026-09-01", "1 September 2026"),
                            ("2026-09-02", "2 September 2026"),
                            ("2026-09-03", "3 September 2026"),
                        ],
                    )
                ],
            },
            "attributes": {
                "dataSet": [definition("TITLE", [("EXR", "Exchange rates")])],
                "series": [
                    definition("UNIT", [("USD", "US dollar")]),
                    definition("DECIMALS", [("4", "Four decimals")]),
                ],
                "observation": [
                    definition(
                        "OBS_STATUS",
                        [("A", "Normal value"), ("P", "Provisional")],
                    )
                ],
            },
        },
    }


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        super().__init__(json.dumps(document, separators=(",", ":")).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def fetched(document=None, **overrides):
    arguments = {
        "flow": "EXR",
        "series_key": "D.USD.EUR.SP00.A",
        "start_period": "2026-09-01",
        "end_period": "2026-09-03",
        "timeout": 17,
    }
    arguments.update(overrides)
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=JsonResponse(document if document is not None else fixture_document()),
    ) as urlopen:
        records = MODULE.fetch_observations(**arguments)
    return records, urlopen


class FixtureHandler(BaseHTTPRequestHandler):
    document = fixture_document()
    requests = []

    def do_GET(self):
        type(self).requests.append(
            {"path": self.path, "accept": self.headers.get("Accept")}
        )
        body = json.dumps(type(self).document, separators=(",", ":")).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", MODULE.ACCEPT)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


class DagStub:
    active = None

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.tasks = []

    def __enter__(self):
        type(self).active = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        type(self).active = None
        return False


def child_failure(completed, secrets=()):
    stderr = completed.stderr
    for secret in secrets:
        stderr = stderr.replace(secret, "<redacted>")
    stderr = stderr[-2000:]
    return f"child exited {completed.returncode}; stderr: {stderr}"


class EcbSdmxStatisticsTests(unittest.TestCase):
    def test_normalizes_complete_fixture_in_period_order_with_revision_lineage(self):
        records, _ = fetched()

        self.assertEqual(
            [record["period"] for record in records],
            ["2026-09-01", "2026-09-02", "2026-09-03"],
        )
        first = records[0]
        self.assertEqual(first["source"], "ecb_sdmx_statistics")
        self.assertEqual(first["flow"], "EXR")
        self.assertEqual(first["series_key"], "D.USD.EUR.SP00.A")
        self.assertEqual(first["rate"], 1.1684)
        self.assertEqual(
            first["dimensions"],
            {
                "FREQ": "D",
                "CURRENCY": "USD",
                "CURRENCY_DENOM": "EUR",
                "EXR_TYPE": "SP00",
                "EXR_SUFFIX": "A",
            },
        )
        self.assertEqual(first["dimension_labels"]["CURRENCY"], "US dollar")
        self.assertEqual(first["attributes"]["OBS_STATUS"]["id"], "P")
        self.assertEqual(first["series_attributes"]["DECIMALS"]["id"], "4")
        self.assertEqual(first["dataset_attributes"]["TITLE"]["id"], "EXR")
        self.assertEqual(first["source_metadata"]["response_id"], "fixture-response-1")
        self.assertEqual(first["source_metadata"]["dataset_action"], "Replace")
        self.assertEqual(
            first["source_metadata"]["dataset_valid_from"],
            "2026-09-03T09:00:00Z",
        )
        self.assertIn("European Central Bank", first["source_metadata"]["attribution"])
        self.assertEqual(len(first["id"]), 64)
        self.assertEqual({record["fetched_at"] for record in records}.__len__(), 1)
        self.assertEqual(
            datetime.fromisoformat(first["fetched_at"]).tzinfo,
            timezone.utc,
        )

    def test_ids_are_deterministic_across_rate_status_and_revision_changes(self):
        first, _ = fetched()
        changed = fixture_document()
        changed["header"]["id"] = "revision-2"
        changed["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]["0"] = [
            9.99,
            0,
        ]
        second, _ = fetched(changed)

        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertNotEqual(first[0]["rate"], second[0]["rate"])
        self.assertNotEqual(first[0]["attributes"], second[0]["attributes"])
        self.assertNotEqual(first[0]["id"], first[1]["id"])

    def test_request_is_exact_bounded_json_and_uses_timeout_and_user_agent(self):
        _, urlopen = fetched(timeout=23)

        request = urlopen.call_args.args[0]
        parsed = urllib.parse.urlsplit(request.full_url)
        self.assertEqual(parsed.path, "/service/data/EXR/D.USD.EUR.SP00.A")
        self.assertEqual(
            urllib.parse.parse_qs(parsed.query),
            {
                "startPeriod": ["2026-09-01"],
                "endPeriod": ["2026-09-03"],
                "format": ["jsondata"],
            },
        )
        self.assertEqual(request.get_header("Accept"), MODULE.ACCEPT)
        self.assertEqual(request.get_header("Accept-encoding"), "identity")
        self.assertEqual(request.get_header("User-agent"), MODULE.USER_AGENT)
        self.assertEqual(urlopen.call_args.kwargs, {"timeout": 23})

    def test_default_window_is_seven_complete_utc_days(self):
        self.assertEqual(
            MODULE.default_date_window(
                datetime(2026, 9, 17, 23, 59, tzinfo=timezone.utc)
            ),
            ("2026-09-10", "2026-09-16"),
        )

    def test_request_validation_rejects_invalid_bounds_before_http(self):
        cases = [
            ({"start_period": "2026-09-03", "end_period": "2026-09-01"}, "after"),
            ({"start_period": "2026-09-01", "end_period": "2026-10-02"}, "31"),
            ({"start_period": "2026-9-1"}, "YYYY-MM-DD"),
            ({"series_key": "D..EUR.SP00.A"}, "complete"),
            ({"flow": "EXR/../bad"}, "ECB code"),
            ({"timeout": 0}, "positive"),
        ]
        for overrides, message in cases:
            with self.subTest(overrides=overrides):
                arguments = {
                    "flow": "EXR",
                    "series_key": "D.USD.EUR.SP00.A",
                    "start_period": "2026-09-01",
                    "end_period": "2026-09-03",
                    "timeout": 17,
                }
                arguments.update(overrides)
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.fetch_observations(**arguments)
                urlopen.assert_not_called()

    def test_rejects_malformed_dimensions_series_and_periods(self):
        mutations = [
            (
                lambda document: document["structure"]["dimensions"]["series"].append(
                    definition("EXTRA", [("X", "Extra")])
                ),
                "dimension count",
            ),
            (
                lambda document: document["structure"]["dimensions"]["series"][1][
                    "values"
                ][0].update({"id": "GBP"}),
                "requested code",
            ),
            (
                lambda document: document["dataSets"][0]["series"].update(
                    {"0:0:0:0:1": {"attributes": [], "observations": {}}}
                ),
                "at most one series",
            ),
            (
                lambda document: document["structure"]["dimensions"]["observation"][
                    0
                ]["values"][0].update({"id": "2026-08-31"}),
                "outside the requested window",
            ),
            (
                lambda document: document["structure"]["dimensions"]["observation"][
                    0
                ]["values"][1].update({"id": "2026-09-01"}),
                "value identifiers",
            ),
            (
                lambda document: document["structure"]["dimensions"]["series"][1].update(
                    {"id": "FREQ"}
                ),
                "identifiers",
            ),
        ]
        for mutation, message in mutations:
            with self.subTest(message=message):
                document = fixture_document()
                mutation(document)
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.parse_response(
                        document,
                        "EXR",
                        "D.USD.EUR.SP00.A",
                        "2026-09-01",
                        "2026-09-03",
                        "2026-09-17T12:00:00+00:00",
                    )

    def test_rejects_invalid_values_attributes_and_observation_indexes(self):
        mutations = [
            (
                lambda document: document["dataSets"][0]["series"]["0:0:0:0:0"][
                    "observations"
                ]["0"].__setitem__(0, float("nan")),
                "finite number",
            ),
            (
                lambda document: document["dataSets"][0]["series"]["0:0:0:0:0"][
                    "observations"
                ]["0"].__setitem__(0, "1.2"),
                "finite number",
            ),
            (
                lambda document: document["dataSets"][0]["series"]["0:0:0:0:0"][
                    "observations"
                ]["0"].__setitem__(1, 9),
                "invalid value index",
            ),
            (
                lambda document: document["dataSets"][0]["series"]["0:0:0:0:0"][
                    "observations"
                ].update({"03": [1.2, 0]}),
                "invalid index",
            ),
            (
                lambda document: document["dataSets"][0]["series"]["0:0:0:0:0"][
                    "observations"
                ]["0"].extend([0]),
                "more attributes",
            ),
        ]
        for mutation, message in mutations:
            with self.subTest(message=message):
                document = fixture_document()
                mutation(document)
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.parse_response(
                        document,
                        "EXR",
                        "D.USD.EUR.SP00.A",
                        "2026-09-01",
                        "2026-09-03",
                        "2026-09-17T12:00:00+00:00",
                    )

    def test_json_decoder_rejects_duplicate_keys_and_nonfinite_constants(self):
        payloads = [
            b'{"header":{},"header":{}}',
            b'{"rate":NaN}',
            b'{"rate":Infinity}',
        ]
        for payload in payloads:
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    MODULE.load_response(io.BytesIO(payload))

    def test_http_and_network_failures_propagate_without_stdout(self):
        failures = [
            urllib.error.HTTPError(MODULE.API_BASE, 503, "unavailable", {}, None),
            urllib.error.URLError("offline"),
        ]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                stdout = io.StringIO()
                with mock.patch.object(
                    MODULE.urllib.request, "urlopen", side_effect=failure
                ), contextlib.redirect_stdout(stdout):
                    with self.assertRaises(type(failure)):
                        MODULE.main(
                            [
                                "EXR",
                                "D.USD.EUR.SP00.A",
                                "2026-09-01",
                                "2026-09-03",
                            ]
                        )
                self.assertEqual(stdout.getvalue(), "")

    def test_late_malformed_observation_cannot_emit_partial_output(self):
        document = fixture_document()
        document["dataSets"][0]["series"]["0:0:0:0:0"]["observations"]["2"] = [
            "bad",
            0,
        ]
        stdout = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)
        ), contextlib.redirect_stdout(stdout):
            with self.assertRaisesRegex(ValueError, "finite number"):
                MODULE.main(
                    [
                        "EXR",
                        "D.USD.EUR.SP00.A",
                        "2026-09-01",
                        "2026-09-03",
                    ]
                )
        self.assertEqual(stdout.getvalue(), "")

    def test_fixture_http_to_subprocess_ndjson_execution(self):
        FixtureHandler.requests = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            local_base = f"http://127.0.0.1:{server.server_port}/service/data"
            child_code = (
                "import importlib.util,sys;"
                "s=importlib.util.spec_from_file_location('ecb',sys.argv.pop(1));"
                "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
                "m.API_BASE=sys.argv.pop(1);m.main(sys.argv[1:])"
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    child_code,
                    str(SCRIPT),
                    local_base,
                    "EXR",
                    "D.USD.EUR.SP00.A",
                    "2026-09-01",
                    "2026-09-03",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=10,
                check=False,
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        if completed.returncode:
            self.fail(child_failure(completed, secrets=(str(ROOT), local_base)))
        self.assertEqual(completed.stderr, "")
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(len(records), 3)
        self.assertEqual(
            [record["period"] for record in records],
            ["2026-09-01", "2026-09-02", "2026-09-03"],
        )
        self.assertEqual(len(FixtureHandler.requests), 1)
        self.assertEqual(FixtureHandler.requests[0]["accept"], MODULE.ACCEPT)
        self.assertIn("/EXR/D.USD.EUR.SP00.A?", FixtureHandler.requests[0]["path"])

    def test_subprocess_failure_reporting_keeps_status_bounds_and_redacts(self):
        completed = subprocess.CompletedProcess(
            ["fixture"], 7, "", "x" * 2500 + " secret-token"
        )
        report = child_failure(completed, secrets=("secret-token",))
        self.assertIn("exited 7", report)
        self.assertIn("<redacted>", report)
        self.assertNotIn("secret-token", report)
        self.assertLessEqual(len(report), 2030)

    def test_source_config_drives_bounded_disabled_extractor_invocation(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["schedule"].count(" "), 4)
        self.assertEqual(config["cadence"], {"auto": False})
        self.assertEqual(config["rate_limit"].split(";")[0], "one bounded ECB SDMX data request per daily run")
        self.assertIn("European Central Bank", config["attribution"])

        args = MODULE.build_parser().parse_args(config["args"])
        self.assertEqual(args.flow, "EXR")
        self.assertEqual(args.series_key, "D.USD.EUR.SP00.A")
        self.assertIsNone(args.start_period)
        self.assertIsNone(args.end_period)
        empty_document = fixture_document()
        empty_document["structure"]["dimensions"]["observation"][0]["values"] = []
        empty_document["dataSets"][0]["series"] = {}
        with mock.patch.object(
            MODULE, "default_date_window", return_value=("2026-09-10", "2026-09-16")
        ), mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(empty_document),
        ) as urlopen:
            records = MODULE.fetch_observations(
                args.flow,
                args.series_key,
                args.start_period,
                args.end_period,
                args.timeout,
            )
        self.assertEqual(records, [])
        query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(urlopen.call_args.args[0].full_url).query
        )
        self.assertEqual(query["startPeriod"], ["2026-09-10"])
        self.assertEqual(query["endPeriod"], ["2026-09-16"])

    def test_actual_dag_factory_discovers_source_with_runtime_stubs(self):
        airflow = ModuleType("airflow")
        airflow.DAG = DagStub
        providers = ModuleType("airflow.providers")
        standard = ModuleType("airflow.providers.standard")
        operators = ModuleType("airflow.providers.standard.operators")
        python_operator = ModuleType("airflow.providers.standard.operators.python")

        def make_operator(**kwargs):
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
            "fixture_extract_dags", DAG_FACTORY
        )
        dag_module = importlib.util.module_from_spec(dag_spec)
        assert dag_spec.loader is not None
        with mock.patch.dict(sys.modules, stubs):
            dag_spec.loader.exec_module(dag_module)

        dag = getattr(dag_module, "extract__ecb_sdmx_statistics")
        self.assertEqual(dag.schedule, "41 16 * * *")
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertIn("D.USD.EUR.SP00.A", dag.doc_md)
        self.assertEqual(len(dag.tasks), 1)
        self.assertEqual(dag.tasks[0].task_id, "run")
        self.assertEqual(dag.tasks[0].retries, 0)


if __name__ == "__main__":
    unittest.main()
