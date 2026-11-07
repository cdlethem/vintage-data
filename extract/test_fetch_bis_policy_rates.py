import contextlib
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType, SimpleNamespace
import urllib.error
from unittest import mock

import yaml


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = Path(__file__).parent / "scripts" / "fetch_bis_policy_rates.py"
SOURCE_CONFIG = Path(__file__).parent / "sources" / "bis_policy_rates.yml"
DAG_FACTORY = ROOT / "orchestration" / "dags" / "extract_dags.py"
SPEC = importlib.util.spec_from_file_location("fetch_bis_policy_rates", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

START_PERIOD = "2026-09-01"
END_PERIOD = "2026-11-07"
FETCHED_AT = "2026-11-07T06:34:00+00:00"


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
        "meta": {
            "id": "fixture-response-1",
            "prepared": "2026-11-07T05:43:49",
            "test": False,
        },
        "data": {
            "dataSets": [
                {
                    "reportingBegin": "2026-09-01T00:00:00",
                    "reportingEnd": "2026-11-03T23:59:59",
                    "action": "Information",
                    "attributes": [0, 0],
                    "series": {
                        "0:0": {
                            "attributes": [None, 0, 0, 0, None, 0],
                            "observations": {
                                "0": ["4.5", 0, 0, None],
                                "1": ["4.5", 0, 0, None],
                                "2": ["4.25", 0, 0, None],
                            },
                        },
                        "0:1": {
                            "attributes": [None, 1, 0, 0, None, 0],
                            "observations": {
                                "1": ["0.5", 0, 0, None],
                                "2": [None, 1, 0, None],
                            },
                        },
                        "1:0": {
                            "attributes": [None, 0, 0, 0, None, 1],
                            "observations": {
                                "3": ["4.5", 0, 0, None],
                                "4": ["4.25", 0, 0, None],
                            },
                        },
                    },
                }
            ],
            "structure": {
                "dimensions": {
                    "dataSet": [],
                    "series": [
                        definition("FREQ", [("M", "Monthly"), ("D", "Daily")]),
                        definition(
                            "REF_AREA",
                            [("US", "United States"), ("CH", "Switzerland")],
                        ),
                    ],
                    "observation": [
                        definition(
                            "TIME_PERIOD",
                            [
                                ("2026-09", "September 2026"),
                                ("2026-10", "October 2026"),
                                ("2026-11", "November 2026"),
                                ("2026-11-02", "2 November 2026"),
                                ("2026-11-03", "3 November 2026"),
                            ],
                        )
                    ],
                },
                "attributes": {
                    "dataset": [
                        definition("UNIT_MEASURE", [("368", "Per cent per year")]),
                        definition("UNIT_MULT", [("0", "Units")]),
                    ],
                    "series": [
                        definition("TIME_FORMAT", []),
                        definition(
                            "COMPILATION",
                            [
                                ("Offical central bank rate.", "Offical central bank rate."),
                                ("Repo rate.", "Repo rate."),
                            ],
                        ),
                        definition("DECIMALS", [("1", "One decimal")]),
                        definition(
                            "SOURCE_REF",
                            [("https://www.bis.org/statistics/cbpol", "BIS")],
                        ),
                        definition("SUPP_INFO_BREAKS", []),
                        definition(
                            "TITLE",
                            [
                                ("US policy rate", "US policy rate"),
                                ("CH policy rate", "CH policy rate"),
                            ],
                        ),
                    ],
                    "observation": [
                        definition(
                            "OBS_STATUS", [("A", "Normal"), ("M", "Missing")]
                        ),
                        definition("OBS_CONF", [("F", "Free")]),
                        definition("OBS_PRE_BREAK", []),
                    ],
                },
            },
        },
    }


def advanced_daily_document():
    """Fixture plus one new business-day observation for the daily US series."""
    document = fixture_document()
    document["data"]["structure"]["dimensions"]["observation"][0]["values"].append(
        {"id": "2026-11-04", "name": "4 November 2026"}
    )
    document["data"]["dataSets"][0]["series"]["1:0"]["observations"]["5"] = [
        "4.25",
        0,
        0,
        None,
    ]
    return document


class JsonResponse(io.BytesIO):
    status = 200

    def __init__(self, document):
        super().__init__(json.dumps(document, separators=(",", ":")).encode("utf-8"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def fetched(document=None, **overrides):
    arguments = {
        "start_period": START_PERIOD,
        "end_period": END_PERIOD,
        "timeout": 17,
        "fetched_at": FETCHED_AT,
    }
    arguments.update(overrides)
    with mock.patch.object(
        MODULE.urllib.request,
        "urlopen",
        return_value=JsonResponse(document if document is not None else fixture_document()),
    ) as urlopen:
        records, diagnostics = MODULE.fetch_policy_rates(**arguments)
    return records, diagnostics, urlopen


def run_main(argv, document=None, fetch_side_effect=None):
    """Run MODULE.main against a mocked fetch, capturing stdout/stderr."""
    if fetch_side_effect is not None:
        fetcher = mock.Mock(side_effect=fetch_side_effect)
    elif document is not None:
        records, diagnostics, _ = fetched(document)
        fetcher = mock.Mock(return_value=(records, diagnostics))
    else:
        records, diagnostics, _ = fetched()
        fetcher = mock.Mock(return_value=(records, diagnostics))
    stdout, stderr = io.StringIO(), io.StringIO()
    with mock.patch.object(
        MODULE, "default_window", return_value=(START_PERIOD, END_PERIOD)
    ), mock.patch.object(MODULE, "fetch_policy_rates", fetcher), \
            contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        status = MODULE.main(argv)
    summary_lines = [
        line
        for line in stderr.getvalue().splitlines()
        if line.startswith(MODULE.SUMMARY_PREFIX)
    ]
    summary = json.loads(summary_lines[0][len(MODULE.SUMMARY_PREFIX):])
    return status, [json.loads(line) for line in stdout.getvalue().splitlines()], summary


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


class BisPolicyRatesTests(unittest.TestCase):
    def test_normalizes_complete_fixture_across_frequencies(self):
        records, diagnostics, urlopen = fetched()
        self.assertEqual(len(records), 6)
        self.assertEqual(
            [(r["ref_area"], r["frequency"], r["period"], r["rate"]) for r in records],
            [
                ("CH", "M", "2026-10", 0.5),
                ("US", "D", "2026-11-02", 4.5),
                ("US", "D", "2026-11-03", 4.25),
                ("US", "M", "2026-09", 4.5),
                ("US", "M", "2026-10", 4.5),
                ("US", "M", "2026-11", 4.25),
            ],
        )
        first = records[0]
        self.assertEqual(first["source"], MODULE.SOURCE)
        self.assertEqual(first["fetched_at"], FETCHED_AT)
        self.assertEqual(first["flow"], MODULE.FLOW)
        self.assertEqual(first["ref_area_name"], "Switzerland")
        self.assertEqual(first["unit"], "per cent per year")
        self.assertEqual(first["series_attributes"]["COMPILATION"], "Repo rate.")
        self.assertEqual(
            first["source_metadata"]["prepared"], "2026-11-07T05:43:49"
        )
        self.assertEqual(diagnostics["missing_observations_dropped"], 1)
        self.assertEqual(diagnostics["series"], 3)
        self.assertEqual(diagnostics["status"], 200)
        requested = urlopen.call_args.args[0]
        self.assertEqual(requested.full_url, MODULE.build_url(START_PERIOD, END_PERIOD))
        self.assertEqual(requested.headers["Accept"], MODULE.ACCEPT)
        self.assertIn("User-agent", requested.headers)

    def test_missing_observation_is_dropped_and_counted(self):
        for marker in (None, "NaN"):
            with self.subTest(marker=marker):
                document = fixture_document()
                document["data"]["dataSets"][0]["series"]["0:1"]["observations"] = {
                    "1": [marker, 1, 0, None]
                }
                records, diagnostics, _ = fetched(document)
                self.assertEqual(
                    [(r["ref_area"], r["frequency"]) for r in records],
                    [("US", "D"), ("US", "D"), ("US", "M"), ("US", "M"), ("US", "M")],
                )
                self.assertEqual(diagnostics["missing_observations_dropped"], 1)

    def test_stable_observation_ids_are_deterministic(self):
        expected = MODULE.stable_observation_id("M", "US", "2026-11")
        records, _, _ = fetched()
        self.assertEqual(
            [r["id"] for r in records],
            [
                MODULE.stable_observation_id("M", "CH", "2026-10"),
                MODULE.stable_observation_id("D", "US", "2026-11-02"),
                MODULE.stable_observation_id("D", "US", "2026-11-03"),
                MODULE.stable_observation_id("M", "US", "2026-09"),
                MODULE.stable_observation_id("M", "US", "2026-10"),
                expected,
            ],
        )
        self.assertEqual(len({r["id"] for r in records}), len(records))

    def test_default_window_rolls_back_from_today(self):
        now = datetime(2026, 11, 7, tzinfo=timezone.utc)
        self.assertEqual(MODULE.default_window(now, 97), ("2026-08-02", "2026-11-07"))

    def test_rejects_invalid_documents(self):
        mutations = [
            ("value", "not a number"),
            ("boolean", "unsupported type"),
            ("infinite", "finite number"),
            ("index", "out of range"),
            ("window", "outside the requested window"),
            ("unit", "per cent per year"),
            ("dimensions", "exactly FREQ, REF_AREA"),
        ]
        for kind, message in mutations:
            with self.subTest(kind=kind):
                document = fixture_document()
                if kind == "value":
                    document["data"]["dataSets"][0]["series"]["1:0"][
                        "observations"
                    ]["4"] = ["abc", 0, 0, None]
                elif kind == "boolean":
                    document["data"]["dataSets"][0]["series"]["1:0"][
                        "observations"
                    ]["4"] = [True, 0, 0, None]
                elif kind == "infinite":
                    document["data"]["dataSets"][0]["series"]["1:0"][
                        "observations"
                    ]["4"] = ["Infinity", 0, 0, None]
                elif kind == "index":
                    document["data"]["dataSets"][0]["series"]["1:0"]["observations"][
                        "99"
                    ] = ["4.25", 0, 0, None]
                elif kind == "window":
                    document["data"]["structure"]["dimensions"]["observation"][0][
                        "values"
                    ].insert(0, {"id": "2025-01", "name": "January 2025"})
                elif kind == "unit":
                    document["data"]["structure"]["attributes"]["dataset"][0][
                        "values"
                    ].append({"id": "999", "name": "Something else"})
                    document["data"]["dataSets"][0]["attributes"] = [1, 0]
                elif kind == "dimensions":
                    document["data"]["structure"]["dimensions"]["series"].append(
                        definition("SECTOR", [("A", "All")])
                    )
                with self.assertRaisesRegex(ValueError, message):
                    MODULE.parse_response(
                        document, START_PERIOD, END_PERIOD, FETCHED_AT
                    )

    def test_rejects_documents_missing_the_data_envelope(self):
        for removed in ("data", "meta", "structure", "dimensions", "dataSets"):
            with self.subTest(removed=removed):
                document = fixture_document()
                if removed == "data":
                    del document["data"]
                elif removed == "meta":
                    del document["meta"]
                elif removed == "structure":
                    del document["data"]["structure"]
                elif removed == "dimensions":
                    del document["data"]["structure"]["dimensions"]
                elif removed == "dataSets":
                    del document["data"]["dataSets"]
                with self.assertRaises(ValueError):
                    MODULE.parse_response(
                        document, START_PERIOD, END_PERIOD, FETCHED_AT
                    )

    def test_json_decoder_rejects_duplicate_keys_and_nonfinite_constants(self):
        payloads = [
            b'{"data":{"dataSets":[]},"data":{"dataSets":[]}}',
            b'{"rate":NaN}',
            b'{"rate":Infinity}',
        ]
        for payload in payloads:
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    MODULE.load_response(io.BytesIO(payload))

    def test_http_and_network_failures_report_without_stdout(self):
        failures = [
            urllib.error.HTTPError(MODULE.API_BASE, 503, "unavailable", {}, None),
            urllib.error.URLError("offline"),
        ]
        for failure in failures:
            with self.subTest(failure=type(failure).__name__):
                status, records, summary = run_main(
                    ["--no-state"], fetch_side_effect=failure
                )
                self.assertEqual(status, 1)
                self.assertEqual(records, [])
                self.assertEqual(summary["health"], "failed")
                self.assertEqual(summary["records"], 0)
                self.assertIn("error", summary)

    def test_late_malformed_observation_cannot_emit_partial_output(self):
        document = fixture_document()
        document["data"]["dataSets"][0]["series"]["1:0"]["observations"]["4"] = [
            "bad",
            0,
        ]
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(
            MODULE, "default_window", return_value=(START_PERIOD, END_PERIOD)
        ), mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            return_value=JsonResponse(document),
        ), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            status = MODULE.main(["--no-state"])
        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("VINTAGE_RUN_SUMMARY", stderr.getvalue())

    def test_first_run_backfills_then_watermarks_suppress_reruns(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            argv = ["--state-file", str(state_file)]

            status, records, summary = run_main(argv)
            self.assertEqual(status, 0)
            self.assertEqual(len(records), 6)
            self.assertEqual(summary["health"], "healthy")
            self.assertEqual(summary["records"], 6)
            self.assertEqual(summary["metrics"]["new_records"], 6)
            self.assertEqual(summary["metrics"]["repeated_records"], 0)
            state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state["version"], MODULE.STATE_VERSION)
            self.assertEqual(
                state["last_period"],
                {"M:US": "2026-11", "M:CH": "2026-10", "D:US": "2026-11-03"},
            )

            status, records, summary = run_main(argv)
            self.assertEqual(status, 0)
            self.assertEqual(
                [(r["ref_area"], r["frequency"], r["period"]) for r in records],
                [("CH", "M", "2026-10"), ("US", "D", "2026-11-03"), ("US", "M", "2026-11")],
            )
            self.assertEqual(summary["records"], 3)
            self.assertEqual(summary["metrics"]["new_records"], 0)
            self.assertEqual(summary["metrics"]["repeated_records"], 3)

            status, records, summary = run_main(argv, document=advanced_daily_document())
            self.assertEqual(status, 0)
            self.assertEqual(
                [(r["ref_area"], r["frequency"], r["period"]) for r in records],
                [("CH", "M", "2026-10"), ("US", "D", "2026-11-04"), ("US", "M", "2026-11")],
            )
            self.assertEqual(summary["records"], 3)
            self.assertEqual(summary["metrics"]["new_records"], 1)
            self.assertEqual(summary["metrics"]["repeated_records"], 2)
            state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(state["last_period"]["D:US"], "2026-11-04")

    def test_corrupt_state_rebaselines_and_is_rewritten(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_file = Path(tmp) / "state.json"
            state_file.write_text("not json at all", encoding="utf-8")
            argv = ["--state-file", str(state_file)]
            status, records, summary = run_main(argv)
            self.assertEqual(status, 0)
            self.assertEqual(len(records), 6)
            state = json.loads(state_file.read_text(encoding="utf-8"))
            self.assertEqual(len(state["last_period"]), 3)

    def test_no_state_never_persists(self):
        with tempfile.TemporaryDirectory() as tmp:
            status, records, summary = run_main(["--no-state"])
            self.assertEqual(status, 0)
            self.assertEqual(len(records), 6)
            self.assertEqual(list(Path(tmp).iterdir()), [])

    def test_source_config_drives_bounded_extractor_invocation(self):
        config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["schedule"].count(" "), 4)
        self.assertEqual(config["cadence"], {"auto": False})
        self.assertEqual(
            config["run_summary"], {"required": True}
        )
        self.assertIn("Bank for International Settlements", config["attribution"])
        self.assertIn("BIS", config["licence"])
        self.assertTrue(config["attribution_required"])

        args = MODULE.build_parser().parse_args(config["args"])
        self.assertEqual(args.lookback_days, 365)
        self.assertEqual(args.timeout, 60)
        self.assertIsNone(args.state_file)
        self.assertFalse(args.no_state)

    def test_subprocess_ndjson_execution_with_summary(self):
        FixtureHandler.requests = []
        server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            local_base = f"http://127.0.0.1:{server.server_port}/api/v1/data"
            child_code = (
                "import importlib.util,sys;"
                "s=importlib.util.spec_from_file_location('bis',sys.argv.pop(1));"
                "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
                "m.API_BASE=sys.argv.pop(1);sys.exit(m.main(sys.argv[1:]))"
            )
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    child_code,
                    str(SCRIPT),
                    local_base,
                    "--no-state",
                    "--lookback-days",
                    "97",
                    "--timeout",
                    "10",
                ],
                cwd=ROOT,
                text=True,
                capture_output=True,
                timeout=30,
                check=False,
            )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

        if completed.returncode:
            self.fail(child_failure(completed, secrets=(str(ROOT), local_base)))
        records = [json.loads(line) for line in completed.stdout.splitlines()]
        self.assertEqual(len(records), 6)
        self.assertEqual(records[0]["ref_area"], "CH")
        stderr_lines = completed.stderr.splitlines()
        self.assertEqual(len(stderr_lines), 1)
        self.assertTrue(stderr_lines[0].startswith("VINTAGE_RUN_SUMMARY\t"))
        summary = json.loads(stderr_lines[0][len("VINTAGE_RUN_SUMMARY\t"):])
        self.assertEqual(summary["health"], "healthy")
        self.assertEqual(summary["records"], 6)
        self.assertEqual(len(FixtureHandler.requests), 1)
        self.assertEqual(FixtureHandler.requests[0]["accept"], MODULE.ACCEPT)
        self.assertIn("/WS_CBPOL/all/all?", FixtureHandler.requests[0]["path"])
        self.assertIn("startPeriod=2026-08-02", FixtureHandler.requests[0]["path"])
        self.assertIn("endPeriod=" + datetime.now(timezone.utc).date().isoformat(),
                      FixtureHandler.requests[0]["path"])

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

        dag = getattr(dag_module, "extract__bis_policy_rates")
        self.assertEqual(dag.schedule, "34 06 * * *")
        self.assertTrue(dag.is_paused_upon_creation)
        self.assertEqual(dag.max_active_runs, 1)
        self.assertIn("fetch_bis_policy_rates.py", dag.doc_md)
        self.assertEqual(len(dag.tasks), 1)
        self.assertEqual(dag.tasks[0].task_id, "run")
        self.assertEqual(dag.tasks[0].retries, 1)


if __name__ == "__main__":
    unittest.main()
