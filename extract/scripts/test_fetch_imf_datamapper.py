import contextlib
from datetime import datetime
import http.client
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

def credential_like_malformed_year_fixture():
    secret = "year-key-secret"
    year = (
        "https://fixture-user:fixture-password@example.test/\n "
        f"password={secret} "
        + ("x" * 2000)
    )
    return {"values": {"NGDP_RPCH": {"USA": {year: 1.0}}}}, secret, year


def live_record(country, year, value):
    return {
        "source": "imf_datamapper",
        "fetched_at": "2026-09-17T00:00:00+00:00",
        "id": MODULE.stable_observation_id("NGDP_RPCH", country, year),
        "indicator": "NGDP_RPCH",
        "country": country,
        "year": year,
        "value": value,
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


class ReadFailureResponse:
    status = 200

    def __init__(self, failure):
        self.failure = failure

    def getcode(self):
        return self.status

    def read(self, limit):
        raise self.failure

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


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

    def test_http_protocol_failures_while_opening_and_reading_are_clear(self):
        failures = (
            (
                http.client.RemoteDisconnected("connection closed"),
                "network/protocol failure: RemoteDisconnected",
                False,
            ),
            (
                http.client.IncompleteRead(b'{"values":', 100),
                "network/protocol failure: incomplete response",
                True,
            ),
        )
        for failure, message, during_read in failures:
            with self.subTest(message=message):
                side_effect = None if during_read else failure
                response = ReadFailureResponse(failure) if during_read else None
                with mock.patch.object(
                    MODULE.urllib.request,
                    "urlopen",
                    return_value=response,
                    side_effect=side_effect,
                ):
                    with self.assertRaisesRegex(MODULE.IMFDataMapperError, message):
                        list(MODULE.fetch_imf_datamapper())

    def test_cli_read_failure_is_nonzero_quiet_and_emits_no_observations(self):
        response = ReadFailureResponse(http.client.IncompleteRead(b"partial", 50))
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(MODULE.urllib.request, "urlopen", return_value=response):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    MODULE.main([])

        self.assertNotEqual(raised.exception.code, 0)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("network/protocol failure: incomplete response", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_cli_redacts_and_bounds_malformed_upstream_year_diagnostic(self):
        document, secret, year = credential_like_malformed_year_fixture()
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(document)
        ):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as raised:
                    MODULE.main([])

        diagnostic = stderr.getvalue()
        framing = len("fetch_imf_datamapper: \n")
        self.assertNotEqual(raised.exception.code, 0)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("invalid year", diagnostic)
        self.assertIn("password=[redacted]", diagnostic)
        self.assertIn("https://[redacted]@example.test/", diagnostic)
        self.assertNotIn(secret, diagnostic)
        self.assertNotIn("fixture-user", diagnostic)
        self.assertNotIn("fixture-password", diagnostic)
        self.assertNotIn(year, diagnostic)
        self.assertNotIn("\nhttps://", diagnostic)
        self.assertEqual(len(diagnostic.splitlines()), 1)
        self.assertNotIn("Traceback", diagnostic)
        self.assertLessEqual(len(diagnostic), MODULE.MAX_DIAGNOSTIC_CHARS + framing)

    def test_actual_cli_argument_errors_are_redacted_single_line_and_bounded(self):
        secret = "cli-secret-value"
        authenticated_url = "https://cli-user:cli-password@example.test/private"
        unsafe = (
            f"password={secret} {authenticated_url}\n\t\x1b" + ("x" * 2000)
        )
        cases = (
            ([unsafe], "invalid choice"),
            (["NGDP_RPCH", f"--unknown={unsafe}"], "unrecognized arguments"),
            (["--timeout", unsafe], "invalid int value"),
        )
        framing = len("fetch_imf_datamapper: argument error: \n")
        for arguments, category in cases:
            with self.subTest(category=category):
                completed = subprocess.run(
                    [sys.executable, str(SCRIPT), *arguments],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )

                self.assertNotEqual(completed.returncode, 0)
                self.assertEqual(completed.stdout, "")
                self.assertIn(category, completed.stderr)
                self.assertIn("password=[redacted]", completed.stderr)
                self.assertIn("https://[redacted]@example.test/private", completed.stderr)
                self.assertNotIn(secret, completed.stderr)
                self.assertNotIn("cli-user", completed.stderr)
                self.assertNotIn("cli-password", completed.stderr)
                self.assertNotIn("\t", completed.stderr)
                self.assertNotIn("\x1b", completed.stderr)
                self.assertEqual(len(completed.stderr.splitlines()), 1)
                self.assertNotIn("Traceback", completed.stderr)
                self.assertLessEqual(
                    len(completed.stderr), MODULE.MAX_DIAGNOSTIC_CHARS + framing
                )

    def assert_actual_cli_credential_value_is_redacted(
        self, arguments, sensitive_prefix, sensitive_suffix
    ):
        completed = subprocess.run(
            [sys.executable, str(SCRIPT), "NGDP_RPCH", *arguments],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        framing = len("fetch_imf_datamapper: argument error: \n")
        self.assertNotEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout, "")
        self.assertIn("unrecognized arguments", completed.stderr)
        self.assertIn("[redacted]", completed.stderr)
        self.assertNotIn(sensitive_prefix, completed.stderr)
        self.assertNotIn(sensitive_suffix, completed.stderr)
        self.assertNotIn("\t", completed.stderr)
        self.assertNotIn("\x1b", completed.stderr)
        self.assertEqual(len(completed.stderr.splitlines()), 1)
        self.assertNotIn("Traceback", completed.stderr)
        self.assertLessEqual(
            len(completed.stderr), MODULE.MAX_DIAGNOSTIC_CHARS + framing
        )

    def test_actual_cli_separate_credential_values_are_fully_redacted(self):
        labels = (
            "authorization",
            "proxy-authorization",
            "api-key",
            "apikey",
            "api_key",
            "access-token",
            "accesstoken",
            "access_token",
            "token",
            "password",
            "secret",
        )
        value_kinds = ("ordinary", "oversized", "whitespace", "control")
        for label in labels:
            for value_kind in value_kinds:
                sensitive_prefix = f"SENSITIVE-PREFIX-separate-{label}-{value_kind}"
                sensitive_suffix = f"SENSITIVE-SUFFIX-separate-{label}-{value_kind}"
                if value_kind == "ordinary":
                    value = sensitive_prefix + "-value-" + sensitive_suffix
                elif value_kind == "oversized":
                    value = sensitive_prefix + ("z" * 5000) + sensitive_suffix
                elif value_kind == "whitespace":
                    value = sensitive_prefix + " value with spaces " + sensitive_suffix
                else:
                    value = sensitive_prefix + "\nwith\ttabs\x1b" + sensitive_suffix

                with self.subTest(label=label, value_kind=value_kind):
                    self.assert_actual_cli_credential_value_is_redacted(
                        [f"--{label}", value], sensitive_prefix, sensitive_suffix
                    )

    def test_actual_cli_inline_credential_values_are_fully_redacted(self):
        labels = (
            "authorization",
            "proxy-authorization",
            "api-key",
            "apikey",
            "api_key",
            "access-token",
            "accesstoken",
            "access_token",
            "token",
            "password",
            "secret",
        )
        value_kinds = ("ordinary", "oversized", "whitespace", "control")
        for separator in ("=", ":"):
            for label in labels:
                for value_kind in value_kinds:
                    sensitive_prefix = (
                        f"SENSITIVE-PREFIX-inline-{separator}-{label}-{value_kind}"
                    )
                    sensitive_suffix = (
                        f"SENSITIVE-SUFFIX-inline-{separator}-{label}-{value_kind}"
                    )
                    if value_kind == "ordinary":
                        value = sensitive_prefix + "-value-" + sensitive_suffix
                    elif value_kind == "oversized":
                        value = sensitive_prefix + ("z" * 5000) + sensitive_suffix
                    elif value_kind == "whitespace":
                        value = sensitive_prefix + " value with spaces " + sensitive_suffix
                    else:
                        value = sensitive_prefix + "\nwith\ttabs\x1b" + sensitive_suffix

                    with self.subTest(
                        separator=separator, label=label, value_kind=value_kind
                    ):
                        self.assert_actual_cli_credential_value_is_redacted(
                            [f"--{label}{separator}{value}"],
                            sensitive_prefix,
                            sensitive_suffix,
                        )
    def test_actual_cli_help_remains_normal(self):
        completed = subprocess.run(
            [sys.executable, str(SCRIPT), "--help"],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )

        self.assertEqual(completed.returncode, 0)
        self.assertIn("usage:", completed.stdout)
        self.assertIn("NGDP_RPCH", completed.stdout)
        self.assertEqual(completed.stderr, "")

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

    def test_live_wrapper_preserves_child_status_and_bounds_redacted_stderr(self):
        secret = "top-secret-value"
        child_stderr = f"password={secret} " + ("x" * 2000)
        completed = subprocess.CompletedProcess([], 7, stdout="", stderr=child_stderr)
        stderr = io.StringIO()
        with mock.patch.object(subprocess, "run", return_value=completed):
            with contextlib.redirect_stderr(stderr):
                status = run_live()

        diagnostic = stderr.getvalue()
        self.assertEqual(status, 7)
        self.assertIn("fetch exited 7", diagnostic)
        self.assertIn("password=[redacted]", diagnostic)
        self.assertNotIn(secret, diagnostic)
        self.assertLessEqual(len(diagnostic), MODULE.MAX_DIAGNOSTIC_CHARS + 80)

    def test_live_wrapper_rejects_duplicate_ids_with_different_values(self):
        first = live_record("USA", "2024", 2.5)
        revised = live_record("USA", "2024", 9.75)
        child_stdout = "\n".join(json.dumps(record) for record in (first, revised))
        completed = subprocess.CompletedProcess([], 0, stdout=child_stdout, stderr="")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(subprocess, "run", return_value=completed):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = run_live()

        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("line 2 has duplicate id", stderr.getvalue())

    def test_live_wrapper_reports_exact_unique_id_count(self):
        records = (
            live_record("USA", "2024", 2.5),
            live_record("CAN", "2031", -0.25),
        )
        child_stdout = "\n".join(json.dumps(record) for record in records)
        completed = subprocess.CompletedProcess([], 0, stdout=child_stdout, stderr="")
        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(subprocess, "run", return_value=completed):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                status = run_live()

        self.assertEqual(status, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(
            stdout.getvalue(),
            "live validation passed: 2 NGDP_RPCH country observations, 2 unique IDs\n",
        )


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
        detail = MODULE._safe_diagnostic_detail(completed.stderr or "no stderr")
        print(
            f"live validation failed: fetch exited {completed.returncode}: {detail}",
            file=sys.stderr,
        )
        return completed.returncode

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
        detail = MODULE._safe_diagnostic_detail(exc)
        print(f"live validation failed: {detail}", file=sys.stderr)
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
