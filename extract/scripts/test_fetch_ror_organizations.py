import contextlib
from datetime import date
import importlib.util
import io
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest import mock
import urllib.error
import urllib.parse


SCRIPT = Path(__file__).with_name("fetch_ror_organizations.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "ror_organizations.yml"
EXTRACT_DAGS = SCRIPT.parents[2] / "orchestration" / "dags" / "extract_dags.py"
SPEC = importlib.util.spec_from_file_location("fetch_ror_organizations", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

LIVE_STDERR_LIMIT = 4096
LIVE_TIMEOUT = 45
LIVE_ROR_ID = "https://ror.org/042nb2s44"


class JsonResponse(io.BytesIO):
    def __init__(self, document):
        payload = document if isinstance(document, bytes) else json.dumps(document).encode("utf-8")
        super().__init__(payload)
        self.headers = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


def organization(ror_id="https://ror.org/03yrm5c26", **changes):
    value = {
        "id": ror_id,
        "names": [
            {"value": "Example University", "types": ["ror_display", "label"], "lang": "en"},
            {"value": "Université Exemple", "types": ["label"], "lang": "fr"},
        ],
        "status": "active",
        "types": ["education", "funder"],
        "locations": [
            {
                "geonames_id": 5128581,
                "geonames_details": {
                    "name": "New York City",
                    "lat": 40.71427,
                    "lng": -74.00597,
                    "country_code": "US",
                    "country_name": "United States",
                },
            }
        ],
        "external_ids": [
            {"type": "fundref", "all": ["100000001"], "preferred": "100000001"}
        ],
        "links": [{"type": "website", "value": "https://example.edu/"}],
        "domains": ["example.edu"],
        "relationships": [
            {"type": "parent", "label": "Example System", "id": "https://ror.org/01ggx4157"}
        ],
        "established": 1901,
        "admin": {
            "created": {"date": "2018-11-14", "schema_version": "1.0"},
            "last_modified": {"date": "2026-09-16", "schema_version": "2.1"},
        },
        "future_field": {"nested": [True, None, 7]},
    }
    value.update(changes)
    return value


def page(total, items):
    return {"number_of_results": total, "time_taken": 3, "items": items}


def http_error(code, headers=None):
    return urllib.error.HTTPError(
        MODULE.API_URL,
        code,
        "error",
        headers or {},
        io.BytesIO(b"upstream detail"),
    )


def query_of(call):
    return urllib.parse.parse_qs(
        urllib.parse.urlsplit(call.args[0].full_url).query,
        keep_blank_values=True,
    )


def load_config_through_dag_factory():
    """Execute the repository's real source loader with Airflow surfaces stubbed."""
    loaded_configs = []

    class FakeDAG:
        def __init__(self, **kwargs):
            self.dag_id = kwargs["dag_id"]
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return False

    def fake_operator(**kwargs):
        loaded_configs.append(kwargs["op_kwargs"]["cfg"])
        return SimpleNamespace(**kwargs)

    airflow = ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_providers = ModuleType("airflow.providers")
    airflow_standard = ModuleType("airflow.providers.standard")
    airflow_operators = ModuleType("airflow.providers.standard.operators")
    airflow_python = ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = fake_operator
    pendulum = ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: (args, kwargs)
    cadence_plan = ModuleType("cadence_plan")
    cadence_plan.plan_sources = lambda: {}
    cadence_plan.effective_schedule = lambda cfg, plan: (cfg["schedule"], "declared")
    extract_runner = ModuleType("extract_runner")
    extract_runner.SCRIPTS_DIR = SCRIPT.parent
    extract_runner.run = lambda cfg: cfg
    modules = {
        "airflow": airflow,
        "airflow.providers": airflow_providers,
        "airflow.providers.standard": airflow_standard,
        "airflow.providers.standard.operators": airflow_operators,
        "airflow.providers.standard.operators.python": airflow_python,
        "pendulum": pendulum,
        "cadence_plan": cadence_plan,
        "extract_runner": extract_runner,
    }
    spec = importlib.util.spec_from_file_location("test_ror_extract_dags", EXTRACT_DAGS)
    dag_module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    with mock.patch.dict(sys.modules, modules):
        spec.loader.exec_module(dag_module)
    configs = [cfg for cfg in loaded_configs if cfg.get("name") == MODULE.SOURCE]
    if len(configs) != 1:
        raise AssertionError(f"existing loader produced {len(configs)} ROR configs")
    return configs[0], dag_module


def redact_stderr(value):
    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
    text = re.sub(
        r"(?im)(\bauthorization\s*:\s*)[^\r\n]*",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(
        r"(?i)(\b(?:api[_-]?key|token|password)\s*[:=]\s*)[^\s&]+",
        r"\1[REDACTED]",
        text,
    )
    text = re.sub(
        r"(?i)\b(https?://)[^/\s@]+@",
        r"\1[REDACTED]@",
        text,
    )
    if len(text) > LIVE_STDERR_LIMIT:
        omitted = len(text) - LIVE_STDERR_LIMIT
        text = text[:LIVE_STDERR_LIMIT] + f"\n...[{omitted} stderr characters omitted]"
    return text


def live_failure(category, exit_status, stderr):
    detail = redact_stderr(stderr).strip() or "<empty>"
    return f"live-smoke category={category} exit_status={exit_status} stderr={detail}"


def run_live_smoke():
    """Run only when explicitly requested; normal unittest discovery is offline."""
    argv = [
        sys.executable,
        str(SCRIPT),
        "--query",
        LIVE_ROR_ID,
        "--max-pages",
        "1",
        "--max-records",
        "20",
        "--max-requests",
        "3",
        "--timeout",
        "10",
        "--retries",
        "1",
        "--retry-budget",
        "30",
    ]
    with (
        tempfile.TemporaryFile(mode="w+b") as stdout_file,
        tempfile.TemporaryFile(mode="w+b") as stderr_file,
    ):
        try:
            child = subprocess.Popen(argv, stdout=stdout_file, stderr=stderr_file)
        except OSError as exc:
            print(live_failure("launch_error", None, str(exc)), file=sys.stderr)
            return 1
        try:
            exit_status = child.wait(timeout=LIVE_TIMEOUT)
        except subprocess.TimeoutExpired:
            child.kill()
            exit_status = child.wait()
            stderr_file.seek(0)
            print(
                live_failure("timeout", exit_status, stderr_file.read(LIVE_STDERR_LIMIT + 1)),
                file=sys.stderr,
            )
            return 1
        stdout_file.seek(0)
        stdout = stdout_file.read(1024 * 1024 + 1)
        stderr_file.seek(0)
        stderr = stderr_file.read(LIVE_STDERR_LIMIT + 1)

    if exit_status != 0:
        print(live_failure("child_exit", exit_status, stderr), file=sys.stderr)
        return 1
    if len(stdout) > 1024 * 1024:
        print(live_failure("stdout_limit", exit_status, stderr), file=sys.stderr)
        return 1
    lines = [line for line in stdout.decode("utf-8", errors="replace").splitlines() if line.strip()]
    if not lines:
        print(live_failure("empty_output", exit_status, stderr), file=sys.stderr)
        return 1
    try:
        records = [json.loads(line) for line in lines]
    except json.JSONDecodeError as exc:
        print(live_failure("invalid_ndjson", exit_status, f"{stderr!r}; {exc}"), file=sys.stderr)
        return 1
    if not all(
        isinstance(record, dict)
        and record.get("source") == MODULE.SOURCE
        and MODULE.ROR_ID_RE.fullmatch(record.get("id", ""))
        and record.get("fetched_at")
        and isinstance(record.get("raw"), dict)
        for record in records
    ):
        print(live_failure("invalid_envelope", exit_status, stderr), file=sys.stderr)
        return 1
    print(f"live-smoke ok exit_status=0 records={len(records)} target={LIVE_ROR_ID}")
    return 0


class FetchRorOrganizationsTests(unittest.TestCase):
    def fetch(self, documents, **changes):
        arguments = {
            "days_back": 2,
            "max_pages": 5,
            "max_records": 100,
            "max_requests": 15,
            "timeout": 17,
            "retries": 0,
            "retry_budget": 60,
            "today": date(2026, 9, 17),
        }
        arguments.update(changes)
        responses = [JsonResponse(document) for document in documents]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            records = MODULE.fetch_organizations(**arguments)
        return records, urlopen

    def test_preserves_complete_v2_record_inside_valid_envelope(self):
        wire = organization()
        records, _ = self.fetch([page(1, [wire])])

        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(set(record), {"source", "fetched_at", "id", "raw"})
        self.assertEqual(record["source"], MODULE.SOURCE)
        self.assertRegex(record["fetched_at"], r"\+00:00$")
        self.assertEqual(record["id"], wire["id"])
        self.assertEqual(record["raw"], wire)
        self.assertIsNot(record["raw"], wire)
        self.assertEqual(record["raw"]["future_field"], {"nested": [True, None, 7]})
        self.assertEqual(record["raw"]["status"], "active")
        self.assertEqual(record["raw"]["names"][1]["lang"], "fr")

    def test_empty_result_is_valid_and_stops_after_one_page(self):
        records, urlopen = self.fetch([page(0, [])])
        self.assertEqual(records, [])
        urlopen.assert_called_once()

    def test_multi_page_result_uses_one_timestamp_and_encoded_window(self):
        first_items = [
            organization(f"https://ror.org/{number:09d}") for number in range(1, 21)
        ]
        second_item = organization("https://ror.org/000000021")
        records, urlopen = self.fetch([page(21, first_items), page(21, [second_item])])

        self.assertEqual(len(records), 21)
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(len({record["fetched_at"] for record in records}), 1)
        for page_number, call in enumerate(urlopen.call_args_list, 1):
            query = query_of(call)
            self.assertEqual(query["page"], [str(page_number)])
            self.assertEqual(
                query["query.advanced"],
                ["admin.last_modified.date:[2026-09-16 TO 2026-09-17]"],
            )
            self.assertEqual(query["filter"], [MODULE.ALL_STATUSES_FILTER])
            request = call.args[0]
            self.assertIn("query.advanced=admin.last_modified.date%3A%5B", request.full_url)
            self.assertIn("filter=status%3Aactive%2Cstatus%3Ainactive", request.full_url)
            self.assertEqual(request.get_header("Accept"), "application/json")
            self.assertEqual(request.get_header("Accept-encoding"), "identity")
            self.assertTrue(request.get_header("User-agent"))
            self.assertEqual(call.kwargs, {"timeout": 17.0})

    def test_implicit_window_is_frozen_when_utc_midnight_passes_during_retry(self):
        real_datetime = MODULE.datetime
        crossed_midnight = False

        class MidnightClock(real_datetime):
            @classmethod
            def now(cls, tz=None):
                day = 18 if crossed_midnight else 17
                value = real_datetime(2026, 9, day, tzinfo=MODULE.timezone.utc)
                return value.astimezone(tz) if tz is not None else value.replace(tzinfo=None)

        first_items = [
            organization(f"https://ror.org/{number:09d}") for number in range(1, 21)
        ]
        responses = iter(
            (
                http_error(429, {"Retry-After": "0"}),
                JsonResponse(page(21, first_items)),
                JsonResponse(page(21, [organization("https://ror.org/000000021")])),
            )
        )

        def open_after_midnight(*args, **kwargs):
            nonlocal crossed_midnight
            crossed_midnight = True
            response = next(responses)
            if isinstance(response, BaseException):
                raise response
            return response

        stdout = io.StringIO()
        with (
            mock.patch.object(MODULE, "datetime", MidnightClock),
            mock.patch.object(
                MODULE.urllib.request, "urlopen", side_effect=open_after_midnight
            ) as urlopen,
            contextlib.redirect_stdout(stdout),
        ):
            MODULE.main(["--days-back", "2", "--retries", "1"])

        self.assertEqual(len(stdout.getvalue().splitlines()), 21)
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(
            [query_of(call)["page"] for call in urlopen.call_args_list],
            [["1"], ["1"], ["2"]],
        )
        self.assertEqual(
            {
                query_of(call)["query.advanced"][0]
                for call in urlopen.call_args_list
            },
            {"admin.last_modified.date:[2026-09-16 TO 2026-09-17]"},
        )

    def test_window_boundaries_are_inclusive_and_validate_utc_date_input(self):
        self.assertEqual(
            MODULE.last_modified_window(1, date(2026, 1, 1)),
            ("2026-01-01", "2026-01-01"),
        )
        self.assertEqual(
            MODULE.last_modified_window(2, date(2026, 1, 1)),
            ("2025-12-31", "2026-01-01"),
        )
        self.assertEqual(
            MODULE.last_modified_window(7, date(2024, 3, 1)),
            ("2024-02-24", "2024-03-01"),
        )
        with self.assertRaisesRegex(ValueError, "today must be a date"):
            MODULE.last_modified_window(2, "2026-09-17")

    def test_targeted_query_and_filter_modes_are_encoded_and_do_not_add_defaults(self):
        cases = (
            ({"query": 'Université Example + "lab"'}, "query", 'Université Example + "lab"'),
            ({"filter_expression": "country.country_code:GB,status:inactive"}, "filter", "country.country_code:GB,status:inactive"),
        )
        for changes, key, expected in cases:
            with self.subTest(key=key):
                records, urlopen = self.fetch([page(0, [])], days_back=None, **changes)
                self.assertEqual(records, [])
                query = query_of(urlopen.call_args)
                self.assertEqual(query[key], [expected])
                self.assertNotIn("query.advanced", query)
                if key == "query":
                    self.assertNotIn("filter", query)
                self.assertNotIn(" ", urlopen.call_args.args[0].full_url)

    def test_incompatible_or_unbounded_modes_fail_before_http(self):
        cases = (
            ({"query": "one", "filter_expression": "status:active"}, "incompatible"),
            ({"query": "one", "days_back": 2}, "days_back"),
            ({"filter_expression": "status:active", "days_back": 2}, "days_back"),
            ({"query": "   ", "days_back": None}, "non-empty"),
            ({"filter_expression": "x" * (MODULE.MAX_FILTER_LENGTH + 1), "days_back": None}, "no longer"),
        )
        for changes, message in cases:
            with self.subTest(changes=changes):
                arguments = {
                    "days_back": None,
                    "max_pages": 1,
                    "max_records": 20,
                    "max_requests": 3,
                    "retries": 0,
                }
                arguments.update(changes)
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.fetch_organizations(**arguments)
                urlopen.assert_not_called()

    def test_every_documented_status_is_requested_and_preserved(self):
        statuses = ("active", "inactive", "withdrawn")
        items = [
            organization(f"https://ror.org/00000000{index}", status=status)
            for index, status in enumerate(statuses, 1)
        ]
        records, urlopen = self.fetch([page(3, items)])
        self.assertEqual([record["raw"]["status"] for record in records], list(statuses))
        self.assertEqual(
            query_of(urlopen.call_args)["filter"],
            ["status:active,status:inactive,status:withdrawn"],
        )

    def test_reported_record_and_page_ceilings_fail_early_with_diagnostics(self):
        cases = (
            ({"max_records": 20}, page(21, [organization()]), "above max_records=20"),
            ({"max_pages": 1}, page(21, [organization()]), "requiring 2 pages above max_pages=1"),
        )
        for changes, document, message in cases:
            with self.subTest(changes=changes):
                with self.assertRaisesRegex(MODULE.TruncationError, message):
                    self.fetch([document], **changes)

    def test_short_duplicate_changing_and_overfull_pages_fail_closed(self):
        twenty = [organization(f"https://ror.org/{number:09d}") for number in range(1, 21)]
        cases = (
            ([page(2, [organization()])], "ended after 1 of 2"),
            ([page(2, [organization(), organization()])], "1 unique organizations"),
            ([page(21, twenty), page(22, [organization("https://ror.org/000000021")])], "changed from 21 to 22"),
            ([page(21, twenty + [organization("https://ror.org/000000021")])], "above page size"),
        )
        for documents, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.RorError, message):
                    self.fetch(documents)

    def test_invalid_limits_fail_before_http(self):
        cases = (
            ({"days_back": 0}, "days_back"),
            ({"days_back": MODULE.MAX_DAYS_BACK + 1}, "days_back"),
            ({"max_pages": 0}, "max_pages"),
            ({"max_pages": MODULE.HARD_MAX_PAGES + 1}, "max_pages"),
            ({"max_records": 0}, "max_records"),
            ({"max_records": MODULE.HARD_MAX_RECORDS + 1}, "max_records"),
            ({"max_requests": 0}, "max_requests"),
            ({"timeout": 0}, "timeout"),
            ({"timeout": MODULE.MAX_TIMEOUT + 1}, "timeout"),
            ({"retries": MODULE.MAX_RETRIES + 1}, "retries"),
            ({"retry_budget": 0}, "retry_budget"),
        )
        for changes, message in cases:
            with self.subTest(changes=changes):
                with mock.patch.object(MODULE.urllib.request, "urlopen") as urlopen:
                    with self.assertRaisesRegex(ValueError, message):
                        MODULE.fetch_organizations(**changes)
                urlopen.assert_not_called()

    def test_rejects_malformed_responses_and_organization_ids(self):
        cases = (
            ([], "response must be an object"),
            ({"items": []}, "number_of_results"),
            ({"number_of_results": True, "items": []}, "number_of_results"),
            ({"number_of_results": 0, "items": {}}, "items must be a list"),
            (page(0, [organization()]), "zero-result"),
            (page(1, []), "pagination ended"),
            (page(1, ["not an object"]), "non-object"),
            (page(1, [organization(id="03yrm5c26")]), "invalid id"),
        )
        for document, message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(MODULE.RorError, message):
                    self.fetch([document])

    def test_malformed_json_and_oversize_body_fail(self):
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(b"not-json")
        ):
            with self.assertRaisesRegex(MODULE.RorError, "malformed JSON"):
                MODULE.fetch_organizations(retries=0)
        oversized = b"{" + b" " * MODULE.MAX_RESPONSE_BYTES
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=JsonResponse(oversized)
        ):
            with self.assertRaisesRegex(MODULE.RorError, "byte limit"):
                MODULE.fetch_organizations(retries=0)

    def test_rate_limit_retry_succeeds_with_bounded_delay(self):
        sleeps = []
        client = MODULE.RorClient(
            timeout=17,
            retries=1,
            retry_budget=30,
            max_requests=3,
            clock=lambda: 0,
            sleeper=sleeps.append,
        )
        responses = [http_error(429, {"Retry-After": "3"}), JsonResponse(page(0, []))]
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=responses
        ) as urlopen:
            client.opener = MODULE.urllib.request.urlopen
            records = MODULE.fetch_organizations(
                max_pages=1,
                max_records=20,
                max_requests=3,
                timeout=17,
                retries=1,
                retry_budget=30,
                client=client,
            )
        self.assertEqual(records, [])
        self.assertEqual(sleeps, [3.0])
        self.assertEqual(urlopen.call_count, 2)
        self.assertEqual(client.requests_attempted, 2)

    def test_http_failures_retries_request_ceiling_and_rate_delay_are_bounded(self):
        nonretrying = MODULE.RorClient(
            timeout=5, retries=2, retry_budget=30, max_requests=3, clock=lambda: 0
        )
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=http_error(404)
        ) as urlopen:
            nonretrying.opener = MODULE.urllib.request.urlopen
            with self.assertRaisesRegex(MODULE.RorError, "HTTP 404"):
                nonretrying.get_page({"query": "target", "page": 1})
        urlopen.assert_called_once()

        sleeps = []
        retrying = MODULE.RorClient(
            timeout=5,
            retries=2,
            retry_budget=30,
            max_requests=3,
            clock=lambda: 0,
            sleeper=sleeps.append,
        )
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[TimeoutError("timed out")] * 3,
        ) as urlopen:
            retrying.opener = MODULE.urllib.request.urlopen
            with self.assertRaisesRegex(MODULE.RorError, "after 3 attempts"):
                retrying.get_page({"query": "target", "page": 1})
        self.assertEqual(urlopen.call_count, 3)
        self.assertEqual(sleeps, [1.0, 2.0])

        ceiling = MODULE.RorClient(
            timeout=5, retries=1, retry_budget=30, max_requests=1, clock=lambda: 0,
            sleeper=lambda _: None,
        )
        with mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=TimeoutError("timed out")
        ) as urlopen:
            ceiling.opener = MODULE.urllib.request.urlopen
            with self.assertRaisesRegex(MODULE.RetryBudgetError, "max_requests=1"):
                ceiling.get_page({"query": "target", "page": 1})
        urlopen.assert_called_once()

        capped = MODULE.RorClient(
            timeout=5, retries=1, retry_budget=120, max_requests=2, clock=lambda: 0
        )
        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=http_error(429, {"Retry-After": "61"}),
        ):
            capped.opener = MODULE.urllib.request.urlopen
            with self.assertRaisesRegex(MODULE.RetryBudgetError, "local 60s cap"):
                capped.get_page({"query": "target", "page": 1})

    def test_main_truncation_is_visible_nonzero_and_never_emits_partial_ndjson(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(page(21, [organization()])),
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
            self.assertRaises(SystemExit) as raised,
        ):
            MODULE.main(["--max-pages", "1", "--max-records", "20", "--retries", "0"])
        self.assertEqual(raised.exception.code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("refusing truncated output", stderr.getvalue())
        self.assertIn(MODULE.SOURCE, stderr.getvalue())

    def test_default_cli_allows_a_legitimate_empty_scheduled_window(self):
        stdout = io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=JsonResponse(page(0, []))
            ) as urlopen,
            contextlib.redirect_stdout(stdout),
        ):
            MODULE.main([])
        self.assertEqual(stdout.getvalue(), "")
        query = query_of(urlopen.call_args)
        self.assertIn("query.advanced", query)
        self.assertEqual(query["filter"], [MODULE.ALL_STATUSES_FILTER])

    def test_real_source_loader_and_configured_cli_are_bounded_disabled_and_offline(self):
        config, dag_module = load_config_through_dag_factory()
        self.assertEqual(config["name"], MODULE.SOURCE)
        self.assertEqual(config["script"], SCRIPT.name)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["schedule"], "23 7 * * *")
        self.assertEqual(config["sink"], "local")
        self.assertTrue(hasattr(dag_module, "extract__ror_organizations"))

        args = MODULE.build_parser().parse_args(config["args"])
        self.assertLessEqual(args.days_back, MODULE.MAX_DAYS_BACK)
        self.assertLessEqual(args.max_pages, MODULE.HARD_MAX_PAGES)
        self.assertLessEqual(args.max_records, MODULE.HARD_MAX_RECORDS)
        self.assertLessEqual(args.max_requests, MODULE.HARD_MAX_REQUESTS)
        self.assertLessEqual(args.timeout, MODULE.MAX_TIMEOUT)
        self.assertLessEqual(args.retries, MODULE.MAX_RETRIES)
        self.assertLessEqual(args.retry_budget, MODULE.MAX_RETRY_BUDGET)
        self.assertGreaterEqual(args.max_requests, args.max_pages * (args.retries + 1))
        self.assertLessEqual(args.retry_budget, config["timeout_minutes"] * 60)

        stdout = io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request, "urlopen", return_value=JsonResponse(page(0, []))
            ) as urlopen,
            contextlib.redirect_stdout(stdout),
        ):
            MODULE.main(config["args"])
        self.assertEqual(stdout.getvalue(), "")
        urlopen.assert_called_once()

        text = SOURCE_CONFIG.read_text(encoding="utf-8")
        self.assertIn("inactive, and withdrawn", text)
        self.assertIn("full-registry export", text)
        self.assertIn("network-enabled live smoke", text)
        self.assertIn("external gates", text)

    def test_compact_ndjson_envelope_passes_existing_runner_contract(self):
        stdout = io.StringIO()
        with (
            mock.patch.object(
                MODULE.urllib.request,
                "urlopen",
                return_value=JsonResponse(page(1, [organization()])),
            ),
            contextlib.redirect_stdout(stdout),
        ):
            MODULE.main(["--query", "example university", "--max-pages", "1", "--retries", "0"])
        line = stdout.getvalue().strip()
        self.assertNotIn(": ", line)
        record = json.loads(line)
        self.assertTrue(record["source"])
        self.assertTrue(record["fetched_at"])
        self.assertTrue(record["id"])

    def test_live_failure_diagnostics_are_bounded_redacted_and_categorized(self):
        secrets = (
            "bearer-synthetic-credential",
            "synthetic-api-key",
            "synthetic-token",
            "synthetic-password",
            "synthetic-user",
            "synthetic-url-password",
        )
        stderr = (
            "request failed\n"
            "Authorization: Bearer bearer-synthetic-credential\n"
            "api_key=synthetic-api-key token: synthetic-token "
            "password=synthetic-password\n"
            "upstream=https://synthetic-user:synthetic-url-password@example.invalid/path\n"
            + "x" * (LIVE_STDERR_LIMIT + 100)
        )
        diagnostic = live_failure("child_exit", 7, stderr)
        self.assertIn("category=child_exit", diagnostic)
        self.assertIn("exit_status=7", diagnostic)
        self.assertIn("Authorization: [REDACTED]", diagnostic)
        self.assertIn("api_key=[REDACTED]", diagnostic)
        self.assertIn("token: [REDACTED]", diagnostic)
        self.assertIn("password=[REDACTED]", diagnostic)
        self.assertIn("https://[REDACTED]@example.invalid/path", diagnostic)
        for secret in secrets:
            self.assertNotIn(secret, diagnostic)
        self.assertIn("stderr characters omitted", diagnostic)
        self.assertLess(len(diagnostic), LIVE_STDERR_LIMIT + 250)

    def test_live_smoke_entrypoint_is_not_used_by_normal_discovery(self):
        self.assertEqual(LIVE_ROR_ID, "https://ror.org/042nb2s44")
        self.assertIn("--query", [
            sys.executable,
            str(SCRIPT),
            "--query",
            LIVE_ROR_ID,
        ])
        self.assertNotIn("--live-smoke", MODULE.build_parser().format_help())


if __name__ == "__main__":
    if sys.argv[1:] == ["--live-smoke"]:
        raise SystemExit(run_live_smoke())
    unittest.main()
