from __future__ import annotations

import base64
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import traceback
from types import ModuleType
import urllib.error
import urllib.request
from unittest import mock

import pytest
import yaml


SCRIPT = Path(__file__).with_name("fetch_ncbi_datasets.py")
SOURCE_CONFIG = SCRIPT.parents[1] / "sources" / "ncbi_datasets.yml"
REPO_ROOT = SCRIPT.parents[2]
EXTRACT_RUNNER = REPO_ROOT / "orchestration" / "include" / "extract_runner.py"
DAG_FACTORY = REPO_ROOT / "orchestration" / "dags" / "extract_dags.py"
INCLUDE_DIR = DAG_FACTORY.parent.parent / "include"
SPEC = importlib.util.spec_from_file_location("fetch_ncbi_datasets", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

ACCESSION = "GCF_000001405.40"
SECOND_ACCESSION = "GCA_000001635.9"
API_KEY = "sensitive-test-api-key"

CLI_FIXTURE_RUNNER = r'''
import base64
import importlib.util
import io
import json
from pathlib import Path
import sys
import urllib.error

script = Path(sys.argv[1])
fixture = json.loads(sys.argv[2])
spec = importlib.util.spec_from_file_location("fixture_ncbi_datasets", script)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

class Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

def open_fixture(request, **kwargs):
    assert request.full_url.startswith(module.OFFICIAL_API_BASE + "/")
    if fixture.get("forbid_transport"):
        raise AssertionError("transport called")
    failure_kind = fixture.get("failure_kind")
    failure_detail = fixture.get("failure_detail", "fixture failure")
    if failure_kind == "http":
        raise urllib.error.HTTPError(
            f"https://{failure_detail}@attacker.invalid/{failure_detail}",
            503,
            failure_detail,
            {},
            io.BytesIO(failure_detail.encode("utf-8")),
        )
    if failure_kind == "transport":
        raise urllib.error.URLError(failure_detail)
    status = fixture.get("http_status")
    if status is not None:
        raise urllib.error.HTTPError(request.full_url, status, "fixture error", {}, None)
    return Response(base64.b64decode(fixture["payload"]))

module.open_official_request = open_fixture
raise SystemExit(module.main(sys.argv[3:]))
'''


def assembly_report(accession: str = ACCESSION, **changes):
    report = {
        "accession": accession,
        "current_accession": accession,
        "paired_accession": "GCA_000001405.29",
        "source_database": "SOURCE_DATABASE_REFSEQ",
        "organism": {
            "organism_name": "Homo sapiens",
            "tax_id": "9606",
            "infraspecific_names": {"breed": "reference"},
        },
        "assembly_info": {
            "assembly_name": "GRCh38.p14",
            "assembly_level": "Chromosome",
            "release_date": "2022-02-03",
        },
        "future_field": {"preserved": True},
    }
    report.update(changes)
    return report


def document(accession: str = ACCESSION):
    return {"reports": [assembly_report(accession)], "total_count": 1}


class JsonResponse(io.BytesIO):
    def __init__(self, value):
        payload = value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")
        super().__init__(payload)
        self.headers = {"Content-Type": "application/json"}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()




def run_cli(
    args: list[str],
    *,
    response=None,
    http_status: int | None = None,
    failure_kind: str | None = None,
    failure_detail: str | None = None,
    forbid_transport: bool = False,
    api_key: str | None = None,
    endpoint_override: str | None = None,
):
    """Run the real CLI with test-only transport and bounded, redacted evidence."""
    if response is None:
        response = document()
    payload = response if isinstance(response, bytes) else json.dumps(response).encode("utf-8")
    fixture = json.dumps(
        {
            "payload": base64.b64encode(payload).decode("ascii"),
            "http_status": http_status,
            "failure_kind": failure_kind,
            "failure_detail": failure_detail,
            "forbid_transport": forbid_transport,
        }
    )
    env = os.environ.copy()
    if api_key is None:
        env.pop("NCBI_API_KEY", None)
    else:
        env["NCBI_API_KEY"] = api_key
    if endpoint_override is None:
        env.pop("NCBI_DATASETS_API_BASE", None)
    else:
        env["NCBI_DATASETS_API_BASE"] = endpoint_override
    result = subprocess.run(
        [sys.executable, "-c", CLI_FIXTURE_RUNNER, str(SCRIPT), fixture, *args],
        cwd=SCRIPT.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    if api_key:
        assert api_key not in result.stderr, "child stderr exposed NCBI_API_KEY"
    stderr = result.stderr.replace(api_key, "[REDACTED]") if api_key else result.stderr
    assert len(stderr.encode("utf-8")) <= 4096, (
        f"child stderr exceeded test evidence bound; exit={result.returncode}"
    )
    return result.returncode, result.stdout, stderr


def load_extract_runner():
    sys.path.insert(0, str(INCLUDE_DIR))
    try:
        spec = importlib.util.spec_from_file_location("ncbi_test_extract_runner", EXTRACT_RUNNER)
        runner = importlib.util.module_from_spec(spec)
        assert spec.loader is not None
        spec.loader.exec_module(runner)
        return runner
    finally:
        sys.path.remove(str(INCLUDE_DIR))


def test_request_construction_normalization_ids_timestamp_and_pacing():
    opened = []
    sleeps = []
    responses = [JsonResponse(document(ACCESSION)), JsonResponse(document(SECOND_ACCESSION))]

    def opener(request, **kwargs):
        opened.append((request, kwargs))
        return responses.pop(0)

    records = MODULE.fetch_genome_reports(
        [ACCESSION, SECOND_ACCESSION],
        timeout=17,
        api_key="",
        opener=opener,
        sleeper=sleeps.append,
    )

    assert [record["id"] for record in records] == [ACCESSION, SECOND_ACCESSION]
    assert [record["accession"] for record in records] == [ACCESSION, SECOND_ACCESSION]
    assert len({record["fetched_at"] for record in records}) == 1
    assert records[0]["fetched_at"].endswith("+00:00")
    assert records[0]["organism_name"] == "Homo sapiens"
    assert records[0]["tax_id"] == 9606
    assert records[0]["assembly_name"] == "GRCh38.p14"
    assert records[0]["raw"] == assembly_report()
    assert records[0]["source_url"] == MODULE.report_url(ACCESSION)
    assert "?" not in records[0]["source_url"]

    assert [call[0].full_url for call in opened] == [
        MODULE.request_url(ACCESSION),
        MODULE.request_url(SECOND_ACCESSION),
    ]
    assert [call[1] for call in opened] == [{"timeout": 17.0}, {"timeout": 17.0}]
    assert all(call[0].get_header("Accept") == "application/json" for call in opened)
    assert all(call[0].get_header("Api-key") is None for call in opened)
    assert sleeps == [MODULE.REQUEST_DELAY_SECONDS]
    assert MODULE.REQUEST_DELAY_SECONDS > 1 / 5


def test_optional_api_key_is_header_only_and_never_enters_records_or_errors():
    opened = []

    def opener(request, **kwargs):
        opened.append(request)
        return JsonResponse(document())

    with mock.patch.dict(os.environ, {"NCBI_API_KEY": API_KEY}):
        record = MODULE.fetch_genome_reports(
            [ACCESSION], api_key=None, opener=opener
        )[0]
    serialized = json.dumps(record)
    assert opened[0].get_header("Api-key") == API_KEY
    assert API_KEY not in opened[0].full_url
    assert API_KEY not in serialized

    error = urllib.error.HTTPError(
        MODULE.request_url(ACCESSION), 403, f"denied {API_KEY}", {}, io.BytesIO(API_KEY.encode())
    )
    with pytest.raises(MODULE.NCBIDatasetsError) as raised:
        MODULE.fetch_genome_reports(
            [ACCESSION], api_key=API_KEY, opener=mock.Mock(side_effect=error)
        )
    assert raised.value.category == "http_error"
    assert API_KEY not in str(raised.value)

@pytest.mark.parametrize(
    ("upstream_kind", "category", "message"),
    [
        (
            "http",
            "http_error",
            f"request for {ACCESSION} returned an HTTP error",
        ),
        ("transport", "transport_error", f"request for {ACCESSION} failed"),
    ],
)
def test_wrapped_failures_discard_sensitive_exception_chains(
    upstream_kind, category, message
):
    sensitive_reason = "DISTINCTIVE_UPSTREAM_REASON"
    sensitive_url = "DISTINCTIVE_AUTHENTICATED_URL"
    if upstream_kind == "http":
        upstream = urllib.error.HTTPError(
            f"https://{API_KEY}@attacker.invalid/{sensitive_url}",
            503,
            f"{sensitive_reason}:{API_KEY}",
            {},
            io.BytesIO(f"{sensitive_reason}:{API_KEY}".encode("utf-8")),
        )
    else:
        upstream = urllib.error.URLError(
            f"{sensitive_reason}:{sensitive_url}:{API_KEY}"
        )

    with pytest.raises(MODULE.NCBIDatasetsError) as raised:
        MODULE.fetch_genome_reports(
            [ACCESSION], api_key=API_KEY, opener=mock.Mock(side_effect=upstream)
        )

    error = raised.value
    formatted = "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )
    assert error.category == category
    assert str(error) == message
    assert error.__cause__ is None
    assert error.__context__ is None
    assert error.__suppress_context__ is True
    for secret in (API_KEY, sensitive_reason, sensitive_url):
        assert secret not in str(error)
        assert secret not in formatted


@pytest.mark.parametrize(
    ("failure_kind", "category"),
    [("http", "http_error"), ("transport", "transport_error")],
)
def test_cli_wrapped_failures_do_not_disclose_credentials(failure_kind, category):
    api_key = "complete-synthetic-cli-api-key"
    sensitive_fragment = "DISTINCTIVE_CLI_UPSTREAM_FRAGMENT"
    detail = f"{sensitive_fragment}--{api_key}"
    code, stdout, stderr = run_cli(
        [ACCESSION],
        failure_kind=failure_kind,
        failure_detail=detail,
        api_key=api_key,
    )
    assert code != 0
    assert stdout == ""
    assert f"ncbi_datasets: {category}:" in stderr
    assert api_key not in stdout
    assert api_key not in stderr
    assert sensitive_fragment not in stdout
    assert sensitive_fragment not in stderr


MALFORMED_API_KEYS = [
    pytest.param(
        "embedded-sensitive-key\r\nX-Injected: evil",
        ("embedded-sensitive-key", "Injected"),
        id="embedded-crlf",
    ),
    pytest.param(
        "\rleading-cr-sensitive-key",
        ("leading-cr-sensitive-key",),
        id="leading-cr",
    ),
    pytest.param(
        "\nleading-lf-sensitive-key",
        ("leading-lf-sensitive-key",),
        id="leading-lf",
    ),
    pytest.param(
        "trailing-cr-sensitive-key\r",
        ("trailing-cr-sensitive-key",),
        id="trailing-cr",
    ),
    pytest.param(
        "trailing-lf-sensitive-key\n",
        ("trailing-lf-sensitive-key",),
        id="trailing-lf",
    ),
]


@pytest.mark.parametrize("malformed_api_key,sensitive_fragments", MALFORMED_API_KEYS)
def test_callable_rejects_header_unsafe_api_key_before_transport(
    malformed_api_key, sensitive_fragments
):
    opener = mock.Mock()
    with pytest.raises(ValueError) as raised:
        MODULE.fetch_genome_reports(
            [ACCESSION], api_key=malformed_api_key, opener=opener
        )
    opener.assert_not_called()
    message = str(raised.value)
    assert malformed_api_key not in message
    assert all(fragment not in message for fragment in sensitive_fragments)


@pytest.mark.parametrize("malformed_api_key,sensitive_fragments", MALFORMED_API_KEYS)
def test_callable_rejects_header_unsafe_api_key_from_environment(
    malformed_api_key, sensitive_fragments
):
    opener = mock.Mock()
    with mock.patch.dict(os.environ, {"NCBI_API_KEY": malformed_api_key}):
        with pytest.raises(ValueError) as raised:
            MODULE.fetch_genome_reports([ACCESSION], opener=opener)
    opener.assert_not_called()
    message = str(raised.value)
    assert malformed_api_key not in message
    assert all(fragment not in message for fragment in sensitive_fragments)


@pytest.mark.parametrize("malformed_api_key,sensitive_fragments", MALFORMED_API_KEYS)
def test_cli_rejects_header_unsafe_api_key_before_transport(
    malformed_api_key, sensitive_fragments
):
    code, stdout, stderr = run_cli(
        [ACCESSION], forbid_transport=True, api_key=malformed_api_key
    )
    assert code != 0
    assert stdout == ""
    assert "transport called" not in stderr
    assert "NCBI_API_KEY contains characters that are not valid" in stderr
    assert malformed_api_key not in stdout
    assert malformed_api_key not in stderr
    for fragment in sensitive_fragments:
        assert fragment not in stdout
        assert fragment not in stderr


def test_endpoint_override_is_ignored_and_redirects_never_forward_credentials():
    bogus_endpoint = "http://127.0.0.1:1/credential-capture"
    with mock.patch.dict(
        os.environ,
        {"NCBI_DATASETS_API_BASE": bogus_endpoint, "NCBI_API_KEY": API_KEY},
    ):
        opened = []

        def opener(request, **kwargs):
            opened.append(request)
            return JsonResponse(document())

        record = MODULE.fetch_genome_reports([ACCESSION], opener=opener)[0]

    assert opened[0].full_url == MODULE.report_url(ACCESSION)
    assert not opened[0].full_url.startswith(bogus_endpoint)
    assert opened[0].get_header("Api-key") == API_KEY
    assert record["source_url"] == MODULE.report_url(ACCESSION)
    assert API_KEY not in json.dumps(record)

    handler = MODULE.OfficialHTTPSRedirectHandler()
    request = urllib.request.Request(
        MODULE.report_url(ACCESSION), headers={"api-key": API_KEY}
    )
    for unsafe_url in (
        "https://attacker.invalid/collect",
        "http://api.ncbi.nlm.nih.gov/collect",
        "https://api.ncbi.nlm.nih.gov:444/collect",
    ):
        with pytest.raises(MODULE.NCBIDatasetsError) as raised:
            handler.redirect_request(
                request,
                None,
                302,
                "Found",
                {},
                unsafe_url,
            )
        assert raised.value.category == "unsafe_redirect"
        assert API_KEY not in str(raised.value)

    redirected = handler.redirect_request(
        request,
        None,
        302,
        "Found",
        {},
        f"/{ACCESSION}/dataset_report",
    )
    assert redirected.full_url.startswith("https://api.ncbi.nlm.nih.gov/")
    assert redirected.get_header("Api-key") == API_KEY


def test_cli_ignores_endpoint_override_with_api_key():
    code, stdout, stderr = run_cli(
        [ACCESSION],
        api_key=API_KEY,
        endpoint_override="http://127.0.0.1:1/credential-capture",
    )
    assert code == 0, stderr
    assert stderr == ""
    record = json.loads(stdout)
    assert record["source_url"] == MODULE.report_url(ACCESSION)
    assert API_KEY not in stdout


def test_http_non_json_and_malformed_report_fail_with_useful_categories():
    http_error = urllib.error.HTTPError(
        MODULE.request_url(ACCESSION), 503, "unavailable", {}, io.BytesIO(b"upstream")
    )
    cases = [
        (mock.Mock(side_effect=http_error), "http_error"),
        (mock.Mock(return_value=JsonResponse(b"not json")), "non_json_response"),
        (mock.Mock(return_value=JsonResponse({"total_count": 0})), "malformed_response"),
        (
            mock.Mock(
                return_value=JsonResponse(
                    {"reports": [assembly_report("GCF_000001405.39")], "total_count": 1}
                )
            ),
            "malformed_report",
        ),
    ]
    for opener, category in cases:
        with pytest.raises(MODULE.NCBIDatasetsError) as raised:
            MODULE.fetch_genome_reports([ACCESSION], api_key="", opener=opener)
        assert raised.value.category == category


def test_valid_empty_result_is_distinct_from_malformed_response():
    records = MODULE.fetch_genome_reports(
        [ACCESSION],
        api_key="",
        opener=mock.Mock(return_value=JsonResponse({"reports": [], "total_count": 0})),
    )
    assert records == []
    with pytest.raises(MODULE.NCBIDatasetsError, match="reports must be a list"):
        MODULE.fetch_genome_reports(
            [ACCESSION],
            api_key="",
            opener=mock.Mock(return_value=JsonResponse({"reports": None})),
        )


def test_validation_rejects_unversioned_duplicate_and_unbounded_arguments_before_http():
    invalid = [
        ["GCF_000001405"],
        [ACCESSION, ACCESSION],
        [f"GCF_{number:09d}.1" for number in range(MODULE.MAX_ACCESSIONS + 1)],
    ]
    for accessions in invalid:
        opener = mock.Mock()
        with pytest.raises(ValueError):
            MODULE.fetch_genome_reports(accessions, api_key="", opener=opener)
        opener.assert_not_called()


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), float("-inf"), 0.0, -1.0, 120.0001])
def test_callable_rejects_invalid_timeout_before_transport(timeout):
    opener = mock.Mock()
    with pytest.raises(ValueError):
        MODULE.fetch_genome_reports([ACCESSION], timeout=timeout, api_key="", opener=opener)
    opener.assert_not_called()


@pytest.mark.parametrize("timeout", [float.fromhex("0x0.0000000000001p-1022"), MODULE.MAX_TIMEOUT])
def test_callable_accepts_valid_timeout_boundaries(timeout):
    opener = mock.Mock(return_value=JsonResponse(document()))
    records = MODULE.fetch_genome_reports(
        [ACCESSION], timeout=timeout, api_key="", opener=opener
    )
    assert records[0]["id"] == ACCESSION
    assert opener.call_args.kwargs["timeout"] == timeout


@pytest.mark.parametrize("timeout", ["nan", "inf", "-inf", "0", "-1", "120.0001"])
def test_cli_rejects_invalid_timeout_before_transport(timeout):
    code, stdout, stderr = run_cli(
        [ACCESSION, "--timeout", timeout], forbid_transport=True
    )
    assert code != 0
    assert stdout == ""
    assert "timeout" in stderr


@pytest.mark.parametrize("timeout", ["5e-324", "120"])
def test_cli_accepts_valid_timeout_boundaries(timeout):
    code, stdout, stderr = run_cli([ACCESSION, "--timeout", timeout])
    assert code == 0, stderr
    assert json.loads(stdout)["id"] == ACCESSION


def test_cli_ndjson_contract():
    """Exercise the subprocess CLI, every output line, valid empty, and hard failures."""
    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
    code, stdout, stderr = run_cli(list(config["args"]))
    assert code == 0, stderr
    assert stderr == ""
    lines = stdout.splitlines()
    assert lines

    runner = load_extract_runner()
    for line in lines:
        record = json.loads(line)
        assert record["source"] == MODULE.SOURCE
        assert record["id"]
        assert record["fetched_at"]
        runner._check_envelope(line)
    assert ": " not in lines[0]

    empty_code, empty_stdout, empty_stderr = run_cli(
        list(config["args"]), response={"reports": [], "total_count": 0}
    )
    assert (empty_code, empty_stdout, empty_stderr) == (0, "", "")

    failures = [
        ({"message": "unavailable"}, "http_error", 503),
        (b"not-json", "non_json_response", None),
        ({"reports": [{}], "total_count": 1}, "malformed_report", None),
    ]
    for payload, category, status in failures:
        code, failed_stdout, failed_stderr = run_cli(
            list(config["args"]), response=payload, http_status=status, api_key=API_KEY
        )
        assert code != 0
        assert failed_stdout == ""
        assert f"ncbi_datasets: {category}:" in failed_stderr
        assert API_KEY not in failed_stderr


def test_source_dag_discovery():
    """Import the actual repository factory with only Airflow infrastructure stubbed."""
    created_dags = []
    current = []

    class FakeDAG:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.dag_id = kwargs["dag_id"]
            self.tasks = []
            created_dags.append(self)

        def __enter__(self):
            current.append(self)
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            current.pop()

    class FakePythonOperator:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            current[-1].tasks.append(self)

    airflow = ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_providers = ModuleType("airflow.providers")
    airflow_standard = ModuleType("airflow.providers.standard")
    airflow_operators = ModuleType("airflow.providers.standard.operators")
    airflow_python = ModuleType("airflow.providers.standard.operators.python")
    airflow_python.PythonOperator = FakePythonOperator
    pendulum = ModuleType("pendulum")
    pendulum.datetime = lambda *args, **kwargs: (args, kwargs)

    stubs = {
        "airflow": airflow,
        "airflow.providers": airflow_providers,
        "airflow.providers.standard": airflow_standard,
        "airflow.providers.standard.operators": airflow_operators,
        "airflow.providers.standard.operators.python": airflow_python,
        "pendulum": pendulum,
    }
    sys.path.insert(0, str(INCLUDE_DIR))
    try:
        with mock.patch.dict(sys.modules, stubs):
            spec = importlib.util.spec_from_file_location("ncbi_test_extract_dags", DAG_FACTORY)
            factory = importlib.util.module_from_spec(spec)
            assert spec.loader is not None
            spec.loader.exec_module(factory)
    finally:
        sys.path.remove(str(INCLUDE_DIR))

    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
    dag = next(dag for dag in created_dags if dag.dag_id == "extract__ncbi_datasets")
    assert dag.schedule == config["schedule"]
    assert dag.is_paused_upon_creation is True
    assert len(dag.tasks) == 1
    task = dag.tasks[0]
    assert task.python_callable is factory.extract_runner.run
    assert task.op_kwargs["cfg"] == config
    assert task.op_kwargs["cfg"]["script"] == SCRIPT.name
    assert task.op_kwargs["cfg"]["args"] == [ACCESSION, "--timeout", "30"]
    parsed = MODULE.build_parser().parse_args(task.op_kwargs["cfg"]["args"])
    assert parsed.accessions == [ACCESSION]
    assert parsed.timeout == 30
    assert config["enabled"] is False


@pytest.mark.skipif(
    os.environ.get("NCBI_DATASETS_LIVE") != "1",
    reason="set NCBI_DATASETS_LIVE=1 only in an authorized network-enabled workflow",
)
def test_live_configured_accession():
    """Mandatory external gate; explicitly enabled network failures must fail, not skip."""
    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
    env = os.environ.copy()
    env.pop("NCBI_API_KEY", None)
    env.pop("NCBI_DATASETS_API_BASE", None)
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, config["args"])],
        cwd=SCRIPT.parent,
        env=env,
        capture_output=True,
        text=True,
        timeout=config["timeout_minutes"] * 60,
        check=False,
    )
    stderr = result.stderr[:4096]
    assert result.returncode == 0, f"exit={result.returncode}; stderr={stderr}"
    lines = result.stdout.splitlines()
    assert lines, "live extraction returned no records"
    runner = load_extract_runner()
    records = []
    for line in lines:
        runner._check_envelope(line)
        records.append(json.loads(line))
    assert any(record["id"] == ACCESSION for record in records)
    assert all(record["source"] == MODULE.SOURCE for record in records)
    assert API_KEY not in result.stdout
