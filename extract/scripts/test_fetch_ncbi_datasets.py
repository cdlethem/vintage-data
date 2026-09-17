from __future__ import annotations

import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import ModuleType
import urllib.error
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


class FixtureServer:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append(
                    {
                        "path": self.path,
                        "accept": self.headers.get("Accept"),
                        "api_key": self.headers.get("api-key"),
                    }
                )
                if not owner.responses:
                    self.send_error(500, "no fixture response")
                    return
                status, content_type, body = owner.responses.pop(0)
                payload = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def api_base(self):
        host, port = self.server.server_address
        return f"http://{host}:{port}/datasets/v2/genome/accession"

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def run_cli(api_base: str, args: list[str], *, api_key: str | None = None):
    """Run the real CLI and retain bounded, credential-redacted failure evidence."""
    env = os.environ.copy()
    env["NCBI_DATASETS_API_BASE"] = api_base
    if api_key is None:
        env.pop("NCBI_API_KEY", None)
    else:
        env["NCBI_API_KEY"] = api_key
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
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


def test_cli_ndjson_contract():
    """Exercise the subprocess CLI, every output line, valid empty, and hard failures."""
    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
    with FixtureServer([(200, "application/json", document())]) as fixture:
        code, stdout, stderr = run_cli(fixture.api_base, list(config["args"]))
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
    assert fixture.requests == [
        {
            "path": f"/datasets/v2/genome/accession/{ACCESSION}/dataset_report",
            "accept": "application/json",
            "api_key": None,
        }
    ]

    with FixtureServer([(200, "application/json", {"reports": [], "total_count": 0})]) as empty:
        empty_code, empty_stdout, empty_stderr = run_cli(empty.api_base, list(config["args"]))
    assert (empty_code, empty_stdout, empty_stderr) == (0, "", "")

    failures = [
        (503, "application/json", {"message": "unavailable"}, "http_error"),
        (200, "text/plain", b"not-json", "non_json_response"),
        (200, "application/json", {"reports": [{}], "total_count": 1}, "malformed_report"),
    ]
    for status, content_type, payload, category in failures:
        with FixtureServer([(status, content_type, payload)]) as failed:
            code, failed_stdout, failed_stderr = run_cli(
                failed.api_base, list(config["args"]), api_key=API_KEY
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
