"""Offline DAG/loader integration plus the separately authorized live gate.

The normal pytest path is fixture-only and never opens the network. An operator
may explicitly run this file with ``--live`` after review; that mode executes the
real extractor with the mandated one-record bound, validates its envelope, and
lands it into an isolated temporary DuckDB destination. It performs no
production writes and reads no credentials.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from datetime import datetime
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import types
from unittest import mock

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "extract" / "scripts" / "fetch_inaturalist_observations.py"
SOURCE_CONFIG = REPO_ROOT / "extract" / "sources" / "inaturalist_observations.yml"
DAG_FACTORY = REPO_ROOT / "orchestration" / "dags" / "extract_dags.py"
LOAD_ROOT = REPO_ROOT / "load"
SOURCE = "inaturalist_observations"

if str(LOAD_ROOT) not in sys.path:
    sys.path.insert(0, str(LOAD_ROOT))

# Required offline dependencies are deliberately imported, not skipped.
from loader.config import load_config
from loader.queue import Job
from loader.service import LoaderService


class FakeDAG:
    active = None

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.kwargs = kwargs
        self.tasks = []

    def __enter__(self):
        FakeDAG.active = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        FakeDAG.active = None


class FakePythonOperator:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        if FakeDAG.active is None:
            raise AssertionError("PythonOperator created outside a DAG context")
        FakeDAG.active.tasks.append(self)


def _factory_modules():
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
        cfg["schedule"],
        "declared (cadence detection off)",
    )
    extract_runner = types.ModuleType("extract_runner")
    extract_runner.SCRIPTS_DIR = SCRIPT.parent
    extract_runner.run = lambda cfg: cfg
    return {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.python": python_operator,
        "pendulum": pendulum,
        "cadence_plan": cadence_plan,
        "extract_runner": extract_runner,
    }


def _load_real_dag_factory():
    spec = importlib.util.spec_from_file_location("inaturalist_test_extract_dags", DAG_FACTORY)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    with mock.patch.dict(sys.modules, _factory_modules()):
        spec.loader.exec_module(module)
    return module


def fixture_record(observation_id, fetched_at="2026-09-17T12:34:56+00:00"):
    return {
        "source": SOURCE,
        "id": observation_id,
        "fetched_at": fetched_at,
        "observation_id": observation_id,
        "observed_on": "2026-09-16",
        "time_observed_at": None,
        "taxon_id": 47158,
        "taxon_name": "Corvus brachyrhynchos",
        "taxon": {
            "id": 47158,
            "name": "Corvus brachyrhynchos",
            "rank": "species",
        },
        "latitude": 40.1,
        "longitude": -74.2,
        "geoprivacy": "obscured",
        "taxon_geoprivacy": None,
        "obscured": True,
        "quality_grade": "research",
        "uri": f"https://www.inaturalist.org/observations/{observation_id}",
        "license_code": "cc-by-nc",
        "observer": {"id": 9, "login": "observer", "name": None},
        "photos": [
            {
                "id": 700,
                "license_code": "cc-by",
                "attribution": "(c) Observer, CC BY",
            }
        ],
    }


def _write_load_config(root: Path) -> Path:
    config = root / "load.yml"
    config.write_text(
        "\n".join(
            (
                "destination: isolated",
                "destinations:",
                "  isolated:",
                "    type: duckdb",
                f"    database: {root / 'warehouse.duckdb'}",
                "    lock_timeout_s: 1",
                "    max_attempts: 1",
                "paths:",
                f"  source_root: {root / 'raw'}",
                f"  queue_dir: {root / 'queue'}",
                "schemas:",
                "  raw: raw",
                "  meta: _load",
                "defaults:",
                "  min_age_s: 0",
                "  keep_payload: true",
                "  column_types:",
                "    source: string",
                "    id: string",
                "    fetched_at: timestamp",
                "cadence:",
                "  enabled: false",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    return config


def _land_records(root: Path, records: list[dict]):
    raw_file = (
        root
        / "raw"
        / f"source={SOURCE}"
        / "dt=2026-09-17"
        / f"{SOURCE}_fixture.ndjson"
    )
    raw_file.parent.mkdir(parents=True, exist_ok=True)
    raw_file.write_text(
        "".join(json.dumps(record, separators=(",", ":")) + "\n" for record in records),
        encoding="utf-8",
    )
    config = load_config(_write_load_config(root))
    service = LoaderService(config)
    job = Job(
        "fixture-load",
        {"kind": "files", "paths": [str(raw_file)], "min_age_s": 0},
        root / "fixture-load.json",
    )
    result = service.run_job(job)
    return service, result


def _query_landed_envelopes(service: LoaderService):
    return service.destination.con.execute(
        'SELECT id, source, fetched_at, _payload '
        'FROM raw."inaturalist_observations" ORDER BY id'
    ).fetchall()


def _validate_landed_records(service: LoaderService, records: list[dict]) -> None:
    rows = _query_landed_envelopes(service)
    expected = sorted(records, key=lambda record: str(record["id"]))
    if len(rows) != len(expected):
        raise ValueError(f"row count mismatch: loaded {len(rows)}, expected {len(expected)}")
    for row, record in zip(rows, expected):
        loaded_id, loaded_source, loaded_fetched_at, loaded_payload = row
        expected_fetched_at = datetime.fromisoformat(record["fetched_at"].replace("Z", "+00:00"))
        if (loaded_id, loaded_source, loaded_fetched_at) != (
            str(record["id"]),
            record["source"],
            expected_fetched_at,
        ):
            raise ValueError(f"envelope mismatch for record {record['id']}")
        if isinstance(loaded_payload, str):
            loaded_payload = json.loads(loaded_payload)
        if loaded_payload != record:
            raise ValueError(f"retained payload mismatch for record {record['id']}")


def _query_populated_metadata(service: LoaderService):
    return service.destination.con.execute(
        'SELECT id, source, taxon_id, taxon_name, geoprivacy, obscured, '
        'quality_grade, license_code, uri, observer, photos '
        'FROM raw."inaturalist_observations" ORDER BY id'
    ).fetchall()


def test_real_yaml_registers_through_existing_dag_factory_with_safe_settings():
    config = yaml.safe_load(SOURCE_CONFIG.read_text(encoding="utf-8"))
    module = _load_real_dag_factory()
    dag = getattr(module, "extract__inaturalist_observations")

    assert config["name"] == SOURCE
    assert config["script"] == SCRIPT.name
    assert config["enabled"] is False
    assert config["schedule"] == "17 6 * * *"
    assert config["args"] == [
        "--taxon-id",
        "3",
        "--per-page",
        "100",
        "--max-pages",
        "2",
        "--timeout",
        "30",
        "--retries",
        "2",
        "--retry-backoff",
        "1",
    ]
    assert dag.dag_id == "extract__inaturalist_observations"
    assert dag.kwargs["schedule"] == config["schedule"]
    assert dag.kwargs["is_paused_upon_creation"] is True
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["default_args"]["retries"] == 1
    assert len(dag.tasks) == 1
    assert dag.tasks[0].kwargs["op_kwargs"]["cfg"] == config
    assert dag.tasks[0].kwargs["execution_timeout"].total_seconds() == 600


def test_source_documents_snapshot_rate_licensing_overlap_and_deferred_activation():
    text = SOURCE_CONFIG.read_text(encoding="utf-8")
    for phrase in (
        "bounded newest-first snapshot",
        "staggered daily",
        "Retry-After",
        "licences vary by record",
        "private locations are never requested",
        "Media URLs/files are not collected",
        "overlap GBIF",
        "--live integration gate",
    ):
        assert phrase in text
    assert re.search(r"(?m)^enabled: false$", text)


def test_fixture_records_land_through_actual_loader_and_read_back(tmp_path):
    records = [fixture_record(101), fixture_record(102)]
    service, result = _land_records(tmp_path, records)
    try:
        assert result["status"] == "ok", result
        assert result["files_loaded"] == 1
        assert result["rows_loaded"] == 2
        _validate_landed_records(service, records)
        rows = _query_populated_metadata(service)
        assert [(row[0], row[1]) for row in rows] == [("101", SOURCE), ("102", SOURCE)]
        assert all(row[2:8] == (47158, "Corvus brachyrhynchos", "obscured", True, "research", "cc-by-nc") for row in rows)
        assert rows[0][8].endswith("/101")
        observer = json.loads(rows[0][9]) if isinstance(rows[0][9], str) else rows[0][9]
        photos = json.loads(rows[0][10]) if isinstance(rows[0][10], str) else rows[0][10]
        assert observer["login"] == "observer"
        assert photos[0]["license_code"] == "cc-by"
        ledger = service.destination.con.execute(
            'SELECT source, rows_loaded, status FROM _load."files"'
        ).fetchall()
        assert ledger == [(SOURCE, 2, "loaded")]
    finally:
        service.destination.close()


def test_nullable_optional_metadata_lands_without_inferred_columns(tmp_path):
    record = fixture_record(101)
    nullable_fields = (
        "observed_on",
        "time_observed_at",
        "taxon_id",
        "taxon_name",
        "taxon",
        "latitude",
        "longitude",
        "geoprivacy",
        "taxon_geoprivacy",
        "quality_grade",
        "uri",
        "license_code",
        "observer",
        "photos",
    )
    record.update(dict.fromkeys(nullable_fields))
    service, result = _land_records(tmp_path, [record])
    try:
        assert result["status"] == "ok", result
        assert result["files_loaded"] == 1
        assert result["rows_loaded"] == 1
        columns = service.destination.list_columns("raw", SOURCE)
        assert columns is not None
        assert set(nullable_fields).isdisjoint(columns)
        _validate_landed_records(service, [record])
    finally:
        service.destination.close()


def _bounded_stderr(stderr: str | bytes) -> str:
    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", errors="replace")
    text = stderr[:2000]
    patterns = (
        r"(?i)(authorization\s*[:=]\s*)(\S+)",
        r"(?i)((?:api[_-]?key|token|password)\s*[:=]\s*)(\S+)",
    )
    for pattern in patterns:
        text = re.sub(pattern, r"\1[REDACTED]", text)
    return text.strip()



class LiveFailure(RuntimeError):
    def __init__(self, exit_code: int, category: str, detail: str):
        super().__init__(f"{category}: {detail}")
        self.exit_code = exit_code
        self.category = category


def _validate_live_output(stdout: str) -> list[dict]:
    lines = [line for line in stdout.splitlines() if line.strip()]
    if not lines:
        raise LiveFailure(65, "empty_output", "extractor emitted no NDJSON records")
    records = []
    for line_number, line in enumerate(lines, 1):
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise LiveFailure(65, "invalid_ndjson", f"line {line_number}: {exc.msg}") from exc
        if not isinstance(record, dict):
            raise LiveFailure(65, "invalid_envelope", f"line {line_number} is not an object")
        missing = [key for key in ("source", "id", "fetched_at") if key not in record]
        if missing or record.get("source") != SOURCE:
            raise LiveFailure(
                65,
                "invalid_envelope",
                f"line {line_number} has missing/invalid envelope fields {missing}",
            )
        records.append(record)
    return records


def test_live_output_requires_nonempty_valid_source_envelope():
    with pytest.raises(LiveFailure, match="empty_output"):
        _validate_live_output("")
    with pytest.raises(LiveFailure, match="invalid_ndjson"):
        _validate_live_output("not-json\n")
    with pytest.raises(LiveFailure, match="invalid_envelope"):
        _validate_live_output(json.dumps({"source": "wrong", "id": 1, "fetched_at": "now"}))


def test_live_wrapper_preserves_child_exit_and_bounds_redacted_stderr():
    child = subprocess.CompletedProcess(
        args=[],
        returncode=23,
        stdout="",
        stderr="authorization: top-secret\n" + "x" * 3000,
    )
    with mock.patch.object(subprocess, "run", return_value=child):
        with pytest.raises(LiveFailure) as raised:
            run_live()
    assert raised.value.exit_code == 23
    assert raised.value.category == "extractor_exit_23"
    assert "top-secret" not in str(raised.value)
    assert "[REDACTED]" in str(raised.value)
    assert len(str(raised.value)) < 2100


def run_live() -> None:
    argv = [
        sys.executable,
        str(SCRIPT),
        "--taxon-id",
        "3",
        "--per-page",
        "1",
        "--max-pages",
        "1",
    ]
    try:
        child = subprocess.run(
            argv,
            cwd=SCRIPT.parent,
            text=True,
            capture_output=True,
            timeout=180,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stderr = _bounded_stderr(exc.stderr or "")
        raise LiveFailure(124, "extractor_timeout", stderr or "180-second bound exceeded") from exc
    if child.returncode:
        raise LiveFailure(
            child.returncode,
            f"extractor_exit_{child.returncode}",
            _bounded_stderr(child.stderr) or "no child stderr",
        )
    records = _validate_live_output(child.stdout)
    with tempfile.TemporaryDirectory(prefix="inaturalist-live-") as directory:
        service, result = _land_records(Path(directory), records)
        try:
            if result["status"] != "ok" or result["rows_loaded"] != len(records):
                raise LiveFailure(70, "loader_failure", json.dumps(result, default=str)[:2000])
            try:
                _validate_landed_records(service, records)
            except Exception as exc:
                raise LiveFailure(
                    70,
                    "loader_readback_failure",
                    str(exc)[:2000] or type(exc).__name__,
                ) from exc
        finally:
            service.destination.close()
    print(
        json.dumps(
            {
                "status": "ok",
                "source": SOURCE,
                "records": len(records),
                "envelope_validated": True,
                "loader_readback": True,
            },
            separators=(",", ":"),
        )
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="iNaturalist source integration checks")
    parser.add_argument(
        "--live",
        action="store_true",
        help="run the authorized bounded network and isolated-loader activation gate",
    )
    args = parser.parse_args(argv)
    if not args.live:
        parser.error("direct execution requires explicit --live; offline checks run via pytest")
    try:
        run_live()
    except LiveFailure as exc:
        print(f"LIVE_VALIDATION_FAILED category={exc.category}: {exc}", file=sys.stderr)
        return exc.exit_code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
