"""Job-board DAG: one independent Airflow task per ATS provider config.

Each ``job_boards_<provider>.yml`` is a first-class source config, sink
partition, and monitoring baseline. This dedicated factory composes those
otherwise independent sources into one DAG TaskGroup so providers run in
parallel and retain separate retry, timeout, manifest, and failure state.
"""
from datetime import timedelta
from pathlib import Path

import pendulum
import yaml
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

import extract_runner

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = REPO_ROOT / "pipelines" / "extract" / "sources"
REQUIRED = ("name", "provider", "script", "schedule")

configs = []
for path in sorted(SOURCES_DIR.glob("job_boards_*.yml")):
    cfg = yaml.safe_load(path.read_text())
    if cfg.get("dag_factory") != "job_boards":
        continue
    missing = [key for key in REQUIRED if not cfg.get(key)]
    if missing:
        raise ValueError(f"{path.name}: missing required keys {missing}")
    if not (extract_runner.SCRIPTS_DIR / cfg["script"]).is_file():
        raise ValueError(f"{path.name}: script {cfg['script']} not found")
    configs.append(cfg)

if not configs:
    raise ValueError("no job_boards provider configs found")
providers = [cfg["provider"] for cfg in configs]
names = [cfg["name"] for cfg in configs]
if len(providers) != len(set(providers)):
    raise ValueError(f"duplicate job-board providers: {providers}")
if len(names) != len(set(names)):
    raise ValueError(f"duplicate job-board source names: {names}")
schedules = {cfg["schedule"] for cfg in configs}
if len(schedules) != 1:
    raise ValueError(f"job-board provider schedules must match: {schedules}")

enabled = [cfg for cfg in configs if cfg.get("enabled", True)]
with DAG(
    dag_id="extract__job_boards",
    schedule=schedules.pop(),
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=not enabled,
    tags=["extract", "jobs"],
    doc_md=(
        "Public company ATS boards. Each provider is an independent source task "
        "under `providers`, with its own raw partition and monitoring baseline."
    ),
) as dag:
    with TaskGroup(group_id="providers", tooltip="Independent public ATS providers"):
        for provider_cfg in enabled:
            PythonOperator(
                task_id=provider_cfg["provider"],
                python_callable=extract_runner.run,
                op_kwargs={"cfg": provider_cfg},
                retries=provider_cfg.get("retries", 1),
                retry_delay=timedelta(minutes=5),
                execution_timeout=timedelta(
                    minutes=provider_cfg.get("timeout_minutes", 60)
                ),
            )
