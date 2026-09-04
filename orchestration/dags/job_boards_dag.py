"""Job-board DAG: one independent Airflow task per ATS provider config.

Each ``job_boards_<provider>.yml`` is a first-class source config, sink
partition, and monitoring baseline. This dedicated factory composes those
otherwise independent sources into one DAG TaskGroup so providers run in
parallel and retain separate retry, timeout, manifest, and failure state.
"""
import logging
from datetime import timedelta
from pathlib import Path

import extract_runner
import pendulum
import yaml
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, TaskGroup

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = REPO_ROOT / "pipelines" / "extract" / "sources"
REQUIRED = ("name", "provider", "script", "schedule")

# Each config is validated in its own try/except, like extract_dags.py: a bad
# yml costs that one provider, never all ~11 of them. A module-level raise here
# is an Airflow import error, which removes the whole DAG.
configs = []
for path in sorted(SOURCES_DIR.glob("job_boards_*.yml")):
    try:
        cfg = yaml.safe_load(path.read_text())
        if cfg.get("dag_factory") != "job_boards":
            continue
        missing = [key for key in REQUIRED if not cfg.get(key)]
        if missing:
            raise ValueError(f"missing required keys {missing}")
        if not (extract_runner.SCRIPTS_DIR / cfg["script"]).is_file():
            raise ValueError(f"script {cfg['script']} not found")
        configs.append(cfg)
    except Exception:
        log.exception("skipping job-board config %s", path.name)

# Cross-config invariants: drop the offender rather than the DAG. Duplicates are
# resolved first-wins so the surviving providers keep running.
seen_providers, seen_names, unique = set(), set(), []
for cfg in configs:
    if cfg["provider"] in seen_providers or cfg["name"] in seen_names:
        log.error("duplicate job-board provider/name %r/%r; skipping it",
                  cfg["provider"], cfg["name"])
        continue
    seen_providers.add(cfg["provider"])
    seen_names.add(cfg["name"])
    unique.append(cfg)
configs = unique

schedules = {cfg["schedule"] for cfg in configs}
if len(schedules) > 1:
    # A TaskGroup has one schedule by construction; the most common one wins and
    # the outliers are named, rather than every provider going dark.
    from collections import Counter
    winner = Counter(cfg["schedule"] for cfg in configs).most_common(1)[0][0]
    log.error("job-board schedules disagree %s; using %r and skipping the rest",
              schedules, winner)
    configs = [cfg for cfg in configs if cfg["schedule"] == winner]
    schedules = {winner}

if not configs:
    log.error("no usable job_boards provider configs found; DAG not created")

enabled = [cfg for cfg in configs if cfg.get("enabled", True)]

if configs:
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
