"""DAG factory: one Airflow DAG per extract/sources/*.yml.

Adding a source means adding a yml, never a DAG file. Each yml is parsed
inside its own try/except so a malformed config skips only itself — the
error lands in the dag-processor log and every sibling DAG still loads.

A source that sets ``cadence: {auto: true}`` has its schedule taken from the
cadence plan the load layer publishes (see orchestration/include/cadence_plan.py),
falling back to the yml's own ``schedule`` whenever the plan is missing, stale
or out of the bounds the yml declares.
"""
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
import yaml
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import cadence_plan
import extract_runner

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCES_DIR = REPO_ROOT / "extract" / "sources"
REQUIRED_KEYS = ("name", "script", "schedule")

# One plan read per parse, not one per source config.
CADENCE_PLAN = cadence_plan.plan_sources()
log = logging.getLogger(__name__)

for path in sorted(SOURCES_DIR.glob("*.yml")):
    try:
        cfg = yaml.safe_load(path.read_text())
        if cfg.get("dag_factory") not in (None, "extract"):
            continue
        missing = [k for k in REQUIRED_KEYS if not cfg.get(k)]
        if missing:
            raise ValueError(f"missing required keys {missing}")
        if not (extract_runner.SCRIPTS_DIR / cfg["script"]).is_file():
            raise ValueError(f"script {cfg['script']} not found in {extract_runner.SCRIPTS_DIR}")
        schedule, cadence_note = cadence_plan.effective_schedule(cfg, CADENCE_PLAN)

        dag = DAG(
            dag_id=f"extract__{cfg['name']}",
            schedule=schedule,
            start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
            catchup=False,
            max_active_runs=1,
            is_paused_upon_creation=not cfg.get("enabled", True),
            tags=["extract"],
            default_args={
                "retries": cfg.get("retries", 1),
                "retry_delay": timedelta(minutes=5),
            },
            doc_md=(
                f"`{cfg['script']} {' '.join(map(str, cfg.get('args', [])))}`\n\n"
                f"**Schedule**: `{schedule}` — {cadence_note}\n\n"
                f"**Cadence**: {cfg.get('cadence_note', 'n/a')}\n\n"
                f"**Rate limit**: {cfg.get('rate_limit', 'n/a')}"
            ),
        )
        with dag:
            PythonOperator(
                task_id="run",
                python_callable=extract_runner.run,
                op_kwargs={"cfg": cfg},
                execution_timeout=timedelta(minutes=cfg.get("timeout_minutes", 10)),
            )
        globals()[dag.dag_id] = dag
    except Exception:
        log.exception("skipping source config %s", path.name)
