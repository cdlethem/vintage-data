"""Airflow DAG factory for provider-backed typed bot runs."""
from __future__ import annotations

import logging
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.configuration import conf
from airflow.sdk.exceptions import AirflowFailException
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import get_current_context

REPO_ROOT = Path(__file__).resolve().parents[2]
BOTS_ROOT = REPO_ROOT / "bots"
if str(BOTS_ROOT) not in sys.path:
    sys.path.insert(0, str(BOTS_ROOT))

import bot_runner
import provider_dashboard

log = logging.getLogger(__name__)


def _identity(context: dict) -> dict:
    ti = context["ti"]
    return {
        "dag_id": ti.dag_id,
        "run_id": context["dag_run"].run_id,
        "task_id": ti.task_id,
        "map_index": getattr(ti, "map_index", -1),
        "try_number": getattr(ti, "try_number", 1),
    }


def _run(bot_dir: str) -> dict:
    cfg = bot_runner.load_bot(bot_dir)
    context = get_current_context()
    if cfg["name"] in {"task_executor", "pr_reviewer"}:
        result = provider_dashboard.run_admitted(context, cfg, bot_runner)
    else:
        result = bot_runner.run(cfg, identity=_identity(context))
    projection = result.xcom()
    log.info(
        "bot=%s outcome=%s retry_class=%s reason=%s report_id=%s",
        cfg["name"],
        result.outcome,
        result.retry_class,
        result.reason_code,
        projection.get("report_projection_id"),
    )
    if result.retry_class in {"capacity", "transient"}:
        raise AirflowException(result.reason_code)
    if result.outcome in {"timed_out", "failed"}:
        raise AirflowFailException(result.reason_code)
    return projection


def _has_work(bot_dir: str) -> bool:
    pending = bot_runner.work_pending(bot_runner.load_bot(bot_dir))
    log.info("downstream bot=%s work_pending=%s", bot_dir, pending)
    return pending


models_cfg = None
if provider_dashboard.enabled():
    try:
        models_cfg = bot_runner.load_models()
    except Exception:
        log.exception("bot model configuration unavailable; omitting all live bot DAGs")
if provider_dashboard.enabled() and models_cfg is not None:
    for path in bot_runner.discover(BOTS_ROOT):
        try:
            cfg = bot_runner.load_bot(path)
            bot_runner.resolve_bot_models(cfg, models_cfg)
            schedule = cfg["schedule"]
            dag = DAG(
                dag_id=f"bot__{cfg['name']}",
                schedule=None if schedule == "manual" else schedule,
                start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
                catchup=False,
                max_active_runs=1,
                is_paused_upon_creation=not cfg.get("enabled", True),
                tags=["bots", "bot-dashboard", f"bot:{cfg['name']}"],
                default_args={
                    "retries": 1,
                    "retry_delay": timedelta(minutes=5),
                },
            )
            with dag:
                run_task = PythonOperator(
                    task_id="run",
                    python_callable=_run,
                    op_kwargs={"bot_dir": cfg["dir"]},
                    execution_timeout=timedelta(minutes=cfg["timeout_minutes"] + 2),
                    pool=(
                        conf.get(
                            "bot_dashboard",
                            "executor_pool",
                            fallback="bot_dashboard_executor",
                        )
                        if cfg["name"] in {"task_executor", "pr_reviewer"}
                        else "default_pool"
                    ),
                    queue=(
                        conf.get(
                            "bot_dashboard",
                            "executor_queue",
                            fallback="bot_dashboard_executor",
                        )
                        if cfg["name"] in {"task_executor", "pr_reviewer"}
                        else "default"
                    ),
                )
                for downstream in cfg["triggers"]:
                    downstream_dir = str(BOTS_ROOT / downstream)
                    gate = ShortCircuitOperator(
                        task_id=f"has_work__{downstream}",
                        python_callable=_has_work,
                        op_kwargs={"bot_dir": downstream_dir},
                    )
                    trigger = TriggerDagRunOperator(
                        task_id=f"trigger__{downstream}",
                        trigger_dag_id=f"bot__{downstream}",
                        trigger_run_id=(
                            f"evidence__{cfg['name']}__"
                            "{{ dag_run.run_id | replace(':', '_') }}"
                        ),
                        conf={
                            "triggered_by": cfg["name"],
                            "source_run_id": "{{ dag_run.run_id }}",
                        },
                        skip_when_already_exists=True,
                        wait_for_completion=False,
                    )
                    run_task >> gate >> trigger
            globals()[dag.dag_id] = dag
        except Exception:
            log.exception("skipping invalid bot definition %s", path)
