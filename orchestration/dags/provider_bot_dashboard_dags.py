"""Durable dispatcher and maintenance DAGs for the optional bot dashboard provider."""

from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.configuration import conf
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
BOTS_ROOT = REPO_ROOT / "bots"
if str(BOTS_ROOT) not in sys.path:
    sys.path.insert(0, str(BOTS_ROOT))

import provider_dashboard


def _enabled() -> bool:
    return provider_dashboard.enabled()


def _executor_available() -> bool:
    """Read authoritative readiness; SDK workers omit server-only configuration."""
    readiness = provider_dashboard.DashboardClient.from_environment().execution_readiness()
    return readiness.get("ready") is True


def _claim() -> list[dict]:
    return provider_dashboard.claim_pending(
        conf.getint("bot_dashboard", "max_queued_executions", fallback=20)
    )


def _follow_ups(**context) -> list[dict]:
    """Airflow cannot map over a custom XCom key, so republish the bounded list."""
    result = context["ti"].xcom_pull(task_ids="maintain") or {}
    return list(result.get("follow_up_bots") or [])


if _enabled():
    with DAG(
        dag_id="bot_dashboard__dispatch",
        schedule="* * * * *",
        start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=10),
        tags=["bot-dashboard"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    ) as bot_dashboard__dispatch:
        available = ShortCircuitOperator(
            task_id="executor_available", python_callable=_executor_available
        )
        claims = PythonOperator(task_id="claim", python_callable=_claim)
        triggers = TriggerDagRunOperator.partial(task_id="trigger").expand_kwargs(claims.output)
        available >> claims >> triggers

    with DAG(
        dag_id="bot_dashboard__maintenance",
        schedule="* * * * *",
        start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=10),
        tags=["bot-dashboard"],
        default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    ) as bot_dashboard__maintenance:
        maintenance = PythonOperator(
            task_id="maintain", python_callable=provider_dashboard.run_maintenance
        )
        follow_ups = PythonOperator(task_id="follow_ups", python_callable=_follow_ups)
        triggers = TriggerDagRunOperator.partial(task_id="trigger_follow_up").expand_kwargs(
            follow_ups.output
        )
        maintenance >> follow_ups >> triggers

    from airflow.providers.vintage.bot_dashboard.concurrency import scheduler_limits
    executive_concurrency = scheduler_limits().get("executive", 1)

    with DAG(
        dag_id="bot__executive",
        schedule="* * * * *",
        start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        dagrun_timeout=timedelta(minutes=6),
        max_active_tasks=executive_concurrency,
        is_paused_upon_creation=False,
        tags=["bot-dashboard", "autopilot"],
        default_args={"retries": 0},
    ) as bot__executive:
        def _executive(**context):
            from executive_runner import run
            return run(context)

        PythonOperator.partial(task_id="run", python_callable=_executive,
                       max_active_tis_per_dag=executive_concurrency,
                       execution_timeout=timedelta(minutes=4), do_xcom_push=False).expand(
                           op_kwargs=[{"decision_slot": slot} for slot in range(executive_concurrency)])

    with DAG(
        dag_id="bot_dashboard__validation",
        schedule="* * * * *",
        start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        max_active_tasks=1,
        dagrun_timeout=timedelta(minutes=6),
        is_paused_upon_creation=False,
        tags=["bot-dashboard", "validation"],
        default_args={"retries": 0},
    ) as bot_dashboard__validation:
        def _validation(**context):
            from validation_runner import run
            return run(context)

        PythonOperator(
            task_id="run", python_callable=_validation,
            queue=conf.get("bot_dashboard", "executor_queue", fallback="bot_dashboard_executor"),
            execution_timeout=timedelta(minutes=5), do_xcom_push=False,
        )
