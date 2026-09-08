"""DAG factory for tag-selected dbt transform jobs."""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import transform_runner

log = logging.getLogger(__name__)

try:
    entries = transform_runner.read_job_entries()
except Exception:
    log.exception("transform job configuration could not be read")
    entries = []

duplicate_names = transform_runner.duplicate_values(entries, "name")
duplicate_schedules = transform_runner.duplicate_values(entries, "schedule")

for entry in entries:
    try:
        cfg = transform_runner.validate_job(
            entry,
            duplicate_names=duplicate_names,
            duplicate_schedules=duplicate_schedules,
        )
        name = cfg["name"]
        with DAG(
            dag_id=f"transform__{name}",
            schedule=cfg["schedule"],
            start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
            catchup=False,
            max_active_runs=1,
            is_paused_upon_creation=False,
            tags=["transform", "dbt", name],
            default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
            doc_md=(
                f"Builds dbt models selected by `+tag:{name}` after parse and "
                "manifest-policy validation. Cross-cadence writes are serialized "
                "by the shared transform lock."
            ),
        ) as dag:
            build = PythonOperator(
                task_id="build",
                python_callable=transform_runner.run,
                op_kwargs={"job": name},
                execution_timeout=timedelta(minutes=cfg["timeout_minutes"]),
            )
            publish = PythonOperator(
                task_id="publish_marts",
                python_callable=transform_runner.publish_marts,
                op_kwargs={"build_result": build.output},
                execution_timeout=timedelta(minutes=25),
            )
            build >> publish
        globals()[dag.dag_id] = dag
    except Exception:
        log.exception("skipping transform job definition %r", entry)
