"""DAG factory for isolated family/cadence dbt transform jobs."""
from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import transform_runner

log = logging.getLogger(__name__)

try:
    jobs = transform_runner.load_jobs()
    families = transform_runner.discover_families()
except Exception:
    log.exception("transform family discovery could not be read")
    jobs = {}
    families = []

for entry in families:
    family = entry.get("family")
    cadences = entry.get("cadences")
    if not isinstance(family, str) or not isinstance(cadences, list):
        log.error("skipping malformed transform family discovery entry %r", entry)
        continue
    for cadence in cadences:
        try:
            cfg = jobs[cadence]
            with DAG(
                dag_id=f"transform__{family}__{cadence}",
                schedule=cfg["schedule"],
                start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
                catchup=False,
                max_active_runs=1,
                is_paused_upon_creation=False,
                tags=["transform", "dbt", family, cadence],
                default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
                doc_md=(
                    f"Builds the isolated `{family}` dbt family at `{cadence}`, "
                    "streams its accepted marts into the serving database under "
                    "the same transform lock, then synchronizes Lightdash. "
                    "No export files are retained; a failed transfer rebuilds."
                ),
            ) as dag:
                build = PythonOperator(
                    task_id="build_and_publish",
                    python_callable=transform_runner.run,
                    op_kwargs={"job": cadence, "family": family},
                    # The build allowance plus the retired publication task's
                    # allowance, set here so importing Airflow and the runtime
                    # cannot disagree about whether publication is enabled.
                    execution_timeout=timedelta(minutes=cfg["timeout_minutes"] + 25),
                )
                sync = PythonOperator(
                    task_id="sync_lightdash",
                    python_callable=transform_runner.sync_lightdash,
                    op_kwargs={"build_result": build.output},
                    execution_timeout=timedelta(minutes=30),
                )
                build >> sync
            globals()[dag.dag_id] = dag
        except Exception:
            log.exception(
                "skipping transform DAG definition for family=%r cadence=%r",
                family,
                cadence,
            )
