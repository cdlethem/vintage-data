"""Airflow DAGs for the bots. In your DAGs folder:

    from airflow_bots.dags import build
    globals().update(build())          # reads BOTS_CONFIG

Creates ``bots_heal`` (sweep failures, answer `/bot` requests; one mapped task
per agent run) and one ``bots_job__<name>`` DAG per scheduled job. Parsing only
reads the YAML file; nothing touches the network until a task runs.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from . import config


def build(path: str | os.PathLike | None = None) -> dict:
    from airflow.sdk import DAG, get_current_context, task
    from airflow.sdk.exceptions import AirflowSkipException

    cfg = config.load(path)
    config_path = str(cfg.path)
    start = datetime(2026, 1, 1)
    dags = {}

    def env():
        from .workflows import Env
        return Env.create(config.load(config_path))

    if cfg.heal.enabled:
        timeout = timedelta(minutes=cfg.agent(cfg.heal.agent).timeout_minutes + 15)
        with DAG("bots_heal", schedule=cfg.heal.schedule, start_date=start, catchup=False, max_active_runs=1,
                 tags=["bots"], doc_md="Diagnose failing tasks and answer `/bot` requests. See airflow-bots README.",
                 default_args={"retries": 0}) as dag:

            @task(task_id="plan")
            def plan_task() -> list[dict]:
                from .workflows import plan
                return plan(env())

            @task(task_id="work", max_active_tis_per_dagrun=cfg.limits.concurrency, execution_timeout=timeout,
                  map_index_template="{{ bots_label }}")
            def work_task(item: dict) -> dict:
                from .workflows import OverBudget, work
                label = f"{item['dag_id']}.{item['task_id']}" if item["kind"] == "failure" else f"#{item['number']}"
                get_current_context()["bots_label"] = label
                try:
                    return work(env(), item)
                except OverBudget as exc:
                    raise AirflowSkipException(str(exc)) from None

            work_task.expand(item=plan_task())
        dags[dag.dag_id] = dag

    for name, job in cfg.jobs.items():
        timeout = timedelta(minutes=cfg.agent(job.agent).timeout_minutes + 15)
        with DAG(f"bots_job__{name}", schedule=job.schedule, start_date=start, catchup=False, max_active_runs=1,
                 tags=["bots"], is_paused_upon_creation=not job.enabled, default_args={"retries": 0},
                 doc_md=f"Scheduled bot job `{name}`; prompt: `{job.prompt}`.") as dag:

            @task(task_id="run", execution_timeout=timeout)
            def run_task(job_name: str = name) -> dict:
                from .workflows import OverBudget, run_job
                try:
                    return run_job(env(), job_name)
                except OverBudget as exc:
                    raise AirflowSkipException(str(exc)) from None

            run_task()
        dags[dag.dag_id] = dag
    return dags
