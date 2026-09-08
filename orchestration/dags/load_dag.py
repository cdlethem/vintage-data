"""The load DAG: scan the sink, load what's new, then re-decide cadences.

One task, one job, one writer. The DAG's own settings (schedule, per-run file
cap, timeouts) come from ``load/config/load.yml``, so the whole load
layer is configured in one place, the same way each extract source is.

Backfill and incremental are the same operation here: the job loads every sink
file the destination's ledger has not recorded. The first run happens to find
everything; later runs find the handful written since.

The second task is algorithmic scheduling cadence detection: with the newest
batches now in the warehouse, each managed source's newest run is compared with
the run before it and its schedule steps one rung faster (new records) or
slower (nothing new). It is downstream of the load because it judges loaded
data, and it is a separate task so a scheduling problem never reds a good load.
"""
import logging
from datetime import timedelta

import load_runner
import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

log = logging.getLogger(__name__)

try:
    cfg = load_runner.load_config()
    dag_cfg = cfg.dag

    with DAG(
        dag_id=dag_cfg.dag_id,
        schedule=dag_cfg.schedule,
        start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,          # the writer is serial; queueing runs helps nobody
        tags=["load"],
        default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
        doc_md=f"""
Loads new files from `{cfg.source_root}` into
`{cfg.destination.get('name')}` → schema `{cfg.raw_schema}`, insert-only.

* work is handed to the single writer service (`extract-loader`) through
  `{cfg.queue_dir}`; this task submits and waits
* already-loaded files are skipped via the ledger `{cfg.meta_schema}.files`
* up to **{dag_cfg.max_files_per_run}** files per run; leftovers roll into the next run

Then `cadence` re-decides the schedule of every source with `cadence.auto` in its
yml, comparing its newest run with the run before it and publishing the result to
`{cfg.cadence.plan_path}` (read by the extract DAG factory).
Decisions are logged in `{cfg.meta_schema}.cadence_decisions`.
""",
    ) as dag:
        load = PythonOperator(
            task_id="load",
            python_callable=load_runner.run,
            op_kwargs={"max_files": dag_cfg.max_files_per_run},
            execution_timeout=timedelta(minutes=dag_cfg.timeout_minutes),
        )
        cadence = PythonOperator(
            task_id="cadence",
            python_callable=load_runner.run_cadence,
            execution_timeout=timedelta(minutes=10),
            # A cadence decision is worth exactly one attempt: the next load
            # pass re-evaluates from scratch anyway.
            retries=0,
        )
        load >> cadence
except Exception:
    log.exception("load DAG not created")
