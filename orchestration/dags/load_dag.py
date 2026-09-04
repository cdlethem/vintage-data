"""The load DAG: scan the sink, load what's new into the warehouse RAW schema.

One task, one job, one writer. The DAG's own settings (schedule, per-run file
cap, timeouts) come from ``load/config/load.yml``, so the whole load
layer is configured in one place, the same way each extract source is.

Backfill and incremental are the same operation here: the job loads every sink
file the destination's ledger has not recorded. The first run happens to find
everything; later runs find the handful written since.
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
""",
    ) as dag:
        PythonOperator(
            task_id="load",
            python_callable=load_runner.run,
            op_kwargs={"max_files": dag_cfg.max_files_per_run},
            execution_timeout=timedelta(minutes=dag_cfg.timeout_minutes),
        )
except Exception:
    log.exception("load DAG not created")
