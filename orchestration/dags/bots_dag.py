"""DAG factory: one Airflow DAG per bots/<name>/bot.yml.

Bots are recurring agent invocations (pipeline health assessment today; script
staging and schedule tuning next). They are scheduled by the same scheduler as
extract and load rather than by a second mechanism, so retries, logs, pausing
and history all work the way every other task here does.

A bot whose ``schedule`` is ``manual`` gets a DAG with no schedule: still
triggerable from the UI or `airflow dags trigger`, never automatic.
"""
import logging
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
BOTS_ROOT = REPO_ROOT / "bots"
if str(BOTS_ROOT) not in sys.path:
    sys.path.insert(0, str(BOTS_ROOT))

import bot_runner  # noqa: E402

log = logging.getLogger(__name__)


def _run(bot_dir: str) -> dict:
    """Task body: run the bot, fail the task only on a real error.

    A gate skip (`skipped`) and a busy model (`busy`) are normal outcomes and
    leave the task green — the next scheduled run retries. Broken wiring
    (unknown model alias, missing key, dead context command) raises.
    """
    result = bot_runner.run(bot_dir)
    log.info("bot %s: %s (model=%s) -> %s",
             result.bot, result.status, result.model_alias, result.report_path)
    log.info("report:\n%s", result.report)
    return {
        "bot": result.bot,
        "status": result.status,
        "model": result.model_alias,
        "report_path": result.report_path,
    }


for path in bot_runner.discover(BOTS_ROOT):
    try:
        cfg = bot_runner.load_bot(path)
        schedule = cfg["schedule"]
        dag = DAG(
            dag_id=f"bot__{cfg['name']}",
            schedule=None if schedule == "manual" else schedule,
            start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
            catchup=False,
            max_active_runs=1,
            is_paused_upon_creation=not cfg.get("enabled", True),
            tags=["bots"],
            default_args={"retries": cfg.get("retries", 0),
                          "retry_delay": timedelta(minutes=5)},
            doc_md=(
                f"**{cfg.get('description', cfg['name'])}**\n\n"
                f"Model alias: `{cfg.get('model') or 'default'}` "
                f"(resolved in `bots/models.yml`)\n\n"
                f"Reports: `bots/runs/{cfg['name']}/`"
            ),
        )
        with dag:
            PythonOperator(
                task_id="run",
                python_callable=_run,
                op_kwargs={"bot_dir": cfg["dir"]},
                execution_timeout=timedelta(minutes=cfg.get("timeout_minutes", 15)),
            )
        globals()[dag.dag_id] = dag
    except Exception:
        log.exception("skipping bot definition %s", path)
