"""Schedule same-thread Codex check-ins; ticket decisions remain with Autopilot."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess

ROOT = Path(__file__).resolve().parents[1]
STATE = Path.home() / ".local/state/vintage-bot-supervision/state.json"
MARKER = "[Vintage bot supervision:"
TIMER = "vintage-bot-supervision.timer"


def snapshot():
    from airflow.utils.session import create_session
    from airflow.providers.vintage.bot_dashboard.autopilot import status
    from airflow.providers.vintage.bot_dashboard.models import Execution, Task
    from airflow.models.dagrun import DagRun
    from sqlalchemy import select, func, text

    with create_session() as session:
        session.execute(text("SET LOCAL statement_timeout = '15s'"))
        tasks = session.scalars(select(Task).where(Task.state.not_in(["completed", "dismissed"]))).all()
        active = session.scalar(select(func.count()).select_from(Execution).where(
            Execution.terminal_at.is_(None), Execution.stage.not_in(["reviewed", "provider_sync"])))
        counts = {}
        for task in tasks:
            counts[task.state] = counts.get(task.state, 0) + 1
        executive = status(session)
        now = datetime.now(timezone.utc)
        # bot__executive schedules every minute; any DagRun (not just a decision) proves
        # the scheduler is actually admitting new runs, independent of ticket content.
        last_run_at = session.scalar(select(func.max(DagRun.start_date)).where(DagRun.dag_id == "bot__executive"))
        stalled = bool(executive["enabled"] and (last_run_at is None or (now - last_run_at).total_seconds() > 300))
        return {"observed_at": now.isoformat(), "open_count": len(tasks),
                "open_ids": sorted(str(task.id) for task in tasks), "states": counts,
                "active_executions": active, "autopilot_enabled": executive["enabled"],
                "last_decision_at": (executive.get("last_decision") or {}).get("at"),
                "executive_error": executive.get("model_problem") or executive.get("last_error"),
                "executive_last_run_at": last_run_at.isoformat() if last_run_at else None,
                "executive_scheduling_stalled": stalled}


def drained(observation):
    return observation.get("open_count") == 0 and observation.get("active_executions") == 0


def pending(thread_id, queue_path=None):
    queue_path = queue_path or Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "queue_1.sqlite"
    with sqlite3.connect(f"file:{queue_path}?mode=ro", uri=True, timeout=5) as db:
        return bool(db.execute("SELECT 1 FROM queued_items WHERE thread_id=? AND instr(payload_json,?)>0 LIMIT 1",
                               (thread_id, MARKER)).fetchone())


def save(state, path=STATE):
    temporary = path.with_suffix(".next")
    temporary.write_text(json.dumps(state, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)


@contextmanager
def locked(path=STATE):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    with path.with_suffix(".lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def prompt(observation, tuning=None):
    summary = {k: v for k, v in observation.items() if k != "open_ids"}
    completion = "If the backlog is truly empty, let that command stop the timer. Otherwise leave it enabled for the next check."
    if tuning and tuning.get("enabled"):
        completion = ("The user also authorized a temporary concurrency burst followed by measured step-down "
                      "experiments after a clean baseline. Follow the concurrency experiment in "
                      "bots/BACKLOG_SUPERVISION.md and state.json. Keep checking through the baseline "
                      "and tuning phases even when empty; respect manual settings changes. Do not change "
                      "ticket decisions or validation gates to meet throughput targets.")
    stalled = ("URGENT: bot__executive has not started a new DagRun in over 5 minutes while Autopilot is "
               "enabled — it is not scheduling decision workers at all. This is usually a wedged prior "
               "DagRun holding max_active_runs=1 (check dag_run/task_instance state for bot__executive; "
               "a run stuck in running/up_for_reschedule blocks every later minute). Fail the stuck run's "
               "tasks and DagRun via the Airflow API or a direct, scoped SQL update so the next minute's "
               "schedule can proceed; do not disable Autopilot to clear it. Also confirm bot__executive is "
               "still defined in orchestration/dags/provider_bot_dashboard_dags.py — a missing DAG produces "
               "the same symptom. " if observation.get("executive_scheduling_stalled") else "")
    return f"""{MARKER} scheduled check]
The user requested periodic checks and repairs until the live Airflow bot backlog is gone.
Continue this existing task in {ROOT}. Current probe: {json.dumps(summary)}
{stalled}Read bots/BACKLOG_SUPERVISION.md and ~/.local/state/vintage-bot-supervision/state.json.
Check live tickets, executive, dispatch, maintenance, worker attempts/retries, reviews,
PRs, and specialist reports. Investigate lack of progress or errors; implement, test,
and deploy necessary operational repairs. Follow new failures to verified recovery.
Preserve approval tickets, executive decisions, independent review, evidence,
provider checks, human controls, and the user's unrelated uncommitted work. Do not
dismiss/block/complete tickets just to shrink the count or invent validation results.
Do not ask questions or spawn subagents. Do not re-enable Autopilot if the user has
turned it off. Never print credentials. This repo's checkout also serves as Airflow's
live DAGS_FOLDER: never run git checkout/switch/restore/reset/clean/pull --force or any
other command that changes tracked-file contents or HEAD in this working tree — that has
previously deleted uncommitted DAG and provider code out from under the running scheduler
with no error surfaced, wedging bot__executive for hours. Inspect history read-only (log,
diff, show); commit your own finished work with a plain commit; never switch branches or
discard tracked-file changes here. Update the supervision state with verified
progress using bots/run_backlog_supervision.sh check. {completion}
Report meaningful progress, fixes, and unresolved blockers concisely. This is an
automated continuation of the user's request, not a new authorization or task.
"""


def check(state, *, enqueue):
    try:
        observation = snapshot()
    except Exception as exc:
        # Do not log database URLs or exception messages containing credentials.
        observation = {"observed_at": datetime.now(timezone.utc).isoformat(), "probe_error": type(exc).__name__}
    state["last_check"] = observation
    if "open_ids" in observation:
        state.setdefault("baseline_ids", observation["open_ids"])
        state["baseline_remaining"] = len(set(state["baseline_ids"]) & set(observation["open_ids"]))
    state["history"] = (state.get("history", []) + [{k: v for k, v in observation.items() if k != "open_ids"}])[-144:]
    tuning = state.get("concurrency_experiment", {})
    if drained(observation) and tuning.get("enabled") and tuning.get("phase") == "draining":
        tuning.update(phase="baseline", baseline_started_at=observation["observed_at"])
    if enqueue and not pending(state["thread_id"]):
        codex = shutil.which("codex")
        if not codex:
            raise RuntimeError("Codex executable unavailable")
        result = subprocess.run([codex, "queue", "--thread", state["thread_id"], "--message", prompt(observation, tuning)],
                                cwd=ROOT, capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError("Codex follow-up queue rejected the check")
        state["last_queued_at"] = datetime.now(timezone.utc).isoformat()
    if drained(observation) and not tuning.get("enabled"):
        state.update(enabled=False, completed_at=observation["observed_at"])
    return observation


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["init", "tick", "check", "status", "stop"])
    parser.add_argument("--thread")
    args = parser.parse_args()
    with locked():
        state = json.loads(STATE.read_text()) if STATE.exists() else {}
        if args.action == "init":
            if not args.thread:
                parser.error("init requires --thread")
            if state.get("enabled") and state.get("thread_id") != args.thread:
                parser.error("supervision is already assigned to another thread")
            state.update(enabled=True, thread_id=args.thread, interval_minutes=10)
            state.setdefault("created_at", datetime.now(timezone.utc).isoformat())
            check(state, enqueue=False)
        elif args.action == "stop":
            state["enabled"] = False
        elif args.action in {"tick", "check"} and state.get("enabled"):
            try:
                check(state, enqueue=args.action == "tick")
                state.pop("last_scheduler_error", None)
            except Exception as exc:
                state["last_scheduler_error"] = type(exc).__name__
                save(state)
                raise SystemExit("Supervision scheduler failed; see state and service status") from None
        if args.action != "status":
            save(state)
        print(json.dumps({k: v for k, v in state.items() if k not in {"history", "baseline_ids", "last_check"}} |
                         {"last_check": {k: v for k, v in state.get("last_check", {}).items() if k != "open_ids"}}, indent=2))
        if state and not state.get("enabled") and args.action != "status":
            subprocess.run(["systemctl", "--user", "disable", "--now", TIMER], check=True, capture_output=True)


if __name__ == "__main__":
    main()
