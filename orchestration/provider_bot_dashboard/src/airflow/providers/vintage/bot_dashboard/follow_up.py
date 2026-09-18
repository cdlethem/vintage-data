"""Bind specialist follow-ups to their recorded, trusted merge identity."""
import uuid
import json
import re
from sqlalchemy import select
from airflow.models.dagrun import DagRun
from .models import Execution, Revision, Task, Event
from . import service

SUPPORTED_BOTS = {"source_scheduling", "analytics_engineer", "data_analyst"}


def context(session, identity):
    bot = identity["dag_id"].removeprefix("bot__")
    if bot not in SUPPORTED_BOTS or identity["dag_id"] != "bot__" + bot:
        raise service.DomainError("Unsupported specialist follow-up")
    run = session.scalar(select(DagRun).where(DagRun.dag_id == identity["dag_id"], DagRun.run_id == identity["run_id"]))
    conf = run.conf if run else {}
    try:
        task_id = uuid.UUID(conf.get("task_id", ""))
    except (ValueError, AttributeError):
        raise service.DomainError("Follow-up task identity is unavailable") from None
    execution = session.scalar(select(Execution).where(Execution.task_id == task_id,
                                                      Execution.execution_id == conf.get("execution_id")))
    base_run = f"follow_up__{task_id}__{execution.sequence}__{bot}" if execution else ""
    valid_run = identity["run_id"] == base_run or bool(base_run and re.fullmatch(re.escape(base_run) + r"__recheck__[0-9]{14}", identity["run_id"]))
    is_planning = identity["run_id"].startswith("planning__")
    if not is_planning and (not execution or not execution.merged_at
            or (execution.provider_state or {}).get("state") != "merged"
            or not execution.trusted_head_sha
            or (execution.provider_state or {}).get("head_sha") != execution.trusted_head_sha
            or not valid_run):
        raise service.PreconditionFailed("Follow-up requires its recorded trusted merge")
    task = session.get(Task, task_id)
    revision = session.scalar(select(Revision).where(Revision.task_id == task_id).order_by(Revision.revision_number.desc()).limit(1))
    if not task or not revision or bot not in revision.follow_up_bots:
        raise service.PreconditionFailed("Specialist follow-up is not requested")
    request = None
    if is_planning:
        from . import planning
        if not execution:
            raise service.PreconditionFailed("Planning execution is unavailable")
        request = planning.validate(session, identity, conf, task, execution, revision, bot)
    value = {"pending_count": 1, "backlog_count": 1, "gap_backlog_count": 1, "selected": {
        "work_kind": "requested_planning_follow_up" if is_planning else "merged_ticket_follow_up", "parent_task_id": str(task_id),
        "title": task.title, "state": task.state, "planned_resolution": task.planned_resolution,
        "resource_keys": revision.resource_keys, "allowed_path_globs": revision.allowed_path_globs,
        "verification_commands": revision.verification_commands, "evidence": revision.evidence,
        "execution_id": execution.execution_id, "pr_number": execution.pr_number,
        "head_sha": execution.trusted_head_sha, "merged_at": execution.merged_at.isoformat() if execution.merged_at else None,
        "review_verdict": execution.review_verdict,
    }, "instruction": "Inspect only this parent ticket's remaining requirements within your specialist role. A merge is not deployment, warehouse availability, or live validation. Report unavailable capabilities precisely; do not select unrelated backlog work or invent a new ticket merely to record a blocker."}
    if request:
        value["selected"]["planning_request"] = request
        value["instruction"] += " The executive requested a distinct planning deliverable before merge. Produce a proposal only when justified by evidence; it will be linked to the parent and await separate approval. Do not repeat the parent's existing implementation."
    value["selected"]["evidence"] = list(revision.evidence or [])
    value["evidence_omitted_count"] = 0
    while len(json.dumps(value, ensure_ascii=False).encode()) > 22 * 1024 and value["selected"]["evidence"]:
        value["selected"]["evidence"].pop()
        value["evidence_omitted_count"] += 1
    if len(json.dumps(value, ensure_ascii=False).encode()) > 22 * 1024:
        raise service.PreconditionFailed("Follow-up plan exceeds its context budget")
    return value


def record(session, report, payload):
    """Attach immutable specialist findings to the requesting ticket; no decision."""
    if report.bot_name not in SUPPORTED_BOTS or not report.run_id.startswith(("follow_up__", "planning__")):
        return
    if report.dag_id != "bot__" + report.bot_name:
        raise service.PreconditionFailed("Specialist report identity does not match its route")
    selected = context(session, {"dag_id": report.dag_id, "run_id": report.run_id})["selected"]
    task = service._locked_task(session, selected["parent_task_id"])
    parent_id = task.id if selected.get("work_kind") == "requested_planning_follow_up" else None
    # Report submission is idempotent; keep evidence unique even on reconciliation replay.
    prior = session.scalars(select(Event).where(Event.task_id == task.id, Event.event_type == "specialist_follow_up")).all()
    if any((event.payload or {}).get("report_id") == str(report.id) for event in prior):
        return parent_id
    service._event(session, task, "specialist_follow_up", "system", report.bot_name,
                   payload={"report_id": str(report.id), "report_sha256": report.sha256,
                            "execution_id": selected["execution_id"], "head_sha": selected["head_sha"],
                            "status": payload["status"], "summary": payload["summary"]})
    return parent_id
