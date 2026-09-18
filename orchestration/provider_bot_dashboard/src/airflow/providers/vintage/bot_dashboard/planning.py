"""Audited, read-only specialist handoffs before a blocked PR can merge."""
from sqlalchemy import select
from airflow.models.dagrun import DagRun
from .models import Event, Execution, Revision, Task, RunReport, utcnow
from . import service

BOTS = {"analytics_engineer", "data_analyst", "source_scheduling"}


def options(session, task, execution, revision):
    if (task.state not in {"blocked", "in_review"} or not execution or not revision
            or execution.stage != "reviewed" or not execution.pr_number
            or not execution.trusted_head_sha or execution.merged_at
            or execution.review_verdict != "changes_requested"
            or (execution.provider_state or {}).get("state") != "open"
            or (execution.provider_state or {}).get("head_sha") != execution.trusted_head_sha):
        return []
    requested = {e.payload.get("bot") for e in session.scalars(select(Event).where(
        Event.task_id == task.id, Event.event_type == "planning_requested"))
        if e.payload.get("execution_id") == execution.execution_id
        and e.payload.get("revision_number") == revision.revision_number
        and e.payload.get("head_sha") == execution.trusted_head_sha}
    return sorted((set(revision.follow_up_bots or []) & BOTS) - requested)


def request(session, task, bot, rationale):
    execution = session.scalar(select(Execution).where(Execution.task_id == task.id)
                               .order_by(Execution.sequence.desc()).limit(1))
    revision = session.scalar(select(Revision).where(Revision.task_id == task.id)
                              .order_by(Revision.revision_number.desc()).limit(1))
    if bot not in options(session, task, execution, revision):
        raise service.PreconditionFailed("A new, configured specialist planning handoff is not available")
    run_id = f"planning__{task.id}__{execution.sequence}__r{revision.revision_number}__{bot}"
    service._event(session, task, "planning_requested", "system", "executive", payload={
        "bot": bot, "execution_id": execution.execution_id, "head_sha": execution.trusted_head_sha,
        "revision_number": revision.revision_number, "run_id": run_id, "request": rationale,
    })
    # This is only a planning request. Parent state, execution and review are unchanged.


def pending(session, limit=100):
    result = []
    events = session.scalars(select(Event).join(Task, Task.id == Event.task_id).where(
        Event.event_type == "planning_requested", Event.actor_id == "executive",
        Event.actor_kind == "system", Task.state.not_in(("completed", "dismissed")),
        ~select(RunReport.id).where(RunReport.run_id == Event.payload["run_id"].as_string(),
                                   RunReport.bot_name == Event.payload["bot"].as_string()).exists())
        .order_by(Event.id.desc()).limit(limit)).all()
    for event in events:
        p = event.payload
        if p.get("bot") not in BOTS:
            continue
        dag_id = "bot__" + p["bot"]
        if session.scalar(select(DagRun.id).where(DagRun.dag_id == dag_id, DagRun.run_id == p["run_id"])):
            continue
        # Recheck scope/revision before dispatch; stale requests remain audit evidence.
        revision = session.scalar(select(Revision).where(Revision.task_id == event.task_id)
                                  .order_by(Revision.revision_number.desc()).limit(1))
        if not revision or revision.revision_number != p["revision_number"] or p["bot"] not in revision.follow_up_bots:
            continue
        result.append({"trigger_dag_id": dag_id, "trigger_run_id": p["run_id"],
            "conf": {"task_id": str(event.task_id), "execution_id": p["execution_id"],
                     "planning_request_id": event.id},
            "skip_when_already_exists": True, "wait_for_completion": False})
    return result


def validate(session, identity, conf, task, execution, revision, bot):
    event_id = conf.get("planning_request_id")
    event = session.get(Event, event_id) if isinstance(event_id, int) and not isinstance(event_id, bool) else None
    p = event.payload if event else {}
    expected = f"planning__{task.id}__{execution.sequence}__r{revision.revision_number}__{bot}"
    if (not event or event.task_id != task.id or event.event_type != "planning_requested"
            or event.actor_id != "executive" or event.actor_kind != "system"
            or p.get("bot") != bot or p.get("execution_id") != execution.execution_id
            or p.get("head_sha") != execution.trusted_head_sha
            or p.get("revision_number") != revision.revision_number
            or p.get("run_id") != expected or identity["run_id"] != expected
            or task.state in {"completed", "dismissed"}
            or not execution.pr_number or not execution.trusted_head_sha
            or (execution.provider_state or {}).get("state") not in {"open", "merged"}
            or (execution.provider_state or {}).get("head_sha") != execution.trusted_head_sha):
        raise service.PreconditionFailed("Planning requires its exact recorded executive request and trusted PR")
    return p["request"]


def link(session, parent_id, child, report):
    if not parent_id or child.id == parent_id:
        return
    parent = service._locked_task(session, str(parent_id))
    events = session.scalars(select(Event).where(Event.task_id == parent_id,
                                               Event.event_type == "follow_up_ticket_linked")).all()
    if any(e.payload.get("task_id") == str(child.id) for e in events):
        return
    service._event(session, parent, "follow_up_ticket_linked", "system", report.bot_name,
                   payload={"task_id": str(child.id), "title": child.title,
                            "report_id": str(report.id), "report_sha256": report.sha256})
    # New prerequisite evidence warrants a fresh decision, including while a
    # previous wait is cooling down; never change the parent's actual decision.
    parent.version += 1
    parent.updated_at = utcnow()
