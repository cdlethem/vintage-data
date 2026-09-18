"""Executive decisions use the same ticket services as people, under a revocable lease.

The model gets evidence and a small action vocabulary, never tools or credentials.
Each ticket has at most one decision lease. Turning off invalidates outstanding
leases; changed tickets or execution evidence require a new model decision.
"""
from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from datetime import timedelta
from typing import Literal

import httpx
from pydantic import Field, model_validator
from sqlalchemy import case, func, or_, select
from sqlalchemy.orm import Session
from airflow.configuration import conf
from airflow.models.variable import Variable

from .api_models import InternalIdentity, PatchTask, StrictBody
from .models import Event, Execution, Policy, Revision, RunReport, Task, utcnow
from .model_settings import model_for_role
from . import service

KEY = "bot_dashboard_autopilot"
ACTOR = {"actor_id": "executive", "actor_kind": "system"}


class Toggle(StrictBody):
    enabled: bool
    version: int = Field(ge=0)


class FailedDecision(StrictBody):
    reason_code: Literal["executive_decision_failed", "decision_context_changed", "model_rate_limited"] = "executive_decision_failed"
    lease_id: str = Field(pattern=r"^[a-f0-9]{64}$")


class ClaimRequest(StrictBody):
    identity: InternalIdentity | None = None


class Decision(StrictBody):
    lease_id: str = Field(pattern=r"^[a-f0-9]{64}$")
    action: Literal["accept", "assign", "configure", "start", "revise", "ready", "merge", "complete", "block", "dismiss", "restore", "wait", "request_follow_up"]
    rationale: str = Field(min_length=10, max_length=8000)
    profile: Literal["junior", "senior", "staff"] | None = None
    changes: PatchTask | None = None
    specialist: Literal["analytics_engineer", "data_analyst", "source_scheduling"] | None = None

    @model_validator(mode="after")
    def coherent(self):
        if (self.action == "assign") != (self.profile is not None):
            raise ValueError("only assignment requires a profile")
        if (self.action == "configure") != (self.changes is not None):
            raise ValueError("only configure requires changes")
        if (self.action == "request_follow_up") != (self.specialist is not None):
            raise ValueError("only request_follow_up requires a specialist")
        if not self.rationale.strip():
            raise ValueError("a decision requires reasoning")
        return self


def _control(session: Session, *, lock=False):
    # The existing singleton policy also serializes first-time creation.
    if lock:
        session.scalar(select(Policy).where(Policy.category == "*").with_for_update())
    stmt = select(Variable).where(Variable.key == KEY)
    row = session.scalar(stmt.with_for_update() if lock else stmt)
    return row, json.loads(row.val) if row else {"enabled": False, "version": 0}


def _save(session, row, state):
    if row is None:
        row = Variable(key=KEY)
        session.add(row)
    row.val = json.dumps(state, default=str)
    session.flush()


def _model(session):
    """Any model mapped to the executive role is accepted; the mapping is the control."""
    return model_for_role(session, "executive")


def _leases(state):
    # Adopt an in-flight pre-concurrency lease without revoking its decision.
    leases = state.setdefault("leases", [])
    legacy = state.pop("lease", None)
    if legacy and not any(item["id"] == legacy["id"] for item in leases):
        leases.append(legacy)
    return leases


def _concurrency(session):
    from .concurrency import _read
    return _read(session)[1]["limits"].get("executive", 1)


def status(session: Session) -> dict:
    _, state = _control(session)
    try:
        model = _model(session)
        problem = None
    except service.PreconditionFailed as exc:
        model, problem = None, str(exc)
    active = [lease for lease in _leases(state) if lease["expires_at"] > utcnow().isoformat()]
    return {key: state.get(key) for key in ("enabled", "version", "updated_at", "updated_by", "last_checked_at", "last_decision", "last_error")} | {
        "model": model["model"] if model else None,
        "model_problem": problem,
        "deciding": bool(active) and bool(state.get("enabled")),
        "task_id": active[0]["task_id"] if active else None,
        "task_ids": [lease["task_id"] for lease in active],
        "active_decisions": len(active),
        "concurrency": _concurrency(session),
    }


def set_enabled(session: Session, body: Toggle, actor_id: str) -> dict:
    row, state = _control(session, lock=True)
    if body.version != state["version"]:
        raise service.Conflict("Autopilot settings changed; refresh before trying again")
    if body.enabled:
        _model(session)
    state.update(enabled=body.enabled, version=state["version"] + 1,
                 updated_at=utcnow().isoformat(), updated_by=actor_id, lease=None, leases=[])
    # Preserve a bounded toggle audit in addition to Airflow action logging.
    state["history"] = (state.get("history", []) + [{"enabled": body.enabled, "actor_id": actor_id, "at": state["updated_at"]}])[-100:]
    _save(session, row, state)
    return status(session)


def failed(session: Session, body: FailedDecision) -> dict:
    row, state = _control(session, lock=True)
    lease = next((item for item in _leases(state) if item["id"] == body.lease_id), None)
    if not lease:
        return {"status": "unchanged"}
    state.update(leases=[item for item in _leases(state) if item["id"] != body.lease_id], last_error="The executive could not complete its decision. The executive will retry; the ticket remains available for manual action.")
    if body.reason_code == "model_rate_limited":
        state["last_error"] = "The executive connection is rate limited. Select another supported connection in Models & connections or wait for provider capacity; approval decisions remain pending."
    elif body.reason_code == "decision_context_changed":
        state["last_error"] = "The decision context changed before it could be applied. The executive will reassess the current ticket shortly."
    delay = 1 if body.reason_code == "decision_context_changed" else 15
    task = service._locked_task(session, lease["task_id"])
    service._event(session, task, "executive_error", "system", "executive", payload={
        "reason": state["last_error"], "result_version": task.version,
        "revisit_at": (utcnow() + timedelta(minutes=delay)).isoformat(), "lease_id": body.lease_id,
        "reason_code": body.reason_code})
    _save(session, row, state)
    return {"status": "recorded"}


def _snapshot(session: Session, task_id: str) -> dict:
    detail = service.get_task(session, task_id)
    # Bound model context without changing the authoritative history.
    detail["revisions"] = detail["revisions"][-2:]
    detail["events"] = detail["events"][-20:]
    detail["executions"] = detail["executions"][-3:]
    rows = session.scalars(select(Execution).where(Execution.task_id == uuid.UUID(task_id)).order_by(Execution.sequence.desc()).limit(1)).all()
    if rows:
        execution = rows[0]
        from . import planning
        revision = session.scalar(select(Revision).where(Revision.task_id == uuid.UUID(task_id))
                                  .order_by(Revision.revision_number.desc()).limit(1))
        detail["follow_up_options"] = planning.options(session, session.get(Task, uuid.UUID(task_id)), execution, revision)
        hashes = [execution.executor_report_sha256, execution.review_report_sha256]
        suffix = f"{task_id}__{execution.sequence}__r{execution.revision}"
        reports = session.scalars(select(RunReport).where(or_(
            RunReport.sha256.in_([h for h in hashes if h]),
            RunReport.run_id.in_(["task__" + suffix, "review__" + suffix]),
        )).order_by(RunReport.created_at.desc()).limit(4)).all()
        detail["reports"] = [{"bot": report.bot_name, "outcome": report.outcome, "reason_code": report.reason_code,
                              "failure_detail": report.failure_detail, "payload": report.body_json} for report in reports]
    return detail


def _digest(detail):
    # Sync timestamps/duration counters can move without changing evidence.
    executions = [{k: v for k, v in e.items() if k not in {"synced_at", "lifecycle_durations_ms"}} for e in detail["executions"]]
    evidence = [detail["version"], executions, detail["events"], detail.get("reports"), detail.get("follow_up_options")]
    return hashlib.sha256(json.dumps(evidence, sort_keys=True, default=str).encode()).hexdigest()


def _actions(detail):
    state = detail["state"]
    latest = detail["executions"][-1] if detail["executions"] else None
    if latest and not latest["terminal_at"] and latest["stage"] not in {"reviewed", "provider_sync"}:
        return []  # An admitted executor/reviewer owns this decision point.
    actions = ["wait"]
    if state == "proposed": actions += ["accept", "dismiss", "block"]
    if state == "accepted": actions += ["assign", "configure", "start", "dismiss", "block"]
    if state == "blocked": actions += ["restore", "configure", "start", "dismiss"]
    if state == "in_progress": actions += ["ready", "block"]
    if state == "in_review": actions += ["configure", "revise", "block"]
    if state in {"in_review", "ready"} and latest and latest["pr_number"]:
        if latest["review_verdict"] == "approved" or not latest["reviewer_required"]:
            actions += ["ready"] if state == "in_review" else ["merge", "complete"]
    if state == "ready" and (not latest or not latest["pr_number"]): actions += ["complete", "block"]
    if detail.get("follow_up_options"):
        actions += ["request_follow_up"]
    return actions


def _owner_failed(session: Session, identity: dict | None) -> bool:
    if not identity:
        return False
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    # A mapped decision can fail while sibling decisions keep the DAG running.
    owner_state = session.scalar(select(TaskInstance.state).where(
        TaskInstance.dag_id == identity["dag_id"], TaskInstance.run_id == identity["run_id"],
        TaskInstance.task_id == identity["task_id"], TaskInstance.map_index == identity.get("map_index", -1),
    ))
    if owner_state in {"failed", "upstream_failed", "removed"}:
        return True
    state = session.scalar(select(DagRun.state).where(DagRun.dag_id == identity["dag_id"], DagRun.run_id == identity["run_id"]))
    if state != "failed":
        return False
    live = session.scalar(select(TaskInstance.task_id).where(
        TaskInstance.dag_id == identity["dag_id"], TaskInstance.run_id == identity["run_id"],
        or_(TaskInstance.state.is_(None), TaskInstance.state.not_in(["success", "failed", "skipped", "upstream_failed", "removed"])),
    ).limit(1))
    return live is None


def _claimed_response(lease, detail):
    return {"status": "claimed", "lease_id": lease["id"], "expires_at": lease["expires_at"],
            "model": lease["model"], "task": detail, "actions": lease["actions"]}


def claim(session: Session, identity: dict | None = None) -> dict:
    if identity and identity["dag_id"] != "bot__executive":
        raise service.DomainError("Executive claims require the executive DAG identity")
    row, state = _control(session, lock=True)
    if not state.get("enabled"):
        return {"status": "off"}
    try:
        model = _model(session)
    except service.PreconditionFailed:
        # A saved human configuration can become unavailable while enabled.
        # Keep the mapping/toggle and approval guard; status exposes the problem.
        state["last_checked_at"] = utcnow().isoformat()
        _save(session, row, state)
        return {"status": "model_unavailable"}
    now = utcnow()
    state["leases"] = [item for item in _leases(state) if item["expires_at"] > now.isoformat()]
    for lease in list(state["leases"]):
        if _owner_failed(session, lease.get("owner")):
            failed(session, FailedDecision(lease_id=lease["id"]))
            row, state = _control(session, lock=True)
            state["leases"] = [item for item in _leases(state) if item["expires_at"] > now.isoformat()]
        elif identity and lease.get("owner") == identity:
            detail = _snapshot(session, lease["task_id"])
            if _digest(detail) != lease["digest"]:
                raise service.Conflict("Ticket evidence changed before the executive could resume")
            return _claimed_response(lease, detail)
    if len(_leases(state)) >= _concurrency(session):
        _save(session, row, state)
        return {"status": "busy"}
    leased_tasks = {item["task_id"] for item in _leases(state)}
    last = select(Event.task_id, func.max(Event.created_at).label("at")).where(Event.event_type.in_(["executive_decision", "executive_error"])).group_by(Event.task_id).subquery()
    active_worker = select(Execution.id).where(Execution.task_id == Task.id, Execution.terminal_at.is_(None),
        Execution.stage.not_in(["reviewed", "provider_sync"])).exists()
    aging = [last.c.at.asc().nullsfirst(), Task.priority, Task.created_at]
    # Four cycles favor finishing work already underway. Every fifth cycle uses
    # pure age ordering so new recommendations and blockers cannot starve.
    latest_verdict = select(Execution.review_verdict).where(Execution.task_id == Task.id).order_by(Execution.sequence.desc()).limit(1).scalar_subquery()
    executed_revision = select(Execution.revision).where(Execution.task_id == Task.id).order_by(Execution.sequence.desc()).limit(1).scalar_subquery()
    planned_revision = select(func.max(Revision.revision_number)).where(Revision.task_id == Task.id).scalar_subquery()
    latest_action = select(Event.payload["action"].as_string()).where(
        Event.task_id == Task.id, Event.event_type == "executive_decision",
    ).order_by(Event.sequence.desc()).limit(1).scalar_subquery()
    latest_result = select(Event.payload["result"].as_string()).where(
        Event.task_id == Task.id, Event.event_type == "executive_decision",
    ).order_by(Event.sequence.desc()).limit(1).scalar_subquery()
    # An assigned ticket whose plan was just refined should return for an explicit
    # start decision before another full pass over unprepared accepted tickets.
    # This changes selection only; Astra and the normal admission gates still decide.
    prepared = ((Task.state == "accepted") & (Task.assignee_kind == "bot")
                & Task.assignee_profile.in_(["junior", "senior", "staff"])
                & (latest_action == "configure") & (latest_result == "applied"))
    phase = case((Task.state == "ready", 0),
                 ((Task.state == "in_review") & (latest_verdict == "approved"), 1),
                 ((Task.state == "in_review") & (planned_revision > executed_revision), 2),
                 (Task.state == "in_review", 3), (prepared, 4), (Task.state == "accepted", 5),
                 (Task.state == "in_progress", 6), (Task.state == "proposed", 7), else_=8)
    ordering = aging if now.minute % 5 == 0 else [phase, *aging]
    candidates = session.scalars(select(Task).outerjoin(last, Task.id == last.c.task_id)
        .where(Task.state.in_(["proposed", "accepted", "blocked", "in_progress", "in_review", "ready"]), ~active_worker)
        .order_by(*ordering).limit(100)).all()
    state["last_checked_at"] = now.isoformat()
    for task in candidates:
        if str(task.id) in leased_tasks:
            continue
        detail = _snapshot(session, str(task.id))
        actions = _actions(detail)
        if len(actions) < 2:
            continue
        latest = session.scalar(select(Event).where(Event.task_id == task.id, Event.event_type.in_(["executive_decision", "executive_error"])).order_by(Event.sequence.desc()).limit(1))
        if latest:
            retry = latest.payload.get("revisit_at")
            if retry and retry > now.isoformat() and latest.payload.get("result_version") == task.version:
                continue
        lease = {"id": secrets.token_hex(32), "task_id": str(task.id), "version": task.version,
                 "digest": _digest(detail), "model": model, "actions": actions,
                 "owner": identity,
                 "expires_at": (now + timedelta(minutes=5)).isoformat()}
        _leases(state).append(lease)
        _save(session, row, state)
        return _claimed_response(lease, detail)
    _save(session, row, state)
    return {"status": "idle"}


def _latest_execution(session, task):
    return session.scalar(select(Execution).where(Execution.task_id == task.id).order_by(Execution.sequence.desc()).limit(1).with_for_update())


def _trusted_change(provider, execution):
    if not execution or not execution.pr_number or not execution.trusted_head_sha:
        raise service.PreconditionFailed("A published, trusted PR is required")
    if provider.config.project != execution.repository or provider.config.provider != execution.provider:
        raise service.PreconditionFailed("Repository configuration changed")
    value = provider.read_change(execution.pr_number)
    expected = {"provider": execution.provider, "number": execution.pr_number, "head_sha": execution.trusted_head_sha,
                "head_ref": execution.branch, "base_ref": execution.target_branch, "author_id": execution.service_account_id}
    if any(value.get(k) != v for k, v in expected.items()) or value.get("state") not in {"open", "merged"}:
        raise service.PreconditionFailed("PR identity or reviewed head changed; a new review is required")
    if execution.reviewer_required and execution.review_verdict != "approved":
        raise service.PreconditionFailed("Independent reviewer approval is required")
    return value


def _perform(session, task, decision, lease):
    from .execution import require_executor_preconditions
    from .git_provider import get_provider
    action = decision.action
    kwargs = {"task_id": str(task.id), "version": task.version, **ACTOR}
    if action == "wait": return
    if action == "assign":
        service.assign_task(session, **kwargs, actor_name="Executive", kind="bot", profile=decision.profile, reviewer_required=True)
    elif action == "request_follow_up":
        from . import planning
        planning.request(session, task, decision.specialist, decision.rationale)
    elif action == "configure":
        changes = decision.changes.model_dump(exclude_none=True, exclude={"version"})
        if decision.changes.version != task.version:
            raise service.Conflict("Execution settings version changed")
        if not changes or set(changes) - {"planned_resolution", "verification_commands", "allowed_path_globs", "resource_keys", "follow_up_bots"}:
            raise service.DomainError("The executive may edit only the execution plan and scope")
        service.patch_task(session, **kwargs, changes=changes)
    elif action in {"start", "revise"}:
        require_executor_preconditions()
        model_for_role(session, f"executor_{task.assignee_profile}")
        if task.reviewer_required: model_for_role(session, "pr_reviewer")
        # Bound repeated retries; new evidence/scope should resolve a recurring failure.
        from .models import Revision
        current_revision = session.scalar(select(func.max(Revision.revision_number)).where(Revision.task_id == task.id))
        failures = session.scalar(select(func.count()).select_from(Execution).where(Execution.task_id == task.id, Execution.revision == current_revision, Execution.terminal_at.is_not(None)))
        if failures >= 3:
            raise service.PreconditionFailed("Three executions of this plan have ended; revise the plan to resolve the underlying blocker before another automated retry")
        service.start_task(session, **kwargs, idempotency_key="executive:" + lease["id"], revision=action == "revise",
                           max_queued=conf.getint("bot_dashboard", "max_queued_executions", fallback=20))
    elif action in {"ready", "merge", "complete"}:
        execution = _latest_execution(session, task)
        if execution and execution.pr_number:
            provider = get_provider()
            observation = _trusted_change(provider, execution)
            if action == "ready" and observation["draft"]:
                provider.mark_ready(execution.pr_number, execution.trusted_head_sha)
                observation = _trusted_change(provider, execution)
            if action == "merge":
                if observation["draft"]:
                    raise service.PreconditionFailed("PR must be ready for review before merging")
                if observation["state"] != "merged":
                    provider.merge_change(execution.pr_number, execution.trusted_head_sha)
                # Maintenance observes the merge and schedules existing follow-ups.
                execution.synced_at = None
                return
            if action == "complete" and (observation["state"] != "merged" or not execution.merged_at):
                raise service.PreconditionFailed("Completion requires an observed merge and follow-up scheduling")
            execution.provider_state = {**observation, "review_verdict": execution.review_verdict}
            evidence = {"label": "Reviewed and merged PR", "url": execution.pr_url,
                        "observation": "Trusted head " + execution.trusted_head_sha}
        else:
            if action == "ready":
                # Human-performed work uses the same existing evidence gate.
                service.transition_task(session, **kwargs, to_state="ready", reason=decision.rationale)
                return
            if execution and execution.terminal_reason_code == "no_change":
                evidence = {"label": "Verified no-change execution", "observation": f"Execution {execution.sequence}: no_change"}
            else:
                verified = session.scalar(select(Event).where(Event.task_id == task.id, Event.event_type == "evidence_added",
                    Event.actor_kind == "user").order_by(Event.sequence.desc()).limit(1))
                if execution or not verified:
                    raise service.PreconditionFailed("Completion requires a verified no-change execution, a merged PR, or human verification evidence")
                evidence = {"label": "Human verification reviewed", "observation": f"Executive reviewed the evidence recorded in activity event {verified.sequence}"}
        if action == "complete":
            service.add_event_items(session, **kwargs, event_type="comment_added", items=[decision.rationale])
            kwargs["version"] = task.version
            service.add_event_items(session, **kwargs, event_type="evidence_added", items=[evidence])
            kwargs["version"] = task.version
        service.transition_task(session, **kwargs, to_state="completed" if action == "complete" else "ready", reason=decision.rationale)
    else:
        target = {"accept": "accepted", "dismiss": "dismissed", "block": "blocked", "restore": task.blocked_from_state}[action]
        service.transition_task(session, **kwargs, to_state=target, reason=decision.rationale)


def decide(session: Session, decision: Decision) -> dict:
    from .git_provider import GitProviderError, get_provider
    row, state = _control(session, lock=True)
    if decision.lease_id in state.get("receipts", []):
        return {"status": "already_applied"}
    lease = next((item for item in _leases(state) if item["id"] == decision.lease_id), None)
    if not state.get("enabled") or not lease or lease["expires_at"] <= utcnow().isoformat():
        raise service.Conflict("Autopilot is off or this decision lease has expired")
    if _model(session) != lease["model"]:
        raise service.Conflict("The executive model mapping changed")
    task = service._locked_task(session, lease["task_id"], lease["version"])
    if _digest(_snapshot(session, str(task.id))) != lease["digest"]:
        raise service.Conflict("Ticket evidence changed while the executive was deciding")
    if decision.action not in lease["actions"]:
        raise service.DomainError("This action is not available at the current decision point")
    result, error = "applied", None
    try:
        with session.begin_nested():
            _perform(session, task, decision, lease)
            session.flush()
    except (service.DomainError, GitProviderError, httpx.HTTPError) as exc:
        result = "deferred"
        error = str(exc)[:500] if isinstance(exc, service.DomainError) else "Git provider could not apply this decision; existing checks remain in force"
    delay = 60 if result == "deferred" else 15 if decision.action == "wait" else 1
    now = utcnow()
    payload = {"action": decision.action, "rationale": decision.rationale, "model": lease["model"]["model"],
               "result": result, "error": error, "lease_id": lease["id"], "context_sha256": lease["digest"],
               "result_version": task.version, "revisit_at": (now + timedelta(minutes=delay)).isoformat()}
    event = service._event(session, task, "executive_decision", "system", "executive", payload=payload)
    session.flush()
    # Mirror reasoning onto the actual PR. A comment failure never undoes an action.
    execution = _latest_execution(session, task)
    if execution and execution.pr_number:
        try:
            provider = get_provider()
            if provider.config.project == execution.repository and provider.config.provider == execution.provider:
                provider.upsert_comment(execution.pr_number, f"<!-- executive:{lease['id']} -->",
                    f"### Executive decision\n\n**{decision.action.title()} · {result}**\n\n{decision.rationale}\n\n"
                    f"Ticket: `{task.id}` · Decision {event.sequence}\n" + (f"\nGate: {error}\n" if error else ""))
        except (GitProviderError, httpx.HTTPError):
            service._event(session, task, "executive_comment_failed", "system", "executive",
                           payload={"reason": "PR comment could not be posted; the decision and rationale are retained on this ticket", "lease_id": lease["id"]})
    state.update(leases=[item for item in _leases(state) if item["id"] != decision.lease_id], last_error=error, last_decision={"task_id": str(task.id), "title": task.title,
                 "at": now.isoformat(), "action": decision.action, "rationale": decision.rationale, "result": result})
    state["receipts"] = (state.get("receipts", []) + [lease["id"]])[-50:]
    _save(session, row, state)
    return {"status": result, "task_id": str(task.id), "action": decision.action, "error": error}
