"""Executive decisions use the same ticket services as people, under a revocable lease.

The model gets evidence and a small action vocabulary, never tools or credentials.
Each ticket has at most one decision lease. Turning off invalidates outstanding
leases; changed tickets or execution evidence require a new model decision.
"""
from __future__ import annotations

import hashlib
import fnmatch
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
from .models import Event, Execution, Policy, Revision, RunReport, Task, ValidationGate, utcnow
from .model_settings import model_for_role
from . import service

KEY = "bot_dashboard_autopilot"
ACTOR = {"actor_id": "executive", "actor_kind": "system"}
EXECUTIVE_META_EVENTS = {"executive_decision", "executive_error", "executive_comment_failed"}


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
    action: Literal["accept", "assign", "configure", "start", "revise", "repair", "repair_conflict", "retry_review", "advance", "ready", "merge", "complete", "block", "dismiss", "restore", "request_follow_up"]
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
        "capacity_recheck_at": state.get("capacity_recheck_at"),
        "capacity_probe_active": bool(state.get("capacity_probe_lease_id")),
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
        state["capacity_recheck_at"] = (utcnow() + timedelta(minutes=15)).isoformat()
        state["capacity_probe_lease_id"] = None
    elif body.reason_code == "decision_context_changed":
        state["last_error"] = "The decision context changed before it could be applied. The executive will reassess the current ticket shortly."
    elif state.get("capacity_probe_lease_id") == body.lease_id:
        state["capacity_recheck_at"] = None
        state["capacity_probe_lease_id"] = None
    delay = 1 if body.reason_code == "decision_context_changed" else 15
    task = service._locked_task(session, lease["task_id"])
    service._event(session, task, "executive_error", "system", "executive", payload={
        "reason": state["last_error"], "result_version": task.version,
        "external_context_sha256": lease.get("external_digest"),
        "revisit_at": (utcnow() + timedelta(minutes=delay)).isoformat(), "lease_id": body.lease_id,
        "reason_code": body.reason_code})
    _save(session, row, state)
    return {"status": "recorded"}


def _snapshot(session: Session, task_id: str) -> dict:
    detail = service.get_task(session, task_id)
    from .validation_recipes import available_capabilities, installed_recipe_catalog, ValidationRecipeError
    try:
        capabilities = available_capabilities()
        detail["validation_recipes"] = {
            key: value for key, value in installed_recipe_catalog().items()
            if value["capability"] in capabilities
        }
        detail["validation_capabilities"] = sorted(capabilities)
    except ValidationRecipeError as exc:
        detail["validation_recipes"] = {}
        detail["validation_capability_error"] = str(exc)
    # Bound model context without changing the authoritative history. Preserve
    # material events separately so repeated executive bookkeeping cannot push
    # the evidence which should wake a parked ticket out of the model context.
    detail["revisions"] = detail["revisions"][-2:]
    events = detail["events"]
    detail["events"] = [event for event in events if event["event_type"] not in EXECUTIVE_META_EVENTS][-20:]
    detail["executive_history"] = [event for event in events if event["event_type"] == "executive_decision"][-5:]
    detail["executions"] = detail["executions"][-3:]
    linked = session.scalars(select(Event).where(
        Event.task_id == uuid.UUID(task_id), Event.event_type == "follow_up_ticket_linked",
    ).order_by(Event.sequence.desc()).limit(20)).all()
    child_ids = []
    for event in linked:
        try:
            child_ids.append(uuid.UUID(str(event.payload.get("task_id"))))
        except (TypeError, ValueError):
            continue
    children = {row.id: row for row in session.scalars(select(Task).where(Task.id.in_(child_ids))).all()} if child_ids else {}
    detail["linked_follow_ups"] = [{
        "task_id": str(child_id),
        "title": event.payload.get("title"),
        "state": children[child_id].state if child_id in children else "missing",
        "completed_at": children[child_id].completed_at.isoformat() if child_id in children and children[child_id].completed_at else None,
    } for event in reversed(linked) if (child_id := _payload_uuid(event.payload.get("task_id")))]
    rows = session.scalars(select(Execution).where(Execution.task_id == uuid.UUID(task_id)).order_by(Execution.sequence.desc()).limit(1)).all()
    if rows:
        execution = rows[0]
        if execution.terminal_at and not execution.pr_number:
            detail["user_implementation_evidence"] = session.scalar(select(Event.sequence).where(
                Event.task_id == execution.task_id, Event.event_type == "evidence_added",
                Event.actor_kind == "user", Event.created_at > execution.terminal_at,
            ).order_by(Event.sequence.desc()).limit(1))
        from . import planning
        revision = session.scalar(select(Revision).where(Revision.task_id == uuid.UUID(task_id))
                                  .order_by(Revision.revision_number.desc()).limit(1))
        detail["follow_up_options"] = planning.options(session, session.get(Task, uuid.UUID(task_id)), execution, revision)
        detail["planning_requests"] = planning.status(session, execution.task_id)
        try:
            from .model_recovery import new_review_evidence
            from .conflict_recovery import conflict_repair_eligibility
        except ImportError:
            # Keep the executive usable during rolling provider upgrades where
            # these optional recovery helpers have not landed together yet.
            detail["new_review_evidence"] = False
            detail["conflict_repair"] = None
        else:
            detail["new_review_evidence"] = new_review_evidence(session, execution)
            detail["conflict_repair"] = conflict_repair_eligibility(session, task_id)
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
    evidence = [detail["version"], detail["state"], detail.get("blocked_from_state"), executions,
                detail["events"], detail.get("reports"), detail.get("follow_up_options"),
                detail.get("linked_follow_ups"), detail.get("validation_gates"), detail.get("planning_requests"),
                detail.get("validation_recipes"), detail.get("validation_capabilities"),
                detail.get("validation_capability_error"), detail.get("user_implementation_evidence")]
    return hashlib.sha256(json.dumps(evidence, sort_keys=True, default=str).encode()).hexdigest()

def _external_digest(detail):
    """Evidence that can wake an administrative or deterministically blocked decision.

    The executive's own assignment, plan edits and planning requests are not
    new instructions to itself. Their effects are reflected in the ordinary
    lease digest, but must not reset the per-evidence decision history.
    """
    executions = [{k: v for k, v in e.items() if k not in {"synced_at", "lifecycle_durations_ms"}}
                  for e in detail["executions"]]
    gates = [{k: gate.get(k) for k in (
        "gate_key", "stage", "recipe", "owner", "required_capability", "subject",
        "dependencies", "status", "evidence", "required", "recipe_args", "last_error",
    )} for gate in detail.get("validation_gates", [])]
    evidence = [
        detail["state"], detail.get("blocked_from_state"),
        [event for event in detail["events"]
         if not (event["actor_kind"] == "system" and event["actor_id"] == "executive")],
        executions, gates, detail.get("reports"), detail.get("linked_follow_ups"),
        detail.get("planning_requests"), detail.get("follow_up_options"),
        detail.get("validation_recipes"), detail.get("validation_capabilities"),
        detail.get("validation_capability_error"), detail.get("user_implementation_evidence"),
    ]
    return hashlib.sha256(json.dumps(evidence, sort_keys=True, default=str).encode()).hexdigest()


_ADMIN_ACTIONS = {"assign", "configure", "request_follow_up"}


def _remaining_actions(detail, latest, actions, external):
    if not latest or latest.event_type != "executive_decision":
        return actions
    payload = latest.payload
    if payload.get("external_context_sha256") == external:
        if payload.get("retry_on_change") and payload.get("result") == "deferred":
            return []
        actions = [action for action in actions if action not in payload.get("suppressed_actions", [])]
    # A guarded merge does not justify repeating the merge until maintenance
    # observes it, but provider errors remain retryable after their cooldown.
    unchanged = (payload.get("result_version") == detail["version"]
                 and payload.get("result_context_sha256") == _digest(detail))
    if unchanged and payload.get("result") == "applied" and payload.get("action") == "merge":
        actions = [action for action in actions if action not in {"merge", "advance"}]
    return actions


def _payload_uuid(value):
    try:
        return uuid.UUID(str(value))
    except (TypeError, ValueError):
        return None


def _actions(detail):
    state = detail["state"]
    latest = detail["executions"][-1] if detail["executions"] else None
    current_revision = max((r["revision_number"] for r in detail.get("revisions", [])), default=0)
    if latest and not latest["terminal_at"] and latest["stage"] not in {"reviewed", "provider_sync"}:
        return []  # An admitted executor/reviewer owns this decision point.
    actions = []
    if state == "proposed": actions += ["accept", "dismiss"]
    if state == "accepted":
        if detail.get("assignee_kind") == "human":
            return []  # Human-owned implementation or authorization is not an executive decision.
        actions += ["configure", "start", "dismiss"]
        if detail.get("assignee_kind") != "bot" or detail.get("assignee_profile") not in {"junior", "senior", "staff"}:
            actions.insert(0, "assign")
    if state == "blocked":
        actions += ["restore", "configure", "start", "dismiss"]
        review_state = (latest or {}).get("provider_state") or {}
        if (
            detail.get("blocked_from_state") == "in_review"
            and latest
            and latest.get("review_verdict") == "unable_to_review"
            and review_state.get("review_failure_kind") in {
                None, "transport_failed", "format_failed", "evidence_unavailable"
            }
        ):
            actions += ["retry_review"]
    if state == "in_progress":
        actions += ["ready"]
        if (latest and latest.get("terminal_at") and latest.get("admission_kind") == "executor"
                and latest.get("terminal_reason_code") not in {None, "no_change"}
                and not latest.get("pr_number") and not latest.get("pr_url")):
            actions += ["configure", "start"]
    if state == "in_review":
        actions += ["configure"]
        review_state = (latest or {}).get("provider_state") or {}
        if (
            latest
            and latest.get("review_verdict") == "unable_to_review"
            and review_state.get("review_failure_kind") in {
                None, "transport_failed", "format_failed", "evidence_unavailable"
            }
        ):
            actions += ["retry_review"]
        if latest and latest.get("review_verdict") == "changes_requested" and review_state.get("review_repair"):
            actions += ["repair"]
        # Revising a published execution consumes a newer admitted plan; it is
        # not a way to retry an independent review of the same revision.
        if not latest or latest["terminal_at"] or current_revision > latest["revision"]:
            actions += ["revise"]
    if state in {"in_review", "ready"} and latest and latest["pr_number"]:
        if latest["review_verdict"] == "approved" or not latest["reviewer_required"]:
            if state == "in_review":
                actions += ["ready", "advance"]
            elif latest.get("merged_at") and (latest.get("provider_state") or {}).get("state") == "merged":
                actions += ["complete"]
            else:
                actions += ["merge", "advance"]
    if state == "ready" and (not latest or not latest["pr_number"]):
        if (not latest or (latest.get("terminal_reason_code") == "no_change"
                           and latest["revision"] == current_revision)
                or detail.get("user_implementation_evidence")):
            actions += ["complete"]
    if state == "ready":
        actions += ["configure"]
        if (latest and latest.get("pr_number") and current_revision > latest["revision"]
                and any(gate.get("required") and gate.get("stage") == "merge"
                        and gate.get("status") in {"pending", "failed"}
                        and gate.get("owner") == "validation-service"
                        and gate.get("subject") == latest.get("trusted_head_sha")
                        for gate in detail.get("validation_gates", []))):
            actions += ["revise"]
    if detail.get("follow_up_options"):
        actions += ["request_follow_up"]
    if detail.get("new_review_evidence") and state in {"blocked", "in_review"} and "retry_review" not in actions:
        actions += ["retry_review"]
    if (((detail.get("conflict_repair") or {}).get("recoverable")
            and (detail.get("conflict_repair") or {}).get("mode") == "terminal_seed") or (
            state in {"ready", "in_review", "blocked"} and latest and not latest.get("terminal_at")
            and latest.get("pr_number") and not latest.get("merged_at")
            and (latest.get("provider_state") or {}).get("mergeability") == "conflicting")):
        # A repository conflict is owned by Autopilot. Make the guarded repair
        # the only decision so it cannot be mislabeled as a wait or human blocker.
        actions = ["repair_conflict"]
    if state == "blocked" and not (
        detail.get("blocked_from_state") in {"accepted", "in_progress"}
        and latest and latest.get("terminal_at")
        and latest.get("admission_kind") == "executor"
        and latest.get("terminal_reason_code") not in {None, "no_change"}
        and not latest.get("pr_number") and not latest.get("pr_url")
    ):
        actions = [action for action in actions if action != "start"]
    failures = sum(e.get("revision") == current_revision and bool(e.get("terminal_at"))
                   for e in detail["executions"])
    if failures >= 3:
        actions = [action for action in actions if action not in {"start", "revise", "repair", "repair_conflict"}]
        if not actions:
            actions = ["configure"]  # Exhaustion requires a corrected plan, not an invisible dead end.
    if detail.get("assignee_kind") != "bot" or detail.get("assignee_profile") not in {"junior", "senior", "staff"}:
        actions = [action for action in actions if action not in {"start", "revise", "repair", "repair_conflict"}]
    elif state == "accepted" and "start" in actions:
        actions += ["advance"]
    pending_stages = {gate["stage"] for gate in detail.get("validation_gates", [])
                      if gate["required"] and gate["status"] != "passed"}
    if "publication" in pending_stages:
        actions = [action for action in actions if action not in {"ready", "merge", "complete", "advance"}]
    elif "merge" in pending_stages:
        actions = [action for action in actions if action not in {"merge", "complete", "advance"}]
    if (state == "ready" and latest and latest.get("pr_number")
            and not latest.get("merged_at") and "repair_conflict" not in actions):
        unresolved = [child for child in detail.get("linked_follow_ups", [])
                      if child["state"] not in {"completed", "dismissed", "missing"}]
        if any(child["state"] != "blocked" for child in unresolved):
            # A worker or approval is still advancing the linked prerequisite.
            return []
        if unresolved:
            # A blocked child cannot satisfy the merge gate; its failure is
            # material evidence for a bounded parent plan correction, never a
            # reason to bypass the child and merge anyway.
            actions = [action for action in actions if action not in {"merge", "advance", "complete"}]
    if state == "ready" and "complete" in actions:
        return ["complete"]  # A finished implementation needs closure, not another plan edit.
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
    probe_id = state.get("capacity_probe_lease_id")
    if probe_id and not any(item["id"] == probe_id for item in _leases(state)):
        state["capacity_probe_lease_id"] = None
        probe_id = None
    recheck_at = state.get("capacity_recheck_at")
    if recheck_at and recheck_at > now.isoformat():
        _save(session, row, state)
        return {"status": "capacity_wait", "recheck_at": recheck_at}
    if recheck_at and probe_id:
        _save(session, row, state)
        return {"status": "capacity_probe", "recheck_at": recheck_at}
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
    latest_mergeability = select(Execution.provider_state["mergeability"].as_string()).where(
        Execution.task_id == Task.id,
    ).order_by(Execution.sequence.desc()).limit(1).scalar_subquery()
    latest_terminal_at = select(Execution.terminal_at).where(
        Execution.task_id == Task.id,
    ).order_by(Execution.sequence.desc()).limit(1).scalar_subquery()
    latest_admission_kind = select(Execution.admission_kind).where(
        Execution.task_id == Task.id,
    ).order_by(Execution.sequence.desc()).limit(1).scalar_subquery()
    latest_terminal_reason = select(Execution.terminal_reason_code).where(
        Execution.task_id == Task.id,
    ).order_by(Execution.sequence.desc()).limit(1).scalar_subquery()
    latest_pr_number = select(Execution.pr_number).where(
        Execution.task_id == Task.id,
    ).order_by(Execution.sequence.desc()).limit(1).scalar_subquery()
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
    retryable_failed = (
        (
            (Task.state == "in_progress")
            | ((Task.state == "blocked") & Task.blocked_from_state.in_(["accepted", "in_progress"]))
        )
        & latest_terminal_at.is_not(None)
        & (latest_admission_kind == "executor")
        & latest_terminal_reason.is_not(None)
        & (latest_terminal_reason != "no_change")
        & latest_pr_number.is_(None)
    )
    failed_conflict_repair = retryable_failed & (
        latest_terminal_reason.in_(("unresolved_revision_seed_conflict", "revision_seed_apply_failed"))
    )
    phase = case((failed_conflict_repair, 0),
                 (retryable_failed, 1),
                 (latest_mergeability == "conflicting", 2),
                 (Task.state == "ready", 3),
                 ((Task.state == "in_review") & (latest_verdict == "approved"), 4),
                 ((Task.state == "in_review") & (planned_revision > executed_revision), 5),
                 (Task.state == "in_review", 6), (prepared, 7), (Task.state == "accepted", 8),
                 (Task.state == "in_progress", 9), (Task.state == "proposed", 10), else_=11)
    retry_recency = case((retryable_failed, latest_terminal_at), else_=None).desc().nullslast()
    ordering = aging if now.minute % 5 == 0 else [phase, retry_recency, *aging]
    candidates = session.scalars(select(Task).outerjoin(last, Task.id == last.c.task_id)
        .where(Task.state.in_(["proposed", "accepted", "blocked", "in_progress", "in_review", "ready"]), ~active_worker)
        .order_by(*ordering).limit(100)).all()
    state["last_checked_at"] = now.isoformat()
    for task in candidates:
        if str(task.id) in leased_tasks:
            continue
        detail = _snapshot(session, str(task.id))
        actions = _actions(detail)
        if not actions:
            continue
        latest = session.scalar(select(Event).where(Event.task_id == task.id, Event.event_type.in_(["executive_decision", "executive_error"])).order_by(Event.sequence.desc()).limit(1))
        prior_decision = latest if latest and latest.event_type == "executive_decision" else session.scalar(
            select(Event).where(Event.task_id == task.id, Event.event_type == "executive_decision")
            .order_by(Event.sequence.desc()).limit(1))
        external = _external_digest(detail)
        actions = _remaining_actions(detail, prior_decision, actions, external)
        if not actions:
            continue
        if latest:
            retry = latest.payload.get("revisit_at")
            if (retry and retry > now.isoformat()
                    and (latest.payload.get("external_context_sha256") or external) == external
                    and latest.payload.get("result_version") == task.version):
                continue
        suppressed = (prior_decision.payload.get("suppressed_actions", [])
                      if prior_decision and prior_decision.payload.get("external_context_sha256") == external else [])
        lease = {"id": secrets.token_hex(32), "task_id": str(task.id), "version": task.version,
                 "digest": _digest(detail), "external_digest": external,
                 "suppressed_actions": suppressed, "model": model, "actions": actions,
                 "owner": identity,
                 "expires_at": (now + timedelta(minutes=5)).isoformat()}
        _leases(state).append(lease)
        if recheck_at:
            state["capacity_probe_lease_id"] = lease["id"]
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


_REPAIR_EXCLUDED_WORDS = {
    "credential", "secret", "security", "permission", "production", "deploy",
    "migration", "schema migration", "drop table", "delete data", "destructive",
    "public contract", "api contract",
}


def _review_repair_changes(session: Session, task: Task, execution: Execution) -> dict:
    """Turn one small review finding into a scoped revision, or fail closed."""
    repair = (execution.provider_state or {}).get("review_repair")
    if not isinstance(repair, dict):
        raise service.PreconditionFailed("Reviewer did not request a bounded repair")
    if task.category in {"credential", "storage"}:
        raise service.PreconditionFailed("This category requires explicit repair planning")
    instructions = repair.get("instructions")
    paths = repair.get("paths")
    expectations = repair.get("check_expectations")
    if (
        not isinstance(instructions, str)
        or not isinstance(paths, list)
        or not 1 <= len(paths) <= 3
        or len(paths) != len(set(paths))
        or not isinstance(expectations, list)
        or not 1 <= len(expectations) <= 10
    ):
        raise service.PreconditionFailed("Reviewer repair request is malformed")
    lowered = " ".join([instructions, *[str(value) for value in expectations]]).lower()
    if any(word in lowered for word in _REPAIR_EXCLUDED_WORDS):
        raise service.PreconditionFailed("Review repair requires explicit planning because it is high risk")
    revision = session.scalar(
        select(Revision).where(Revision.task_id == task.id)
        .order_by(Revision.revision_number.desc()).limit(1)
    )
    if revision is None or not revision.allowed_path_globs or not revision.verification_commands:
        raise service.PreconditionFailed("Accepted repair scope or verification is unavailable")
    if any(
        not isinstance(path, str)
        or path.startswith(("/", "../"))
        or "\\" in path
        or ".." in path.split("/")
        or not any(fnmatch.fnmatchcase(path, pattern) for pattern in revision.allowed_path_globs)
        for path in paths
    ):
        raise service.PreconditionFailed("Reviewer repair paths exceed the accepted scope")
    review_text = (
        task.planned_resolution.rstrip()
        + "\n\nReviewer-requested bounded repair:\n"
        + instructions.strip()
        + "\nAffected paths: " + ", ".join(paths)
        + "\nRegression expectations:\n- " + "\n- ".join(str(value).strip() for value in expectations)
    )
    if len(review_text) > 20_000:
        raise service.PreconditionFailed("Reviewer repair instructions exceed the task plan bound")
    return {"planned_resolution": review_text}


def _perform(session, task, decision, lease):
    from .execution import require_executor_preconditions
    from .git_provider import get_provider
    action = decision.action
    kwargs = {"task_id": str(task.id), "version": task.version, **ACTOR}
    if action == "advance":
        if task.state == "accepted":
            return _perform(session, task, decision.model_copy(update={"action": "start"}), lease)
        if task.state == "in_review":
            _perform(session, task, decision.model_copy(update={"action": "ready"}), lease)
            return _perform(session, task, decision.model_copy(update={"action": "merge"}), lease)
        if task.state == "ready":
            execution = _latest_execution(session, task)
            if execution and execution.merged_at and (execution.provider_state or {}).get("state") == "merged":
                return _perform(session, task, decision.model_copy(update={"action": "complete"}), lease)
            return _perform(session, task, decision.model_copy(update={"action": "merge"}), lease)
        raise service.PreconditionFailed("No guarded continuation is available")
    # No no-op decision is available: active work is owned by its worker and
    # other tickets become eligible only when their material evidence changes.
    if action == "assign":
        service.assign_task(session, **kwargs, actor_name="Executive", kind="bot", profile=decision.profile, reviewer_required=True)
    elif action == "request_follow_up":
        from . import planning
        planning.request(session, task, decision.specialist, decision.rationale)
    elif action == "configure":
        changes = decision.changes.model_dump(exclude_none=True, exclude={"version"})
        if decision.changes.version != task.version:
            raise service.Conflict("Execution settings version changed")
        if not changes or set(changes) - {"planned_resolution", "verification_commands", "allowed_path_globs", "resource_keys", "follow_up_bots", "acceptance_gates"}:
            raise service.DomainError("The executive may edit only the execution plan and scope")
        service.patch_task(session, **kwargs, changes=changes)
    elif action == "retry_review":
        from .model_recovery import retry_review_result
        retry_review_result(
            session, str(task.id), version=task.version, actor_id="executive",
            idempotency_key="executive:" + lease["id"],
            max_queued=conf.getint("bot_dashboard", "max_queued_executions", fallback=20),
        )
    elif action == "repair_conflict":
        require_executor_preconditions()
        model_for_role(session, f"executor_{task.assignee_profile}")
        model_for_role(session, "pr_reviewer")
        service.start_task(
            session, **kwargs, idempotency_key="executive:" + lease["id"], conflict_repair=True,
            max_queued=conf.getint("bot_dashboard", "max_queued_executions", fallback=20),
        )
    elif action == "repair":
        execution = _latest_execution(session, task)
        changes = _review_repair_changes(session, task, execution)
        service.patch_task(session, **kwargs, changes=changes)
        require_executor_preconditions()
        model_for_role(session, f"executor_{task.assignee_profile}")
        model_for_role(session, "pr_reviewer")
        service.start_task(
            session, str(task.id), version=task.version, actor_id="executive",
            actor_kind="system", idempotency_key="executive:" + lease["id"], revision=True,
            max_queued=conf.getint("bot_dashboard", "max_queued_executions", fallback=20),
        )
    elif action in {"start", "revise"}:
        if task.assignee_kind != "bot" or task.assignee_profile not in {"junior", "senior", "staff"}:
            raise service.PreconditionFailed("Assign an allowed bot profile before starting execution")
        require_executor_preconditions()
        model_for_role(session, f"executor_{task.assignee_profile}")
        if task.reviewer_required: model_for_role(session, "pr_reviewer")
        # Bound repeated retries; new evidence/scope should resolve a recurring failure.
        current_revision = session.scalar(select(func.max(Revision.revision_number)).where(Revision.task_id == task.id))
        failures = session.scalar(select(func.count()).select_from(Execution).where(Execution.task_id == task.id, Execution.revision == current_revision, Execution.terminal_at.is_not(None)))
        if failures >= 3:
            raise service.PreconditionFailed("Three executions of this plan have ended; revise the plan to resolve the underlying blocker before another automated retry")
        service.start_task(session, **kwargs, idempotency_key="executive:" + lease["id"], revision=action == "revise",
                           max_queued=conf.getint("bot_dashboard", "max_queued_executions", fallback=20))
    elif action in {"ready", "merge", "complete"}:
        stages = {
            "ready": ("publication",),
            "merge": ("publication", "merge"),
            # Completion means the reviewed implementation is merged, not that
            # production activation or a live source check has passed.
            "complete": ("publication", "merge"),
        }[action]
        service.require_validation_gates(session, task.id, *stages)
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
            current_revision = session.scalar(select(func.max(Revision.revision_number)).where(
                Revision.task_id == task.id))
            if (execution and execution.terminal_reason_code == "no_change"
                    and execution.revision == current_revision):
                evidence = {"label": "Verified no-change execution", "observation": f"Execution {execution.sequence}: no_change"}
            else:
                query = select(Event).where(Event.task_id == task.id, Event.event_type == "evidence_added",
                    Event.actor_kind == "user")
                if execution:
                    if execution.pr_number or not execution.terminal_at:
                        raise service.PreconditionFailed("Completion requires a reviewed merge or human implementation evidence")
                    query = query.where(Event.created_at > execution.terminal_at)
                verified = session.scalar(query.order_by(Event.sequence.desc()).limit(1))
                if not verified:
                    raise service.PreconditionFailed("Completion requires current no-change evidence, a reviewed merge, or human implementation evidence")
                evidence = {"label": "Human implementation evidence", "observation": f"User evidence recorded in activity event {verified.sequence}"}
        if action == "complete":
            # Keep outstanding live requirements visible without claiming that
            # merge evidence is evidence of production recovery.
            pending = session.scalars(select(ValidationGate).where(
                ValidationGate.task_id == task.id,
                ValidationGate.required.is_(True),
                ValidationGate.stage.in_(("activation", "completion")),
                ValidationGate.status != "passed",
            )).all()
            evidence["production_validation"] = "unverified" if pending else "not_established_by_implementation"
            if pending:
                evidence["pending_gate_keys"] = [gate.gate_key for gate in pending]
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
    result, error, retry_on_change = "applied", None, False
    try:
        with session.begin_nested():
            _perform(session, task, decision, lease)
            session.flush()
    except (service.DomainError, GitProviderError, httpx.HTTPError) as exc:
        result = "deferred"
        error = str(exc)[:500] if isinstance(exc, service.DomainError) else "Git provider could not apply this decision; existing checks remain in force"
        # Admission capacity and provider transport recover independently of
        # this ticket; other rejected preconditions need new material evidence.
        retry_on_change = isinstance(exc, service.DomainError) and str(exc) != "execution admission queue is full"
    delay = 60 if result == "deferred" else 1
    now = utcnow()
    session.expire(task, ["events", "revisions"])
    detail = _snapshot(session, str(task.id))
    result_digest = _digest(detail)
    external_digest = _external_digest(detail)
    suppressed = set(lease.get("suppressed_actions", []))
    if result == "applied" and decision.action in _ADMIN_ACTIONS:
        suppressed.add(decision.action)
    elif external_digest != lease.get("external_digest"):
        suppressed.clear()
    payload = {"action": decision.action, "rationale": decision.rationale, "model": lease["model"]["model"],
               "result": result, "error": error, "lease_id": lease["id"], "context_sha256": lease["digest"],
               "result_context_sha256": result_digest, "result_version": task.version,
               "external_context_sha256": external_digest, "suppressed_actions": sorted(suppressed),
               "retry_on_change": retry_on_change,
               "revisit_at": (now + timedelta(minutes=delay)).isoformat()}
    event = service._event(session, task, "executive_decision", "system", "executive", payload=payload)
    session.flush()
    # Mirror reasoning onto the actual PR. A comment failure never undoes an action.
    execution = _latest_execution(session, task)
    if execution and execution.pr_number:
        try:
            provider = get_provider()
            if provider.config.project == execution.repository and provider.config.provider == execution.provider:
                provider.upsert_comment(execution.pr_number, f"<!-- executive:task:{task.id} -->",
                    f"### Executive decision\n\n**{decision.action.title()} · {result}**\n\n{decision.rationale}\n\n"
                    f"Ticket: `{task.id}` · Decision {event.sequence}\n" + (f"\nGate: {error}\n" if error else ""))
        except (GitProviderError, httpx.HTTPError):
            service._event(session, task, "executive_comment_failed", "system", "executive",
                           payload={"reason": "PR comment could not be posted; the decision and rationale are retained on this ticket", "lease_id": lease["id"]})
    state.update(leases=[item for item in _leases(state) if item["id"] != decision.lease_id], last_error=error,
                 capacity_recheck_at=None, capacity_probe_lease_id=None,
                 last_decision={"task_id": str(task.id), "title": task.title,
                 "at": now.isoformat(), "action": decision.action, "rationale": decision.rationale, "result": result})
    state["receipts"] = (state.get("receipts", []) + [lease["id"]])[-50:]
    _save(session, row, state)
    return {"status": result, "task_id": str(task.id), "action": decision.action, "error": error}
