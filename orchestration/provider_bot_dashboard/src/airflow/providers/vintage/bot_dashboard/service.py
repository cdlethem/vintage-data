"""Single transactional boundary for bot-dashboard domain state."""
from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload
from airflow.configuration import conf
from airflow.sdk.observability import stats

from .models import (
    Event,
    Execution,
    Policy,
    Revision,
    RunBudgetClaim,
    RunReport,
    Task,
    TASK_STATES,
    utcnow,
)

MANAGER_DAG_ID = "bot__manager"
EXECUTOR_DAG_ID = "bot__task_executor"
HUMAN_ACTION_STATES = {"proposed", "accepted", "blocked", "in_review", "ready"}
# Imported history is evidence, never current freshness: it never ran in Airflow.
LIVE_RUN = RunReport.dag_id.not_like("legacy\\_\\_%", escape="\\")
LEGAL = {
    "proposed": {"accepted", "dismissed", "blocked"},
    "accepted": {"in_progress", "dismissed", "blocked"},
    "in_progress": {"in_review", "ready", "blocked", "completed"},
    "in_review": {"in_progress", "ready", "blocked"},
    "ready": {"completed", "in_progress", "blocked"},
    "blocked": {"dismissed"},
    "completed": {"accepted"},
    "dismissed": {"accepted"},
}

class DomainError(RuntimeError):
    code = "domain_error"
    status_code = 422

class NotFound(DomainError):
    code = "not_found"
    status_code = 404

class Conflict(DomainError):
    code = "version_conflict"
    status_code = 409
    def __init__(self, message: str, current: dict | None = None):
        super().__init__(message)
        self.current = current


class SpendCapReached(Conflict):
    code = "spend_cap_reached"

class PreconditionFailed(DomainError):
    code = "precondition_failed"
    status_code = 412


def _bounded_payload(value: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    if len(encoded) > 65_536:
        raise DomainError("event payload exceeds 65536 bytes")
    return value


def _fingerprint(event_type: str, actor_kind: str, actor_id: str, from_state: str | None, to_state: str | None, payload: dict) -> str:
    body = json.dumps([event_type, actor_kind, actor_id, from_state, to_state, payload], sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode()).hexdigest()


def _event(session: Session, task: Task, event_type: str, actor_kind: str, actor_id: str, *, from_state: str | None = None, to_state: str | None = None, payload: dict[str, Any] | None = None) -> Event:
    payload = _bounded_payload(payload or {})
    # Airflow sessions disable autoflush. Include audit rows already staged in
    # this transaction so multiple events cannot reuse the persisted maximum.
    pending_sequence = max((row.sequence for row in session.new if isinstance(row, Event) and row.task_id == task.id), default=0)
    persisted_sequence = session.scalar(select(func.coalesce(func.max(Event.sequence), 0)).where(Event.task_id == task.id))
    sequence = max(pending_sequence, persisted_sequence) + 1
    row = Event(task_id=task.id, sequence=sequence, event_type=event_type, actor_kind=actor_kind, actor_id=str(actor_id), from_state=from_state, to_state=to_state, payload=payload, fingerprint=_fingerprint(event_type, actor_kind, str(actor_id), from_state, to_state, payload))
    session.add(row)
    return row

LIFECYCLE_PHASES = (
    "proposal_to_acceptance",
    "acceptance_to_execution",
    "execution_to_pr",
    "pr_to_review",
    "review_to_merge",
    "merge_to_completion",
)


def _milliseconds(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return max(0, int((end - start).total_seconds() * 1000))


def lifecycle_durations(
    task: Task,
    execution: Execution | None = None,
    *,
    events: Iterable[Event] = (),
) -> dict[str, int | None]:
    """Project auditable task lifecycle intervals without exposing event bodies."""
    execution_started = execution.created_at if execution else None
    review_started = None
    for event in events:
        if event.event_type == "execution_started" and execution is not None:
            payload = event.payload or {}
            if payload.get("sequence") == execution.sequence and payload.get("revision") == execution.revision:
                execution_started = event.created_at
        if event.event_type == "state_changed" and event.to_state == "in_review":
            review_started = event.created_at
    published = execution.published_at if execution else None
    reviewed = execution.review_commented_at if execution else None
    merged = execution.merged_at if execution else None
    values = {
        "proposal_to_acceptance": _milliseconds(task.created_at, task.accepted_at),
        "acceptance_to_execution": _milliseconds(task.accepted_at, execution_started),
        "execution_to_pr": _milliseconds(execution_started, published),
        "pr_to_review": _milliseconds(published, reviewed or review_started),
        "review_to_merge": _milliseconds(reviewed or review_started, merged),
        "merge_to_completion": _milliseconds(merged, task.completed_at),
    }
    for phase, duration in values.items():
        if duration is not None:
            stats.timing("bot_dashboard.task.lifecycle_latency", duration, tags={"phase": phase})
    return values



def task_dict(task: Task, *, detail: bool = False) -> dict[str, Any]:
    value = {
        "id": str(task.id), "related_task_id": str(task.related_task_id) if task.related_task_id else None,
        "owning_dag_id": task.owning_dag_id, "source": task.source,
        "source_bot": task.source_bot,
        "recommendation_key": task.recommendation_key, "title": task.title,
        "category": task.category, "state": task.state, "priority": task.priority,
        "planned_resolution": task.planned_resolution, "assignee_kind": task.assignee_kind,
        "assignee_id": task.assignee_id, "assignee_name": task.assignee_name,
        "assignee_profile": task.assignee_profile, "reviewer_required": task.reviewer_required,
        "blocked_from_state": task.blocked_from_state, "version": task.version,
        "next_actor": "human" if task.state in HUMAN_ACTION_STATES else "bot" if task.state == "in_progress" and task.assignee_kind == "bot" else None,
        "created_at": task.created_at.isoformat(), "updated_at": task.updated_at.isoformat(),
        "accepted_at": task.accepted_at.isoformat() if task.accepted_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "dismissed_at": task.dismissed_at.isoformat() if task.dismissed_at else None,
    }
    if detail:
        value["revisions"] = [{
            "revision_number": r.revision_number, "report_schema": r.report_schema,
            "report_date": r.report_date, "title": r.title, "action": r.action,
            "why_now": r.why_now, "expected_benefit": r.expected_benefit,
            "resources": r.resources, "risk": r.risk, "rollback": r.rollback,
            "verification": r.verification, "suggested_executor": r.suggested_executor,
            "resurface_task_id": str(r.resurface_task_id) if r.resurface_task_id else None,
            "resurface_reason": r.resurface_reason, "created_at": r.created_at.isoformat(),
            "evidence": r.evidence,
            "verification_commands": r.verification_commands,
            "allowed_path_globs": r.allowed_path_globs,
            "resource_keys": r.resource_keys,
            "follow_up_bots": r.follow_up_bots,
        } for r in task.revisions]
        value["events"] = [{
            "sequence": e.sequence, "event_type": e.event_type, "actor_kind": e.actor_kind,
            "actor_id": e.actor_id, "from_state": e.from_state, "to_state": e.to_state,
            "payload": e.payload, "fingerprint": e.fingerprint, "created_at": e.created_at.isoformat(),
        } for e in task.events]
    return value


def _locked_task(session: Session, task_id: str | uuid.UUID, version: int | None = None, *, detail: bool = False) -> Task:
    try:
        identity = uuid.UUID(str(task_id))
    except ValueError as exc:
        raise NotFound("task not found") from exc
    stmt = select(Task).where(Task.id == identity).with_for_update()
    if detail:
        stmt = stmt.options(selectinload(Task.revisions), selectinload(Task.events))
    task = session.scalar(stmt)
    if task is None:
        raise NotFound("task not found")
    if version is not None and task.version != version:
        raise Conflict("task version is stale", task_dict(task))
    return task


def get_task(session: Session, task_id: str) -> dict:
    task = session.scalar(select(Task).where(Task.id == uuid.UUID(task_id)).options(selectinload(Task.revisions), selectinload(Task.events)))
    if task is None:
        raise NotFound("task not found")
    value = task_dict(task, detail=True)
    executions = session.scalars(select(Execution).where(Execution.task_id == task.id).order_by(Execution.sequence, Execution.revision)).all()
    value["lifecycle_durations_ms"] = lifecycle_durations(task, executions[-1] if executions else None, events=task.events)
    value["executions"] = [{
        "sequence": row.sequence, "revision": row.revision, "dispatch_state": row.dispatch_state,
        "stage": row.stage, "profile": row.profile, "reviewer_required": row.reviewer_required,
        "branch": row.branch, "provider": row.provider, "repository": row.repository,
        "base_sha": row.base_sha, "patch_sha256": row.patch_sha256,
        "patch_byte_count": row.patch_byte_count, "verification_manifest": row.verification_manifest,
        "pr_number": row.pr_number, "pr_url": row.pr_url,
        "trusted_head_sha": row.trusted_head_sha, "review_verdict": row.review_verdict,
        "provider_state": row.provider_state,
        "executor_deadline_at": row.executor_deadline_at.isoformat() if row.executor_deadline_at else None,
        "review_deadline_at": row.review_deadline_at.isoformat() if row.review_deadline_at else None,
        "synced_at": row.synced_at.isoformat() if row.synced_at else None,
        "terminal_reason_code": getattr(row, "terminal_reason_code", None),
        "terminal_failure_class": getattr(row, "terminal_failure_class", None),
        "lifecycle_durations_ms": lifecycle_durations(task, row, events=task.events),
    } for row in executions]
    return value


def _cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode()).decode().rstrip("=")


def _offset(cursor: str | None) -> int:
    if not cursor:
        return 0
    try:
        return int(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode())
    except Exception as exc:
        raise DomainError("invalid cursor") from exc


def list_tasks(session: Session, *, states: Iterable[str] = (), category: str | None = None, assignee: str | None = None, source: str | None = None, search: str | None = None, limit: int = 25, cursor: str | None = None, sort: str = "priority") -> dict:
    limit = min(max(limit, 1), 100)
    clauses = []
    state_list = [s for s in states if s]
    if state_list:
        if any(s not in TASK_STATES for s in state_list): raise DomainError("invalid state filter")
        clauses.append(Task.state.in_(state_list))
    if category: clauses.append(Task.category == category)
    if source: clauses.append(Task.source == source)
    if assignee == "unassigned": clauses.append(Task.assignee_kind.is_(None))
    elif assignee: clauses.append(or_(Task.assignee_id == assignee, Task.assignee_profile == assignee))
    if search:
        term = f"%{search[:200]}%"
        clauses.append(or_(Task.title.ilike(term), Task.planned_resolution.ilike(term), Task.recommendation_key.ilike(term)))
    where = and_(*clauses) if clauses else True
    ordering = {"priority": (Task.priority.asc(), Task.updated_at.desc(), Task.id.asc()), "updated": (Task.updated_at.desc(), Task.id.asc()), "created": (Task.created_at.desc(), Task.id.asc())}.get(sort)
    if ordering is None: raise DomainError("invalid sort")
    start = _offset(cursor)
    total = session.scalar(select(func.count()).select_from(Task).where(where))
    rows = session.scalars(select(Task).where(where).order_by(*ordering).offset(start).limit(limit + 1)).all()
    more = len(rows) > limit
    rows = rows[:limit]
    return {"items": [task_dict(row) for row in rows], "total": total, "next_cursor": _cursor(start + limit) if more else None}


def create_manual_task(session: Session, *, title: str, category: str, priority: int, planned_resolution: str, actor_id: str, actor_name: str, evidence: list[dict] | None = None) -> dict:
    if category == "other" or category in {"reliability", "new_source", "cadence", "load", "storage", "architecture", "credential", "cost"}: pass
    else: raise DomainError("invalid category")
    identity = uuid.uuid4()
    task = Task(id=identity, source="manual", recommendation_key=f"manual-{identity}", title=title, category=category, state="accepted", priority=priority, planned_resolution=planned_resolution, accepted_at=utcnow())
    session.add(task); session.flush()
    session.add(Revision(task_id=task.id, revision_number=1, title=title, action=planned_resolution, actor_id=actor_id, report_schema="manual_v1"))
    _event(session, task, "created", "user", actor_id, to_state="accepted", payload={"actor_name": actor_name})
    if evidence: _event(session, task, "evidence_added", "user", actor_id, payload={"items": evidence})
    session.flush()
    return task_dict(task)


def patch_task(
    session: Session,
    task_id: str,
    *,
    version: int,
    actor_id: str,
    changes: dict[str, Any],
) -> dict:
    task = _locked_task(session, task_id, version)
    allowed = {
        "planned_resolution",
        "verification_commands",
        "allowed_path_globs",
        "resource_keys",
        "follow_up_bots",
        "reviewer_required",
    }
    if task.source == "manual":
        allowed |= {"title", "category", "priority"}
    invalid = set(changes) - allowed
    if invalid:
        raise DomainError(f"task fields are not editable: {sorted(invalid)}")
    latest = session.scalar(
        select(Revision)
        .where(Revision.task_id == task.id)
        .order_by(Revision.revision_number.desc())
        .limit(1)
    )
    if latest is None:
        raise PreconditionFailed("task has no recommendation revision")
    revision_fields = {
        "verification_commands",
        "allowed_path_globs",
        "resource_keys",
        "follow_up_bots",
    }
    before: dict[str, Any] = {}
    for key, value in changes.items():
        if key in revision_fields:
            before[key] = getattr(latest, key)
        else:
            before[key] = getattr(task, key)
            setattr(task, key, value)
    if set(changes) & (revision_fields | {"planned_resolution", "title"}):
        session.add(
            Revision(
                task_id=task.id,
                revision_number=latest.revision_number + 1,
                report_schema=latest.report_schema,
                report_date=latest.report_date,
                title=changes.get("title", task.title),
                action=changes.get("planned_resolution", task.planned_resolution),
                why_now=latest.why_now,
                expected_benefit=latest.expected_benefit,
                resources=latest.resources,
                risk=latest.risk,
                rollback=latest.rollback,
                verification=latest.verification,
                suggested_executor=latest.suggested_executor,
                resurface_task_id=latest.resurface_task_id,
                resurface_reason=latest.resurface_reason,
                source_dag_id=latest.source_dag_id,
                source_run_id=latest.source_run_id,
                source_task_id=latest.source_task_id,
                source_map_index=latest.source_map_index,
                actor_id=actor_id,
                evidence=latest.evidence,
                verification_commands=changes.get(
                    "verification_commands", latest.verification_commands
                ),
                allowed_path_globs=changes.get(
                    "allowed_path_globs", latest.allowed_path_globs
                ),
                resource_keys=changes.get("resource_keys", latest.resource_keys),
                follow_up_bots=changes.get(
                    "follow_up_bots", latest.follow_up_bots
                ),
            )
        )
    task.version += 1
    task.updated_at = utcnow()
    _event(
        session,
        task,
        "task_edited",
        "user",
        actor_id,
        payload={"before": before, "after": changes},
    )
    session.flush()
    return task_dict(task)


def assign_task(session: Session, task_id: str, *, version: int, actor_id: str, actor_name: str, kind: str, profile: str | None = None, reviewer_required: bool = True) -> dict:
    task = _locked_task(session, task_id, version)
    if kind == "human":
        task.assignee_kind, task.assignee_id, task.assignee_name, task.assignee_profile = "human", actor_id, actor_name, None
    elif kind == "bot" and profile in {"junior", "senior", "staff"}:
        task.assignee_kind, task.assignee_id, task.assignee_name, task.assignee_profile = "bot", None, None, profile
        task.reviewer_required = reviewer_required
    else: raise DomainError("invalid assignment")
    task.version += 1; task.updated_at = utcnow()
    _event(session, task, "assigned", "user", actor_id, payload={"kind": kind, "profile": profile, "reviewer_required": reviewer_required})
    session.flush(); return task_dict(task)


def add_event_items(session: Session, task_id: str, *, version: int, actor_id: str, event_type: str, items: list[Any]) -> dict:
    task = _locked_task(session, task_id, version)
    _event(session, task, event_type, "user", actor_id, payload={"items": items})
    task.version += 1; task.updated_at = utcnow(); session.flush()
    return task_dict(task)


def transition_task(session: Session, task_id: str, *, version: int, actor_id: str, to_state: str, reason: str | None = None) -> dict:
    task = _locked_task(session, task_id, version)
    previous = task.state
    if to_state == "blocked" and previous not in {"blocked", "completed", "dismissed"}:
        task.blocked_from_state = previous
    elif previous == "blocked" and to_state == task.blocked_from_state:
        task.blocked_from_state = None
    elif to_state not in LEGAL.get(previous, set()):
        raise DomainError(f"illegal transition {previous} -> {to_state}")
    if (to_state in {"blocked", "dismissed"} or previous in {"blocked", "dismissed", "completed"}) and not reason:
        raise DomainError("a reason is required")
    if to_state == "dismissed" and session.scalar(select(Execution.id).where(Execution.task_id == task.id, Execution.terminal_at.is_(None)).limit(1)):
        raise PreconditionFailed("resolve the active execution before archiving this task")
    if previous == "completed" and to_state == "accepted":
        merged = session.scalar(
            select(func.count())
            .select_from(Execution)
            .where(
                Execution.task_id == task.id,
                Execution.pr_number.is_not(None),
                Execution.provider_state["state"].as_string() == "merged",
            )
        )
        if merged:
            raise PreconditionFailed(
                "merged work must reopen as a related new task"
            )
    if previous == "in_review" and to_state == "ready":
        execution = session.scalar(
            select(Execution)
            .where(
                Execution.task_id == task.id,
                Execution.pr_number.is_not(None),
            )
            .order_by(Execution.sequence.desc(), Execution.revision.desc())
            .limit(1)
            .with_for_update()
        )
        provider_state = execution.provider_state if execution else {}
        trusted = bool(
            execution
            and execution.trusted_head_sha
            and provider_state
            and provider_state.get("head_sha") == execution.trusted_head_sha
            and provider_state.get("state") in {"open", "merged"}
            and (
                provider_state.get("state") == "merged"
                or not provider_state.get("draft")
            )
            and (
                not execution.reviewer_required
                or provider_state.get("review_verdict") == "approved"
            )
        )
        if not trusted:
            raise PreconditionFailed(
                "ready requires a trusted provider head and reviewer policy"
            )
    if previous == "in_progress" and to_state == "ready":
        verified = session.scalar(
            select(func.count())
            .select_from(Event)
            .where(
                Event.task_id == task.id,
                or_(
                    Event.event_type == "evidence_added",
                    and_(
                        Event.event_type == "execution_finalized",
                        Event.payload["code"].as_string() == "no_change",
                    ),
                ),
            )
        )
        if not verified:
            raise PreconditionFailed("ready requires verified no-change evidence")
    if to_state == "completed":
        comments = session.scalar(select(func.count()).select_from(Event).where(Event.task_id == task.id, Event.event_type == "comment_added", Event.actor_kind == "user"))
        evidence = session.scalar(select(func.count()).select_from(Event).where(Event.task_id == task.id, Event.event_type == "evidence_added", Event.actor_kind == "user"))
        if not comments or not evidence: raise PreconditionFailed("completion requires a human note and evidence")
        task.completed_at = utcnow()
        for execution in session.scalars(
            select(Execution)
            .where(
                Execution.task_id == task.id,
                Execution.terminal_at.is_(None),
                Execution.pr_number.is_not(None),
            )
            .with_for_update()
        ).all():
            provider_state = execution.provider_state or {}
            if (
                execution.trusted_head_sha
                and provider_state.get("state") == "merged"
                and provider_state.get("head_sha") == execution.trusted_head_sha
            ):
                if not execution.merged_at:
                    execution.merged_at = utcnow()
                execution.stage = "terminal"
                execution.dispatch_state = "terminal"
                execution.terminal_reason_code = "merged"
                execution.terminal_failure_class = "TerminalOutcome"
                execution.terminal_detail = "provider change merged at the trusted publication head"
                execution.terminal_at = utcnow()
                _event(
                    session,
                    task,
                    "execution_finalized",
                    "system",
                    "completion",
                    from_state=previous,
                    to_state="completed",
                    payload={"code": "merged", "execution_id": execution.execution_id},
                )
    if to_state == "dismissed": task.dismissed_at = utcnow()
    if to_state == "accepted": task.accepted_at = utcnow()
    task.state = to_state; task.version += 1; task.updated_at = utcnow()
    _event(session, task, "state_changed", "user", actor_id, from_state=previous, to_state=to_state, payload={"reason": reason} if reason else {})
    session.flush(); return task_dict(task)


def start_task(session: Session, task_id: str, *, version: int, actor_id: str, idempotency_key: str, revision: bool = False, max_queued: int = 20) -> dict:
    try:
        identity = uuid.UUID(str(task_id))
    except ValueError as exc:
        raise NotFound("task not found") from exc
    replay = session.scalar(
        select(Execution)
        .where(
            Execution.task_id == identity,
            Execution.idempotency_key == idempotency_key,
        )
        .with_for_update()
    )
    if replay:
        replay_task = _locked_task(session, identity)
        return {
            "task": task_dict(replay_task),
            "admission": {
                "sequence": replay.sequence,
                "revision": replay.revision,
                "dispatch_state": replay.dispatch_state,
            },
        }
    task = _locked_task(session, identity, version)
    if (
        task.assignee_kind != "bot"
        or task.assignee_profile not in {"junior", "senior", "staff"}
    ):
        raise PreconditionFailed("task must be assigned to an allowed bot profile")
    if task.state not in ({"in_review"} if revision else {"accepted"}):
        raise PreconditionFailed("task state cannot start this execution")
    latest_revision = session.scalar(
        select(Revision)
        .where(Revision.task_id == task.id)
        .order_by(Revision.revision_number.desc())
        .limit(1)
    )
    if (
        not task.planned_resolution
        or latest_revision is None
        or not latest_revision.verification_commands
        or not latest_revision.allowed_path_globs
        or latest_revision.suggested_executor not in {"junior", "senior", "staff"}
    ):
        raise PreconditionFailed(
            "bot execution requires resolution, argv verification, path policy, and profile"
        )
    active = session.scalar(
        select(Execution)
        .where(Execution.task_id == task.id, Execution.terminal_at.is_(None))
        .with_for_update()
    )
    if active and not revision:
        raise Conflict("task already has an active execution", task_dict(task))
    queued = session.scalar(
        select(func.count())
        .select_from(Execution)
        .where(
            Execution.terminal_at.is_(None),
            Execution.dispatch_state.in_(("pending", "leased")),
        )
    )
    if active is None and queued >= max_queued:
        raise PreconditionFailed("execution admission queue is full")
    if active is None:
        sequence = (
            session.scalar(
                select(func.coalesce(func.max(Execution.sequence), 0)).where(
                    Execution.task_id == task.id
                )
            )
            + 1
        )
        revision_number = latest_revision.revision_number
        row = Execution(
            execution_id=secrets.token_hex(32),
            task_id=task.id,
            sequence=sequence,
            revision=revision_number,
            idempotency_key=idempotency_key,
            target_run_id=f"task__{task.id}__{sequence}__r{revision_number}",
            profile=task.assignee_profile,
            reviewer_required=task.reviewer_required,
        )
        session.add(row)
    else:
        row = active
        if latest_revision.revision_number <= row.revision:
            raise PreconditionFailed(
                "a revised execution requires a newer task recommendation revision"
            )
        row.revision = latest_revision.revision_number
        row.execution_id = secrets.token_hex(32)
        row.idempotency_key = idempotency_key
        row.target_run_id = f"task__{task.id}__{row.sequence}__r{row.revision}"
        row.claimed_run_id = None
        row.dispatch_state = "pending"
        row.lease_expires_at = None
        row.stage = "admitted"
        row.profile = task.assignee_profile
        row.reviewer_required = task.reviewer_required
        row.executor_deadline_at = None
        row.review_deadline_at = None
        row.base_sha = None
        row.source_artifact_sha256 = None
        row.patch_sha256 = None
        row.patch_byte_count = None
        row.verification_manifest = None
        row.executor_report_sha256 = None
        row.review_report_sha256 = None
        row.review_verdict = None
        row.review_comment_fingerprint = None
        row.review_commented_at = None
        row.published_at = None
        row.merged_at = None
        row.branch = None
        row.provider = None
        row.repository = None
        row.target_branch = None
        row.pr_number = None
        row.pr_url = None
        row.service_account_id = None
        row.trusted_head_sha = None
        row.provider_state = None
        row.provider_fingerprint = None
        row.synced_at = None
        row.terminal_at = None
    _event(
        session,
        task,
        "execution_admitted",
        "user",
        actor_id,
        payload={"sequence": row.sequence, "revision": row.revision},
    )
    task.version += 1
    task.updated_at = utcnow()
    try:
        session.flush()
    except IntegrityError as exc:
        session.rollback()
        replay = session.scalar(
            select(Execution).where(
                Execution.task_id == identity,
                Execution.idempotency_key == idempotency_key,
            )
        )
        if replay:
            current = _locked_task(session, identity)
            return {
                "task": task_dict(current),
                "admission": {
                    "sequence": replay.sequence,
                    "revision": replay.revision,
                    "dispatch_state": replay.dispatch_state,
                },
            }
        current = _locked_task(session, identity)
        raise Conflict(
            "task already has an active execution", task_dict(current)
        ) from exc
    return {
        "task": task_dict(task),
        "admission": {
            "sequence": row.sequence,
            "revision": row.revision,
            "dispatch_state": row.dispatch_state,
        },
    }


def reconcile_manager(session: Session, report: dict, *, dag_id: str, run_id: str, task_id: str = "run", map_index: int = -1) -> dict[str, int]:
    session.scalar(select(Policy).where(Policy.category == "*").with_for_update())
    epoch = _queue_epoch(session)
    source_report = session.scalar(select(RunReport).where(RunReport.dag_id == dag_id, RunReport.run_id == run_id, RunReport.task_id == task_id, RunReport.map_index == map_index).order_by(RunReport.try_number.desc()).limit(1))
    if source_report and _before_epoch(source_report.started_at, epoch):
        return {"created": 0, "revised": 0, "resurfaced": 0}
    created = revised = resurfaced = 0
    for item in report["plan"]:
        existing = session.scalar(select(Task).where(Task.recommendation_key == item["recommendation_key"]).with_for_update())
        if existing is None:
            existing = _matching_open_task(session, item)
        if existing and existing.state not in {"proposed", "dismissed"}:
            continue
        if existing:
            duplicate = session.scalar(select(Revision.id).where(Revision.task_id == existing.id, Revision.source_dag_id == dag_id, Revision.source_run_id == run_id, Revision.source_task_id == task_id, Revision.source_map_index == map_index))
            if duplicate: continue
            revision_number = session.scalar(select(func.coalesce(func.max(Revision.revision_number), 0)).where(Revision.task_id == existing.id)) + 1
            existing.priority = item["priority"]; existing.version += 1; existing.updated_at = utcnow()
            revised += 1
        else:
            existing = Task(source="manager", recommendation_key=item["recommendation_key"], title=item["title"], category=item["category"], state="proposed", priority=item["priority"], planned_resolution=item["action"])
            session.add(existing); session.flush(); revision_number = 1; created += 1
            policy = session.scalar(select(Policy).where(Policy.category == item["category"]).with_for_update()) or session.scalar(select(Policy).where(Policy.category == "*").with_for_update())
            if policy and policy.mode in {"auto_accept", "auto_delegate"}:
                existing.state = "accepted"; existing.accepted_at = utcnow()
                if policy.mode == "auto_delegate":
                    existing.assignee_kind = "bot"; existing.assignee_profile = policy.profile; existing.reviewer_required = policy.reviewer_required
            _event(session, existing, "recommendation_created", "manager", "manager", to_state=existing.state, payload={"policy": policy.mode if policy else "manual"})
        resurface = item.get("resurface")
        resurface_id = uuid.UUID(resurface["dismissed_task_id"]) if resurface else None
        session.add(Revision(task_id=existing.id, revision_number=revision_number, report_schema="manager_v3", report_date=report["report_date"], title=item["title"], action=item["action"], why_now=item["why_now"], expected_benefit=item["expected_benefit"], resources=item["resources"], risk=item["risk"], rollback=item["rollback"], verification=item["verification"], suggested_executor=item["suggested_executor"], resurface_task_id=resurface_id, resurface_reason=resurface["material_change"] if resurface else None, source_dag_id=dag_id, source_run_id=run_id, source_task_id=task_id, source_map_index=map_index))
        if resurface:
            dismissed = session.scalar(select(Task).where(Task.id == resurface_id, Task.state == "dismissed", Task.recommendation_key == item["recommendation_key"]).with_for_update())
            if dismissed:
                _event(session, dismissed, "resurface_requested", "manager", "manager", payload={"material_change": resurface["material_change"], "source_run_id": run_id}); dismissed.version += 1; resurfaced += 1
    session.flush()
    return {"created": created, "revised": revised, "resurfaced": resurfaced}


def queue_summary(session: Session) -> dict:
    """Cheap home-page summary; do not scan reports or execution history."""
    counts = dict(session.execute(select(Task.state, func.count()).group_by(Task.state)).all())
    return {
        "counts": counts,
        "attention_count": sum(counts.get(state, 0) for state in HUMAN_ACTION_STATES),
        "active_count": sum(count for state, count in counts.items() if state not in {"completed", "dismissed"}),
    }


def reset_queue(session: Session, *, actor_id: str, reason: str, apply: bool = False) -> dict:
    """Archive a reviewed queue without deleting evidence or interrupting execution."""
    if not actor_id.strip() or not reason.strip():
        raise DomainError("actor and reason are required")
    # Serialize with recommendation reconciliation, including during a reset.
    session.scalar(select(Policy).where(Policy.category == "*").with_for_update())
    tasks = session.scalars(select(Task).where(Task.state.not_in(("completed", "dismissed"))).order_by(Task.id).with_for_update()).all()
    active = session.scalar(select(Execution.id).where(Execution.terminal_at.is_(None)).limit(1))
    if active or any(task.state in {"in_progress", "in_review", "ready"} for task in tasks):
        raise PreconditionFailed("finish or explicitly resolve active execution/review work before resetting the queue")
    result = {"count": len(tasks), "applied": apply, "tasks": [{"id": str(task.id), "title": task.title, "state": task.state} for task in tasks]}
    if apply:
        now = utcnow()
        for task in tasks:
            previous = task.state
            task.state = "dismissed"
            task.dismissed_at = task.updated_at = now
            task.version += 1
            _event(session, task, "queue_reset", "user", actor_id, from_state=previous, to_state="dismissed", payload={"reason": reason, "cutoff": now.isoformat()})
        session.flush()
    return result


def _queue_epoch(session: Session) -> datetime | None:
    return session.scalar(select(func.max(Event.created_at)).where(Event.event_type == "queue_reset"))


def _before_epoch(start: datetime, epoch: datetime | None) -> bool:
    return bool(epoch and start.replace(tzinfo=timezone.utc) <= epoch.replace(tzinfo=timezone.utc))


def _recommendation_identity(bot: str, proposal: dict, epoch: datetime | None) -> str:
    # Keep proposal identity separate from open-work resource matching: a later,
    # distinct issue on a completed resource must still be able to enter the queue.
    identity = [bot, proposal["category"], proposal["recommendation_key"], str(epoch or "")]
    return "work-" + hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def _same_work_resources(left: set[str], right: set[str]) -> bool:
    if left == right:
        return bool(left)
    # A specialist may rename its proposed mart on the next run. Match that
    # single output by stable source/family scope, retaining every other key.
    # Do not collapse multiple-model plans or source-only tasks with no family.
    left_models = {key for key in left if key.startswith("model:")}
    right_models = {key for key in right if key.startswith("model:")}
    if len(left_models) != 1 or len(right_models) != 1:
        return False
    scope = left - left_models
    return (
        scope == right - right_models
        and any(key.startswith("source:") for key in scope)
        and any(key.startswith(("analytics:", "visualization:")) for key in scope)
    )


def _matching_open_task(session: Session, proposal: dict, bot: str | None = None) -> Task | None:
    candidates = session.scalars(select(Task).where(Task.state.not_in(("completed", "dismissed")), Task.category == proposal["category"]).options(selectinload(Task.revisions)).order_by(Task.created_at).with_for_update()).all()
    title = re.sub(r"[^a-z0-9]+", " ", proposal["title"].lower()).strip()
    resources = set(proposal.get("resource_keys") or [])
    for candidate in candidates:
        if re.sub(r"[^a-z0-9]+", " ", candidate.title.lower()).strip() == title:
            return candidate
        latest = candidate.revisions[-1] if candidate.revisions else None
        # Specialist identity and the full stable scope must agree; sharing
        # just a source is not enough to merge unrelated work.
        if bot and candidate.source_bot == bot and latest and _same_work_resources(resources, set(latest.resource_keys or [])):
            return candidate
    return None


def overview(session: Session) -> dict:
    now = utcnow()
    latest_manager = session.scalar(
        select(RunReport)
        .where(RunReport.bot_name == "manager", LIVE_RUN)
        .order_by(RunReport.finished_at.desc())
        .limit(1)
    )
    latest_useful_manager = session.scalar(
        select(RunReport)
        .where(
            RunReport.bot_name == "manager",
            LIVE_RUN,
            RunReport.outcome == "succeeded",
            RunReport.body_json.is_not(None),
        )
        .order_by(RunReport.finished_at.desc())
        .limit(1)
    )
    counts = dict(
        session.execute(select(Task.state, func.count()).group_by(Task.state)).all()
    )
    actions = session.scalars(
        select(Task)
        .where(Task.state.in_(HUMAN_ACTION_STATES))
        .order_by(Task.priority, Task.updated_at.desc())
        .limit(5)
    ).all()
    manager = {
        "status": latest_manager.outcome if latest_manager else "missing",
        "reason_code": latest_manager.reason_code if latest_manager else "missing",
        "stale": bool(
            latest_manager
            and latest_useful_manager
            and latest_manager.id != latest_useful_manager.id
        ),
    }
    if latest_useful_manager:
        manager["age_seconds"] = max(
            0, int((now - latest_useful_manager.finished_at).total_seconds())
        )
        manager["executive_summary"] = (
            latest_useful_manager.body_json or {}
        ).get("executive_summary")
    bot_health = []
    for bot in (*SPECIALIST_FRESHNESS_MINUTES, "manager", "task_executor", "pr_reviewer"):
        latest = session.scalar(
            select(RunReport)
            .where(RunReport.bot_name == bot, LIVE_RUN)
            .order_by(RunReport.finished_at.desc())
            .limit(1)
        )
        useful = session.scalar(
            select(RunReport)
            .where(
                RunReport.bot_name == bot,
                LIVE_RUN,
                RunReport.outcome == "succeeded",
                RunReport.body_json.is_not(None),
            )
            .order_by(RunReport.finished_at.desc())
            .limit(1)
        )
        age = (
            max(0, int((now - useful.finished_at).total_seconds()))
            if useful
            else None
        )
        sla = SPECIALIST_FRESHNESS_MINUTES.get(bot)
        bot_health.append(
            {
                "bot": bot,
                "outcome": latest.outcome if latest else "missing",
                "retry_class": latest.retry_class if latest else None,
                "reason_code": latest.reason_code if latest else "missing",
                "failure_fingerprint": latest.failure_fingerprint if latest else None,
                "duration_ms": latest.duration_ms if latest else None,
                "total_tokens": latest.total_tokens if latest else None,
                "deadline_consumed_ms": latest.deadline_consumed_ms if latest else None,
                "useful_age_seconds": age,
                "freshness": (
                    "not_configured"
                    if sla is None
                    else "missing"
                    if useful is None
                    else "stale"
                    if age is not None and age > sla * 60
                    else "fresh"
                ),
            }
        )
    queue = {
        f"{stage}:{dispatch}": count
        for stage, dispatch, count in session.execute(
            select(Execution.stage, Execution.dispatch_state, func.count())
            .where(Execution.terminal_at.is_(None))
            .group_by(Execution.stage, Execution.dispatch_state)
        ).all()
    }
    queue_depth = sum(queue.values())
    stats.incr("bot_dashboard.task.queue_depth", queue_depth)
    lifecycle_totals = {
        phase: {"count": 0, "total_ms": 0}
        for phase in LIFECYCLE_PHASES
    }
    for execution, task in session.execute(
        select(Execution, Task).join(Task, Task.id == Execution.task_id)
    ).all():
        for phase, duration in lifecycle_durations(task, execution).items():
            if duration is not None:
                lifecycle_totals[phase]["count"] += 1
                lifecycle_totals[phase]["total_ms"] += duration
    lifecycle = {
        phase: {
            **value,
            "average_ms": (
                round(value["total_ms"] / value["count"])
                if value["count"]
                else None
            ),
        }
        for phase, value in lifecycle_totals.items()
    }
    oldest_sync = session.scalar(
        select(func.min(Execution.synced_at)).where(
            Execution.pr_number.is_not(None), Execution.terminal_at.is_(None)
        )
    )
    return {
        "manager": manager,
        "counts": counts,
        "actions": [task_dict(row) for row in actions],
        "bot_health": bot_health,
        "queue": queue,
        "queue_depth": queue_depth,
        "lifecycle_durations_ms": lifecycle,
        "provider_cache_age_seconds": (
            max(0, int((now - oldest_sync).total_seconds())) if oldest_sync else None
        ),
    }


SPECIALIST_FRESHNESS_MINUTES = {
    "source_discovery": 390,
    "source_vetting": 420,
    "source_scheduling": 420,
    "cadence_review": 300,
    "failure_triage": 90,
    "analytics_engineer": 1440,
    "data_analyst": 1440,
}


def claim_run_budget(
    session: Session,
    *,
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    configured_total_seconds: int,
) -> dict:
    row = session.scalar(
        select(RunBudgetClaim)
        .where(
            RunBudgetClaim.dag_id == dag_id,
            RunBudgetClaim.run_id == run_id,
            RunBudgetClaim.task_id == task_id,
            RunBudgetClaim.map_index == map_index,
        )
        .with_for_update()
    )
    if row is not None:
        if row.configured_total_seconds != configured_total_seconds:
            raise Conflict("logical run budget cannot change")
    else:
        now = utcnow()
        cap_usd = _daily_spend_cap_usd()
        if cap_usd > 0:
            today = now.astimezone(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            today_rows = session.scalars(
                select(RunReport).where(
                    LIVE_RUN,
                    RunReport.created_at >= today,
                )
            ).all()
            spent = sum(
                row.cost_micro_usd or 0
                for row in today_rows
                if row.cost_source != "unavailable"
            )
            if spent >= int(round(cap_usd * 1_000_000)):
                raise SpendCapReached("daily spend cap reached")
        row = RunBudgetClaim(
            dag_id=dag_id,
            run_id=run_id,
            task_id=task_id,
            map_index=map_index,
            configured_total_seconds=configured_total_seconds,
            deadline_at=now + timedelta(seconds=configured_total_seconds),
        )
        session.add(row)
        try:
            session.flush()
        except IntegrityError:
            session.rollback()
            row = session.scalar(
                select(RunBudgetClaim).where(
                    RunBudgetClaim.dag_id == dag_id,
                    RunBudgetClaim.run_id == run_id,
                    RunBudgetClaim.task_id == task_id,
                    RunBudgetClaim.map_index == map_index,
                )
            )
            if row is None or row.configured_total_seconds != configured_total_seconds:
                raise Conflict("logical run budget cannot change")
    return {
        "deadline_at": row.deadline_at.isoformat(),
        "configured_total_seconds": row.configured_total_seconds,
    }


def _usage_counters(rows: list[RunReport]) -> dict[str, int]:
    return {
        "requests": sum(row.model_requests or 0 for row in rows),
        "input_tokens": sum(row.input_tokens or 0 for row in rows),
        "output_tokens": sum(row.output_tokens or 0 for row in rows),
        "cached_input_tokens": sum(row.cached_input_tokens or 0 for row in rows),
        "cache_write_tokens": sum(row.cache_write_tokens or 0 for row in rows),
        "reasoning_tokens": sum(row.reasoning_tokens or 0 for row in rows),
        "total_tokens": sum(row.total_tokens or 0 for row in rows),
        "cost_micro_usd": sum(
            row.cost_micro_usd
            for row in rows
            if row.cost_micro_usd is not None and row.cost_source != "unavailable"
        ),
        "runs": len(rows),
        "priced_runs": sum(
            row.cost_micro_usd is not None and row.cost_source != "unavailable"
            for row in rows
        ),
        "unpriced_runs": sum(
            row.cost_micro_usd is None or row.cost_source == "unavailable"
            for row in rows
        ),
    }


def _daily_spend_cap_usd() -> float:
    try:
        return max(0.0, conf.getfloat("bot_dashboard", "daily_spend_cap_usd", fallback=0.0))
    except (TypeError, ValueError):
        return 0.0


def usage_summary(
    session: Session,
    *,
    days: int,
    now: datetime | None = None,
) -> dict:
    now = (now or utcnow()).astimezone(timezone.utc)
    days = min(max(int(days), 1), 400)
    cutoff = now - timedelta(days=days)
    rows = session.scalars(
        select(RunReport).where(LIVE_RUN, RunReport.created_at >= cutoff)
    ).all()

    def grouped(key):
        groups: dict[str, list[RunReport]] = {}
        for row in rows:
            name = key(row)
            groups.setdefault(name, []).append(row)
        return groups

    by_model = [
        {"model": name, **_usage_counters(group)}
        for name, group in grouped(lambda row: row.model or "unknown").items()
    ]
    by_model.sort(key=lambda item: (-item["cost_micro_usd"], item["model"]))
    by_model = by_model[:40]

    by_bot = [
        {"bot": name, **_usage_counters(group)}
        for name, group in grouped(lambda row: row.bot_name).items()
    ]
    by_bot.sort(key=lambda item: (-item["cost_micro_usd"], item["bot"]))
    by_bot = by_bot[:40]

    def day_name(row: RunReport) -> str:
        observed = row.created_at
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        return observed.astimezone(timezone.utc).date().isoformat()

    by_day = [
        {"date": name, **_usage_counters(group)}
        for name, group in grouped(day_name).items()
    ]
    by_day.sort(key=lambda item: item["date"])
    by_day = by_day[:400]

    cap_usd = _daily_spend_cap_usd()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_rows = session.scalars(
        select(RunReport).where(LIVE_RUN, RunReport.created_at >= today)
    ).all()
    spent_today = sum(
        row.cost_micro_usd or 0
        for row in today_rows
        if row.cost_source != "unavailable"
    )
    cap_micro = int(round(cap_usd * 1_000_000)) if cap_usd > 0 else None
    return {
        "days": days,
        "currency": "USD",
        "generated_at": now.isoformat(),
        "totals": _usage_counters(rows),
        "by_model": by_model,
        "by_bot": by_bot,
        "by_day": by_day,
        "cap": {
            "daily_spend_cap_micro_usd": cap_micro,
            "spent_today_micro_usd": spent_today,
            "exceeded": bool(cap_micro is not None and spent_today >= cap_micro),
        },
    }


def query_run_reports(
    session: Session,
    *,
    bots: list[str],
    days: int,
    outcomes: list[str],
    limit: int,
) -> dict:
    cutoff = utcnow() - timedelta(days=days)
    clauses = [RunReport.bot_name.in_(bots), RunReport.created_at >= cutoff]
    if outcomes:
        clauses.append(RunReport.outcome.in_(outcomes))
    rows = session.scalars(
        select(RunReport)
        .where(*clauses)
        .order_by(
            RunReport.created_at.desc(),
            RunReport.bot_name,
            RunReport.run_id,
            RunReport.try_number.desc(),
        )
        .limit(min(limit, 200))
    ).all()
    return {"items": [run_report_dict(row, include_payload=True) for row in rows]}


def run_report_dict(row: RunReport, *, include_payload: bool = False) -> dict:
    has_usage = bool(
        (row.model_requests or 0)
        or (row.input_tokens or 0)
        or (row.output_tokens or 0)
        or (row.cached_input_tokens or 0)
        or (row.cache_write_tokens or 0)
        or (row.reasoning_tokens or 0)
        or (row.total_tokens or 0)
        or row.cost_micro_usd is not None
    )
    value = {
        "report_id": str(row.id),
        "bot": row.bot_name,
        "dag_id": row.dag_id,
        "run_id": row.run_id,
        "task_id": row.task_id,
        "map_index": row.map_index,
        "try_number": row.try_number,
        "outcome": row.outcome,
        "retry_class": row.retry_class,
        "reason_code": row.reason_code,
        "selected_model": row.model,
        "started_at": row.started_at.isoformat(),
        "finished_at": row.finished_at.isoformat(),
        "deadline_at": row.deadline_at.isoformat(),
        "duration_ms": row.duration_ms,
        "context": {
            "sha256": row.context_sha256,
            "byte_count": row.context_byte_count,
            "build_ms": row.context_build_ms,
        },
        "attempts": row.attempts_json,
        "requests": row.model_requests,
        "input_tokens": row.input_tokens,
        "output_tokens": row.output_tokens,
        "cached_input_tokens": row.cached_input_tokens,
        "cache_write_tokens": row.cache_write_tokens,
        "reasoning_tokens": row.reasoning_tokens,
        "total_tokens": row.total_tokens,
        "cost_micro_usd": row.cost_micro_usd,
        "cost_source": row.cost_source,
        "pricing_id": row.pricing_id,
        "usage_total": (
            {
                "schema": "usage.v1",
                "input_tokens": row.input_tokens,
                "output_tokens": row.output_tokens,
                "cached_input_tokens": row.cached_input_tokens,
                "cache_write_tokens": row.cache_write_tokens,
                "reasoning_tokens": row.reasoning_tokens,
                "total_tokens": row.total_tokens,
                "requests": row.model_requests,
                "cost_micro_usd": row.cost_micro_usd,
                "cost_source": row.cost_source,
                "pricing_id": row.pricing_id,
            }
            if has_usage and row.cost_source is not None
            else None
        ),
        "failure": (
            {
                "class": row.failure_class,
                "code": row.failure_code,
                "fingerprint": row.failure_fingerprint,
                "detail": row.failure_detail,
            }
            if row.failure_fingerprint
            else None
        ),
        "payload_schema": row.report_schema,
        "sha256": row.sha256,
        "byte_count": row.byte_count,
    }
    if include_payload:
        value["payload"] = row.body_json
    return value


def _freshness_entry(session: Session, bot: str, now: datetime) -> dict:
    latest = session.scalar(
        select(RunReport)
        .where(RunReport.bot_name == bot, LIVE_RUN)
        .order_by(RunReport.finished_at.desc())
        .limit(1)
    )
    useful = session.scalar(
        select(RunReport)
        .where(
            RunReport.bot_name == bot,
            LIVE_RUN,
            RunReport.outcome == "succeeded",
            RunReport.body_json.is_not(None),
        )
        .order_by(RunReport.finished_at.desc())
        .limit(1)
    )
    sla = SPECIALIST_FRESHNESS_MINUTES[bot]
    observed = latest.finished_at if latest else None
    if observed and observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    age = max(0, int((now - observed).total_seconds())) if observed else None
    if latest is None:
        freshness = "missing"
    elif latest.outcome in {"failed", "timed_out"}:
        freshness = "failed"
    elif bot in {"analytics_engineer", "data_analyst"} and useful is None:
        active = session.scalar(
            select(func.count())
            .select_from(Task)
            .join(Revision, Revision.task_id == Task.id)
            .where(
                Task.state.not_in(("completed", "dismissed")),
                func.json_array_length(Revision.resource_keys) > 0,
            )
        )
        freshness = "not_due" if not active else "missing"
    else:
        freshness = "fresh" if age is not None and age <= sla * 60 else "stale"
    return {
        "bot": bot,
        "sla_minutes": sla,
        "freshness": freshness,
        "age_seconds": age,
        "latest_run": run_report_dict(latest) if latest else None,
        "latest_useful_payload": (
            run_report_dict(useful, include_payload=True) if useful else None
        ),
    }


def manager_context(session: Session, *, days: int = 7) -> dict:
    now = utcnow()
    evidence = [
        _freshness_entry(session, bot, now)
        for bot in SPECIALIST_FRESHNESS_MINUTES
    ]
    backlog = session.scalars(
        select(Task)
        .where(Task.state.not_in(("completed", "dismissed")))
        .order_by(Task.priority, Task.updated_at.desc(), Task.id)
        .limit(200)
    ).all()
    stale = [item["bot"] for item in evidence if item["freshness"] == "stale"]
    missing = [item["bot"] for item in evidence if item["freshness"] == "missing"]
    failed = [item["bot"] for item in evidence if item["freshness"] == "failed"]
    health_rows = session.scalars(
        select(RunReport)
        .where(
            RunReport.dag_id.startswith("bot__", autoescape=True),
            RunReport.outcome.in_(("failed", "timed_out")),
            RunReport.finished_at >= now - timedelta(days=days),
        )
        .order_by(RunReport.finished_at.desc())
        .limit(50)
    ).all()
    # Specialists deduplicate against in-flight work by resource key, so the
    # backlog must carry the admitted policy keys, not just the task shell.
    resources = {
        task_id: list(keys or [])[:50]
        for task_id, keys in session.execute(
            select(Revision.task_id, Revision.resource_keys)
            .where(Revision.task_id.in_([task.id for task in backlog] or [None]))
            .order_by(Revision.task_id, Revision.revision_number)
        ).all()
    }
    return {
        "context_schema_version": 1,
        "context_kind": "manager",
        "generated_at": now.isoformat(),
        "as_of": now.isoformat(),
        "specialists": evidence,
        "stale_agents": stale,
        "missing_agents": missing,
        "failed_agents": failed,
        "freshness_ok": not (stale or missing or failed),
        "bot_health": [run_report_dict(row) for row in health_rows],
        "backlog": [
            {**task_dict(task), "resource_keys": resources.get(task.id, [])}
            for task in backlog
        ],
    }


def _task_proposals(payload_schema: str, payload: dict) -> list[dict]:
    if payload_schema == "source_vetting_v2":
        return [
            item["task_proposal"]
            for item in payload["decisions"]
            if item.get("task_proposal")
        ]
    if payload_schema == "source_scheduling_v2":
        return [item["task_proposal"] for item in payload["plans"]]
    if payload_schema == "cadence_review_v2":
        return payload["task_proposals"]
    if payload_schema == "failure_triage_v2":
        return [
            item["task_proposal"]
            for item in payload["failures"]
            if item.get("task_proposal")
        ]
    if payload_schema in {"analytics_engineer_v2", "data_analyst_v1"}:
        return [item["task_proposal"] for item in payload["plans"]]
    return []


def reconcile_recommendations(
    session: Session, report: RunReport, payload: dict
) -> dict[str, int]:
    if report.report_schema == "manager_v3":
        return reconcile_manager(
            session,
            payload,
            dag_id=report.dag_id,
            run_id=report.run_id,
            task_id=report.task_id,
            map_index=report.map_index,
        )
    session.scalar(select(Policy).where(Policy.category == "*").with_for_update())
    epoch = _queue_epoch(session)
    created = revised = delegated = 0
    if _before_epoch(report.started_at, epoch):
        return {"created": 0, "revised": 0, "delegated": 0}
    for proposal in _task_proposals(report.report_schema or "", payload):
        recommendation_key = _recommendation_identity(report.bot_name, proposal, epoch)
        task = session.scalar(
            select(Task)
            .where(Task.recommendation_key == recommendation_key)
            .with_for_update()
        )
        if task is None:
            task = _matching_open_task(session, proposal, report.bot_name)
        # A fresh report must not overwrite an admitted human scope or reopen
        # dismissed/completed work. Execution always keeps its admitted revision.
        if task and task.state != "proposed":
            continue
        duplicate = task and session.scalar(
            select(Revision.id).where(
                Revision.task_id == task.id,
                Revision.source_dag_id == report.dag_id,
                Revision.source_run_id == report.run_id,
                Revision.source_task_id == report.task_id,
                Revision.source_map_index == report.map_index,
            )
        )
        if duplicate:
            continue
        if task is None:
            task = Task(
                source="specialist",
                source_bot=report.bot_name,
                recommendation_key=recommendation_key,
                title=proposal["title"],
                category=proposal["category"],
                state="proposed",
                priority=proposal["priority"],
                planned_resolution=proposal["planned_resolution"],
                reviewer_required=True,
            )
            session.add(task)
            session.flush()
            revision_number = 1
            created += 1
        else:
            revision_number = (
                session.scalar(
                    select(func.coalesce(func.max(Revision.revision_number), 0)).where(
                        Revision.task_id == task.id
                    )
                )
                + 1
            )
            task.title = proposal["title"]
            task.priority = proposal["priority"]
            task.planned_resolution = proposal["planned_resolution"]
            task.version += 1
            task.updated_at = utcnow()
            revised += 1
        revision = Revision(
            task_id=task.id,
            revision_number=revision_number,
            report_schema=report.report_schema,
            title=proposal["title"],
            action=proposal["planned_resolution"],
            why_now=proposal["why_now"],
            expected_benefit=proposal["expected_benefit"],
            risk=proposal["risk"],
            rollback=proposal["rollback"],
            suggested_executor=proposal["suggested_executor"],
            evidence=proposal["evidence"],
            verification_commands=proposal["verification_commands"],
            allowed_path_globs=proposal["allowed_path_globs"],
            resource_keys=proposal["resource_keys"],
            follow_up_bots=proposal["follow_up_bots"],
            source_dag_id=report.dag_id,
            source_run_id=report.run_id,
            source_task_id=report.task_id,
            source_map_index=report.map_index,
        )
        session.add(revision)
        policy = session.scalar(
            select(Policy)
            .where(Policy.category.in_((proposal["category"], "*")))
            .order_by(Policy.category.desc())
            .limit(1)
            .with_for_update()
        )
        if task.state == "proposed" and policy and policy.mode in {
            "auto_accept",
            "auto_delegate",
        }:
            task.state = "accepted"
            task.accepted_at = utcnow()
            if policy.mode == "auto_delegate":
                task.assignee_kind = "bot"
                task.assignee_profile = policy.profile
                task.reviewer_required = policy.reviewer_required
                delegated += 1
        _event(
            session,
            task,
            "recommendation_created" if revision_number == 1 else "recommendation_revised",
            "provider",
            report.bot_name,
            to_state=task.state,
            payload={"source_report_id": str(report.id), "policy": policy.mode if policy else "manual"},
        )
        session.flush()
        if (
            policy
            and policy.mode == "auto_delegate"
            and task.assignee_profile
            and task.state == "accepted"
        ):
            start_task(
                session,
                str(task.id),
                version=task.version,
                actor_id="policy",
                idempotency_key=f"specialist:{report.id}:{task.id}",
            )
    return {"created": created, "revised": revised, "delegated": delegated}
