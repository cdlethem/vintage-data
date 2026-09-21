"""Explicit recovery of exhausted provider capacity failures without losing PR evidence."""
import hashlib
from sqlalchemy import select, func
from sqlalchemy.orm import object_session
from .models import Execution, Event
from .service import Conflict, PreconditionFailed, _locked_task, _event, start_task, task_dict
from .model_settings import model_for_role


def retry_model(session, task_id, *, version, actor_id, idempotency_key, max_queued=20):
    return _retry(session, task_id, version=version, actor_id=actor_id,
                  idempotency_key=idempotency_key, max_queued=max_queued)


def retry_review_launch(session, task_id, *, version, actor_id, idempotency_key, max_queued=20):
    """Operator retry after repairing a failed launch; never replace a review verdict."""
    return _retry(session, task_id, version=version, actor_id=actor_id,
                  idempotency_key=idempotency_key, max_queued=max_queued, launch=True)


def retry_review_result(session, task_id, *, version, actor_id, idempotency_key, max_queued=20):
    """Retry a same-head review whose execution produced no usable code verdict."""
    event_type = "review_result_retry_requested"
    task = _locked_task(session, task_id)
    events = session.scalars(
        select(Event).where(Event.task_id == task.id, Event.event_type == event_type)
    ).all()
    if any(event.payload.get("idempotency_key") == idempotency_key for event in events):
        return {"task": task_dict(task), "status": "already_requested"}
    if task.version != version:
        raise Conflict("Task changed; refresh before retrying")
    row = session.scalar(
        select(Execution).where(Execution.task_id == task.id)
        .order_by(Execution.sequence.desc()).limit(1).with_for_update()
    )
    failure_kind = (row.provider_state or {}).get("review_failure_kind") if row else None
    if row and row.review_verdict == "unable_to_review" and failure_kind is None:
        failure_kind = "legacy_untyped"
    previous_state = task.state
    if (
        task.state not in {"in_review", "blocked"}
        or (task.state == "blocked" and task.blocked_from_state != "in_review")
        or row is None
        or row.admission_kind != "pr_reviewer"
        or row.stage != "reviewed"
        or row.review_verdict != "unable_to_review"
        or failure_kind not in {"legacy_untyped", "transport_failed", "format_failed", "evidence_unavailable"}
    ):
        raise PreconditionFailed("Review retry requires a typed unusable-review result")
    _require_same_open_review_head(row)
    if session.scalar(
        select(func.count()).select_from(Execution).where(
            Execution.id != row.id,
            Execution.terminal_at.is_(None),
            Execution.dispatch_state.in_(["pending", "leased"]),
        )
    ) >= max_queued:
        raise PreconditionFailed("Execution admission queue is full")
    model_for_role(session, "pr_reviewer")
    if object_session(task) is not session or object_session(row) is not session:
        raise Conflict("Recovery session was closed during provider lookup; use an independent session")
    prior_run = row.target_run_id
    suffix = hashlib.sha256(idempotency_key.encode()).hexdigest()[:20]
    row.target_run_id = f"review__{task.id}__{row.sequence}__r{row.revision}__retry_{suffix}"
    row.claimed_run_id = None
    row.dispatch_state = "pending"
    row.stage = "review_admitted"
    row.lease_expires_at = None
    row.review_deadline_at = None
    row.review_verdict = None
    row.review_report_sha256 = None
    row.review_comment_fingerprint = None
    row.review_commented_at = None
    row.provider_state = {
        **(row.provider_state or {}),
        "review_verdict": None,
        "review_failure_kind": None,
        "review_repair": None,
    }
    task.state = "in_review"
    task.blocked_from_state = None
    task.version += 1
    _event(session, task, event_type, "system" if actor_id == "executive" else "user", actor_id,
           from_state=previous_state, to_state="in_review", payload={
               "idempotency_key": idempotency_key,
               "previous_run_id": prior_run,
               "run_id": row.target_run_id,
               "previous_failure_kind": failure_kind,
               "sequence": row.sequence,
               "revision": row.revision,
           })
    session.flush()
    return {"task": task_dict(task), "status": "queued"}


def _require_same_open_review_head(row):
    from .git_provider import get_provider, load_repository_config
    config = load_repository_config()
    if config.provider != row.provider or config.project != row.repository:
        raise PreconditionFailed("Review repository changed")
    observed = get_provider(config).read_change(row.pr_number)
    expected = {
        "provider": row.provider,
        "number": row.pr_number,
        "head_sha": row.trusted_head_sha,
        "head_ref": row.branch,
        "base_ref": row.target_branch,
        "author_id": row.service_account_id,
        "state": "open",
    }
    if any(value is None or observed.get(key) != value for key, value in expected.items()):
        raise PreconditionFailed("Review publication identity or trusted head changed")


def _retry(session, task_id, *, version, actor_id, idempotency_key, max_queued, launch=False):
    event_type = 'review_launch_retry_requested' if launch else 'model_retry_requested'
    reason = 'sandbox_exit_1' if launch else 'model_rate_limited'
    task = _locked_task(session, task_id)
    # Replays must not create another run or reset a run that has already started.
    events = session.scalars(select(Event).where(Event.task_id == task.id, Event.event_type == event_type)).all()
    if any(event.payload.get('idempotency_key') == idempotency_key for event in events):
        return {'task': task_dict(task), 'status': 'already_requested'}
    if task.version != version:
        raise Conflict('Task changed; refresh before retrying')
    row = session.scalar(select(Execution).where(Execution.task_id == task.id).order_by(Execution.sequence.desc()).limit(1).with_for_update())
    if task.state not in ({'blocked', 'in_review'} if launch else {'blocked'}) or row is None or row.terminal_at is None or row.terminal_reason_code != reason:
        raise PreconditionFailed('Retry requires the expected terminal failure and an available task')
    previous_state = task.state
    if launch:
        if row.admission_kind != 'pr_reviewer' or row.review_verdict is not None:
            raise PreconditionFailed('Launch retry cannot replace an existing review')
        _require_same_open_review_head(row)
    if task.assignee_kind != 'bot' or task.assignee_profile not in {'junior', 'senior', 'staff'}:
        raise PreconditionFailed('Task must remain assigned to a bot')
    if session.scalar(select(Execution.id).where(Execution.task_id == task.id, Execution.terminal_at.is_(None)).limit(1)):
        raise Conflict('Task already has an active execution')
    role = 'pr_reviewer' if row.admission_kind == 'pr_reviewer' else f'executor_{row.profile}'
    model_for_role(session, role)
    if object_session(task) is not session or object_session(row) is not session:
        raise Conflict('Recovery session was closed during provider lookup; use an independent session')
    if row.admission_kind == 'executor' and not row.pr_number:
        return start_task(session, task_id, version=version, actor_id=actor_id, idempotency_key=idempotency_key, max_queued=max_queued)
    if row.admission_kind != 'pr_reviewer' or not row.pr_number or not row.trusted_head_sha:
        raise PreconditionFailed('Published execution needs an independent review admission with a trusted PR head')
    if session.scalar(select(func.count()).select_from(Execution).where(Execution.terminal_at.is_(None), Execution.dispatch_state.in_(['pending', 'leased']))) >= max_queued:
        raise PreconditionFailed('Execution admission queue is full')
    prior_run = row.target_run_id
    suffix = hashlib.sha256(idempotency_key.encode()).hexdigest()[:20]
    row.target_run_id = f'review__{task.id}__{row.sequence}__r{row.revision}__retry_{suffix}'
    row.claimed_run_id = None
    row.dispatch_state = 'pending'
    row.stage = 'review_admitted'
    row.lease_expires_at = None
    row.review_deadline_at = None
    row.review_verdict = None
    row.review_report_sha256 = None
    row.terminal_at = None
    row.terminal_reason_code = None
    row.terminal_failure_class = None
    row.terminal_detail = None
    task.state = 'in_review'
    task.blocked_from_state = None
    task.version += 1
    _event(session, task, event_type, 'user', actor_id, from_state=previous_state, to_state='in_review', payload={
        'idempotency_key': idempotency_key, 'previous_run_id': prior_run, 'run_id': row.target_run_id,
        'previous_reason_code': reason, 'sequence': row.sequence})
    session.flush()
    return {'task': task_dict(task), 'status': 'queued'}
