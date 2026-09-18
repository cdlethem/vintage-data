"""Explicit recovery of exhausted provider capacity failures without losing PR evidence."""
import hashlib
from sqlalchemy import select, func
from .models import Execution, Event
from .service import Conflict, PreconditionFailed, _locked_task, _event, start_task, task_dict
from .model_settings import model_for_role


def retry_model(session, task_id, *, version, actor_id, idempotency_key, max_queued=20):
    task = _locked_task(session, task_id)
    # Replays must not create another run or reset a run that has already started.
    events = session.scalars(select(Event).where(Event.task_id == task.id, Event.event_type == 'model_retry_requested')).all()
    if any(event.payload.get('idempotency_key') == idempotency_key for event in events):
        return {'task': task_dict(task), 'status': 'already_requested'}
    if task.version != version:
        raise Conflict('Task changed; refresh before retrying')
    row = session.scalar(select(Execution).where(Execution.task_id == task.id).order_by(Execution.sequence.desc()).limit(1).with_for_update())
    if task.state != 'blocked' or row is None or row.terminal_at is None or row.terminal_reason_code != 'model_rate_limited':
        raise PreconditionFailed('Retry requires a blocked task with an exhausted model rate limit')
    if task.assignee_kind != 'bot' or task.assignee_profile not in {'junior', 'senior', 'staff'}:
        raise PreconditionFailed('Task must remain assigned to a bot')
    if session.scalar(select(Execution.id).where(Execution.task_id == task.id, Execution.terminal_at.is_(None)).limit(1)):
        raise Conflict('Task already has an active execution')
    role = 'pr_reviewer' if row.admission_kind == 'pr_reviewer' else f'executor_{row.profile}'
    model_for_role(session, role)
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
    _event(session, task, 'model_retry_requested', 'user', actor_id, from_state='blocked', to_state='in_review', payload={
        'idempotency_key': idempotency_key, 'previous_run_id': prior_run, 'run_id': row.target_run_id,
        'previous_reason_code': 'model_rate_limited', 'sequence': row.sequence})
    session.flush()
    return {'task': task_dict(task), 'status': 'queued'}
