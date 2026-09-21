"""Audited admission for repairing a published candidate that conflicts with its base."""
from __future__ import annotations

import fnmatch
import re
import secrets
import uuid

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .git_provider import get_provider, load_repository_config
from .models import Execution, Revision, Task, ValidationGate, utcnow
from .revision_seed import capture_seed
from .service import Conflict, PreconditionFailed, _event, _locked_task, task_dict
from .execution import require_executor_preconditions
from .model_settings import model_for_role

_SHA = re.compile(r"^[a-f0-9]{40,64}$")
_CANDIDATE_PLACEHOLDERS = {"candidate_head", "pending_candidate"}


def _admission(task: Task, row: Execution) -> dict:
    return {
        "sequence": row.sequence,
        "revision": row.revision,
        "dispatch_state": row.dispatch_state,
        "execution_id": row.execution_id,
        "task": task_dict(task),
    }


def _require_current_candidate(task: Task, row: Execution) -> Revision:
    if task.state not in {"ready", "in_review", "blocked"}:
        raise PreconditionFailed("merge-conflict repair requires a ready, in-review, or blocked task")
    if task.state == "blocked" and task.blocked_from_state not in {"ready", "in_review"}:
        raise PreconditionFailed("blocked task did not originate from a published candidate state")
    if row.terminal_at is not None or row.pr_number is None or not row.patch_sha256:
        raise PreconditionFailed("merge-conflict repair requires an active published candidate")
    if not all((row.provider, row.repository, row.target_branch, row.branch,
                row.service_account_id, row.trusted_head_sha, row.source_artifact_sha256)):
        raise PreconditionFailed("published candidate identity or immutable evidence is incomplete")
    revision = task.revisions[-1] if task.revisions else None
    if revision is None:
        raise PreconditionFailed("merge-conflict repair requires an accepted revision")
    if not revision.verification_commands or not revision.allowed_path_globs:
        raise PreconditionFailed("merge-conflict repair requires fresh verification and path scope")
    return revision


def _observe_dirty_candidate(row: Execution) -> tuple[dict, str]:
    config = load_repository_config()
    if (config.provider, config.project, config.base_branch) != (
        row.provider, row.repository, row.target_branch
    ):
        raise PreconditionFailed("published candidate repository identity changed")
    provider = get_provider(config)
    observation = provider.read_change(row.pr_number)
    expected = {
        "provider": row.provider,
        "number": row.pr_number,
        "head_ref": row.branch,
        "base_ref": row.target_branch,
        "author_id": row.service_account_id,
        "head_sha": row.trusted_head_sha,
        "state": "open",
    }
    if any(observation.get(key) != value for key, value in expected.items()):
        raise PreconditionFailed("published candidate identity or trusted head changed")
    if observation.get("mergeability") != "conflicting":
        raise PreconditionFailed("published candidate is not an open merge conflict")
    base_sha = provider.read_base_identity()
    if not isinstance(base_sha, str) or not _SHA.fullmatch(base_sha):
        raise PreconditionFailed("provider returned an invalid current base identity")
    return observation, base_sha


def _rebind_candidate_gates(session: Session, task: Task, old_head: str) -> list[dict]:
    snapshots: list[dict] = []
    gates = session.scalars(
        select(ValidationGate).where(
            ValidationGate.task_id == task.id,
            ValidationGate.stage.in_(("publication", "merge")),
        ).with_for_update()
    ).all()
    for gate in gates:
        if gate.subject not in _CANDIDATE_PLACEHOLDERS | {old_head}:
            continue
        snapshots.append({
            "gate_key": gate.gate_key,
            "subject": gate.subject,
            "status": gate.status,
            "evidence": gate.evidence,
            "lease_id": gate.lease_id,
            "lease_expires_at": gate.lease_expires_at.isoformat() if gate.lease_expires_at else None,
            "lease_run_id": gate.lease_run_id,
            "attempt": gate.attempt,
            "last_error": gate.last_error,
        })
        gate.subject = "pending_candidate"
        gate.status = "pending"
        gate.evidence = None
        gate.lease_id = None
        gate.lease_expires_at = None
        gate.lease_run_id = None
        gate.attempt = 0
        gate.last_error = None
        gate.version += 1
        gate.updated_at = utcnow()
    return snapshots


def admit_conflict_repair(
    session: Session,
    task_id: str,
    *,
    version: int,
    actor_id: str,
    idempotency_key: str,
    max_queued: int = 20,
    actor_kind: str = "system",
) -> dict:
    """Admit a new, fresh-base repair while preserving the dirty candidate intact."""
    try:
        identity = uuid.UUID(str(task_id))
    except ValueError as exc:
        raise PreconditionFailed("task not found") from exc
    replay = session.scalar(select(Execution).where(
        Execution.task_id == identity, Execution.idempotency_key == idempotency_key,
    ).with_for_update())
    if replay is not None:
        return _admission(_locked_task(session, identity), replay)
    task = _locked_task(session, identity, version, detail=True)
    if task.assignee_kind != "bot" or task.assignee_profile not in {"junior", "senior", "staff"}:
        raise PreconditionFailed("task must be assigned to an allowed bot profile")
    require_executor_preconditions()
    model_for_role(session, f"executor_{task.assignee_profile}")
    model_for_role(session, "pr_reviewer")
    active = session.scalar(select(Execution).where(
        Execution.task_id == task.id, Execution.terminal_at.is_(None),
    ).with_for_update())
    if active is None:
        raise PreconditionFailed("merge-conflict repair requires an active candidate")
    revision = _require_current_candidate(task, active)
    if active.dispatch_state in {"pending", "leased", "reviewing"}:
        raise PreconditionFailed("merge-conflict repair cannot supersede an admitted or reviewing run")
    queued = session.scalar(select(func.count()).select_from(Execution).where(
        Execution.terminal_at.is_(None),
        Execution.dispatch_state.in_(("pending", "leased")),
    ))
    if queued >= max_queued:
        raise PreconditionFailed("execution admission queue is full")
    observation, repair_base_sha = _observe_dirty_candidate(active)
    seed = capture_seed(session, active)
    if seed is None:
        raise PreconditionFailed("published candidate has no immutable repair seed")
    old_paths = (seed.get("verification_manifest") or {}).get("changed_paths")
    if (not isinstance(old_paths, list) or any(
            not isinstance(path, str)
            or not any(fnmatch.fnmatchcase(path, rule) for rule in revision.allowed_path_globs)
            for path in old_paths)):
        raise PreconditionFailed("published candidate patch is outside the current accepted path scope")
    old_execution_id = active.execution_id
    old_head = active.trusted_head_sha
    gate_history = _rebind_candidate_gates(session, task, old_head)
    repair_seed = {
        **seed,
        "repair_base_sha": repair_base_sha,
        "repair_observation": {
            "number": observation["number"],
            "head_sha": observation["head_sha"],
            "base_sha": observation.get("base_sha"),
            "mergeability": observation["mergeability"],
        },
    }
    sequence = session.scalar(select(func.coalesce(func.max(Execution.sequence), 0)).where(
        Execution.task_id == task.id,
    )) + 1
    now = utcnow()
    active.stage = "superseded_conflict_repair"
    active.dispatch_state = "terminal"
    active.terminal_reason_code = "superseded_merge_conflict"
    active.terminal_failure_class = "Superseded"
    active.terminal_detail = "Superseded by an audited fresh-base merge-conflict repair admission."
    active.terminal_at = now
    row = Execution(
        execution_id=secrets.token_hex(32),
        task_id=task.id,
        sequence=sequence,
        revision=revision.revision_number,
        idempotency_key=idempotency_key,
        target_run_id=f"task__{task.id}__{sequence}__r{revision.revision_number}",
        dispatch_state="pending",
        stage="conflict_repair_admitted",
        profile=task.assignee_profile,
        reviewer_required=True,
        base_sha=repair_base_sha,
        provider=active.provider,
        repository=active.repository,
        target_branch=active.target_branch,
    )
    session.add(row)
    task_state = task.state
    task.state = "accepted"
    task.blocked_from_state = None
    task.accepted_at = now
    task.reviewer_required = True
    task.version += 1
    task.updated_at = now
    _event(
        session, task, "merge_conflict_repair_admitted", actor_kind, actor_id,
        from_state=task_state, to_state="accepted",
        payload={
            "sequence": row.sequence,
            "revision": row.revision,
            "execution_id": row.execution_id,
            "repaired_execution_id": old_execution_id,
            "repaired_head_sha": old_head,
            "repair_base_sha": repair_base_sha,
            "mergeability": observation["mergeability"],
            "revision_seed": repair_seed,
            "invalidated_candidate_gates": gate_history,
        },
    )
    try:
        session.flush()
    except Exception as exc:
        raise Conflict("merge-conflict repair admission could not be persisted", task_dict(task)) from exc
    return _admission(task, row)
