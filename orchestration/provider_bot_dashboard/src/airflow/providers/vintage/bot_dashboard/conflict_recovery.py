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

def _seed_branch(session: Session, task: Task, seed: dict) -> str:
    branch = seed.get("branch")
    if isinstance(branch, str) and branch:
        return branch
    execution_id = seed.get("execution_id")
    revision = seed.get("revision")
    if (not isinstance(execution_id, str) or not execution_id
            or not isinstance(revision, int) or revision < 1):
        raise PreconditionFailed("immutable revision seed has no recoverable branch identity")
    source = session.scalar(select(Execution).where(
        Execution.task_id == task.id,
        Execution.execution_id == execution_id,
        Execution.revision == revision,
    ).with_for_update())
    if source is None:
        raise PreconditionFailed("immutable revision seed has no recoverable branch identity")
    branch = source.branch
    if not isinstance(branch, str) or not branch:
        raise PreconditionFailed("immutable revision seed has no recoverable branch identity")
    return branch


def _observe_seed_candidate(session: Session, task: Task, seed: dict) -> tuple[dict, str]:
    config = load_repository_config()
    if (config.provider, config.project, config.base_branch) != (
        seed["provider"], seed["repository"], seed["target_branch"],
    ):
        raise PreconditionFailed("immutable revision seed repository identity changed")
    branch = _seed_branch(session, task, seed)
    provider = get_provider(config)
    observation = provider.read_change(seed["pr_number"])
    expected = {
        "provider": seed["provider"],
        "number": seed["pr_number"],
        "head_ref": branch,
        "base_ref": seed["target_branch"],
        "author_id": config.service_account_id,
        "head_sha": seed["trusted_head_sha"],
        "state": "open",
    }
    if any(observation.get(key) != value for key, value in expected.items()):
        raise PreconditionFailed("seeded candidate identity or trusted head changed")
    if observation.get("draft") or observation.get("mergeability") != "conflicting":
        raise PreconditionFailed("seeded candidate is not an open merge conflict")
    base_sha = provider.read_base_identity()
    if not isinstance(base_sha, str) or not _SHA.fullmatch(base_sha):
        raise PreconditionFailed("provider returned an invalid current base identity")
    return observation, base_sha


_SEED_FAILURE_REASONS = {
    "unresolved_revision_seed_conflict",
    "revision_seed_apply_failed",
}


def _seed_recovery_state(task: Task) -> bool:
    return task.state == "in_progress" or (
        task.state == "blocked" and task.blocked_from_state == "in_progress"
    )


def _terminal_seed_candidate(session: Session, task: Task):
    from .revision_seed import find_seed_lineage, validate_seed_artifacts

    row = session.scalar(select(Execution).where(
        Execution.task_id == task.id,
        Execution.terminal_at.is_not(None),
    ).order_by(Execution.sequence.desc()).limit(1).with_for_update())
    if (row is None or row.terminal_reason_code not in _SEED_FAILURE_REASONS
            or row.pr_number is not None):
        raise PreconditionFailed("no failed unpublished revision seed is recoverable")
    if not _seed_recovery_state(task):
        raise PreconditionFailed("failed revision seed is not the current task candidate")
    event, seed = find_seed_lineage(session, row)
    seed = validate_seed_artifacts(session, seed)
    return row, seed, event


def conflict_repair_eligibility(session: Session, task_id: str) -> dict:
    """Return bounded, non-authoritative recovery metadata for action selection."""
    try:
        task = session.get(Task, uuid.UUID(str(task_id)))
    except ValueError:
        return {"recoverable": False, "reason": "task_not_found"}
    if task is None:
        return {"recoverable": False, "reason": "task_not_found"}
    active = session.scalar(select(Execution).where(
        Execution.task_id == task.id, Execution.terminal_at.is_(None),
    ).order_by(Execution.sequence.desc()).limit(1))
    if active is not None:
        return {
            "recoverable": bool(active.pr_number and active.patch_sha256),
            "mode": "published_candidate", "execution_id": active.execution_id,
            "requires_provider_revalidation": True,
        }
    failed = session.scalar(select(Execution).where(
        Execution.task_id == task.id,
        Execution.terminal_at.is_not(None),
    ).order_by(Execution.sequence.desc()).limit(1))
    from .revision_seed import find_seed_lineage
    event, seed = find_seed_lineage(session, failed) if failed else (None, None)
    if (failed and _seed_recovery_state(task)
            and failed.terminal_reason_code in _SEED_FAILURE_REASONS
            and failed.pr_number is None and isinstance(seed, dict)
            and seed.get("pr_number") and seed.get("trusted_head_sha")):
        return {
            "recoverable": True, "mode": "terminal_seed",
            "execution_id": failed.execution_id,
            "seed_execution_id": seed.get("execution_id"),
            "pr_number": seed["pr_number"],
            "requires_provider_revalidation": True,
        }
    return {"recoverable": False, "reason": "no_recoverable_conflict_candidate"}


def _rebind_candidate_gates(session: Session, task: Task, old_head: str) -> list[dict]:
    snapshots: list[dict] = []
    gates = session.scalars(
        select(ValidationGate).where(
            ValidationGate.task_id == task.id,
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
    """Admit a fresh-base repair from a live PR or a failed immutable seed."""
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
    mode = "published_candidate"
    seed_event = None
    if active is not None:
        revision = _require_current_candidate(task, active)
        if active.dispatch_state in {"pending", "leased", "reviewing"}:
            raise PreconditionFailed("merge-conflict repair cannot supersede an admitted or reviewing run")
        observation, repair_base_sha = _observe_dirty_candidate(active)
        seed = capture_seed(session, active)
        if seed is None:
            raise PreconditionFailed("published candidate has no immutable repair seed")
        source = active
    else:
        mode = "terminal_seed"
        source, seed, _ = _terminal_seed_candidate(session, task)
        revision = task.revisions[-1] if task.revisions else None
        if revision is None or not revision.verification_commands or not revision.allowed_path_globs:
            raise PreconditionFailed("seed recovery requires fresh verification and path scope")
        observation, repair_base_sha = _observe_seed_candidate(session, task, seed)
    queued = session.scalar(select(func.count()).select_from(Execution).where(
        Execution.terminal_at.is_(None),
        Execution.dispatch_state.in_(("pending", "leased")),
    ))
    if queued >= max_queued:
        raise PreconditionFailed("execution admission queue is full")
    old_paths = (seed.get("verification_manifest") or {}).get("changed_paths")
    if (not isinstance(old_paths, list) or any(
            not isinstance(path, str)
            or not any(fnmatch.fnmatchcase(path, rule) for rule in revision.allowed_path_globs)
            for path in old_paths)):
        raise PreconditionFailed("published candidate patch is outside the current accepted path scope")
    old_execution_id = source.execution_id
    old_head = seed["trusted_head_sha"]
    gate_history = _rebind_candidate_gates(session, task, old_head)
    repair_seed = {
        **seed,
        "repair_base_sha": repair_base_sha,
        "repair_observation": {
            "number": observation["number"],
            "head_sha": observation["head_sha"],
            "base_sha": observation.get("base_sha"),
            "mergeability": observation.get("mergeability"),
        },
    }
    sequence = session.scalar(select(func.coalesce(func.max(Execution.sequence), 0)).where(
        Execution.task_id == task.id,
    )) + 1
    now = utcnow()
    if mode == "published_candidate":
        source.stage = "superseded_conflict_repair"
        source.dispatch_state = "terminal"
        source.terminal_reason_code = "superseded_merge_conflict"
        source.terminal_failure_class = "Superseded"
        source.terminal_detail = "Superseded by an audited fresh-base merge-conflict repair admission."
        source.terminal_at = now
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
        provider=seed["provider"],
        repository=seed["repository"],
        target_branch=seed["target_branch"],
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
            "mode": mode,
            "sequence": row.sequence,
            "revision": row.revision,
            "execution_id": row.execution_id,
            "repaired_execution_id": old_execution_id,
            "seed_execution_id": seed["execution_id"],
            "repaired_head_sha": old_head,
            "repair_base_sha": repair_base_sha,
            "mergeability": observation.get("mergeability"),
            "revision_seed": repair_seed,
            "invalidated_candidate_gates": gate_history,
        },
    )
    try:
        session.flush()
    except Exception as exc:
        raise Conflict("merge-conflict repair admission could not be persisted", task_dict(task)) from exc
    return _admission(task, row)
